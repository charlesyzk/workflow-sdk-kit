# Obei Workflow SDK 使用手册

本文面向使用 `obei-workflow-sdk` 开发、运行和发布工作流的工程师，覆盖从定义第一个节点到生产部署、远端工作流注册和故障排查的完整过程。

## 1. SDK 解决什么问题

SDK 在 LangGraph 之上提供统一的工作流运行壳。业务代码负责节点输入、业务处理和输出；SDK 负责：

- Task、Run 和节点执行记录；
- LangGraph 持久 Checkpoint 和中断恢复；
- Artifact 版本、内容哈希和最终产物；
- 持久事件、Redis Stream 和 SSE；
- ARQ 异步投递、Run 租约锁、失败重试与协作式取消；
- OpenAI-compatible、Dify 模型调用和完整输入输出审计；
- SQL Outbox、远端任务绑定和步骤状态同步；
- 从实际运行图自动生成远端工作流注册 JSON。

标准运行链路：

```text
HTTP 202
  → ARQ workflow 队列
  → Redis Run Lock
  → LangGraph
  → TiDB/MySQL Checkpoint 与运行记录
  → Redis Stream / SSE
  → SQL Outbox
  → ARQ execution_sync 队列
  → 外部任务系统
```

## 2. 环境要求与安装

运行要求：

- Python 3.11 或更高版本；
- MySQL 8、TiDB，或仅用于测试的 SQLite；
- Redis；
- 生产异步执行使用 ARQ Worker。

新项目推荐先构建并使用完整模板：

```powershell
.\scripts\build-project-template.ps1
cd .\dist\workflow-project-template
.\scripts\setup.ps1
```

模板已经把 SDK 放在 `vendor/obei-workflow-sdk`，并提供 FastAPI、两个 ARQ Worker、
四环节示例、注册导出和启动脚本。已有成熟宿主结构时才建议只安装 SDK：

```powershell
python -m pip install D:\workflow-sdk-kit\sdk
```

当前仓库结构：

```text
sdk/                                可独立安装的 SDK 包
templates/workflow-project-template 完整模板源码
examples/simple_workflow/            四环节 SDK 集成示例
starter/                             扩展示例和 SDK 回归测试
scripts/build-project-template.ps1   模板构建器
```

## 3. 最小配置

SDK 默认读取当前目录的 `.env`。仅运行本地工作流、不连接外部任务系统时，最小配置为：

```dotenv
WORKFLOW_DATABASE_URL=mysql+pymysql://user:password@127.0.0.1:4000/workflow?charset=utf8mb4
REDIS_URL=redis://127.0.0.1:6379/0
EXECUTION_TASK_ENABLED=false
```

数据库密码包含 `@`、`%`、`:`、`/` 等保留字符时必须进行 URL 编码。例如 `@` 编码为 `%40`，`%` 编码为 `%25`。

本地单元测试也可以使用 SQLite：

```dotenv
WORKFLOW_DATABASE_URL=sqlite:///workflow.db
REDIS_URL=redis://127.0.0.1:6379/0
EXECUTION_TASK_ENABLED=false
```

## 4. 核心抽象

一个工作流由以下部分组成：

| 抽象 | 作用 |
|---|---|
| `WorkflowInput` | 校验 HTTP 提交的工作流输入 |
| State | LangGraph 节点之间传递的共享状态 |
| Node Input/Output | 单个节点的强类型输入输出 |
| `WorkflowNode` | 业务节点，只实现 `execute()` |
| `TaskStep` | 节点稳定标识、本地行为和远端注册元数据 |
| `WorkflowGraph` | 注册节点、普通边和条件边 |
| `Workflow` | 提供 `workflow_type`、输入模型、State 和 `build()` |
| `WorkflowRuntime` | 提交、执行、恢复、重试和取消工作流 |

### 4.1 定义请求和 State

```python
from typing import TypedDict

from pydantic import Field
from obei_workflow_sdk import WorkflowInput


class OrderRequest(WorkflowInput):
    order_id: str = Field(min_length=1, max_length=64)
    amount: float = Field(gt=0)


class OrderState(TypedDict, total=False):
    # SDK 注入的公共字段
    task_id: str
    run_id: str
    actor_id: str
    workflow_type: str
    current_node: str

    # 业务输入和中间结果
    order_id: str
    amount: float
    risk_level: str
    report_ref: str
```

建议使用 `TypedDict(total=False)`：初始 State 只包含请求和公共字段，后续节点逐步补充中间结果。

### 4.2 定义节点输入输出

```python
from pydantic import BaseModel


class RiskInput(BaseModel):
    order_id: str
    amount: float


class RiskOutput(BaseModel):
    risk_level: str
```

节点输入输出使用 Pydantic，可以在业务代码运行前发现类型或字段错误。

### 4.3 实现业务节点

```python
from obei_workflow_sdk import (
    Artifact,
    NodeContext,
    NodeOutput,
    StateField,
    TaskNotification,
    TaskStep,
    WorkflowNode,
)


class RiskNode(WorkflowNode[RiskInput, RiskOutput]):
    step = TaskStep(
        code="risk_check",
        name="订单风险检查",
        step_type="Reasoning",
    )
    input_model = RiskInput
    input_fields = {
        "order_id": StateField("order_id"),
        "amount": StateField("amount"),
    }
    output_fields = {
        "risk_level": StateField("risk_level"),
    }

    async def execute(
        self,
        ctx: NodeContext,
        node_input: RiskInput,
    ) -> NodeOutput[RiskOutput]:
        ctx.raise_if_cancelled()
        ctx.progress(20, "正在分析订单风险")

        risk_level = "high" if node_input.amount >= 100_000 else "normal"
        result = RiskOutput(risk_level=risk_level)

        return NodeOutput(
            data=result,
            artifacts=[
                Artifact(
                    type="risk_result",
                    content=result.model_dump(),
                    content_type="application/json",
                )
            ],
            notification=TaskNotification(
                summary="风险检查完成",
                output=result.model_dump(),
            ),
        )
```

SDK 在 `execute()` 外层自动完成：

- NodeExecution 的开始、成功和失败；
- `attempt` 分配与幂等键；
- Task/Run 当前节点和状态；
- Artifact 落库；
- 持久 Event 和实时事件；
- 远端步骤开始和终态 Outbox；
- 取消检查和异常收口。

### 4.4 StateField 映射规则

`input_fields` 把 State 投影为节点输入：

```python
input_fields = {
    "order_id": StateField("order_id"),
}
```

左侧是节点 Pydantic 输入字段，右侧是 State key。

`output_fields` 把节点输出写回 State：

```python
output_fields = {
    "risk_level": StateField("risk_level"),
}
```

未声明 `output_fields` 时，字典或 Pydantic 输出的全部字段会直接合并到 State。

## 5. 定义工作流图

```python
from obei_workflow_sdk import Workflow, WorkflowGraph


class OrderWorkflow(Workflow):
    workflow_type = "order_analysis"
    version = "1.0"
    input_model = OrderRequest
    state_model = OrderState

    def build(self, graph: WorkflowGraph) -> None:
        graph.add_node("risk_check", RiskNode())
        graph.set_entry_point("risk_check")
        graph.set_finish_point("risk_check")
```

`workflow_type` 是 API、数据库、Worker 和远端注册之间的稳定契约，不要与其他工作流重复。

多节点线性工作流：

```python
def build(self, graph: WorkflowGraph) -> None:
    graph.add_node("load", LoadNode())
    graph.add_node("analyze", AnalyzeNode())
    graph.add_node("report", ReportNode())

    graph.set_entry_point("load")
    graph.add_edge("load", "analyze")
    graph.add_edge("analyze", "report")
    graph.set_finish_point("report")
```

节点注册名必须与 `node.step.code` 完全一致。SDK 禁止注册裸函数，以免绕过节点生命周期和任务系统同步。

### 5.1 条件边

```python
def route(state: OrderState) -> str:
    return "manual" if state.get("risk_level") == "high" else "auto"


graph.add_conditional_edges(
    "risk_check",
    route,
    {
        "manual": "manual_review",
        "auto": "generate_report",
    },
)
```

如果需要导出远端注册 JSON，条件边必须提供显式 `path_map`。只有运行时才能确定目标且没有 `path_map` 的条件边会拒绝导出，避免生成错误依赖。

## 6. TaskStep 完整字段

```python
TaskStep(
    code="confirm_sql",
    name="SQL确认",
    description="用户确认生成的 SQL",
    notify_task_system=True,
    visible=True,
    timeout_seconds=None,
    step_type="Reasoning",
    need_confirmation=True,
    exception_strategy=None,
)
```

| 字段 | 含义 |
|---|---|
| `code` | 稳定节点码，同时用于数据库、Checkpoint、Outbox 和远端 `stepCode` |
| `name` | 面向用户的步骤名 |
| `description` | 可选详细说明 |
| `notify_task_system` | 是否产生远端步骤状态 Outbox |
| `visible` | 是否出现在远端注册定义中 |
| `timeout_seconds` | 预留的节点超时元数据 |
| `step_type` | 远端步骤类型，默认 `Reasoning` |
| `need_confirmation` | 是否需要确认；人工 Gate 会自动设置为 true |
| `exception_strategy` | 原样写入远端 `exceptionStrategy` |

`code` 注册后不要随意修改，否则历史 Checkpoint、节点执行记录和远端步骤无法继续匹配。

## 7. Artifact、事件和取消

### 7.1 Artifact

```python
Artifact(
    type="report",
    content="# 分析报告",
    content_type="text/markdown",
    expose_as="report_ref",
    final=True,
)
```

- 同一个 Task、同一种 `type` 自动递增版本；
- SDK 自动计算 SHA-256 和字节数；
- `expose_as` 把 Artifact ID 写入指定 State key；
- `final=True` 更新 Task 的 `final_artifact_id`。

### 7.2 自定义事件和进度

```python
ctx.progress(50, "已处理一半", processed=100)
ctx.emit("domain_warning", code="LOW_COVERAGE")
```

业务事件会关联 task、run 和当前节点。流式模型 token 只进入 Redis Stream，不逐 token 写数据库。

### 7.3 协作式取消

节点开始前 SDK 会自动检查一次取消。长循环和外部调用边界也应主动检查：

```python
for batch in batches:
    ctx.raise_if_cancelled()
    await process(batch)
```

## 8. 人工确认与恢复

定义 Gate：

```python
from pydantic import BaseModel
from obei_workflow_sdk import HumanGateNode, StateField, TaskStep


class ApprovalInput(BaseModel):
    report_ref: str


class ApprovalGate(HumanGateNode):
    step = TaskStep(code="approve", name="人工审批")
    input_model = ApprovalInput
    input_fields = {"report_ref": StateField("report_ref")}
    artifact_ref_field = "report_ref"
    allowed_decisions = ("CONFIRM", "REJECT")
```

执行到 Gate 后：

1. LangGraph 写入 interrupt 和 Checkpoint；
2. Task/Run 进入 `WAITING_USER`；
3. Event/SSE 返回 `decision_key`；
4. 远端任务状态进入等待确认；
5. 用户提交决定后，SDK 使用同一个 Run 从 Checkpoint 恢复。

提交决定：

```http
POST /api/v1/tasks/{task_id}/decisions
Content-Type: application/json

{
  "decision_key": "interrupt-id",
  "decision": "CONFIRM",
  "feedback": "同意",
  "actor_id": "user-1",
  "artifact_id": "01...",
  "artifact_version": 1,
  "content_hash": "64位SHA256"
}
```

如果 Gate 关联 Artifact，SDK 会校验 Artifact 归属、版本和哈希，防止用户确认已经过期的内容。

## 9. 使用大模型

### 9.1 OpenAI-compatible

配置：

```dotenv
OPENAI_BASE_URL=https://model.example.com/v1
OPENAI_API_KEY=your-key
OPENAI_MODEL=your-model
OPENAI_TIMEOUT_SECONDS=120
```

节点必须显式声明 Adapter 和流式策略：

```python
from obei_workflow_sdk import LLMNodeConfig


class WriterNode(WorkflowNode[WriterInput, WriterOutput]):
    step = TaskStep(code="write", name="模型写作")
    llm_config = LLMNodeConfig(
        adapter="openai",
        stream=True,
        temperature=0.2,
        prompt_version="writer-v1",
    )

    async def execute(self, ctx, node_input):
        answer = await ctx.llm.complete(
            "你是企业分析助手",
            {"question": node_input.question},
            stage="generation",
        )
        return NodeOutput(data={"answer": answer})
```

需要多轮上下文时，由业务 State 保存完整消息或摘要，并在每轮显式传入：

```python
answer = await ctx.llm.chat(
    messages=[
        {"role": "system", "content": "你是企业分析助手"},
        *node_input.history,
        {"role": "user", "content": node_input.question},
    ],
    stage="generation",
)
```

SDK 不会为直接模型虚构 `conversation_id`；请求中的完整 `messages` 会进入模型调用
审计。需要跨节点或重试保留历史时，应把 `history` 声明为工作流 State 字段。

### 9.2 Dify 多 App Registry

一个 Dify Key 对应一个已发布 App。SDK 支持六种运行模式：

| `app_type` | Dify 类型 | SDK 方法 | 会话 |
|---|---|---|---|
| `chat` | Chatbot | `ctx.dify.chat()` | 可选 |
| `agent-chat` | Legacy Agent | `ctx.dify.chat()` | 可选 |
| `agent` | Agent | `ctx.dify.chat()` | 可选 |
| `advanced-chat` | Chatflow | `ctx.dify.chat()` | 可选 |
| `completion` | Text Generator | `ctx.dify.generate()` | 无 |
| `workflow` | Workflow | `ctx.dify.run_workflow()` | 无 |

多 App 配置只保存密钥变量名，真实 Key 放在各自环境变量中：

```dotenv
DIFY_APPS_JSON=[{"name":"analysis_agent","base_url":"https://dify.example.com/v1","app_type":"agent","api_key_env":"DIFY_ANALYSIS_AGENT_KEY"},{"name":"report_workflow","base_url":"https://dify.example.com/v1","app_type":"workflow","api_key_env":"DIFY_REPORT_WORKFLOW_KEY"}]
DIFY_ANALYSIS_AGENT_KEY=app-xxx
DIFY_REPORT_WORKFLOW_KEY=app-yyy
DIFY_USER=your-service
```

`app_name` 必须引用 Registry 中的 `name`，不再是一个任意展示标签：

```python
from obei_workflow_sdk import DifyNodeConfig, NodeOutput, TaskStep, WorkflowNode


class AnalystNode(WorkflowNode):
    step = TaskStep(code="dify_analysis", name="Dify 连续分析")
    dify_config = DifyNodeConfig(
        app_name="analysis-assistant",
        use_history=True,
        conversation_key="main",
        prompt_version="analysis-v1",
    )

    async def execute(self, ctx, node_input):
        answer = await ctx.dify.chat(
            node_input.question,
            inputs={"department": node_input.department},
            stage="analysis",
        )
        return NodeOutput(data={"answer": answer})
```

聊天类 App 调用 `/chat-messages` 并强制 `response_mode=streaming`。默认
`use_history=False`，每次调用都不发送 `conversation_id`，返回的 ID 只进入调用审计，
不会成为下一轮上下文。设置 `use_history=True` 后，SDK 才把 ID 保存到
`obei_workshop_dify_conversation`，并按同一 Task、App 和 `conversation_key` 自动
复用。同一会话由 Redis 租约锁自动串行；不同 `conversation_key` 可以并行。

也可以不启用自动历史，只在某一次调用中明确指定：

```python
answer = await ctx.dify.chat(query, conversation_id=external_conversation_id)
```

Text Generator 和 Workflow 没有 `conversation_id`：

```python
class ReportWorkflowNode(WorkflowNode):
    step = TaskStep(code="report", name="Dify 报告工作流")
    dify_config = DifyNodeConfig(app_name="report_workflow")

    async def execute(self, ctx, node_input):
        outputs = await ctx.dify.run_workflow(
            {"order_id": node_input.order_id},
            stage="report",
        )
        return NodeOutput(data=outputs)
```

```python
text = await ctx.dify.generate(
    {"topic": "季度采购总结", "language": "zh-CN"}
)
```

类型与方法不匹配会在发送 HTTP 前报错，例如 Workflow App 调用 `chat()`。

直接模型和 Dify 对 SSE 暴露相同事件名：`llm_start`、`llm_token`、
`llm_reasoning_token`、`llm_end`、`llm_error`。事件的 `source` 分别是
`direct_model` 和 `dify`，所以前端可以统一渲染正文和推理增量。Dify 的工具、节点、
Workflow 暂停等非文本事件统一发布为 `dify_event`，其中保留 `kind` 和原始 `data`。

旧 `DifyAdapter` 暂时保留用于源代码兼容，但新代码不应再使用
`LLMNodeConfig(adapter="dify")`。单 App 的 `DIFY_CHAT_API_KEY` 仍可兼容，
新项目推荐使用 `DIFY_APPS_JSON` 和每个 App 独立的密钥环境变量。

### 9.3 模型审计

每次调用都会写入 `obei_workshop_llm_invocation`，包括：

- provider、model、prompt_version、stream；
- 完整请求、正文、推理内容；
- usage、供应商 request ID、耗时；
- 成功或失败状态和错误摘要。

生产环境应为模型审计接口增加宿主认证，并根据数据敏感级别设置数据库权限、加密和保留周期。

Dify 调用单独写入 `obei_workshop_dify_invocation`，会话映射写入
`obei_workshop_dify_conversation`。查询接口为：

```http
GET /api/v1/tasks/{task_id}/dify-invocations
```

## 10. 组装 Runtime

推荐在 `my_app/container.py` 中集中管理依赖：

```python
from functools import lru_cache

from obei_workflow_sdk import (
    SQLAlchemyWorkflowStorage,
    WorkflowSettings,
    create_runtime,
)
from my_app.workflows import OrderWorkflow


@lru_cache(maxsize=1)
def get_settings() -> WorkflowSettings:
    return WorkflowSettings()  # type: ignore[call-arg]


@lru_cache(maxsize=1)
def get_storage() -> SQLAlchemyWorkflowStorage:
    return SQLAlchemyWorkflowStorage(
        get_settings().database_url,
        create_tables=False,
    )


@lru_cache(maxsize=1)
def get_runtime():
    return create_runtime(
        [OrderWorkflow()],
        settings=get_settings(),
        storage=get_storage(),
    )
```

`create_runtime` 默认组装：

- `SQLAlchemyWorkflowStorage`；
- `ArqDispatcher`；
- `RedisEventBus`；
- OpenAI/Dify LLM Registry；
- 启用时的 `ExecutionTaskAdapter`，或禁用时的空端口。

测试或特殊宿主可以通过 `storage`、`dispatcher`、`task_system` 参数替换默认组件。

## 11. FastAPI 接入

```python
from fastapi import FastAPI
from obei_workflow_sdk import create_workflow_router
from my_app.container import get_runtime


runtime = get_runtime()
app = FastAPI(title="Workflow Host")
app.include_router(create_workflow_router(runtime))
```

SDK Router 默认前缀为 `/api/v1`，也可以自定义：

```python
app.include_router(create_workflow_router(runtime, prefix="/workflow/v1"))
```

主要接口：

| 方法 | 路径 | 作用 |
|---|---|---|
| `POST` | `/tasks` | 提交工作流，返回 202 |
| `GET` | `/tasks/{task_id}` | 查询任务快照 |
| `GET` | `/tasks/{task_id}/trace` | 查询节点和持久事件 |
| `GET` | `/tasks/{task_id}/events` | SSE 事件和流式 token |
| `POST` | `/tasks/{task_id}/decisions` | 提交人工决定 |
| `POST` | `/tasks/{task_id}/retry` | 重试 FAILED 工作流 |
| `POST` | `/tasks/{task_id}/cancel` | 请求取消 |
| `GET` | `/artifacts/{artifact_id}` | 查询 Artifact |
| `GET` | `/tasks/{task_id}/llm-invocations` | 查询模型审计 |
| `GET` | `/tasks/{task_id}/dify-invocations` | 查询 Dify 应用调用审计 |
| `GET` | `/dify-apps` | 查询已配置的安全 App 别名和类型（不返回 Key/URL） |
| `GET` | `/workflow-types` | 列出已注册工作流 |
| `GET` | `/workflow-types/{type}/registration-definition` | 生成远端注册 JSON |

SDK Router 不包含企业认证。生产宿主必须在外层加入现有的身份认证、租户隔离和接口授权。

## 12. ARQ Worker

创建 `my_app/worker.py`：

```python
from obei_workflow_sdk import (
    create_execution_sync_worker_settings,
    create_workflow_worker_settings,
)
from my_app.container import get_runtime, get_settings, get_storage


WorkflowWorkerSettings = create_workflow_worker_settings(
    get_runtime,
    get_settings,
)

ExecutionSyncWorkerSettings = create_execution_sync_worker_settings(
    get_storage,
    get_settings,
)
```

启动 API 和两个 Worker：

```powershell
python -m uvicorn my_app.main:app --host 0.0.0.0 --port 8090
arq my_app.worker.WorkflowWorkerSettings
arq my_app.worker.ExecutionSyncWorkerSettings
```

两个 Worker 使用不同队列：

- workflow：推进 LangGraph；
- execution_sync：调用外部任务系统。

分队列可以防止远端 HTTP 延迟占用工作流执行槽位。

## 13. 数据库初始化

生产环境不要让每个 API/Worker 实例自动建表。创建独立迁移入口：

```python
from my_app.container import get_storage


def main():
    get_storage().create_tables()


if __name__ == "__main__":
    main()
```

运行：

```powershell
python -m my_app.migrate
```

核心运行表：

```text
obei_workshop_task
obei_workshop_task_run
obei_workshop_task_checkpoint
obei_workshop_task_node_execution
obei_workshop_task_artifact
obei_workshop_task_event
obei_workshop_task_decision
```

技术表：

```text
obei_workshop_execution_binding
obei_workshop_task_system_outbox
obei_workshop_llm_invocation
```

如果应用账号没有 `INDEX` 权限，由 DBA 执行 [TIDB_REQUIRED_INDEXES.sql](TIDB_REQUIRED_INDEXES.sql)。

## 14. 对接外部任务系统

配置：

```dotenv
EXECUTION_TASK_ENABLED=true
EXECUTION_TASK_API_BASE_URL=https://task-system.example.com/ai-task
EXECUTION_TASK_API_KEY=your-key
EXECUTION_TASK_CALLER_ID=your-service
EXECUTION_TASK_EXECUTION_MODE=RecordOnly
EXECUTION_TASK_TIMEOUT_SECONDS=10
EXECUTION_TASK_RETRY_MAX_ATTEMPTS=8
EXECUTION_TASK_RETRY_BASE_SECONDS=1
EXECUTION_TASK_RETRY_MAX_SECONDS=300
EXECUTION_TASK_OUTPUT_MAX_BYTES=8192
```

任务系统默认启用。启用但缺少真实 URL、Key 或 Caller ID 时，配置校验会让进程在启动阶段失败。

同步过程：

1. 本地事实先写入 SQL Outbox；
2. 提交事务后通过 ARQ 唤醒同步 Worker；
3. Worker 按 `event_seq` 顺序调用远端；
4. 网络异常、429、5xx 指数退避；
5. 不可重试 4xx 或超过次数后记录 `FAILED/last_error`；
6. 远端故障不会回滚本地已经完成的工作流事实。

远端必须预先注册与本地 `TaskStep.code` 一致的步骤，否则步骤接口会拒绝未知 `stepCode`。

## 15. 生成远端注册 JSON

### 15.1 Python API

```python
from obei_workflow_sdk import export_workflow_definition
from my_app.workflows import OrderWorkflow


definition = export_workflow_definition(OrderWorkflow())
```

已有 Runtime 时：

```python
definition = runtime.registry.registration_definition("order_analysis")
```

### 15.2 CLI

```powershell
python -m obei_workflow_sdk.export_definition `
  my_app.container:get_runtime `
  order_analysis `
  --output order-analysis.registration.json
```

### 15.3 HTTP

```http
GET /api/v1/workflow-types/order_analysis/registration-definition
```

输出格式：

```json
[
  {
    "stepCode": "risk_check",
    "name": "订单风险检查",
    "stepType": "Reasoning",
    "dependsOn": [],
    "needConfirmation": false,
    "exceptionStrategy": null
  }
]
```

导出规则：

- 节点按稳定拓扑顺序输出；
- `dependsOn` 来自实际 `add_edge()`；
- `HumanGateNode` 自动设置 `needConfirmation: true`；
- `visible=False` 或 `notify_task_system=False` 的内部节点不会导出；
- 内部节点前后的依赖会自动折叠；
- 条件边必须提供显式 `path_map`；
- 环或无法满足的依赖会明确报错。

生成 JSON 后，将其提交给远端注册流程，再由远端系统签发调用 Key。SDK 当前不会在没有明确远端注册 API 契约的情况下自动创建工作流或签发 Key。

## 16. 事件与 SSE

订阅：

```powershell
curl.exe -N "http://127.0.0.1:8090/api/v1/tasks/<task_id>/events"
```

行为：

1. 先从数据库回放 `Last-Event-ID` 之后的持久事件；
2. 再从 Redis Stream 接收实时副本；
3. 使用 `db_event_seq` 消除重叠；
4. 空闲时发送 SSE 心跳；
5. LLM token 和 reasoning token 只走实时流。

客户端断线重连时应发送最后收到的 `Last-Event-ID`。

## 17. 状态、失败和重试

典型状态：

```text
QUEUED → RUNNING → SUCCEEDED
                 → WAITING_USER → QUEUED → RUNNING
                 → FAILED → QUEUED（retry）
                 → CANCELLED
```

只有 `FAILED` 任务可以调用 retry。SDK 沿用原 Run 和最近 Checkpoint恢复本地执行；远端任务系统使用新的 Binding 和重试序号，避免重启已经终态的远端任务。

节点异常会保存异常类型和消息，不保存不可序列化的 Python 对象。

## 18. 测试工作流

SDK 提供显式测试替身：

```python
from obei_workflow_sdk import (
    SQLAlchemyWorkflowStorage,
    WorkflowRegistry,
    WorkflowRuntime,
)
from obei_workflow_sdk.testing import InlineDispatcher, NullTaskSystemAdapter


storage = SQLAlchemyWorkflowStorage("sqlite:///test.db", create_tables=True)
registry = WorkflowRegistry()
registry.register(OrderWorkflow())

runtime = WorkflowRuntime(
    storage,
    registry,
    InlineDispatcher(),
    NullTaskSystemAdapter(),
)

result = runtime.submit(
    "order_analysis",
    {"order_id": "order-1", "amount": 100},
    "tester",
)

assert storage.get_task(result["task_id"])["status"] == "SUCCEEDED"
```

`InlineDispatcher` 会在当前测试进程立即执行工作流，不应进入生产组装。

运行仓库测试：

```powershell
$env:PYTHONPATH = "starter/src"
python -m pytest starter/tests -q
```

## 19. 完整配置索引

| 配置 | 默认值 | 说明 |
|---|---:|---|
| `WORKFLOW_DATABASE_URL` | 必填 | 工作流、Checkpoint、Outbox、LLM 审计数据库 |
| `REDIS_URL` | 必填 | ARQ 队列和 Run Lock |
| `EVENT_BUS_URL` | 复用 Redis | SSE/流式 token 的 Redis |
| `WORKFLOW_PUBLIC_BASE_URL` | 空 | 宿主公开地址 |
| `WORKFLOW_RUN_LOCK_TTL_SECONDS` | 90 | Run 锁租约 |
| `WORKFLOW_RUN_LOCK_RENEW_SECONDS` | 30 | Run 锁续租间隔，必须小于 TTL |
| `ARQ_JOB_TIMEOUT_SECONDS` | 600 | 单个 ARQ Job 超时 |
| `ARQ_WORKFLOW_QUEUE` | workflow | 工作流队列 |
| `ARQ_EXECUTION_SYNC_QUEUE` | execution_sync | 任务系统同步队列 |
| `ARQ_WORKFLOW_MAX_JOBS` | 4 | 工作流 Worker 并发 |
| `ARQ_EXECUTION_SYNC_MAX_JOBS` | 2 | 同步 Worker 并发 |
| `EVENT_STREAM_PREFIX` | workflow | Redis Stream 前缀 |
| `EVENT_STREAM_MAXLEN` | 20000 | Stream 近似最大长度 |
| `EVENT_STREAM_BLOCK_MS` | 10000 | SSE 阻塞读取时间 |
| `EXECUTION_TASK_ENABLED` | true | 是否对接外部任务系统 |
| `OPENAI_*` | 空 | OpenAI-compatible Adapter |
| `DIFY_*` | 空 | 独立的有状态 Dify Chat App 客户端 |

完整任务系统和模型配置参考仓库 [.env.example](../.env.example)。

## 20. 生产部署检查清单

- API、两个 Worker 使用完全相同的 `.env` 和工作流代码版本；
- 独立迁移进程先于 API/Worker 运行；
- Redis 开启持久化并设置资源限制；
- TiDB/MySQL 账号具备所需 DML 权限，DDL/INDEX 由迁移账号负责；
- `renew < ttl`，ARQ Job timeout 覆盖最长节点时间；
- 长节点定期调用 `ctx.raise_if_cancelled()`；
- 对 Router、模型审计、Artifact 和 SSE 增加认证授权；
- 远端步骤 JSON 与当前工作流代码重新导出并审核；
- API Key 不进入源码、日志或 Git；
- 监控 FAILED Task、FAILED Outbox、ARQ 队列堆积和 Run Lock 冲突；
- 对模型输入输出设置访问控制、加密和留存周期；
- 上线前完成真实 Redis、TiDB、模型和任务系统的端到端验收。

## 21. 常见问题

### API 返回 202，但任务一直 QUEUED

检查：

- workflow ARQ Worker 是否运行；
- API 与 Worker 的 `REDIS_URL`、队列名是否一致；
- Worker 是否注册了相同 `workflow_type`；
- Redis 中是否有队列堆积。

### 任务系统步骤返回 404

远端没有注册对应 `TaskStep.code`。重新导出注册 JSON，确认远端 `stepCode` 与本地完全一致。

### 本地成功，但远端任务仍为 Running

检查 `obei_workshop_task_system_outbox` 的 FAILED 行和 `last_error`。常见原因是远端步骤依赖没有全部成功，或者远端固定模板与本地图不一致。

### 注册 JSON 导出失败，提示需要 path_map

条件路由没有静态目标。为 `add_conditional_edges()` 添加显式路由值到节点名的映射。

### 模型节点提示未配置 Adapter

确认节点声明了 `LLMNodeConfig`，并且相应的 URL、API Key、Model 已配置。仅配置环境变量但节点没有显式声明 Adapter，也会被 SDK 拒绝。

### TiDB 建表时报 INDEX 权限不足

应用账号可以继续执行 DML，但缺少的索引应由 DBA 或迁移账号执行 [TIDB_REQUIRED_INDEXES.sql](TIDB_REQUIRED_INDEXES.sql)。

## 22. 继续阅读

- [SDK 功能说明](SDK_FEATURES.md)
- [从零实现四环节工作流](BUILD_FIRST_WORKFLOW.md)
- [TiDB 必需索引](TIDB_REQUIRED_INDEXES.sql)
