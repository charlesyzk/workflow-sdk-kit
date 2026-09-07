# Obei Workflow 完整项目模板

这是一个可以直接解压、安装和二次开发的工作流项目。`vendor/obei-workflow-sdk`
已经包含 SDK，使用者不需要另外下载 SDK，也不需要把 SDK 源码复制进业务包。

模板已经组装好：

- FastAPI 工作流接口；
- 两个相互隔离的 ARQ Worker；
- Redis 任务队列、运行锁和 SSE 实时事件；
- SQLite、MySQL 和 TiDB 持久化；
- OpenAI-compatible 直接模型调用与独立的有状态 Dify Chat App 调用；
- LLM 输入输出审计；
- Artifact、Checkpoint、Trace、重试、取消和人工确认；
- 远端任务系统 Binding、SQL Outbox 和可靠同步；
- 从真实代码图生成远端注册 JSON；
- 一个覆盖四种节点形态的完整示例。

## 1. 目录结构

```text
workflow-project-template/
├─ vendor/
│  └─ obei-workflow-sdk/          内置 SDK，不需要联网下载 SDK
├─ src/workflow_app/
│  ├─ workflows/                  主要业务开发目录
│  │  └─ four_stage.py            四环节示例
│  ├─ container.py                注册工作流并组装 Runtime
│  ├─ main.py                     FastAPI 入口
│  ├─ worker.py                   两个 ARQ Worker 入口
│  └─ migrate.py                  数据库初始化入口
├─ tests/                          基础设施无关测试
├─ scripts/                        安装、测试、导出、启动和停止脚本
├─ generated/registration/         自动生成的远端注册 JSON
├─ docs/                           项目及 SDK 文档
├─ .env.example                    无密钥配置模板
├─ docker-compose.yml              本地 Redis
└─ pyproject.toml                  业务项目包配置
```

正常情况下只修改这些位置：

```text
src/workflow_app/workflows/
src/workflow_app/container.py
.env
tests/
```

不要把业务节点写入 `vendor/obei-workflow-sdk`。这样以后替换 SDK 版本时，不会
覆盖业务代码。

## 2. 首次安装

要求：

- Python 3.11 或更高版本；
- PowerShell 7；
- 本地开发需要 Docker，用来启动 Redis；
- 生产环境可以直接使用已有 Redis，不要求 Docker。

解压后进入项目根目录：

```powershell
cd workflow-project-template
.\scripts\setup.ps1
```

脚本会：

1. 创建 `.venv`；
2. 从 `vendor/obei-workflow-sdk` 安装本地 SDK；
3. 安装业务模板和测试依赖；
4. 在不存在 `.env` 时复制 `.env.example`。

验证模板：

```powershell
.\scripts\test.ps1
```

该测试使用 SQLite 和假的 LLM，不访问模型服务、Redis、TiDB 或远端任务系统。

## 3. 理解四环节示例

示例位于 `src/workflow_app/workflows/four_stage.py`：

```text
validate_input
同步纯函数，不调用 LLM，不流式
  ↓
prepare_context
普通异步生成器，不调用 LLM，自定义 yield 流
  ↓
generate_draft
LLM，stream=True，SDK 自动发布模型 token
  ↓
polish_answer
LLM，stream=False，等待完整响应并生成最终 Artifact
```

### 3.1 普通非流式节点

```python
class ValidateInputNode(WorkflowNode[ValidateInput, ValidateOutput]):
    step = TaskStep(code="validate_input", name="校验输入")
    input_model = ValidateInput
    input_fields = {"question": StateField("question")}
    output_fields = {"validated_question": StateField("validated_question")}

    def execute(self, ctx, node_input):
        normalized = " ".join(node_input.question.split())
        return NodeOutput(
            data=ValidateOutput(validated_question=normalized)
        )
```

适合短时间、无网络 I/O 的规则、校验和数据转换。

### 3.2 普通节点自定义 yield

```python
async def execute(self, ctx, node_input):
    yield NodeStreamChunk(
        "开始读取数据",
        event_type="context_token",
        payload={"stage": "read"},
    )

    result = await load_context(node_input.question)

    yield NodeStreamChunk(
        "数据读取完成",
        event_type="context_token",
        payload={"stage": "complete"},
    )
    yield NodeOutput(data=ContextOutput(context=result))
```

规则：

- 只能 yield `NodeStreamChunk` 或 `NodeOutput`；
- 必须且只能 yield 一个最终 `NodeOutput`；
- 默认 `persist=False`，片段只进入 Redis/SSE；
- `persist=True` 会额外写数据库，适合低频关键事件；
- 不要直接 yield 字符串；
- async generator 最后使用 `yield NodeOutput(...)`，不能 `return NodeOutput(...)`。

### 3.3 流式 LLM 节点

```python
llm_config = LLMNodeConfig(
    adapter="openai",
    stream=True,
    prompt_version="draft-v1",
)

async def execute(self, ctx, node_input):
    text = await ctx.llm.complete(
        "根据上下文生成草稿。",
        {"context": node_input.context},
        stage="draft_generation",
    )
    return NodeOutput(data=DraftOutput(draft=text))
```

`stream=True` 时 SDK 自动发布：

- `llm_start`；
- `llm_token`；
- `llm_reasoning_token`；
- `llm_end` 或 `llm_error`。

即使模型使用流式响应，`complete()` 最终仍返回聚合后的完整正文，便于保存 State
和 Artifact。

### 3.4 非流式 LLM 节点

```python
llm_config = LLMNodeConfig(
    adapter="openai",
    stream=False,
    prompt_version="polish-v1",
)
```

`stream=False` 不产生 token 事件，供应商返回完整响应后才继续，但调用请求、完整
响应、usage、耗时和错误仍然写入 LLM 审计表。

示例中的 `enable_thinking=False` 是 qwen3 系列的供应商参数。如果换用的模型不
支持它，请从 `provider_options` 删除；这不会改变 `stream=False` 契约。

## 4. 编排或新增工作流

每个工作流实现 `build()`：

```python
class FourStageWorkflow(Workflow):
    workflow_type = "simple_llm_workflow"
    version = "1.0"
    input_model = FourStageRequest
    state_model = FourStageState

    def build(self, graph: WorkflowGraph) -> None:
        graph.add_node("validate_input", ValidateInputNode())
        graph.add_node("prepare_context", PrepareContextNode())
        graph.add_node("generate_draft", GenerateDraftNode())
        graph.add_node("polish_answer", PolishAnswerNode())
        graph.set_entry_point("validate_input")
        graph.add_edge("validate_input", "prepare_context")
        graph.add_edge("prepare_context", "generate_draft")
        graph.add_edge("generate_draft", "polish_answer")
        graph.set_finish_point("polish_answer")
```

`workflow_type` 和 `TaskStep.code` 都是远端登记和历史数据使用的稳定标识。应在
首次登记前改成业务名称，登记后不要随意修改。

新增第二个工作流后，在 `container.py` 注册：

```python
return create_runtime(
    [FourStageWorkflow(), OrderAnalysisWorkflow()],
    settings=get_settings(),
    storage=get_storage(),
)
```

API、ARQ Worker、工作流类型列表和注册导出命令会同时识别它。

## 5. 配置数据库和模型

第一次安装已经创建 `.env`。本地最小配置可以保留：

```dotenv
WORKFLOW_DATABASE_URL=sqlite:///./workflow.db
REDIS_URL=redis://127.0.0.1:6379/0
EXECUTION_TASK_ENABLED=false
```

连接 TiDB：

```dotenv
WORKFLOW_DATABASE_URL=mysql+pymysql://应用用户:密码@TiDB主机:4000/数据库?charset=utf8mb4
```

模型配置：

```dotenv
OPENAI_BASE_URL=https://模型服务/v1
OPENAI_API_KEY=模型Key
OPENAI_MODEL=模型名称
OPENAI_TIMEOUT_SECONDS=120
```

Dify 不与直接模型共用 Adapter 抽象。多个 Key 通过 App Registry 配置：

```dotenv
DIFY_APPS_JSON=[{"name":"analysis_agent","base_url":"https://你的Dify地址/v1","app_type":"agent","api_key_env":"DIFY_ANALYSIS_AGENT_KEY"},{"name":"report_workflow","base_url":"https://你的Dify地址/v1","app_type":"workflow","api_key_env":"DIFY_REPORT_WORKFLOW_KEY"}]
DIFY_ANALYSIS_AGENT_KEY=Agent应用Key
DIFY_REPORT_WORKFLOW_KEY=Workflow应用Key
DIFY_USER=your-service
```

节点声明 `DifyNodeConfig(app_name="assistant")` 后调用
`await ctx.dify.chat(query, inputs={...})`。Dify 始终以 streaming 模式调用；默认
不发送 `conversation_id`，所以每次调用都没有历史。需要连续会话时再设置
`use_history=True` 和 `conversation_key`。直接模型与 Dify 都通过 `llm_token` 和
`llm_reasoning_token` 输出增量，前端读取 `source` 区分 `direct_model`/`dify` 即可。

完整节点写法：

```python
from obei_workflow_sdk import DifyNodeConfig, NodeOutput, TaskStep, WorkflowNode


class ContinueConversationNode(WorkflowNode):
    step = TaskStep(code="continue_conversation", name="继续 Dify 会话")
    dify_config = DifyNodeConfig(
        app_name="analysis-assistant",
        use_history=True,
        conversation_key="main",
    )

    async def execute(self, ctx, node_input):
        answer = await ctx.dify.chat(
            node_input.question,
            inputs={"department": node_input.department},
            stage="analysis",
        )
        return NodeOutput(data={"answer": answer})
```

上下文责任不同：直接调用模型时，业务代码必须把需要的历史消息或摘要放入本轮
Prompt/State；Dify 默认无历史，启用 `use_history=True` 后历史由 Dify 保存，SDK
保存并复用 `conversation_id`。相同会话由 Redis 锁自动串行，不同
`conversation_key` 可以并发。单次调用也可以显式传入 `conversation_id`。
Text Generator 使用 `ctx.dify.generate(inputs)`，Workflow 使用
`ctx.dify.run_workflow(inputs)`；这两类调用没有会话 ID。

不要把 `.env`、数据库密码、模型 Key 或任务系统 Key 提交到版本库。模板 ZIP 只
包含 `.env.example`，不会包含构建机器上的 `.env`。

创建数据库表：

```powershell
.\scripts\migrate.ps1
```

生产 TiDB 建议让迁移账号执行 DDL，让应用账号只拥有日常读写权限。

## 6. 生成远端注册 JSON

每次修改节点或边以后执行：

```powershell
.\scripts\export-workflow.ps1 simple_llm_workflow
```

输出位置：

```text
generated/registration/simple_llm_workflow.registration.json
```

该 JSON 来自实际 `build()`，包含：

- `stepCode`；
- `name`；
- `stepType`；
- `dependsOn`；
- `needConfirmation`；
- `exceptionStrategy`。

也可以在 API 启动后读取：

```http
GET /api/v1/workflow-types/simple_llm_workflow/registration-definition
```

## 7. 暂停并完成远端登记

取得 Key 前保持：

```dotenv
EXECUTION_TASK_ENABLED=false
```

人工操作顺序：

1. 把生成的 JSON 提交到远端任务系统；
2. 确认远端节点数量、名称和依赖；
3. 完成工作流登记；
4. 签发该工作流使用的 API Key；
5. 保存 Base URL、Key 和 Caller ID。

然后更新 `.env`：

```dotenv
EXECUTION_TASK_ENABLED=true
EXECUTION_TASK_API_BASE_URL=https://真实任务系统地址/ai-task
EXECUTION_TASK_API_KEY=远端签发的Key
EXECUTION_TASK_CALLER_ID=业务系统标识
EXECUTION_TASK_EXECUTION_MODE=RecordOnly
```

Key 只放 `.env` 或生产密钥管理系统，不写进注册 JSON 或 Python 文件。

## 8. 启动完整工作流服务

一键启动本地 Redis、数据库迁移、API 和两个 ARQ Worker：

```powershell
.\scripts\start-all.ps1
```

进程在后台运行，日志写入：

```text
logs/api.stdout.log
logs/api.stderr.log
logs/workflow-worker.stdout.log
logs/workflow-worker.stderr.log
logs/sync-worker.stdout.log
logs/sync-worker.stderr.log
```

停止：

```powershell
.\scripts\stop-all.ps1
```

使用公司 Redis、不希望脚本启动 Docker 时：

```powershell
.\scripts\start-all.ps1 -SkipRedis
```

也可以分别在三个终端启动：

```powershell
.\scripts\start-api.ps1
.\scripts\start-workflow-worker.ps1
.\scripts\start-sync-worker.ps1
```

API 文档地址：<http://127.0.0.1:8090/docs>。

## 9. 提交并测试任务

完整冒烟脚本：

```powershell
.\scripts\test-api.ps1
```

手工提交：

```http
POST /api/v1/tasks
Content-Type: application/json

{
  "workflow_type": "simple_llm_workflow",
  "actor_id": "demo-user",
  "input": {
    "question": "为什么工作流需要持久化状态？"
  }
}
```

接口返回 HTTP 202 和 `task_id`。API 不在请求线程执行工作流，ARQ Worker 会异步
推进任务。

监听 SSE：

```powershell
curl.exe -N http://127.0.0.1:8090/api/v1/tasks/替换为task_id/events
```

其中第二步产生 `context_token`，第三步产生 `llm_token`，第四步不会产生模型 token。

## 10. HTTP API

| 方法 | 路径 | 用途 |
|---|---|---|
| `GET` | `/health` | 进程和工作流注册检查 |
| `GET` | `/ready` | 数据库连接检查 |
| `POST` | `/api/v1/tasks` | 校验输入、创建 Task/Run、返回 202 |
| `GET` | `/api/v1/tasks/{task_id}` | 查询任务状态和最终 Artifact ID |
| `GET` | `/api/v1/tasks/{task_id}/trace` | 查询节点执行、事件和 Artifact Trace |
| `GET` | `/api/v1/tasks/{task_id}/events` | 数据库回放加 Redis 实时 SSE |
| `GET` | `/api/v1/tasks/{task_id}/llm-invocations` | 查询完整 LLM 调用审计 |
| `GET` | `/api/v1/tasks/{task_id}/dify-invocations` | 查询 Dify 调用、会话与消息审计 |
| `GET` | `/api/v1/dify-apps` | 查询可用 Dify App 别名和类型，不泄露凭据 |
| `GET` | `/api/v1/artifacts/{artifact_id}` | 读取一个版本化 Artifact |
| `POST` | `/api/v1/tasks/{task_id}/cancel` | 请求协作式取消 |
| `POST` | `/api/v1/tasks/{task_id}/retry` | 从最新 Checkpoint 重试失败任务 |
| `POST` | `/api/v1/tasks/{task_id}/decisions` | 提交人工确认决定 |
| `GET` | `/api/v1/workflow-types` | 列出已注册工作流 |
| `GET` | `/api/v1/workflow-types/{type}/registration-definition` | 生成远端登记 JSON |

生产项目必须在 FastAPI 层补充企业认证、租户隔离和接口权限控制。

## 11. 远端不可用时模拟完整链路

如果远端任务系统正在维护，可以保留真实 TiDB、模型、Redis、API 和两个 ARQ
Worker，只把 HTTP 任务系统换成本地协议级 Mock：

```powershell
python .\scripts\run-real-integration.py --mock-task-system
```

该模式不会使用 `NullTaskSystemAdapter`。它仍会建立真实 Binding、写入并投递 11 条
SQL Outbox，同时校验：

- `X-API-Key` 和 `X-Caller-Id`；
- 首次创建 201 和幂等重放 200；
- `RecordOnly` 等执行模式；
- 四个已注册步骤及依赖顺序；
- Task、Step 状态转换；
- 最终 `Success / Completed / 100%`。

远端恢复并取得有效 Key 后，运行真实模式：

```powershell
python .\scripts\run-real-integration.py
```

Key 会通过隐藏输入进入测试进程内存，不会写入 `.env`、日志或结果 JSON。

## 12. 下一步

- 详细扩展方式：[开发指南](docs/DEVELOPMENT_GUIDE.md)
- SDK 功能边界：[SDK 功能说明](../../docs/SDK_FEATURES.md)
- 从零理解节点和编排：[四环节开发教程](../../docs/BUILD_FIRST_WORKFLOW.md)
- 所有 SDK 接口：[SDK 参考手册](../../docs/SDK_USAGE.md)
- TiDB 必需索引：[TiDB SQL](../../docs/TIDB_REQUIRED_INDEXES.sql)
