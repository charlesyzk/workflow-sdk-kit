# 从零实现一个四环节工作流

本文按照真实接入顺序实现下面这条工作流：

```text
validate_input
同步纯函数，不调用 LLM，不流式
  ↓
prepare_context
异步纯函数，不调用 LLM，自定义 yield 流式内容
  ↓
generate_draft
调用 LLM，stream=True，自动输出模型 token
  ↓
polish_answer
调用 LLM，stream=False，等待完整结果并生成最终 Artifact
```

这四个环节覆盖 SDK 最常见的节点形态：

| 环节 | 是否调用 LLM | 是否流式 | 返回方式 |
|---|---:|---:|---|
| `validate_input` | 否 | 否 | `return NodeOutput(...)` |
| `prepare_context` | 否 | 是，自定义业务流 | 多次 `yield NodeStreamChunk`，最后 `yield NodeOutput` |
| `generate_draft` | 是 | 是，模型流 | `LLMNodeConfig(stream=True)` |
| `polish_answer` | 是 | 否 | `LLMNodeConfig(stream=False)` |

仓库中的 [simple_workflow](../examples/simple_workflow) 是本文代码的可运行版本。

## 1. 解压并安装 SDK

别人拿到 SDK 压缩包后，推荐放入业务项目的 `vendor` 目录：

```text
my-project/
  vendor/
    workflow-sdk-kit/
      sdk/
      docs/
      examples/
  src/
    my_app/
```

进入业务项目并安装本地 SDK：

```powershell
cd my-project
python -m pip install -e .\vendor\workflow-sdk-kit\sdk
```

验证：

```powershell
python -c "import obei_workflow_sdk; print('workflow sdk installed')"
```

开发期建议使用 `-e`，SDK 升级后不用反复卸载。生产构建可以使用固定安装：

```powershell
python -m pip install .\vendor\workflow-sdk-kit\sdk
```

## 2. 创建业务应用文件

```text
src/my_app/
  __init__.py
  workflow.py
  container.py
  migrate.py
  main.py
  worker.py
.env
```

下文先编写 `workflow.py`，再组装运行进程。

## 3. 定义 HTTP 请求和共享 State

```python
from typing import TypedDict

from pydantic import Field
from obei_workflow_sdk import WorkflowInput


class SimpleWorkflowRequest(WorkflowInput):
    question: str = Field(min_length=1, max_length=2000)


class SimpleWorkflowState(TypedDict, total=False):
    # SDK 自动注入
    task_id: str
    run_id: str
    actor_id: str
    workflow_type: str
    current_node: str

    # 用户输入
    question: str

    # 四个环节依次写入的结果
    validated_question: str
    prepared_context: str
    draft: str
    answer: str
    answer_ref: str
```

`WorkflowInput` 默认拒绝额外字段。State 使用 `total=False`，因为初始 State 只有请求字段，后面的节点会逐步补充其他字段。

四个业务字段的数据流为：

```text
question
  → validated_question
  → prepared_context
  → draft
  → answer
```

## 4. 第一环节：同步纯函数节点

第一环节只做输入整理和业务校验：

- 不调用模型；
- 不需要流式输出；
- 使用普通同步 `def execute()`；
- 直接 `return NodeOutput(...)`。

### 4.1 输入输出模型

```python
from pydantic import BaseModel


class ValidateInput(BaseModel):
    question: str


class ValidateOutput(BaseModel):
    validated_question: str
```

### 4.2 节点实现

```python
from obei_workflow_sdk import (
    NodeContext,
    NodeOutput,
    StateField,
    TaskStep,
    WorkflowNode,
)


class ValidateInputNode(WorkflowNode[ValidateInput, ValidateOutput]):
    step = TaskStep(
        code="validate_input",
        name="校验输入",
        step_type="Reasoning",
    )
    input_model = ValidateInput
    input_fields = {
        "question": StateField("question"),
    }
    output_fields = {
        "validated_question": StateField("validated_question"),
    }

    def execute(
        self,
        ctx: NodeContext,
        node_input: ValidateInput,
    ) -> NodeOutput[ValidateOutput]:
        ctx.raise_if_cancelled()

        normalized = " ".join(node_input.question.split())
        if not normalized:
            raise ValueError("question cannot be blank")

        return NodeOutput(
            data=ValidateOutput(validated_question=normalized)
        )
```

SDK 会在这个纯函数外自动完成 NodeExecution、状态、事件和远端步骤 Outbox。

### 4.3 什么时候使用同步纯函数

适合：

- 字符串、数字、结构化数据转换；
- 快速规则判断；
- 不涉及网络和磁盘等待的短计算。

不适合在同步节点中执行长时间 `time.sleep()` 或同步网络请求，因为工作流运行在 ARQ 事件循环中。外部 I/O 优先使用 `async def` 和异步客户端。

## 5. 第二环节：纯函数自定义 yield 流

第二环节仍然不调用模型，但希望前端实时看到处理进度：

```text
正在读取已校验问题
模型上下文准备完成
```

### 5.1 输入输出模型

```python
class ContextInput(BaseModel):
    validated_question: str


class ContextOutput(BaseModel):
    prepared_context: str
```

### 5.2 节点实现

```python
from typing import AsyncIterator

from obei_workflow_sdk import NodeStreamChunk


class PrepareContextNode(WorkflowNode[ContextInput, ContextOutput]):
    step = TaskStep(
        code="prepare_context",
        name="准备上下文",
        step_type="Reasoning",
    )
    input_model = ContextInput
    input_fields = {
        "validated_question": StateField("validated_question"),
    }
    output_fields = {
        "prepared_context": StateField("prepared_context"),
    }

    async def execute(
        self,
        ctx: NodeContext,
        node_input: ContextInput,
    ) -> AsyncIterator[NodeStreamChunk | NodeOutput[ContextOutput]]:
        yield NodeStreamChunk(
            "正在读取已校验问题",
            event_type="context_token",
            payload={"stage": "read"},
        )

        ctx.raise_if_cancelled()

        prepared_context = (
            f"用户问题：{node_input.validated_question}\n"
            "回答要求：内容准确、语言简洁，并给出一个明确结论。"
        )

        yield NodeStreamChunk(
            "模型上下文准备完成",
            event_type="context_token",
            payload={"stage": "complete"},
        )

        yield NodeOutput(
            data=ContextOutput(prepared_context=prepared_context)
        )
```

### 5.3 yield 的硬规则

节点写成生成器后：

1. 只能 yield `NodeStreamChunk` 或 `NodeOutput`；
2. 可以 yield 任意数量的 `NodeStreamChunk`；
3. 必须 yield 唯一一个最终 `NodeOutput`；
4. 最终 `NodeOutput` 后不要再 yield；
5. async generator 不能用 `return NodeOutput(...)`；
6. 未 yield `NodeOutput` 会让节点明确失败，而不是伪造成功。

错误写法：

```python
async def execute(self, ctx, node_input):
    yield "plain string"          # 错误：必须是 NodeStreamChunk
    return NodeOutput(data={})    # 错误：async generator 不能返回值
```

正确写法：

```python
async def execute(self, ctx, node_input):
    yield NodeStreamChunk("处理中")
    yield NodeOutput(data={"result": "done"})
```

### 5.4 NodeStreamChunk 字段

```python
NodeStreamChunk(
    content="模型上下文准备完成",
    event_type="context_token",
    payload={"stage": "complete"},
    persist=False,
)
```

| 字段 | 作用 |
|---|---|
| `content` | 增量文本 |
| `event_type` | SSE 的 `event:` 名称，默认 `node_token` |
| `payload` | 附加 JSON 字段 |
| `persist` | 是否同时写数据库事件 |

默认 `persist=False`，适合 token、日志片段和高频进度。Redis Stream 会保留近期数据，但这些片段不会进入数据库回放。

低频关键事实可以：

```python
yield NodeStreamChunk(
    "数据源切换完成",
    event_type="source_switched",
    persist=True,
)
```

持久化普通进度也可以使用：

```python
ctx.progress(50, "已经完成一半")
```

## 6. 第三环节：LLM 流式生成草稿

第三环节调用 OpenAI-compatible 模型，要求供应商流式返回。

### 6.1 输入输出模型

```python
class DraftInput(BaseModel):
    prepared_context: str


class DraftOutput(BaseModel):
    draft: str
```

### 6.2 节点实现

```python
from obei_workflow_sdk import (
    Artifact,
    LLMNodeConfig,
    TaskNotification,
)


class GenerateDraftNode(WorkflowNode[DraftInput, DraftOutput]):
    step = TaskStep(
        code="generate_draft",
        name="流式生成草稿",
        step_type="Reasoning",
    )
    llm_config = LLMNodeConfig(
        adapter="openai",
        stream=True,
        temperature=0.2,
        prompt_version="simple-draft-v1",
    )
    input_model = DraftInput
    input_fields = {
        "prepared_context": StateField("prepared_context"),
    }
    output_fields = {
        "draft": StateField("draft"),
    }

    async def execute(self, ctx, node_input):
        draft = await ctx.llm.complete(
            "你是企业助手。请根据上下文生成一版中文回答草稿。",
            {"context": node_input.prepared_context},
            stage="draft_generation",
        )

        return NodeOutput(
            data=DraftOutput(draft=draft),
            artifacts=[
                Artifact(
                    type="draft",
                    content=draft,
                    content_type="text/markdown",
                    final=False,
                )
            ],
            notification=TaskNotification(
                summary="回答草稿生成完成",
                output={"draft": draft},
            ),
        )
```

### 6.3 stream=True 的行为

SDK 会自动：

- 调用模型供应商的流式接口；
- 把正文增量发布为 `llm_token`；
- 把推理增量发布为 `llm_reasoning_token`；
- 收集完整正文并让 `complete()` 返回字符串；
- 保存完整请求、响应、reasoning、usage 和 request ID；
- 把本次 NodeExecution 关联到模型和 prompt version。

LLM token 不需要业务代码再次 yield。业务节点仍然返回最终 `NodeOutput`，用于 State、Artifact 和后续节点。

## 7. 第四环节：LLM 非流式生成最终回答

第四环节再次调用模型，但明确关闭流式。它接收草稿，等待完整响应后生成最终 Artifact。

### 7.1 输入输出模型

```python
class PolishInput(BaseModel):
    draft: str


class PolishOutput(BaseModel):
    answer: str
```

### 7.2 节点实现

```python
class PolishAnswerNode(WorkflowNode[PolishInput, PolishOutput]):
    step = TaskStep(
        code="polish_answer",
        name="非流式润色回答",
        step_type="Reasoning",
    )
    llm_config = LLMNodeConfig(
        adapter="openai",
        stream=False,
        temperature=0.1,
        prompt_version="simple-polish-v1",
    )
    input_model = PolishInput
    input_fields = {"draft": StateField("draft")}
    output_fields = {"answer": StateField("answer")}

    async def execute(self, ctx, node_input):
        answer = await ctx.llm.complete(
            "请润色草稿，保持事实不变，输出简洁、完整的最终中文回答。",
            {"draft": node_input.draft},
            stage="answer_polish",
            # 参数原样透传给 OpenAI-compatible 供应商。qwen3 系列在简单润色场景
            # 可以关闭深度思考，并限制最大输出，避免非流式请求长期等待。
            provider_options={"enable_thinking": False, "max_tokens": 512},
        )

        return NodeOutput(
            data=PolishOutput(answer=answer),
            artifacts=[
                Artifact(
                    type="answer",
                    content=answer,
                    content_type="text/markdown",
                    final=True,
                )
            ],
            notification=TaskNotification(
                summary="最终回答润色完成",
                output={"answer": answer},
            ),
        )
```

### 7.3 stream=False 的行为

- 不产生 `llm_token`；
- 不产生 `llm_reasoning_token`；
- 节点等待完整响应；
- 完整调用仍写入模型审计表；
- 最终 Artifact 将写入 Task 的 `final_artifact_id`。

`provider_options` 会原样并入供应商请求体。上例中的 `enable_thinking` 是
qwen3 系列支持的参数，适合无需深度推理的润色环节；如果使用的模型不支持该
字段，请删除它或换成该供应商文档规定的等价参数。`max_tokens` 同样受具体模型
约束。它们只是模型参数，节点仍然是明确的 `stream=False` 非流式调用。

本示例使用同一个 OpenAI-compatible Adapter 演示流式和非流式两种直接模型模式，
因此只需要一组模型配置。Dify 是独立应用协议，不再使用
`LLMNodeConfig(adapter="dify")`；需要 Dify 时声明 `DifyNodeConfig` 并调用
`ctx.dify.chat()`、`generate()` 或 `run_workflow()`。只有聊天类 App 显式启用历史时
才持久化 `conversation_id`。完整说明见 `SDK_USAGE.md` 的 Dify 章节。

## 8. 四种节点的差异

| 项目 | validate_input | prepare_context | generate_draft | polish_answer |
|---|---|---|---|---|
| 纯业务代码 | 是 | 是 | 否 | 否 |
| LLM | 无 | 无 | OpenAI-compatible | OpenAI-compatible |
| execute 类型 | 同步函数 | 异步生成器 | 异步函数 | 异步函数 |
| 自定义 yield | 无 | `context_token` | 无 | 无 |
| 模型 token | 无 | 无 | `llm_token` | 无 |
| 模型审计 | 无 | 无 | stream=true | stream=false |
| Artifact | 无 | 无 | draft，中间产物 | answer，最终产物 |

## 9. 编排四环节工作流

```python
from obei_workflow_sdk import Workflow, WorkflowGraph


class SimpleLLMWorkflow(Workflow):
    workflow_type = "simple_llm_workflow"
    version = "1.0"
    input_model = SimpleWorkflowRequest
    state_model = SimpleWorkflowState

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

必须保证：

- `graph.add_node("validate_input", ...)` 与 `TaskStep.code="validate_input"` 一致；
- `workflow_type` 唯一；
- 远端登记后不要随意修改 step code；
- 每条 `add_edge()` 会成为注册 JSON 中下一节点的 `dependsOn`。

## 10. 完整 workflow.py

可以直接复制并修改仓库中的：[workflow.py](../examples/simple_workflow/src/simple_app/workflow.py)。

建议业务项目先运行该文件对应的测试，再逐个替换四个示例节点的实现。不要一开始同时修改节点码、State、远端登记和基础设施配置，否则排障范围会过大。

## 11. 远端登记前的 `.env`

远端还没有登记这个四步图时，暂时关闭任务系统：

```dotenv
WORKFLOW_DATABASE_URL=mysql+pymysql://user:password@127.0.0.1:4000/workflow?charset=utf8mb4
REDIS_URL=redis://127.0.0.1:6379/0
EVENT_BUS_URL=

OPENAI_BASE_URL=https://model.example.com/v1
OPENAI_API_KEY=模型服务Key
OPENAI_MODEL=模型名
OPENAI_TIMEOUT_SECONDS=120

EXECUTION_TASK_ENABLED=false
```

密码中的 `@`、`%`、`:` 等 URL 保留字符需要编码。

`EXECUTION_TASK_ENABLED=false` 只关闭远端 Binding/Outbox，不会关闭：

- 本地 Task/Run；
- 节点记录；
- Artifact；
- Checkpoint；
- 模型调用；
- 模型审计；
- Redis/SSE。

## 12. 组装 Runtime

创建 `container.py`：

```python
from functools import lru_cache

from obei_workflow_sdk import (
    SQLAlchemyWorkflowStorage,
    WorkflowSettings,
    create_runtime,
)

from .workflow import SimpleLLMWorkflow


@lru_cache(maxsize=1)
def get_settings() -> WorkflowSettings:
    return WorkflowSettings()


@lru_cache(maxsize=1)
def get_storage() -> SQLAlchemyWorkflowStorage:
    return SQLAlchemyWorkflowStorage(
        get_settings().database_url,
        create_tables=False,
    )


@lru_cache(maxsize=1)
def get_runtime():
    return create_runtime(
        [SimpleLLMWorkflow()],
        settings=get_settings(),
        storage=get_storage(),
    )
```

实际示例：[container.py](../examples/simple_workflow/src/simple_app/container.py)。

## 13. 先运行基础设施无关测试

仓库示例测试使用：

- SQLite；
- `InlineDispatcher`；
- `NullTaskSystemAdapter`；
- Fake OpenAI Adapter；
- 内存 Recording EventBus。

执行：

```powershell
python -m pip install -e .\examples\simple_workflow
python -m pytest .\examples\simple_workflow\tests -q
```

测试验证：

- 四个节点全部成功；
- `context_token` 恰好两条；
- 只有流式 LLM 产生 `llm_token`；
- 两次模型调用的 stream 分别为 true、false；
- 四步注册 JSON 的依赖正确。

测试文件：[test_simple_workflow.py](../examples/simple_workflow/tests/test_simple_workflow.py)。

## 14. 生成四步远端登记 JSON

运行：

```powershell
python -m obei_workflow_sdk.export_definition `
  simple_app.container:get_runtime `
  simple_llm_workflow `
  --output simple_llm_workflow.registration.json
```

生成结果：

```json
[
  {
    "stepCode": "validate_input",
    "name": "校验输入",
    "stepType": "Reasoning",
    "dependsOn": [],
    "needConfirmation": false,
    "exceptionStrategy": null
  },
  {
    "stepCode": "prepare_context",
    "name": "准备上下文",
    "stepType": "Reasoning",
    "dependsOn": ["validate_input"],
    "needConfirmation": false,
    "exceptionStrategy": null
  },
  {
    "stepCode": "generate_draft",
    "name": "流式生成草稿",
    "stepType": "Reasoning",
    "dependsOn": ["prepare_context"],
    "needConfirmation": false,
    "exceptionStrategy": null
  },
  {
    "stepCode": "polish_answer",
    "name": "非流式润色回答",
    "stepType": "Reasoning",
    "dependsOn": ["generate_draft"],
    "needConfirmation": false,
    "exceptionStrategy": null
  }
]
```

也可以在 API 启动后获取：

```http
GET /api/v1/workflow-types/simple_llm_workflow/registration-definition
```

## 15. 暂停：人工去远端任务系统登记

此时先不要启用真实任务同步。

需要人工完成：

1. 把 `simple_llm_workflow.registration.json` 提交到任务系统；
2. 工作流标识使用 `simple_llm_workflow`；
3. 确认远端展示四个步骤；
4. 确认依赖顺序为：

```text
validate_input
  → prepare_context
  → generate_draft
  → polish_answer
```

5. 完成工作流登记；
6. 在远端签发 API Key；
7. 保存 Base URL、API Key 和 Caller ID。

## 16. 把远端 Key 填入 `.env`

登记完成后修改：

```dotenv
EXECUTION_TASK_ENABLED=true
EXECUTION_TASK_API_BASE_URL=https://真实任务系统地址/ai-task
EXECUTION_TASK_API_KEY=远端新签发的Key
EXECUTION_TASK_CALLER_ID=你的业务系统标识
EXECUTION_TASK_EXECUTION_MODE=RecordOnly
EXECUTION_TASK_TIMEOUT_SECONDS=10
EXECUTION_TASK_RETRY_MAX_ATTEMPTS=8
EXECUTION_TASK_RETRY_BASE_SECONDS=1
EXECUTION_TASK_RETRY_MAX_SECONDS=300
```

Key 不写入：

- Python 文件；
- 注册 JSON；
- Dockerfile；
- README；
- Git。

启用任务系统但缺少 Base URL、Key 或 Caller ID 时，SDK 会在进程启动阶段报错。

## 17. 初始化数据库

创建 `migrate.py`：

```python
from .container import get_storage


def main() -> None:
    get_storage().create_tables()


if __name__ == "__main__":
    main()
```

执行：

```powershell
python -m simple_app.migrate
```

生产环境建议使用独立迁移账号，API 和 Worker 保持 `create_tables=False`。

实际示例：[migrate.py](../examples/simple_workflow/src/simple_app/migrate.py)。

## 18. 创建 FastAPI 入口

`main.py`：

```python
from fastapi import FastAPI
from obei_workflow_sdk import create_workflow_router

from .container import get_runtime


runtime = get_runtime()
app = FastAPI(title="Simple Workflow")
app.include_router(create_workflow_router(runtime))


@app.get("/health")
def health():
    return {
        "status": "ok",
        "workflow_types": runtime.registry.types(),
    }
```

实际示例：[main.py](../examples/simple_workflow/src/simple_app/main.py)。

## 19. 创建两个 ARQ Worker

`worker.py`：

```python
from obei_workflow_sdk import (
    create_execution_sync_worker_settings,
    create_workflow_worker_settings,
)

from .container import get_runtime, get_settings, get_storage


WorkflowWorkerSettings = create_workflow_worker_settings(
    get_runtime,
    get_settings,
)

ExecutionSyncWorkerSettings = create_execution_sync_worker_settings(
    get_storage,
    get_settings,
)
```

实际示例：[worker.py](../examples/simple_workflow/src/simple_app/worker.py)。

## 20. 启动完整工作流

先确保 Redis 可以访问，然后打开三个终端。

终端一，API：

```powershell
python -m uvicorn simple_app.main:app --host 0.0.0.0 --port 8090
```

终端二，工作流 Worker：

```powershell
arq simple_app.worker.WorkflowWorkerSettings
```

终端三，外部任务系统同步 Worker：

```powershell
arq simple_app.worker.ExecutionSyncWorkerSettings
```

检查：

```powershell
Invoke-RestMethod http://127.0.0.1:8090/health
Invoke-RestMethod http://127.0.0.1:8090/api/v1/workflow-types
```

应看到 `simple_llm_workflow`。

## 21. 提交工作流

```powershell
$body = @{
  workflow_type = "simple_llm_workflow"
  actor_id = "demo-user"
  input = @{
    question = "请用一句话说明工作流 SDK 的作用"
  }
} | ConvertTo-Json -Depth 5

$task = Invoke-RestMethod `
  -Method Post `
  -Uri "http://127.0.0.1:8090/api/v1/tasks" `
  -ContentType "application/json" `
  -Body $body

$task
```

API 应立即返回 202：

```json
{
  "task_id": "01...",
  "run_id": "01...",
  "status": "QUEUED"
}
```

## 22. 订阅流式事件

任务提交后立即订阅：

```powershell
curl.exe -N `
  "http://127.0.0.1:8090/api/v1/tasks/$($task.task_id)/events"
```

### 22.1 第一环节

`validate_input` 不流式，因此只有持久节点事件：

```text
event: node_started
event: node_succeeded
```

### 22.2 第二环节

`prepare_context` 自定义 yield：

```text
event: context_token
data: {"node_name":"prepare_context","stage":"read","content":"正在读取已校验问题"}

event: context_token
data: {"node_name":"prepare_context","stage":"complete","content":"模型上下文准备完成"}
```

### 22.3 第三环节

`generate_draft` 的 LLM 自动流：

```text
event: llm_token
data: {"node_name":"generate_draft","content":"..."}
```

供应商返回推理内容时：

```text
event: llm_reasoning_token
```

### 22.4 第四环节

`polish_answer` 使用 `stream=False`，因此不会出现该节点的 `llm_token`。前端会在完整响应结束后看到 `node_succeeded` 和 `artifact_created`。

## 23. 查询任务、Trace、Artifact 和模型审计

任务快照：

```powershell
$snapshot = Invoke-RestMethod `
  "http://127.0.0.1:8090/api/v1/tasks/$($task.task_id)"
```

节点和事件：

```powershell
$trace = Invoke-RestMethod `
  "http://127.0.0.1:8090/api/v1/tasks/$($task.task_id)/trace"
```

模型审计：

```powershell
$llm = Invoke-RestMethod `
  "http://127.0.0.1:8090/api/v1/tasks/$($task.task_id)/llm-invocations"
```

预期模型审计有两条：

```text
generate_draft  stream=true   SUCCEEDED
polish_answer   stream=false  SUCCEEDED
```

最终 Artifact：

```powershell
Invoke-RestMethod `
  "http://127.0.0.1:8090/api/v1/artifacts/$($snapshot.final_artifact_id)"
```

## 24. 接口清单

| 方法 | 路径 | 用途 | 典型响应 |
|---|---|---|---|
| POST | `/api/v1/tasks` | 提交工作流 | 202 + QUEUED |
| GET | `/api/v1/tasks/{task_id}` | 查询状态、当前节点、最终产物 | Task 快照 |
| GET | `/api/v1/tasks/{task_id}/trace` | 查询四个节点和持久事件 | nodes + events |
| GET | `/api/v1/tasks/{task_id}/events` | SSE、自定义 yield、LLM token | event stream |
| POST | `/api/v1/tasks/{task_id}/decisions` | 提交人工 Gate 决定 | 202 + QUEUED |
| POST | `/api/v1/tasks/{task_id}/retry` | 重试 FAILED 任务 | 202 + QUEUED |
| POST | `/api/v1/tasks/{task_id}/cancel` | 协作式取消 | 202 |
| GET | `/api/v1/artifacts/{artifact_id}` | 查询 draft/answer Artifact | Artifact |
| GET | `/api/v1/tasks/{task_id}/llm-invocations` | 查询两次模型调用 | invocation 列表 |
| GET | `/api/v1/tasks/{task_id}/dify-invocations` | 查询 Dify 应用调用 | invocation 列表 |
| GET | `/api/v1/workflow-types` | 查询可提交工作流 | 类型列表 |
| GET | `/api/v1/workflow-types/{type}/registration-definition` | 在线生成四步登记 JSON | JSON 数组 |

SDK Router 不包含企业认证，业务服务必须在外层增加认证、租户隔离和数据权限。

## 25. 完整验收标准

### 25.1 本地工作流

- Task 和 Run 为 `SUCCEEDED`；
- 四条 NodeExecution 全部为 `SUCCEEDED`；
- 当前节点为 `polish_answer`；
- 存在 draft Artifact；
- 存在 answer Artifact；
- `final_artifact_id` 指向 answer；
- 模型审计有两条；
- 两条模型审计的 stream 分别为 true 和 false；
- Outbox 没有 FAILED 行。

### 25.2 SSE

- `validate_input` 没有业务 token；
- `prepare_context` 有两条 `context_token`；
- `generate_draft` 有 `llm_token`；
- `polish_answer` 没有 `llm_token`；
- 四个节点都有 started/succeeded 事件。

### 25.3 远端任务系统

- 远端 Task 为 Success/Completed；
- `validate_input` 为 Success；
- `prepare_context` 为 Success；
- `generate_draft` 为 Success；
- `polish_answer` 为 Success；
- 进度为 4/4、100%。

## 26. 常见错误

### yield 了字符串

错误：

```python
yield "处理中"
```

正确：

```python
yield NodeStreamChunk("处理中")
```

### 流式节点没有最终 NodeOutput

SDK 会把节点标记失败。最后必须：

```python
yield NodeOutput(data=...)
```

### LLM 节点没有 token

检查：

- `LLMNodeConfig(stream=True)`；
- 模型服务是否支持 SSE；
- 是否订阅 `/events`；
- Redis EventBus 是否配置；
- 观察的是不是非流式 `polish_answer` 节点。

### 非流式节点出现 token

先确认 token 的 `node_name`。前一个 `generate_draft` 会产生 token，后一个 `polish_answer` 不会。

### 远端步骤 404

远端尚未登记这四个 step code，或登记 JSON 与当前代码不一致。重新生成并比较注册 JSON。

### 远端任务一直 Running

检查 Outbox `last_error`，确认四个远端步骤都已按依赖顺序成功，并且最终 Task Status 已投递。

## 27. 示例文件索引

- 四环节工作流：[workflow.py](../examples/simple_workflow/src/simple_app/workflow.py)
- Runtime 组装：[container.py](../examples/simple_workflow/src/simple_app/container.py)
- FastAPI：[main.py](../examples/simple_workflow/src/simple_app/main.py)
- 两个 ARQ Worker：[worker.py](../examples/simple_workflow/src/simple_app/worker.py)
- 数据库迁移：[migrate.py](../examples/simple_workflow/src/simple_app/migrate.py)
- 环境变量模板：[.env.example](../examples/simple_workflow/.env.example)
- 可运行测试：[test_simple_workflow.py](../examples/simple_workflow/tests/test_simple_workflow.py)
- 当前生成的登记 JSON：[simple_llm_workflow.registration.json](../examples/simple_workflow/simple_llm_workflow.registration.json)
