# Obei Workflow SDK 功能说明

本文只回答两个问题：这个 SDK 提供哪些能力，以及各能力应该在什么场景使用。

如果要从空目录开始实现并运行一个工作流，请直接阅读 [从零实现一个工作流](BUILD_FIRST_WORKFLOW.md)。

## 1. 交付和使用方式

默认推荐交付完整模板项目，而不是让新用户自行组装一个裸 SDK。维护者执行：

```powershell
.\scripts\build-project-template.ps1
```

即可得到 `dist/workflow-project-template.zip`。压缩包结构：

```text
workflow-project-template/
  vendor/
    obei-workflow-sdk/         内置 SDK Python 包
  src/workflow_app/            可直接修改的业务应用
  scripts/                     安装、测试、注册和启动脚本
  generated/registration/      远端登记 JSON
  docs/
  .env.example
  pyproject.toml
```

使用者解压后直接执行：

```powershell
.\scripts\setup.ps1
.\scripts\test.ps1
```

`setup.ps1` 会先从 `vendor/obei-workflow-sdk` 安装 SDK，再安装业务项目，不依赖
公网发布 SDK。业务代码放在 `src/workflow_app`，不要修改 vendor 源码。

已经有成熟 FastAPI/Worker 宿主的团队，也可以只拿独立 `sdk/` 目录：

```powershell
python -m pip install .\sdk
```

两种方式安装后都统一从 `obei_workflow_sdk` 导入。完整模板的构建与升级方式见
[完整模板项目交付说明](TEMPLATE_PROJECT.md)。

## 2. 功能总览

| 能力 | SDK 提供的实现 | 使用者负责的内容 |
|---|---|---|
| 工作流建模 | `Workflow`、`WorkflowGraph`、LangGraph | 定义节点和边 |
| 节点生命周期 | 开始、成功、失败、attempt、当前节点 | 只实现 `execute()` |
| 输入输出校验 | Pydantic + `StateField` | 声明节点输入输出模型 |
| 普通节点 | 异步节点；兼容同步函数并在线程执行 | 实现纯代码或业务服务调用，I/O 优先 async |
| 直接模型节点 | OpenAI-compatible、流式 token、完整 messages 审计 | 维护需要的对话历史和 Prompt |
| Dify 应用节点 | 独立 Chat App 客户端、可选 conversation_id、强制流式 | 按需启用历史并声明 inputs |
| 自定义流式输出 | `NodeStreamChunk` + Redis Stream/SSE | 在节点中 yield 业务片段 |
| 状态持久化 | Task、Run、Checkpoint、NodeExecution | 配置 TiDB/MySQL |
| 产物管理 | Artifact 版本、哈希、最终产物 | 决定产物内容和类型 |
| 人工确认 | `HumanGateNode`、Decision、恢复 | 定义允许的决定和审批产物 |
| 驳回回环 | 动态步骤（`{code}_r{N}` + definition）、`exclude_from_export`、`max_rounds` | 画回环边、定义决策值 |
| 分支对账 | 未走到的静态步骤自动上报 `Skipped` | — |
| 异步执行 | ARQ 工作流 Worker | 启动 Worker 和 Redis |
| 并发保护 | Redis Run 租约锁 | 配置 TTL 与续租周期 |
| 事件订阅 | 数据库回放 + Redis Stream + SSE | 前端消费事件 |
| 任务系统 | Binding、SQL Outbox、重试 | 远端登记工作流并填写 Key |
| 注册配置 | 从实际代码图生成 JSON | 把 JSON 提交远端登记 |
| HTTP API | 提交、查询、追踪、决定、重试、取消 | 添加企业认证和租户权限 |
| 测试 | Inline Dispatcher 和空任务系统 | 编写业务断言和集成测试 |

## 3. 工作流与节点

### 3.1 Workflow

每个工作流提供：

- 稳定的 `workflow_type`；
- HTTP 请求模型 `input_model`；
- LangGraph 共享状态 `state_model`；
- 注册节点和边的 `build()`。

同一个 `build()` 同时用于：

- 编译实际执行图；
- 计算节点依赖；
- 生成远端工作流注册 JSON。

因此业务项目不需要维护第二份流程配置。

### 3.2 普通节点

普通节点适合：

- 数据清洗和规则判断；
- 数据库或内部 HTTP 服务调用；
- 文件生成；
- 任何不需要 LLM 的业务逻辑。

普通节点可以：

- 直接返回 `NodeOutput`；
- `async def` 后返回 `NodeOutput`；
- 实现为同步或异步生成器，连续 yield `NodeStreamChunk`，最后 yield 一个 `NodeOutput`。

### 3.3 直接模型节点与 Dify 应用节点

直接模型节点通过 `LLMNodeConfig` 显式声明：

- `adapter`：当前为 `openai`；
- `stream`：是否使用供应商流式接口；
- `model`：可覆盖默认模型；
- `temperature`；
- `prompt_version`。

节点未声明 `LLMNodeConfig` 时访问 `ctx.llm` 会立即报错，防止某个节点在不知情的情况下调用模型。

直接模型的多轮历史由业务 State 管理，并通过 `ctx.llm.chat(messages=[...])` 每轮
完整发送；`ctx.llm.complete()` 是只构造当前 system/user 消息的便捷方法。

Dify 不是可互换的模型 Adapter。SDK Registry 支持 Chatbot、Legacy Agent、Agent、
Chatflow、Text Generator、Workflow 六种模式，每个 `app_name` 绑定独立 Key 和
协议类型。聊天类默认声明 `DifyNodeConfig()` 时不发送 `conversation_id`。只有显式
设置 `use_history=True` 后，SDK 才从首次流式响应取得 ID、写入
`obei_workshop_dify_conversation` 并在同一 Task 后续调用中复用。也可以在单次
`ctx.dify.chat(conversation_id="...")` 中使用调用方指定的会话。并行历史分支必须
使用不同 `conversation_key`；相同 Key 由 Redis 会话锁串行。Text Generator 和
Workflow 分别使用 `ctx.dify.generate()`、`ctx.dify.run_workflow()`，没有会话 ID。

## 4. 流式输出

SDK 有两类互不冲突的流式输出。

### 4.1 LLM 自动流

`LLMNodeConfig(stream=True)` 时：

- OpenAI-compatible Adapter 使用 SSE；
- 正文增量发布为 `llm_token`；
- 推理增量发布为 `llm_reasoning_token`；
- 最终完整正文、reasoning、usage 和 request ID 写模型审计表；
- 节点仍然拿到完整正文并返回最终 `NodeOutput`。

Dify 调用始终使用 streaming，并复用相同的 `llm_start`、`llm_token`、
`llm_reasoning_token`、`llm_end`、`llm_error` 事件名。消费者通过统一字段处理：

```json
{"source":"direct_model|dify","provider":"openai|dify","content":"增量文本"}
```

因此前端只需要一套流式渲染逻辑；Dify 事件另外带 `app_name` 和
`app_type`，直接模型事件带 `model`。Dify Workflow、工具和节点等非文本事件以
`dify_event` 发布，保留规范化 `kind` 与原始 `data`，不会伪装成文本 token。

### 4.2 业务自定义 yield 流

普通节点可以：

```python
async def execute(self, ctx, node_input):
    yield NodeStreamChunk("正在读取数据", event_type="load_token")
    yield NodeStreamChunk("正在计算", event_type="load_token")
    yield NodeOutput(data={"result": "done"})
```

规则：

- 只能 yield `NodeStreamChunk` 或 `NodeOutput`；
- 必须且只能 yield 一个最终 `NodeOutput`；
- 默认 `persist=False`，片段只进入 Redis Stream/SSE；
- `persist=True` 会同时写数据库，适合低频关键事实，不适合逐 token 数据；
- `ctx.progress()` 适合持久化的阶段进度；
- 自定义事件名会成为 SSE 的 `event:` 字段。

## 5. 持久化和恢复

SDK 使用七张核心运行表：

```text
obei_workshop_task
obei_workshop_task_run
obei_workshop_task_checkpoint
obei_workshop_task_node_execution
obei_workshop_task_artifact
obei_workshop_task_event
obei_workshop_task_decision
```

另外使用三张技术表：

```text
obei_workshop_execution_binding
obei_workshop_task_system_outbox
obei_workshop_llm_invocation
```

Checkpoint 允许工作流在进程重启、人工中断或失败重试后恢复。Task 和 Run 状态在同一事务更新，节点执行保存 attempt、时间、模型和错误摘要。

## 6. Artifact

`Artifact` 用于保存稳定业务结果：

- JSON、Markdown、文本或 URL；
- 同一任务、同一类型自动递增版本；
- 自动计算内容哈希和字节数；
- 可以把 Artifact ID 暴露到 State；
- `final=True` 设置任务最终产物。

人工确认节点可以绑定 Artifact ID、版本和哈希，防止用户确认旧版本内容。

## 7. 人工确认

`HumanGateNode` 会触发 LangGraph interrupt：

```text
RUNNING → WAITING_USER → 用户提交 Decision → QUEUED → RUNNING
```

SDK 自动：

- 保存 Checkpoint；
- 产生 `decision_key`；
- 校验允许的决定；
- 校验 Artifact 归属、版本和哈希；
- 保存 Decision；
- 使用原 Run 恢复。

远端注册 JSON 中，`HumanGateNode` 自动导出为 `needConfirmation: true`。

支持**驳回回环**（0.3.2 起）：`REJECT` 可路由回前序节点重跑，SDK 自动以
`{stepCode}_r{N}` 动态步骤上报远端（同一 taskId 内多轮留痕、审计完整）；回环边用
`exclude_from_export` 标记后不进注册清单，因此**无需重新注册**。完整用法见
[SDK_USAGE.md](SDK_USAGE.md) 8.1 节。

## 8. ARQ 异步执行

API 只做同步校验、创建 Task/Run 和发送消息，然后返回 HTTP 202。实际执行由两个独立 Worker 完成：

- workflow Worker：推进 LangGraph；
- execution_sync Worker：同步外部任务系统。

分队列避免远端 HTTP 延迟占用工作流槽位。Run Lock 防止 ARQ 至少一次投递导致同一个 Run 并发执行。

## 9. 任务系统集成

工作流本地执行不直接等待任务系统 HTTP。节点和 Runtime 只写 SQL Outbox，独立 Worker 按顺序派发：

```text
TASK_CREATE
TASK_STATUS Running
STEP_START
STEP_STATUS Success/Failed
TASK_STATUS Success/Failed/WaitingUser
```

能力包括：

- 本地 Task 与远端 Task Binding；
- 幂等键；
- 每个任务严格递增的事件序号；
- 网络异常、429、5xx 指数退避；
- 4xx 和超限错误记录到 `last_error`；
- 本地任务重试时创建新的远端 Binding。
- 一个本地 Run 可以按 `retry_seq` 保留多个 Binding/远端 taskId；新 Binding 会先
  补发 Checkpoint 中已经成功的远端步骤，再从失败节点继续。
- execution_sync Worker 每 15 秒扫描一次到期的 PENDING Outbox，补偿丢失的 Redis 唤醒。
- 条件分支未走到的静态步骤在任务成功前自动上报 `Skipped`（0.3.1 起），满足远端
  「全部步骤终态才允许任务 Success」的约束；
- 回环重跑的轮次以**动态步骤**上报（未知 stepCode + definition 自动创建，0.3.2 起）；
- 多宿主共用 Redis 时，`ARQ_WORKFLOW_QUEUE` / `ARQ_EXECUTION_SYNC_QUEUE` 必须按宿主区分，
  否则 Worker 会抢到其他宿主的任务。

使用者必须先把 SDK 生成的步骤 JSON登记到远端，再把远端签发的 Key 填入：

```dotenv
EXECUTION_TASK_ENABLED=true
EXECUTION_TASK_API_BASE_URL=https://task-system.example.com/ai-task
EXECUTION_TASK_API_KEY=远端签发的Key
EXECUTION_TASK_CALLER_ID=业务系统标识
```

Key 不写在 Python 代码或注册 JSON 中。

## 10. 远端注册 JSON

SDK 从实际图生成：

```json
[
  {
    "stepCode": "validate_input",
    "name": "校验输入",
    "stepType": "Reasoning",
    "dependsOn": [],
    "needConfirmation": false,
    "exceptionStrategy": null
  }
]
```

支持 Python、CLI 和 HTTP 三种导出方式。导出器会：

- 按稳定拓扑顺序排序；
- 根据边生成 `dependsOn`；
- 自动识别人工 Gate；
- 隐藏内部节点并折叠依赖；
- 拒绝没有显式 `path_map` 的条件边；
- 检测环和无法满足的依赖。

## 11. 事件和 SSE

SSE 订阅先记录 Redis Stream tail，再分页读完数据库持久事件，最后消费 tail 之后
缓存的实时事件。客户端可以使用 `Last-Event-ID` 断线续读。

常见事件：

- `task_accepted`；
- `workflow_started` / `workflow_finished` / `workflow_failed`；
- `node_started` / `node_succeeded` / `node_failed`；
- `artifact_created`；
- `workflow_waiting_user`；
- `llm_token` / `llm_reasoning_token`；
- 用户自定义的 `NodeStreamChunk.event_type`。

## 12. HTTP API

SDK 提供 FastAPI Router：

| 方法 | 路径 | 作用 |
|---|---|---|
| POST | `/api/v1/tasks` | 提交工作流，返回 202 |
| GET | `/api/v1/tasks/{task_id}` | 查询任务状态和最终产物 ID |
| GET | `/api/v1/tasks/{task_id}/trace` | 查询节点和持久事件 |
| GET | `/api/v1/tasks/{task_id}/execution-bindings` | 查询全部远端 taskId/Binding 重试历史 |
| GET | `/api/v1/tasks/{task_id}/events` | SSE 实时事件 |
| POST | `/api/v1/tasks/{task_id}/decisions` | 提交人工决定 |
| POST | `/api/v1/tasks/{task_id}/retry` | 重试 FAILED 任务 |
| POST | `/api/v1/tasks/{task_id}/cancel` | 请求取消 |
| GET | `/api/v1/artifacts/{artifact_id}` | 查询 Artifact |
| GET | `/api/v1/tasks/{task_id}/llm-invocations` | 查询模型审计 |
| GET | `/api/v1/tasks/{task_id}/dify-invocations` | 查询 Dify 应用调用审计 |
| GET | `/api/v1/dify-apps` | 查询 Dify App 别名与类型 |
| GET | `/api/v1/workflow-types` | 列出工作流类型 |
| GET | `/api/v1/workflow-types/{type}/registration-definition` | 生成注册 JSON |

Router 不包含企业认证。业务项目需要在外层增加认证、租户隔离和数据权限。

## 13. 配置与扩展

`create_runtime()` 提供默认生产组装，同时允许替换：

- Storage；
- Dispatcher；
- TaskSystem Adapter；
- LLM Registry；
- EventBus。

调用方可直接传入 `event_bus`、`llm_registry` 和 `dify_registry`；显式传
`event_bus=None` 可关闭实时事件。自定义“先 run、后 stream”的模型协议可继承
`RunStreamAdapter`，实现 `run()` 与 `stream()` 后注册进 `LLMRegistry`。

测试时可使用 SQLite、`InlineDispatcher` 和 `NullTaskSystemAdapter`，不需要启动 Redis 和远端任务系统。

## 14. 当前边界

- SDK 生成远端注册 JSON，但不在未知 API 契约下自动登记或签发 Key；
- 远端 Key 由用户登记后填写到环境变量；
- SDK Router 不负责企业认证；
- 高频自定义流默认不持久化，Redis Stream 裁剪后不能从数据库恢复；
- 业务外部调用的幂等性仍由业务节点负责；
- 长节点必须在循环或外部调用边界主动检查取消；
- 生产 DDL 和索引应由独立迁移账号执行。

## 15. 下一步

新项目优先使用 [完整模板项目](TEMPLATE_PROJECT.md)；需要理解节点实现细节时继续
阅读 [从零实现一个工作流](BUILD_FIRST_WORKFLOW.md)。
