# 模板开发与交付指南

本文补充主 README 中没有展开的项目约定、测试边界、部署进程和 SDK 升级方式。

## 1. 业务代码与 SDK 的边界

模板把 SDK 放在 `vendor/obei-workflow-sdk`，把业务应用放在 `src/workflow_app`。

业务项目负责：

- Pydantic 请求、节点输入和节点输出模型；
- State 字段；
- 节点业务逻辑、Prompt 和模型策略；
- Workflow 节点、边和人工确认点；
- FastAPI 的认证、租户和权限；
- 环境配置及生产部署。

SDK 负责：

- Task、Run、Checkpoint、NodeExecution 生命周期；
- Artifact 版本、哈希和最终产物；
- LangGraph 编译和恢复；
- ARQ 投递、运行租约锁和队列隔离；
- Redis Stream 与 SSE；
- 直接模型客户端、Dify Chat App 客户端、统一流式 token 和分层调用审计；
- 远端任务系统 Binding、Outbox、幂等和重试；
- 注册 JSON 导出；
- 通用 HTTP Router。

不要从业务代码导入 `obei_workflow_sdk` 的内部模块。稳定公共类型都从包根导入：

```python
from obei_workflow_sdk import Workflow, WorkflowNode, NodeOutput
```

## 2. 新增节点

推荐顺序：

1. 定义节点输入模型；
2. 定义节点输出模型；
3. 声明 `TaskStep`；
4. 声明 `input_fields` 和 `output_fields`；
5. 实现 `execute()`；
6. 写基础设施无关测试；
7. 加入图并重新生成注册 JSON。

节点输入只读取明确声明的 State 字段。输出经过 Pydantic 模型后再映射回 State，
可以尽早发现拼写和类型错误。

## 3. 错误、重试和幂等

业务节点不需要手工更新节点状态。抛出异常后 SDK 会：

- 把 NodeExecution 标记为 `FAILED`；
- 保存异常类型和摘要；
- 追加 `node_failed` 和 `workflow_failed` 事件；
- 写远端步骤失败 Outbox；
- 把 Task/Run 收口为 `FAILED`。

用户调用重试接口后，工作流从持久 Checkpoint 恢复。外部写操作仍应由业务节点
使用自己的稳定幂等键，通常组合 `ctx.run_id`、`ctx.node_code` 和业务操作 ID；
循环节点还应加入稳定的迭代 ID。`ctx.attempt` 只用于审计，不能放进业务幂等键，
否则 Checkpoint 重试会生成新键并重复执行外部副作用。远端任务重试产生的新
`taskId` 也不能作为业务幂等键的一部分，本地 operation key 在 Binding 切换后保持不变。

## 4. 自定义实时输出

三种机制的选择：

| 需求 | 使用方式 | 是否默认落库 |
|---|---|---:|
| LLM token | `LLMNodeConfig(stream=True)` | 否 |
| Dify 文本增量 | `chat()` / `generate()`（始终流式） | 否 |
| Dify Workflow/工具事件 | `dify_event` | 否 |
| 普通业务片段 | `yield NodeStreamChunk(...)` | 否 |
| 低频可靠进度 | `ctx.progress(...)` 或 `persist=True` | 是 |

不要把每个 token 都写数据库。高频内容通过 Redis/SSE 传输，完整模型响应最终写入
`obei_workshop_llm_invocation`，兼顾实时性与可审计性。

## 5. 数据库表

七张核心工作流表：

```text
obei_workshop_task
obei_workshop_task_run
obei_workshop_task_checkpoint
obei_workshop_task_node_execution
obei_workshop_task_artifact
obei_workshop_task_event
obei_workshop_task_decision
```

三张技术表：

```text
obei_workshop_execution_binding
obei_workshop_task_system_outbox
obei_workshop_llm_invocation
```

Dify 启用后还使用两张独立表，避免把有状态应用调用伪装成模型调用：

```text
obei_workshop_dify_conversation
obei_workshop_dify_invocation
```

Dify Registry 中每个别名对应一个独立 App Key 和 app_type。聊天类默认不发送或
复用 `conversation_id`；显式设置 `use_history=True` 后才保存复用，相同会话由
Redis 锁自动串行。Text Generator 和 Workflow 没有 conversation_id。

远端任务同步关闭时 Binding 和 Outbox 没有数据是正常行为，不影响本地七张表和 LLM
审计。生产 TiDB 索引见 `docs/sdk/TIDB_REQUIRED_INDEXES.sql`。

## 6. 进程模型

生产至少运行三个独立进程：

```text
FastAPI
  └─ 校验请求、创建 Task/Run、投递 ARQ 消息、返回 202

workflow Worker
  └─ 消费 workflow 队列，推进 LangGraph

execution-sync Worker
  └─ 消费 execution_sync 队列，派发 SQL Outbox
```

API 不执行长任务，任务系统 HTTP 也不占用工作流 Worker 并发。多实例部署时，Redis
运行锁会避免同一 Run 被同时推进。

## 7. 测试分层

建议保留三层测试：

1. 单元测试：节点纯函数；
2. 工作流测试：SQLite、`InlineDispatcher`、Fake LLM；
3. 集成测试：真实 TiDB、Redis、模型和远端任务系统。

前两层进入每次 CI。第三层使用独立测试账号和数据库，密钥由 CI 密钥系统注入，
不要保存在测试文件或构建产物中。

每次改图后至少断言：

- 节点执行顺序和状态；
- stream true/false 是否符合预期；
- Artifact 数量和最终产物；
- 注册 JSON 的 `dependsOn`；
- 失败路径和重试路径。

## 8. 更新内置 SDK

业务项目中的 `vendor/obei-workflow-sdk` 是交付时的固定版本。升级流程：

1. 获取经过测试的新 SDK 目录；
2. 完整替换 `vendor/obei-workflow-sdk`，不要混合覆盖文件；
3. 重新执行 `scripts/setup.ps1`；
4. 运行 `scripts/test.ps1`；
5. 对真实数据库执行迁移评估；
6. 在测试环境完成真实模型和任务系统回归；
7. 再部署生产。

SDK 版本由 `vendor/obei-workflow-sdk/pyproject.toml` 定义，业务项目在自己的
`pyproject.toml` 中固定对应版本，避免环境解析到错误的公网同名包。

## 9. 交付前检查

- `.env` 未进入压缩包；
- `EXECUTION_TASK_ENABLED` 在未登记环境保持 false；
- 工作流类型和所有 `step.code` 已稳定；
- 注册 JSON 是从当前代码重新生成的；
- 单元测试和工作流测试通过；
- 数据库索引由 DBA 确认；
- API 已增加认证、租户和权限；
- API、两个 Worker 使用同一版本代码和同一配置；
- 日志、指标和告警已接入团队平台。
