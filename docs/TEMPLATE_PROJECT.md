# 完整模板项目交付说明

本文面向 SDK 维护者，说明为什么使用完整模板交付、模板源码与生成产物的关系，
以及如何验证一个可以发送给业务团队的 ZIP。

## 1. 为什么交付完整模板

只交付 SDK 时，接入团队仍需要自己解决：

- SDK 放在哪个目录；
- 怎样离线安装；
- FastAPI 如何挂载 Router；
- ARQ Worker 怎样声明和启动；
- Redis、数据库和 LLM 如何使用同一份配置；
- 怎样导出远端注册 JSON；
- 远端登记前后如何切换任务系统；
- 如何写第一条覆盖流式和非流式场景的工作流。

完整模板把这些非业务选择固定下来。使用者主要修改 `workflows/`，不需要复制 SDK
内部实现或重新发明启动结构。

## 2. 源码与产物

模板业务源码位于：

```text
templates/workflow-project-template/
```

这里故意不长期保存 `vendor/obei-workflow-sdk` 的副本，避免 SDK 修改后模板中的
复制版本落后。构建时，脚本从当前 `sdk/` 同步生成 vendor 包。

执行：

```powershell
.\scripts\build-project-template.ps1
```

输出：

```text
dist/workflow-project-template/
dist/workflow-project-template.zip
```

只有 `dist` 中的目录和 ZIP 是完整、可直接交付的模板。不要把
`templates/workflow-project-template` 原始目录单独发给使用者，因为它没有内置
构建后的 SDK。

## 3. 构建器做什么

`scripts/build-project-template.ps1` 会：

1. 验证模板和 SDK 源码存在；
2. 把输出路径限制在当前仓库内；
3. 清理精确的旧 `dist/workflow-project-template` 产物；
4. 复制模板业务源码；
5. 把当前 `sdk/` 放入 `vendor/obei-workflow-sdk`；
6. 复制仍然有效的 SDK 文档；
7. 清理 `__pycache__`、`.pytest_cache`、`egg-info` 和字节码；
8. 写入不含密钥的 `BUILD_INFO.json`；
9. 拒绝包含 `.env` 或常见 API Key 形态的产物；
10. 生成 ZIP。

构建不会复制仓库根目录真实 `.env`。

## 4. 模板内的开发路径

业务团队解压后的标准路径：

```text
setup
  → 运行假 LLM 测试
  → 修改节点
  → 编排图
  → 重新测试
  → 生成 registration JSON
  → 人工远端登记
  → 填写签发 Key
  → 数据库迁移
  → 启动 Redis、API 和两个 ARQ Worker
  → API/SSE/数据库/远端任务系统联调
```

模板 README 已完整描述每一步，并列出所有 HTTP API。

## 5. 维护者验证

构建后至少运行：

```powershell
cd .\dist\workflow-project-template
.\scripts\setup.ps1
.\scripts\test.ps1
.\scripts\export-workflow.ps1 simple_llm_workflow
.\scripts\migrate.ps1
```

验证 ZIP：

- 可以解压；
- `.env.example` 和 `.gitignore` 存在；
- `.env` 不存在；
- `vendor/obei-workflow-sdk/pyproject.toml` 存在；
- 四环节测试通过；
- 注册 JSON 有四个节点和正确依赖；
- SQLite 或测试 TiDB 建表成功；
- `/health` 和 `/ready` 成功；
- 配置真实模型后，两个 LLM 审计的 stream 分别为 true、false；
- 填入新签发任务 Key 后，Outbox 全部发送成功。

远端登记并拿到新 Key 后，维护者可以运行仓库级真实联调脚本：

```powershell
python .\scripts\run-real-integration.py
```

脚本会隐藏读取 Key，只把它放入本次 API/Worker 子进程的环境变量；不会修改
`.env`、模板或 ZIP。它会通过 HTTP 提交四环节任务、订阅 SSE、等待两个 ARQ
Worker、核对 TiDB 十张表与 Outbox、查询远端绑定任务，然后自动结束测试进程。

远端任务系统暂时不可用时，可以只替换 HTTP 对端，其余组件仍使用真实实现：

```powershell
python .\scripts\run-real-integration.py --mock-task-system
```

本地 Mock 会校验必填鉴权头、201/200 幂等创建、执行模式、四个已注册步骤、依赖
顺序、状态转换和最终 100% 进度。SDK 仍通过真实 SQL Outbox 和独立 ARQ 同步
Worker 调用这些接口，不会使用 `NullTaskSystemAdapter` 绕过集成逻辑。

## 6. 两类交付物

建议保留两种渠道：

1. 完整模板 ZIP：默认推荐，适合新项目和希望快速接入的团队；
2. 独立 `sdk/` 包：只提供给已经有成熟宿主结构的团队。

无论哪一种，都不要在压缩包中预置真实数据库、模型或任务系统密钥。
