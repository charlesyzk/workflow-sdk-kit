# Obei Workflow SDK Kit

这是 Obei 工作流 SDK 的开发与交付仓库。推荐交付物不再是让使用者分别理解
`sdk/`、`starter/` 和示例，而是一个已经内置 SDK、可直接解压开发的完整模板项目。

## 推荐交付方式

构建模板：

```powershell
.\scripts\build-project-template.ps1
```

生成：

```text
dist/workflow-project-template/       可直接进入和开发的完整目录
dist/workflow-project-template.zip    可直接发送给使用者的压缩包
```

生成的模板包含：

- `vendor/obei-workflow-sdk`：当前 SDK 的干净源码包；
- `src/workflow_app`：FastAPI、ARQ Worker、Runtime 和四环节示例；
- `scripts`：安装、测试、迁移、注册导出、启动和停止脚本；
- `generated/registration`：从真实图生成的四步登记 JSON；
- `docs`：模板开发指南和有效的 SDK 参考文档；
- `.env.example`：不含真实密钥的完整配置模板。

使用者拿到 ZIP 后只需要：

```powershell
.\scripts\setup.ps1
.\scripts\test.ps1
# 修改 src/workflow_app/workflows
.\scripts\export-workflow.ps1 simple_llm_workflow
# 去远端登记并把签发的 Key 填入 .env
.\scripts\start-all.ps1
```

## 仓库结构

```text
sdk/                              可独立安装的 obei-workflow-sdk
templates/workflow-project-template/
                                  完整模板的业务源码和脚本
examples/simple_workflow/          四环节 SDK 集成示例
starter/                           SDK 生产组件的扩展示例和回归测试
docs/                              当前有效文档
scripts/build-project-template.ps1 模板交付构建器
dist/                              生成产物，不提交版本库
```

模板构建器只复制白名单文档，不复制仓库根目录 `.env`，并会扫描常见 API Key
形态。SDK 的 `__pycache__`、测试缓存和 `egg-info` 也会从交付物中清理。

## 四环节模板

模板自带的 `simple_llm_workflow` 覆盖：

1. 同步纯函数，不调用 LLM；
2. 普通节点通过 `yield NodeStreamChunk` 自定义实时输出；
3. `stream=True` 的 LLM 节点，自动发送模型 token；
4. `stream=False` 的 LLM 节点，等待完整响应并保存最终 Artifact。

同一份 `Workflow.build()` 同时驱动 LangGraph 执行和远端注册 JSON，不需要手工
维护第二份流程依赖。

## 文档

建议按顺序阅读：

- [当前状态与整体设计](docs/CURRENT_STATUS.md)
- [完整模板交付说明](docs/TEMPLATE_PROJECT.md)
- [SDK 功能说明](docs/SDK_FEATURES.md)
- [从零实现四环节工作流](docs/BUILD_FIRST_WORKFLOW.md)
- [SDK API 与生产参考](docs/SDK_USAGE.md)
- [TiDB 必需索引](docs/TIDB_REQUIRED_INDEXES.sql)

旧的五分钟快速开始和旧 Starter 运行文档已经被模板 README、模板开发指南和上述
文档覆盖，不再保留重复版本。
