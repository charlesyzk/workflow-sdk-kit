"""ARQ CLI 使用的两个 WorkerSettings 入口。

这里只负责把 Starter 的依赖工厂交给 SDK，不复制任何重试或执行逻辑。工作流和
任务系统同步使用不同队列/并发配置，避免慢速外部 HTTP 请求占满工作流执行槽位。
"""

from obei_workflow_sdk import (
    create_execution_sync_worker_settings,
    create_workflow_worker_settings,
)

from .container import get_runtime, get_settings, get_storage


# CLI 启动方式：arq starter_app.worker.WorkflowWorkerSettings
WorkflowWorkerSettings = create_workflow_worker_settings(get_runtime, get_settings)
# CLI 启动方式：arq starter_app.worker.ExecutionSyncWorkerSettings
ExecutionSyncWorkerSettings = create_execution_sync_worker_settings(get_storage, get_settings)
