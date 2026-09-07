"""ARQ CLI 入口。

工作流推进和任务系统同步使用两个独立队列。前者不受远端 HTTP 延迟影响；后者
从 SQL Outbox 可靠投递并按配置重试。两个 Worker 必须和 API 使用同一个 .env。
"""

from obei_workflow_sdk import (
    create_execution_sync_worker_settings,
    create_workflow_worker_settings,
)

from .container import get_runtime, get_settings, get_storage


# 启动命令：arq workflow_app.worker.WorkflowWorkerSettings
WorkflowWorkerSettings = create_workflow_worker_settings(get_runtime, get_settings)

# 启动命令：arq workflow_app.worker.ExecutionSyncWorkerSettings
ExecutionSyncWorkerSettings = create_execution_sync_worker_settings(
    get_storage,
    get_settings,
)
