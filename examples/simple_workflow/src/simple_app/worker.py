"""两个 ARQ WorkerSettings 的 CLI 入口。"""

from obei_workflow_sdk import (
    create_execution_sync_worker_settings,
    create_workflow_worker_settings,
)

from .container import get_runtime, get_settings, get_storage


WorkflowWorkerSettings = create_workflow_worker_settings(get_runtime, get_settings)
ExecutionSyncWorkerSettings = create_execution_sync_worker_settings(get_storage, get_settings)

