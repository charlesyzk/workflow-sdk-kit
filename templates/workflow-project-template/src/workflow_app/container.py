"""应用依赖组装入口。

API、两个 ARQ Worker、数据库迁移命令和注册 JSON 导出命令都从本模块取得同一
份 Settings、Storage 与 Runtime，避免不同进程注册了不同的工作流或连接到不同
基础设施。新增工作流时，只需要在 ``get_runtime`` 的列表中增加一个实例。
"""

from functools import lru_cache

from obei_workflow_sdk import SQLAlchemyWorkflowStorage, WorkflowSettings, create_runtime

from .workflows import FourStageWorkflow


@lru_cache(maxsize=1)
def get_settings() -> WorkflowSettings:
    """每个进程只解析一次当前目录的 .env。"""

    return WorkflowSettings()  # type: ignore[call-arg]


@lru_cache(maxsize=1)
def get_storage() -> SQLAlchemyWorkflowStorage:
    """创建运行时 Storage，但不在 API/Worker 启动阶段隐式修改数据库结构。"""

    return SQLAlchemyWorkflowStorage(get_settings().database_url, create_tables=False)


@lru_cache(maxsize=1)
def get_runtime():
    """组装数据库、ARQ、Redis 事件总线、LLM 和远端任务系统。

    SDK 会根据 ``EXECUTION_TASK_ENABLED`` 决定是否启用远端任务系统。登记并取得
    Key 以前保持 false，本地工作流、Artifact、Checkpoint 和 LLM 审计仍会正常
    运行；取得 Key 后只改 .env，无需修改工作流代码。
    """

    return create_runtime(
        # 新增工作流后放入这个列表，API、Worker 和注册导出会同时识别它。
        [FourStageWorkflow()],
        settings=get_settings(),
        storage=get_storage(),
    )
