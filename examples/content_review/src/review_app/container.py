"""示例应用的配置、Storage 和 Runtime 组装入口。"""

from functools import lru_cache

from obei_workflow_sdk import SQLAlchemyWorkflowStorage, WorkflowSettings, create_runtime

from .workflow import ContentReviewWorkflow


@lru_cache(maxsize=1)
def get_settings() -> WorkflowSettings:
    return WorkflowSettings()  # type: ignore[call-arg]


@lru_cache(maxsize=1)
def get_storage() -> SQLAlchemyWorkflowStorage:
    # 生产进程不隐式执行 DDL；建表由 review_app.migrate 单独完成。
    return SQLAlchemyWorkflowStorage(get_settings().database_url, create_tables=False)


@lru_cache(maxsize=1)
def get_runtime():
    return create_runtime(
        [ContentReviewWorkflow()],
        settings=get_settings(),
        storage=get_storage(),
    )
