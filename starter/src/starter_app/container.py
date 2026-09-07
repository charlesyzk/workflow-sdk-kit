from functools import lru_cache

from obei_workflow_sdk import (
    SQLAlchemyWorkflowStorage,
    WorkflowSettings,
    create_runtime,
)

from .workflows import DifyWriterWorkflow, NumberAnalysisWorkflow, OpenAIWriterWorkflow, TaskSystemSmokeWorkflow


@lru_cache(maxsize=1)
def get_settings() -> WorkflowSettings:
    """进程内只解析一次环境配置，保证后续组件使用同一快照。"""
    return WorkflowSettings()  # type: ignore[call-arg]


@lru_cache(maxsize=1)
def get_storage() -> SQLAlchemyWorkflowStorage:
    """创建数据库仓储；生产建表由 migrate 进程负责，应用进程不隐式改表。"""
    return SQLAlchemyWorkflowStorage(get_settings().database_url, create_tables=False)


@lru_cache(maxsize=1)
def get_runtime():
    """一行完成 ARQ、事件总线、LLM 和任务系统的生产组装。

    API 和两个 Worker 都从这个缓存工厂取得 Runtime。每个进程有独立缓存，但会
    读取同一份环境配置和工作流列表，从而保证发布端、消费端及注册 JSON 导出端
    对 workflow_type 的认识一致。
    """
    # 所有对外可提交的工作流集中注册在这里。新增工作流后，API 类型列表、ARQ
    # Worker 执行和 export_definition CLI 会同时生效，无需在三处重复配置。
    return create_runtime(
        [NumberAnalysisWorkflow(), OpenAIWriterWorkflow(), DifyWriterWorkflow(), TaskSystemSmokeWorkflow()],
        settings=get_settings(),
        storage=get_storage(),
    )
