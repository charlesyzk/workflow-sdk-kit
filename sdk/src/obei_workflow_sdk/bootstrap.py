"""Opinionated production assembly helpers for small workflow hosts."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from .dispatcher import ArqDispatcher
from .event_bus import RedisEventBus
from .execution_task import ExecutionTaskAdapter
from .graph import Workflow, WorkflowRegistry
from .llm import create_llm_registry
from .dify import create_dify_registry
from .runtime import WorkflowRuntime
from .settings import WorkflowSettings
from .storage import SQLAlchemyWorkflowStorage
from .task_system import DisabledTaskSystemAdapter


def create_runtime(
    workflows: Iterable[Workflow],
    *,
    settings: WorkflowSettings | None = None,
    storage: Any = None,
    dispatcher: Any = None,
    task_system: Any = None,
    create_tables: bool = False,
) -> WorkflowRuntime:
    """用最少参数组装可生产运行的工作流 Runtime。

    默认组装关系为：SQLAlchemy Storage + ARQ Dispatcher + Redis EventBus +
    LLM Registry + ExecutionTaskAdapter。调用方通常只传工作流列表；测试、私有云
    或已有基础设施的宿主可以逐项覆盖组件，而不需要复制 Starter 的 container。

    ``create_tables`` 只适合本地开发或一次性测试。生产环境应保持 False，并由
    独立迁移进程执行 DDL，避免多个 API/Worker 实例并发尝试修改表结构。
    """
    resolved_settings = settings or WorkflowSettings()  # type: ignore[call-arg]
    # 先确定 Storage，因为任务系统 Outbox、LLM 审计和 Runtime 都共享它；这可
    # 保证节点事实与待发送事件写入同一个数据库，而不是跨库产生一致性窗口。
    resolved_storage = storage or SQLAlchemyWorkflowStorage(
        resolved_settings.database_url,
        create_tables=create_tables,
    )
    # 注册阶段立即校验 workflow_type 重复，配置错误会在进程启动时失败，而不会
    # 等到第一个用户请求到达后才暴露。
    registry = WorkflowRegistry()
    for workflow in workflows:
        registry.register(workflow)
    resolved_dispatcher = dispatcher or ArqDispatcher(
        resolved_settings.redis_url,
        queue=resolved_settings.arq_workflow_queue,
    )
    # 禁用模式是显式选择，不模拟远端成功；启用时始终通过 SQL Outbox 投递。
    resolved_task_system = task_system or (
        ExecutionTaskAdapter(resolved_storage, resolved_settings)
        if resolved_settings.execution_task_enabled
        else DisabledTaskSystemAdapter()
    )
    return WorkflowRuntime(
        storage=resolved_storage,
        registry=registry,
        dispatcher=resolved_dispatcher,
        task_system=resolved_task_system,
        settings=resolved_settings,
        event_bus=RedisEventBus(
            resolved_settings.resolved_event_bus_url,
            resolved_settings.event_stream_prefix,
            resolved_settings.event_stream_maxlen,
        ),
        llm_registry=create_llm_registry(resolved_settings),
        dify_registry=create_dify_registry(resolved_settings),
    )
