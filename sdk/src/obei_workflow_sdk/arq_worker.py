"""Reusable ARQ WorkerSettings factories for workflow hosts."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any

from arq import Retry, func
from arq.connections import RedisSettings

from .execution_task import ExecutionTaskApiError, dispatch_execution_outbox
from .run_lock import WorkflowRunLocked, WorkflowRunLockLost


def create_workflow_worker_settings(
    runtime_factory: Callable[[], Any],
    settings_factory: Callable[[], Any],
) -> type:
    """生成消费工作流推进队列的 ARQ WorkerSettings 类。

    使用工厂而不是固定全局 Worker，是为了让 SDK 不依赖宿主的模块路径和工作流
    注册方式。宿主只需传入两个无参工厂，并把返回类暴露为模块变量，ARQ CLI
    即可按 ``arq module.WorkflowWorkerSettings`` 启动。
    """
    settings = settings_factory()

    async def advance(ctx: dict[str, Any], run_id: str):
        # 直接 await Runtime 的异步入口，避免在 ARQ 已运行的事件循环中嵌套
        # asyncio.run。租约冲突不是业务失败，延迟一个续租周期后重试即可。
        try:
            return await runtime_factory()._execute(run_id)
        except (WorkflowRunLocked, WorkflowRunLockLost) as exc:
            raise Retry(defer=settings.run_lock_renew_seconds) from exc

    class WorkerSettings:
        # function name 必须与 ArqDispatcher 默认发布名一致；keep_result=0 表示
        # 运行事实以 TiDB 为准，不在 Redis 长期保存一份重复结果。
        functions = [func(advance, name="advance_workflow", max_tries=10, keep_result=0)]
        redis_settings = RedisSettings.from_dsn(settings.redis_url)
        queue_name = settings.arq_workflow_queue
        max_jobs = settings.arq_workflow_max_jobs
        job_timeout = settings.arq_job_timeout_seconds
        keep_result = 0

    return WorkerSettings


def create_execution_sync_worker_settings(
    storage_factory: Callable[[], Any],
    settings_factory: Callable[[], Any],
) -> type:
    """生成消费外部任务系统 Outbox 唤醒消息的 ARQ WorkerSettings 类。

    消息只携带本地 task_id，真实待发送内容始终从 SQL Outbox 读取。即使 Redis
    消息重复、Worker 崩溃或网络抖动，数据库中的顺序和幂等键仍是最终事实源。
    """
    settings = settings_factory()

    async def sync(ctx: dict[str, Any], task_id: str):
        try:
            # 当前 HTTP 客户端和 SQLAlchemy Storage 是同步实现，放到线程执行，
            # 防止一次远端慢请求阻塞 ARQ 事件循环中的其他 Job。
            return await asyncio.to_thread(
                dispatch_execution_outbox,
                storage_factory(),
                settings,
                task_id,
            )
        except ExecutionTaskApiError as exc:
            # job_try 从 1 开始。指数退避与 Outbox 内部的 next_attempt_at 使用同一
            # 组配置，并受 max_seconds 限制，避免远端故障时形成请求风暴。
            attempt = max(0, int(ctx.get("job_try", 1)) - 1)
            delay = min(
                settings.execution_task_retry_max_seconds,
                settings.execution_task_retry_base_seconds * (2**attempt),
            )
            raise Retry(defer=delay) from exc

    class WorkerSettings:
        # max_tries 覆盖网络异常、429 和 5xx；不可重试 4xx 会由派发器直接把
        # Outbox 行标记 FAILED，不会反复进入 ARQ Retry。
        functions = [
            func(
                sync,
                name="sync_execution_task",
                max_tries=settings.execution_task_retry_max_attempts,
                keep_result=0,
            )
        ]
        redis_settings = RedisSettings.from_dsn(settings.redis_url)
        queue_name = settings.arq_execution_sync_queue
        max_jobs = settings.arq_execution_sync_max_jobs
        job_timeout = settings.arq_job_timeout_seconds
        keep_result = 0

    return WorkerSettings
