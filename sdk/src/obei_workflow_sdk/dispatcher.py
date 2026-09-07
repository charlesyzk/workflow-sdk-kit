from __future__ import annotations

import asyncio
from threading import Thread
from typing import Callable, Protocol

from arq import create_pool
from arq.connections import RedisSettings


class Dispatcher(Protocol):
    def bind(self, execute: Callable[[str], dict]) -> None: ...
    def dispatch(self, run_id: str) -> None: ...


def _run_coroutine(factory: Callable[[], object]) -> object:
    """Run a short ARQ client coroutine from sync API or async worker code.

    Runtime lifecycle methods intentionally remain synchronous for embedding in
    scripts and existing hosts. When called by an ARQ worker there is already an
    event loop, so the small publishing coroutine is isolated in a helper thread.
    """
    # FastAPI 的同步路由通常在线程池中调用 dispatch，此时线程内没有事件循环，
    # 可以直接使用 asyncio.run，保持脚本和同步宿主的调用方式足够简单。
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(factory())  # type: ignore[arg-type]

    result: list[object] = []
    error: list[BaseException] = []

    # 当调用发生在 ARQ/LangGraph 的异步执行上下文中，当前线程已经有正在运行的
    # loop，不能嵌套 asyncio.run。这里使用一个短生命周期辅助线程发布消息，
    # 并同步传播异常，确保调用方能感知 Redis 不可用，而不是静默丢任务。
    def runner() -> None:
        try:
            result.append(asyncio.run(factory()))  # type: ignore[arg-type]
        except BaseException as exc:  # pragma: no cover - defensive propagation
            error.append(exc)

    thread = Thread(target=runner, name="obei-arq-publisher", daemon=True)
    thread.start()
    thread.join()
    if error:
        raise error[0]
    return result[0] if result else None


class ArqDispatcher:
    """使用 ARQ 发布任务，同时保留 Runtime 的同步嵌入接口。

    调度器只负责发送 ``function_name(run_id)``，不会在 API 进程执行工作流。
    Worker 必须在相同 Redis、队列名下注册同名函数。工作流执行本身仍由数据库
    Checkpoint 和 Redis Run Lock 保证可恢复与单 Run 互斥，因此 ARQ 至少一次
    投递造成的重复消费不会并发推进同一个 Run。
    """

    def __init__(self, redis_url: str, function_name: str = "advance_workflow", queue: str = "workflow"):
        self.redis_settings = RedisSettings.from_dsn(redis_url)
        self.function_name, self.queue = function_name, queue

    def bind(self, execute: Callable[[str], dict]) -> None:
        # InlineDispatcher 会通过 bind 得到执行函数；生产 ARQ Worker 则通过模块
        # 中的 WorkerSettings 显式注册函数，所以发布端无需保存 execute 引用。
        return None

    def dispatch(self, run_id: str) -> None:
        async def enqueue() -> None:
            # 每次发布创建一个轻量连接池并在 finally 中关闭，避免同步 API 进程
            # 持有依赖特定事件循环的异步 Redis 连接。吞吐量特别高的宿主可注入
            # 自己的 Dispatcher，以应用 lifespan 管理长连接池。
            pool = await create_pool(self.redis_settings)
            try:
                await pool.enqueue_job(self.function_name, run_id, _queue_name=self.queue)
            finally:
                await pool.aclose()

        _run_coroutine(enqueue)
