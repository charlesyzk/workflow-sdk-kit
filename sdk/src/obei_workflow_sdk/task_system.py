from __future__ import annotations

from typing import Any, Protocol


class TaskSystemAdapter(Protocol):
    """Port implemented by the real external task system connector."""

    def task_status(self, task_id: str, status: str, payload: dict[str, Any] | None = None) -> None: ...

    def step_status(
        self,
        task_id: str,
        run_id: str,
        step_code: str,
        step_name: str,
        status: str,
        attempt: int,
        payload: dict[str, Any] | None = None,
        definition: dict[str, Any] | None = None,
    ) -> None: ...


class DisabledTaskSystemAdapter:
    """显式关闭外部任务系统时使用的空端口实现。

    它不会模拟远端任务，也不会宣称远端调用成功；作用只是让纯代码工作流仍可
    复用同一套 Runtime。宿主必须通过 ``EXECUTION_TASK_ENABLED=false`` 明确选择。
    """

    def task_status(self, task_id: str, status: str, payload: dict[str, Any] | None = None) -> None:
        return None

    def step_status(
        self,
        task_id: str,
        run_id: str,
        step_code: str,
        step_name: str,
        status: str,
        attempt: int,
        payload: dict[str, Any] | None = None,
        definition: dict[str, Any] | None = None,
    ) -> None:
        return None

