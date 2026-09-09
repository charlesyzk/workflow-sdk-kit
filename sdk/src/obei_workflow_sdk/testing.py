"""Explicit test doubles. This module is never used by the production Starter."""

from typing import Any, Callable


class InlineDispatcher:
    def bind(self, execute: Callable[[str], dict]) -> None:
        self.execute = execute

    def dispatch(self, run_id: str) -> None:
        self.execute(run_id)


class NullTaskSystemAdapter:
    def task_status(self, task_id: str, status: str, payload: dict[str, Any] | None = None) -> None:
        return None

    def step_status(self, task_id: str, run_id: str, step_code: str, step_name: str, status: str, attempt: int, payload: dict[str, Any] | None = None, definition: dict[str, Any] | None = None) -> None:
        return None
