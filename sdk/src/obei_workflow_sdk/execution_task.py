from __future__ import annotations

from datetime import timedelta
import json
from typing import Any

import httpx
import redis
from sqlalchemy import func, select

from .dispatcher import ArqDispatcher
from .models import ExecutionBinding, TaskSystemOutbox, WorkflowTask, new_id, utcnow


LOCAL_STATUS = {
    "QUEUED": ("Pending", "Queued"), "RUNNING": ("Running", "Running"),
    "WAITING_USER": ("AwaitingConfirmation", "WaitingUser"),
    "SUCCEEDED": ("Success", "Completed"), "FAILED": ("Failed", "Failed"),
    "BLOCKED": ("Failed", "Blocked"), "CANCELLED": ("Failed", "Cancelled"),
}


def compact_output(value: Any, limit: int) -> Any:
    """按 UTF-8 字节数裁剪远端步骤摘要，避免大模型正文撑爆任务系统。"""
    raw = json.dumps(value, ensure_ascii=False, default=str).encode("utf-8")
    if len(raw) <= limit:
        return value
    return {"truncated": True, "bytes": len(raw), "preview": raw[:limit].decode("utf-8", errors="ignore")}


class ExecutionTaskApiError(RuntimeError):
    """保留 HTTP 状态与响应体，并按网络/429/5xx 判定是否可重试。"""
    def __init__(self, message: str, status_code: int | None = None, body: Any = None):
        super().__init__(message)
        self.status_code, self.body = status_code, body

    @property
    def retryable(self) -> bool:
        return self.status_code is None or self.status_code == 429 or self.status_code >= 500


class ExecutionTaskClient:
    """外部任务执行系统的同步 HTTP 客户端，由 execution_sync Worker 使用。"""
    def __init__(self, settings: Any):
        self.client = httpx.Client(
            base_url=settings.execution_task_api_base_url,
            headers={"X-API-Key": settings.execution_task_api_key, "X-Caller-Id": settings.execution_task_caller_id},
            timeout=settings.execution_task_timeout_seconds,
        )

    def call(self, method: str, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        """调用真实 API，并把网络错误和非 2xx 响应统一为领域异常。"""
        try:
            response = self.client.request(method, path, json=payload)
        except httpx.HTTPError as exc:
            raise ExecutionTaskApiError(str(exc)) from exc
        try:
            body = response.json()
        except ValueError:
            body = response.text
        if response.status_code >= 400:
            raise ExecutionTaskApiError(f"{method} {path} -> {response.status_code}", response.status_code, body)
        return body if isinstance(body, dict) else {"status": response.status_code}


class ExecutionTaskAdapter:
    """通过持久 SQL Outbox 和 ARQ Worker 对接真实任务系统。

    Runtime/节点只在本地数据库事务中追加事件，不直接等待远端 HTTP。提交后发送
    ARQ 唤醒消息；即使唤醒消息重复，Outbox 幂等键、事件序号和任务级 Redis 锁
    仍保证同一远端任务按顺序最多产生一次有效状态变更。
    """

    def __init__(self, storage: Any, settings: Any, sync_task_name: str = "sync_execution_task"):
        self.storage, self.settings = storage, settings
        self.publisher = ArqDispatcher(
            settings.redis_url,
            function_name=sync_task_name,
            queue=getattr(settings, "arq_execution_sync_queue", "execution_sync"),
        )

    def _binding(self, db: Any, task_id: str, run_id: str) -> ExecutionBinding:
        """获取最新远端绑定；首次上报时同事务创建绑定和 TASK_CREATE Outbox。"""
        binding = db.scalar(select(ExecutionBinding).where(ExecutionBinding.local_task_id == task_id).order_by(ExecutionBinding.retry_seq.desc()))
        if binding is None:
            binding = ExecutionBinding(local_task_id=task_id, local_run_id=run_id, idempotency_key=f"task:{task_id}:retry:0", retry_seq=0, binding_status="PENDING")
            db.add(binding); db.flush()
            task = db.get(WorkflowTask, task_id)
            self._enqueue(db, binding, "TASK_CREATE", None, {
                "input": {"taskId": task_id, "runId": run_id, "actorId": task.actor_id, "workflowType": task.workflow_type, "input": task.input_payload},
                "idempotentKey": binding.idempotency_key,
                "executionMode": self.settings.execution_task_execution_mode,
            }, f"task-create:{binding.idempotency_key}")
        return binding

    def _enqueue(self, db: Any, binding: ExecutionBinding, event_type: str, step_code: str | None, payload: dict[str, Any], key: str) -> None:
        """以幂等键去重，并为单个本地任务生成严格递增的事件序号。"""
        if db.scalar(select(TaskSystemOutbox.id).where(TaskSystemOutbox.idempotency_key == key)):
            return
        seq = int(db.scalar(select(func.max(TaskSystemOutbox.event_seq)).where(TaskSystemOutbox.local_task_id == binding.local_task_id)) or 0) + 1
        db.add(TaskSystemOutbox(local_task_id=binding.local_task_id, local_run_id=binding.local_run_id, binding_id=binding.id, event_seq=seq, event_type=event_type, step_code=step_code, payload=payload, idempotency_key=key, status="PENDING"))

    def _publish(self, task_id: str) -> None:
        """事务提交后唤醒同步 Worker；消息本身不是事实源。

        同一节点可能连续触发多个唤醒，Worker 的任务级锁会让多余 Job 快速返回
        LOCKED。真正需要发送的内容始终由 ``dispatch_execution_outbox`` 按数据库
        event_seq 批量读取，因此不会因 ARQ 消息乱序而打乱远端步骤依赖。
        """
        self.publisher.dispatch(task_id)

    def task_status(self, task_id: str, status: str, payload: dict[str, Any] | None = None) -> None:
        """把本地任务状态映射为远端状态并持久写入 Outbox。"""
        task = self.storage.get_task(task_id)
        if not task or not task["run_id"]:
            raise ValueError("task/run not found")
        ext, business = LOCAL_STATUS.get(status, ("Running", status))
        with self.storage.session_factory.begin() as db:
            binding = self._binding(db, task_id, task["run_id"])
            # TASK_CREATE already establishes Pending; do not send a redundant
            # Pending -> Pending transition that some remote state machines reject.
            if status != "QUEUED":
                self._enqueue(db, binding, "TASK_STATUS", None, {"status": ext, "businessStatus": business, "output": compact_output(payload or {}, self.settings.execution_task_output_max_bytes)}, f"task-status:{binding.id}:{status}")
        self._publish(task_id)

    def step_status(self, task_id: str, run_id: str, step_code: str, step_name: str, status: str, attempt: int, payload: dict[str, Any] | None = None) -> None:
        """将节点开始/终态转换成远端步骤 API 所需的事件。"""
        with self.storage.session_factory.begin() as db:
            binding = self._binding(db, task_id, run_id)
            if status == "RUNNING":
                event_type = "STEP_START"
                body = {"sessionId": f"{run_id}:{step_code}", "stepInput": {"name": step_name}}
                key = f"step-start:{binding.id}:{step_code}"
            else:
                event_type = "STEP_STATUS"
                remote = "Success" if status == "SUCCEEDED" else "Failed"
                body = {"status": remote, "output": compact_output(payload or {}, self.settings.execution_task_output_max_bytes)}
                key = f"step-status:{binding.id}:{step_code}:{attempt}:{remote}"
            self._enqueue(db, binding, event_type, step_code, body, key)
        self._publish(task_id)

    def retry(self, task_id: str, run_id: str) -> None:
        """Create a new external binding because a remote terminal task cannot restart."""
        with self.storage.session_factory.begin() as db:
            previous = db.scalar(select(ExecutionBinding).where(ExecutionBinding.local_task_id == task_id).order_by(ExecutionBinding.retry_seq.desc()))
            sequence = (previous.retry_seq + 1) if previous else 0
            binding = ExecutionBinding(
                local_task_id=task_id, local_run_id=run_id,
                idempotency_key=f"task:{task_id}:retry:{sequence}", retry_seq=sequence,
                parent_binding_id=previous.id if previous else None, binding_status="PENDING",
            )
            db.add(binding); db.flush()
            task = db.get(WorkflowTask, task_id)
            self._enqueue(db, binding, "TASK_CREATE", None, {
                "input": {"taskId": task_id, "runId": run_id, "actorId": task.actor_id, "workflowType": task.workflow_type, "input": task.input_payload},
                "idempotentKey": binding.idempotency_key,
                "executionMode": self.settings.execution_task_execution_mode,
            }, f"task-create:{binding.idempotency_key}")
        self._publish(task_id)


def dispatch_execution_outbox(storage: Any, settings: Any, task_id: str) -> dict[str, Any]:
    """按 event_seq 顺序把一个任务的待发送 Outbox 行交付到真实 HTTP API。

    每次循环只锁定队首 PENDING 行：前一事件没有成功或确定失败之前，后续事件不会
    越过它发送。网络错误、429、5xx 进入指数退避；不可重试 4xx 或超过最大次数的
    行标记 FAILED，允许后续事实继续同步，并把完整响应摘要保存在 last_error 中。
    """
    # 禁用模式不应建立 Redis/HTTP 连接，也不会生成伪造的远端执行记录。
    if not settings.execution_task_enabled:
        return {"status": "DISABLED", "dispatched": 0}

    lock_client = redis.Redis.from_url(settings.redis_url, decode_responses=True)
    lock_key, owner = f"workflow:execution-sync:{task_id}", new_id()
    if not lock_client.set(lock_key, owner, nx=True, ex=120):
        lock_client.close(); return {"status": "LOCKED"}
    dispatched = 0
    client = ExecutionTaskClient(settings)
    try:
        while True:
            retry_error = None
            with storage.session_factory.begin() as db:
                row = db.scalar(select(TaskSystemOutbox).where(TaskSystemOutbox.local_task_id == task_id, TaskSystemOutbox.status == "PENDING").order_by(TaskSystemOutbox.event_seq).limit(1).with_for_update())
                if row is None:
                    break
                if row.next_attempt_at is not None and row.next_attempt_at > utcnow():
                    raise ExecutionTaskApiError("outbox head is waiting for its retry window")
                binding = db.get(ExecutionBinding, row.binding_id)
                try:
                    if row.event_type == "TASK_CREATE":
                        response = client.call("POST", "/api/v1/execution/tasks", row.payload)
                        binding.external_task_id = response.get("taskId", "")
                        binding.external_task_code = response.get("taskCode", "")
                        binding.binding_status = "BOUND"; binding.external_status = "Pending"
                    elif not binding.external_task_id:
                        raise ExecutionTaskApiError("binding is not BOUND", 409)
                    elif row.event_type == "TASK_STATUS":
                        client.call("PUT", f"/api/v1/execution/tasks/{binding.external_task_id}/status", row.payload)
                        binding.external_status = row.payload.get("status")
                    elif row.event_type == "STEP_START":
                        client.call("POST", f"/api/v1/execution/tasks/{binding.external_task_id}/steps/{row.step_code}/start", row.payload)
                    elif row.event_type == "STEP_STATUS":
                        client.call("PUT", f"/api/v1/execution/tasks/{binding.external_task_id}/steps/{row.step_code}/status", row.payload)
                    row.status = "SUCCEEDED"; dispatched += 1
                except ExecutionTaskApiError as exc:
                    row.attempts += 1
                    row.last_error = {"message": str(exc), "status_code": exc.status_code, "body": exc.body}
                    if not exc.retryable or row.attempts >= settings.execution_task_retry_max_attempts:
                        row.status = "FAILED"
                        if row.event_type == "TASK_CREATE": binding.binding_status = "FAILED"
                    else:
                        # 首次失败等待 base*2^0，随后指数退避，并受生产配置上限保护。
                        delay = min(
                            settings.execution_task_retry_max_seconds,
                            settings.execution_task_retry_base_seconds * (2 ** (row.attempts - 1)),
                        )
                        row.next_attempt_at = utcnow() + timedelta(seconds=delay)
                        retry_error = exc
            if retry_error is not None:
                raise retry_error
        return {"status": "OK", "dispatched": dispatched}
    finally:
        client.client.close()
        try:
            if lock_client.get(lock_key) == owner: lock_client.delete(lock_key)
        finally:
            lock_client.close()
