from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import Any

from langgraph.types import Command

from .checkpoint import SQLAlchemyCheckpointSaver
from .contracts import WorkflowCancelled
from .graph import RuntimeServices, WorkflowGraph, WorkflowRegistry
from .run_lock import RedisWorkflowRunLock, WorkflowRunLocked, WorkflowRunLockLost


TERMINAL_STATUSES = {"SUCCEEDED", "FAILED", "CANCELLED", "BLOCKED"}


class WorkflowRuntime:
    """Generic execution shell shared by every registered workflow."""

    def __init__(self, storage: Any, registry: WorkflowRegistry, dispatcher: Any, task_system: Any, *, settings: Any = None, event_bus: Any = None, llm_registry: Any = None, dify_registry: Any = None):
        self.storage = storage
        self.registry = registry
        self.dispatcher = dispatcher
        self.task_system = task_system
        self.settings = settings
        self.services = RuntimeServices(storage=storage, task_system=task_system, event_bus=event_bus, llm_registry=llm_registry, dify_registry=dify_registry, settings=settings)
        dispatcher.bind(self.execute)

    def submit(self, workflow_type: str, payload: dict[str, Any], actor_id: str) -> dict[str, str]:
        """同步校验并创建 Task/Run，随后只投递消息，不在 HTTP 进程执行工作流。"""
        workflow = self.registry.get(workflow_type)
        validated = workflow.input_model.model_validate(payload).model_dump(mode="json")
        task_id, run_id = self.storage.create_task_run(workflow_type, actor_id, validated)
        try:
            self.task_system.task_status(task_id, "QUEUED", {"workflow_type": workflow_type})
        except Exception:
            pass
        self.dispatcher.dispatch(run_id)
        return {"task_id": task_id, "run_id": run_id, "status": "QUEUED"}

    def execute(self, run_id: str) -> dict[str, Any]:
        """同步嵌入入口；ARQ Worker 直接调用异步 ``_execute``。"""
        return asyncio.run(self._execute(run_id))

    async def _execute(self, run_id: str) -> dict[str, Any]:
        """在租约锁保护下推进、恢复或结束一个持久化 Run。"""
        task, run = self.storage.get_run_entities(run_id)
        if task.status in TERMINAL_STATUSES:
            return {"task_id": task.id, "run_id": run.id, "status": task.status}
        workflow = self.registry.get(task.workflow_type)

        builder = WorkflowGraph(workflow.state_model, self.services)
        workflow.build(builder)
        saver = SQLAlchemyCheckpointSaver(self.storage)
        graph = builder.compile(saver)
        config = {"configurable": {"thread_id": run.external_run_key}}
        initial_state = workflow.initial_state(task, run)
        lock = RedisWorkflowRunLock(self.settings.redis_url, run.id, self.settings.run_lock_ttl_seconds, self.settings.run_lock_renew_seconds) if self.settings else None

        @asynccontextmanager
        async def no_lock():
            yield None

        try:
          async with (lock.hold() if lock else no_lock()):
            fresh_task, _ = self.storage.get_run_entities(run_id)
            if fresh_task.status in TERMINAL_STATUSES:
                return {"task_id": fresh_task.id, "run_id": run.id, "status": fresh_task.status}
            self.storage.set_status(task.id, run.id, "RUNNING")
            self.services.event(task.id, run.id, "workflow_started", status="RUNNING", payload={"workflow_version": workflow.version})
            self.services.safe_task_status(task.id, "RUNNING", {"run_id": run.id})
            # Checkpoint 存在表示这是恢复执行；interrupt 则需要匹配 Decision 后 resume。
            checkpoint = await saver.aget_tuple(config)
            before = await graph.aget_state(config)
            interrupts = tuple(before.interrupts) if checkpoint else ()
            if interrupts:
                current = interrupts[0]
                decision = self.storage.get_decision(run.id, current.id)
                if decision is None:
                    self.storage.set_status(task.id, run.id, "WAITING_USER", (current.value or {}).get("node_name"))
                    return {"task_id": task.id, "run_id": run.id, "status": "WAITING_USER"}
                graph_input: Any = Command(resume={current.id: {"decision": decision.decision, "feedback": decision.feedback or ""}})
            else:
                graph_input = None if checkpoint else initial_state
            result = await graph.ainvoke(graph_input, config=config, durability="sync")
            if lock: lock.ensure_owned()
            final_snapshot = await graph.aget_state(config)
            final_interrupts = tuple(final_snapshot.interrupts)
            if final_interrupts:
                interrupt_item = final_interrupts[0]
                value = interrupt_item.value if isinstance(interrupt_item.value, dict) else {}
                node_name = value.get("node_name")
                self.storage.set_status(task.id, run.id, "WAITING_USER", node_name)
                self.services.event(task.id, run.id, "workflow_waiting_user", node_name=node_name, status="WAITING_USER", payload={"decision_key": interrupt_item.id, **value})
                self.services.safe_task_status(task.id, "WAITING_USER", {"decision_key": interrupt_item.id, **value})
                return {"task_id": task.id, "run_id": run.id, "status": "WAITING_USER", "decision_key": interrupt_item.id, **value}
            status = "CANCELLED" if self.storage.is_cancel_requested(task.id) else "SUCCEEDED"
            current_node = result.get("current_node") if isinstance(result, dict) else None
            self.storage.set_status(task.id, run.id, status, current_node)
            self.services.event(task.id, run.id, "workflow_finished", node_name=current_node, status=status, payload={})
            self.services.safe_task_status(task.id, status, {"run_id": run.id, "current_node": current_node})
            return {"task_id": task.id, "run_id": run.id, "status": status, "current_node": current_node}
        except (WorkflowRunLocked, WorkflowRunLockLost):
            raise
        except WorkflowCancelled:
            self.storage.set_status(task.id, run.id, "CANCELLED")
            self.services.event(task.id, run.id, "workflow_cancelled", status="CANCELLED", payload={})
            self.services.safe_task_status(task.id, "CANCELLED", {"run_id": run.id})
            return {"task_id": task.id, "run_id": run.id, "status": "CANCELLED"}
        except Exception as exc:
            current = self.storage.get_task(task.id)
            current_node = current.get("current_node") if current else None
            self.storage.set_status(task.id, run.id, "FAILED", current_node)
            self.services.event(task.id, run.id, "workflow_failed", node_name=current_node, status="FAILED", payload={"type": type(exc).__name__, "message": str(exc)})
            self.services.safe_task_status(task.id, "FAILED", {"run_id": run.id, "message": str(exc)})
            raise

    def decide(self, task_id: str, decision_key: str, decision: str, feedback: str, actor_id: str, artifact_id: str | None = None, artifact_version: int | None = None, content_hash: str | None = None) -> dict[str, Any]:
        """校验待决 interrupt 与产物版本，保存决定并异步恢复同一个 Run。"""
        task_data = self.storage.get_task(task_id)
        if not task_data or task_data["status"] != "WAITING_USER":
            raise ValueError("workflow is not waiting for a decision")
        task, run = self.storage.get_run_entities(task_data["run_id"])
        workflow = self.registry.get(task.workflow_type)
        builder = WorkflowGraph(workflow.state_model, self.services); workflow.build(builder)
        graph = builder.compile(SQLAlchemyCheckpointSaver(self.storage))
        snapshot = graph.get_state({"configurable": {"thread_id": run.external_run_key}})
        current = next((item for item in snapshot.interrupts if item.id == decision_key), None)
        if current is None:
            raise ValueError("decision is stale or does not match the pending interrupt")
        value = current.value if isinstance(current.value, dict) else {}
        if decision not in value.get("allowed_decisions", ["CONFIRM", "REJECT", "REVISE"]):
            raise ValueError("decision is not allowed")
        pending_artifact = value.get("artifact_ref")
        if pending_artifact:
            if artifact_id != pending_artifact:
                raise ValueError("decision artifact does not match the pending interrupt")
            artifact = self.storage.get_artifact(artifact_id)
            if artifact is None or artifact["version"] != artifact_version or artifact["content_hash"] != content_hash:
                raise ValueError("decision artifact version or hash is stale")
        self.storage.save_decision(task_id, run.id, decision_key, value.get("node_name", ""), decision, feedback, actor_id, pending_artifact)
        self.storage.set_status(task_id, run.id, "QUEUED", value.get("node_name"))
        self.dispatcher.dispatch(run.id)
        return {"task_id": task_id, "run_id": run.id, "status": "QUEUED"}

    def retry(self, task_id: str) -> dict[str, Any]:
        """从最近 Checkpoint 重试；远端终态任务通过新 Binding 记录新一轮执行。"""
        run_id = self.storage.queue_retry(task_id)
        retry_hook = getattr(self.task_system, "retry", None)
        if retry_hook is not None:
            retry_hook(task_id, run_id)
        self.dispatcher.dispatch(run_id)
        return {"task_id": task_id, "run_id": run_id, "status": "QUEUED"}

    def cancel(self, task_id: str) -> dict[str, Any]:
        """设置协作式取消标志；长节点需在外部调用或循环边界主动检查。"""
        if not self.storage.request_cancel(task_id):
            raise ValueError("task not found")
        task = self.storage.get_task(task_id)
        if task and task["status"] == "CANCELLED":
            self.services.event(task_id, task["run_id"], "workflow_cancelled", node_name=task.get("current_node"), status="CANCELLED", payload={"message": "cancelled by user"})
            self.services.safe_task_status(task_id, "CANCELLED", {"run_id": task["run_id"]})
        return {"task_id": task_id, "status": task["status"] if task else "CANCEL_REQUESTED"}
