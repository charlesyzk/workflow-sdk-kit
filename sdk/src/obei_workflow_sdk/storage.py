from __future__ import annotations

import hashlib
import json
from typing import Any

from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session, sessionmaker

from .contracts import Artifact
from .models import (
    Base,
    WorkflowArtifact,
    WorkflowCheckpoint,
    WorkflowDecision,
    WorkflowEvent,
    WorkflowNodeExecution,
    DifyConversation,
    DifyInvocation,
    ExecutionBinding,
    LLMInvocation,
    WorkflowRun,
    WorkflowTask,
    new_id,
    utcnow,
)


class SQLAlchemyWorkflowStorage:
    """Workflow, direct-model, Dify conversation and integration storage implementation."""

    def __init__(self, database_url: str, *, create_tables: bool = False):
        kwargs: dict[str, Any] = {"pool_pre_ping": True}
        if database_url.startswith("sqlite"):
            kwargs["connect_args"] = {"check_same_thread": False}
        self.engine = create_engine(database_url, **kwargs)
        self.session_factory = sessionmaker(self.engine, expire_on_commit=False, class_=Session)
        if create_tables:
            self.create_tables()

    def create_tables(self) -> None:
        """Create missing tables without exceeding the normal TiDB app-account grant.

        TiDB deployments commonly grant ``CREATE`` but deliberately withhold the separate
        ``INDEX`` privilege from the application account.  SQLAlchemy emits non-unique indexes as
        follow-up ``CREATE INDEX`` statements, which would leave a migration half-finished.  On
        MySQL/TiDB, temporarily omit those performance indexes while retaining primary keys and
        UNIQUE constraints (correctness requirements declared inside CREATE TABLE).  A DBA applies
        the documented ``TIDB_REQUIRED_INDEXES.sql`` afterward.  SQLite development databases keep
        automatic secondary-index creation for convenience.
        """

        if self.engine.dialect.name not in {"mysql", "mariadb"}:
            Base.metadata.create_all(self.engine)
            return
        detached: list[tuple[Any, Any]] = []
        for table in Base.metadata.tables.values():
            for index in list(table.indexes):
                # UNIQUE constraints remain attached to the table.  These Index objects are only
                # lookup accelerators and may safely be installed later by the migration account.
                table.indexes.remove(index)
                detached.append((table, index))
        try:
            Base.metadata.create_all(self.engine)
        finally:
            for table, index in detached:
                table.indexes.add(index)

    def create_task_run(self, workflow_type: str, actor_id: str, payload: dict[str, Any]) -> tuple[str, str]:
        """原子创建 Task/Run，再追加一条可供 SSE 回放的受理事件。

        ``input_payload`` 保存结构化原文，``input_text`` 则兼容既有 TiDB 表的
        NOT NULL 约束和全文检索/人工排障场景。输入模型有非空 text 字段时优先
        保存它，否则生成稳定、紧凑且保留中文的 JSON；因此任意工作流都不会再
        因缺少名为 text 的业务字段而违反数据库约束。
        """
        explicit_text = payload.get("text")
        input_text = (
            explicit_text
            if isinstance(explicit_text, str) and explicit_text
            else json.dumps(payload, ensure_ascii=False, default=str, separators=(",", ":"))
        )
        # Task 与 Run 必须在同一事务提交，避免队列里出现只有 task_id、却找不到
        # 可执行 Run 的半成品任务。受理事件在事务成功后追加，确保事件可被回放。
        with self.session_factory.begin() as db:
            task = WorkflowTask(
                actor_id=actor_id,
                workflow_type=workflow_type,
                input_text=input_text,
                input_payload=payload,
                status="QUEUED",
            )
            db.add(task)
            db.flush()
            run = WorkflowRun(
                task_id=task.id,
                external_run_key=new_id(),
                status="QUEUED",
            )
            db.add(run)
            db.flush()
            task_id, run_id = task.id, run.id
        self.append_event(task_id, run_id, "task_accepted", status="QUEUED", payload={"workflow_type": workflow_type})
        return task_id, run_id

    def get_task(self, task_id: str) -> dict[str, Any] | None:
        """返回 API 使用的任务快照以及该任务最新 Run。"""
        with self.session_factory() as db:
            task = db.get(WorkflowTask, task_id)
            if task is None:
                return None
            run = db.scalar(select(WorkflowRun).where(WorkflowRun.task_id == task_id).order_by(WorkflowRun.created_at.desc()))
            return {
                "task_id": task.id,
                "run_id": run.id if run else None,
                "workflow_type": task.workflow_type,
                "actor_id": task.actor_id,
                "input": task.input_payload,
                "status": task.status,
                "current_node": task.current_node,
                "final_artifact_id": task.final_artifact_id,
                "cancel_requested": task.cancel_requested,
                "created_at": task.created_at.isoformat(),
                "updated_at": task.updated_at.isoformat(),
            }

    def get_run_entities(self, run_id: str) -> tuple[WorkflowTask, WorkflowRun]:
        with self.session_factory() as db:
            run = db.get(WorkflowRun, run_id)
            if run is None:
                raise ValueError(f"workflow run not found: {run_id}")
            task = db.get(WorkflowTask, run.task_id)
            if task is None:
                raise ValueError(f"workflow task not found: {run.task_id}")
            db.expunge(run)
            db.expunge(task)
            return task, run

    def set_status(self, task_id: str, run_id: str, status: str, current_node: str | None = None) -> None:
        """同事务更新 Task 与 Run，避免对外状态出现分叉。"""
        with self.session_factory.begin() as db:
            task = db.get(WorkflowTask, task_id)
            run = db.get(WorkflowRun, run_id)
            if task is None or run is None:
                raise ValueError("task or run disappeared")
            task.status = run.status = status
            if current_node is not None:
                task.current_node = run.current_node = current_node

    def request_cancel(self, task_id: str) -> bool:
        with self.session_factory.begin() as db:
            task = db.get(WorkflowTask, task_id)
            if task is None:
                return False
            task.cancel_requested = True
            if task.status in {"QUEUED", "WAITING_USER"}:
                task.status = "CANCELLED"
                run = db.scalar(select(WorkflowRun).where(WorkflowRun.task_id == task_id).order_by(WorkflowRun.created_at.desc()))
                if run is not None:
                    run.status = "CANCELLED"
            return True

    def is_cancel_requested(self, task_id: str) -> bool:
        with self.session_factory() as db:
            return bool(db.scalar(select(WorkflowTask.cancel_requested).where(WorkflowTask.id == task_id)))

    def start_node(self, run_id: str, node_name: str, node_version: str = "1") -> tuple[str, int]:
        """为节点分配递增 attempt 并记录开始时间与幂等键。

        LangGraph 恢复执行时会重新调用被 interrupt 的节点函数；此时复用该节点
        上一段 WAITING_USER 执行记录（attempt 不变），避免人工门恢复被误判为
        一次新的回环轮次。
        """
        with self.session_factory.begin() as db:
            waiting = db.scalar(
                select(WorkflowNodeExecution).where(
                    WorkflowNodeExecution.run_id == run_id,
                    WorkflowNodeExecution.node_name == node_name,
                    WorkflowNodeExecution.status == "WAITING_USER",
                ).order_by(WorkflowNodeExecution.attempt.desc()).limit(1)
            )
            if waiting is not None:
                waiting.status = "RUNNING"
                waiting.finished_at = None
                return waiting.id, int(waiting.attempt)
            latest = db.scalar(
                select(func.max(WorkflowNodeExecution.attempt)).where(
                    WorkflowNodeExecution.run_id == run_id,
                    WorkflowNodeExecution.node_name == node_name,
                )
            ) or 0
            attempt = int(latest) + 1
            row = WorkflowNodeExecution(
                run_id=run_id,
                node_name=node_name,
                node_version=node_version,
                attempt=attempt,
                status="RUNNING",
                idempotency_key=f"node:{run_id}:{node_name}:{attempt}",
                input_refs=[],
                started_at=utcnow(),
            )
            db.add(row)
            db.flush()
            return row.id, attempt

    def finish_node(self, execution_id: str, *, artifact_id: str | None = None, error: Exception | None = None) -> None:
        """收口节点成功或失败事实；失败只保存可序列化的异常摘要。"""
        with self.session_factory.begin() as db:
            row = db.get(WorkflowNodeExecution, execution_id)
            if row is None:
                return
            row.finished_at = utcnow()
            if error is None:
                row.status = "SUCCEEDED"
                row.output_artifact_id = artifact_id
            else:
                row.status = "FAILED"
                row.error_json = {"type": type(error).__name__, "message": str(error)}

    def wait_node(self, execution_id: str) -> None:
        with self.session_factory.begin() as db:
            row = db.get(WorkflowNodeExecution, execution_id)
            if row is not None:
                row.status = "WAITING_USER"
                row.finished_at = utcnow()

    def save_decision(self, task_id: str, run_id: str, decision_key: str, node_name: str, decision: str, feedback: str, actor_id: str, artifact_id: str | None = None) -> str:
        """幂等保存人工决定，并冻结当时审批产物的版本与内容 hash。"""
        with self.session_factory.begin() as db:
            if db.scalar(select(WorkflowDecision).where(WorkflowDecision.decision_key == decision_key)):
                raise ValueError("decision already handled")
            artifact = db.get(WorkflowArtifact, artifact_id) if artifact_id else None
            if artifact_id and (artifact is None or artifact.task_id != task_id):
                raise ValueError("decision artifact does not belong to the task")
            row = WorkflowDecision(
                task_id=task_id, run_id=run_id, decision_key=decision_key,
                node_name=node_name, decision=decision, feedback=feedback,
                actor_id=actor_id, artifact_id=artifact_id,
                artifact_version=artifact.version if artifact else None,
                content_hash=artifact.content_hash if artifact else None,
                decided_at=utcnow(),
            )
            db.add(row)
            db.flush()
            return row.id

    def get_decision(self, run_id: str, decision_key: str) -> WorkflowDecision | None:
        with self.session_factory() as db:
            row = db.scalar(select(WorkflowDecision).where(WorkflowDecision.run_id == run_id, WorkflowDecision.decision_key == decision_key))
            if row is not None:
                db.expunge(row)
            return row

    def queue_retry(self, task_id: str) -> str:
        """只有 FAILED 任务可重新排队，并沿用原 Run/Checkpoint。"""
        with self.session_factory.begin() as db:
            task = db.get(WorkflowTask, task_id)
            if task is None:
                raise ValueError("task not found")
            run = db.scalar(select(WorkflowRun).where(WorkflowRun.task_id == task_id).order_by(WorkflowRun.created_at.desc()))
            if run is None or task.status != "FAILED":
                raise ValueError("task is not retryable")
            task.status = run.status = "QUEUED"
            task.cancel_requested = False
            return run.id

    def events_after(self, task_id: str, sequence: int, limit: int = 1000) -> list[dict[str, Any]]:
        with self.session_factory() as db:
            rows = db.scalars(select(WorkflowEvent).where(WorkflowEvent.task_id == task_id, WorkflowEvent.event_seq > sequence).order_by(WorkflowEvent.event_seq).limit(limit)).all()
            return [{"event_seq": r.event_seq, "event_type": r.event_type, "node_name": r.node_name, "status": r.status, "artifact_id": r.artifact_id, "payload": r.payload, "occurred_at": r.occurred_at.isoformat()} for r in rows]

    def start_llm_invocation(self, task_id: str, run_id: str, node_name: str, provider: str, model: str | None, stream: bool, request: dict[str, Any], prompt_version: str | None = None) -> str:
        """在 HTTP 调用前保存完整模型请求，并关联当前 NodeExecution。"""
        started = utcnow()
        with self.session_factory.begin() as db:
            sequence = int(db.scalar(select(func.count(LLMInvocation.id)).where(LLMInvocation.run_id == run_id, LLMInvocation.node_name == node_name)) or 0) + 1
            row = LLMInvocation(
                task_id=task_id, run_id=run_id, node_name=node_name,
                invocation_key=f"llm:{run_id}:{node_name}:{sequence}", provider=provider,
                model=model, prompt_version=prompt_version, stream=stream, status="RUNNING",
                request_text=json.dumps(request, ensure_ascii=False, default=str), started_at=started,
            )
            execution = db.scalar(select(WorkflowNodeExecution).where(
                WorkflowNodeExecution.run_id == run_id,
                WorkflowNodeExecution.node_name == node_name,
                WorkflowNodeExecution.status == "RUNNING",
            ).order_by(WorkflowNodeExecution.attempt.desc()).limit(1))
            if execution is not None:
                execution.model_name = f"{provider}:{model}" if model else provider
                execution.prompt_version = prompt_version
            db.add(row); db.flush()
            return row.id

    def finish_llm_invocation(self, invocation_id: str, *, response: str | None = None, reasoning: str | None = None, usage: dict[str, Any] | None = None, provider_request_id: str | None = None, error: Exception | None = None) -> None:
        """无论调用成功或失败都完成审计记录并计算实际耗时。"""
        finished = utcnow()
        with self.session_factory.begin() as db:
            row = db.get(LLMInvocation, invocation_id)
            if row is None:
                return
            row.finished_at = finished
            row.duration_ms = int((finished - row.started_at).total_seconds() * 1000)
            if error is not None:
                row.status = "FAILED"
                row.error_json = {"type": type(error).__name__, "message": str(error)}
            else:
                row.status = "SUCCEEDED"
                row.response_text = response
                row.reasoning_text = reasoning
                row.usage = usage
                row.provider_request_id = provider_request_id

    def llm_invocations(self, task_id: str) -> list[dict[str, Any]]:
        with self.session_factory() as db:
            rows = db.scalars(select(LLMInvocation).where(LLMInvocation.task_id == task_id).order_by(LLMInvocation.started_at)).all()
            return [{
                "invocation_id": row.id, "node_name": row.node_name,
                "provider": row.provider, "model": row.model, "prompt_version": row.prompt_version, "stream": row.stream,
                "status": row.status, "request": json.loads(row.request_text),
                "response": row.response_text, "reasoning": row.reasoning_text,
                "usage": row.usage, "provider_request_id": row.provider_request_id,
                "duration_ms": row.duration_ms, "error": row.error_json,
            } for row in rows]

    def get_dify_conversation(
        self, task_id: str, app_name: str, conversation_key: str
    ) -> dict[str, Any] | None:
        """Resolve the durable Dify conversation used by one logical workflow thread."""

        with self.session_factory() as db:
            row = db.scalar(
                select(DifyConversation).where(
                    DifyConversation.task_id == task_id,
                    DifyConversation.app_name == app_name,
                    DifyConversation.conversation_key == conversation_key,
                )
            )
            if row is None:
                return None
            return {
                "conversation_id": row.external_conversation_id,
                "last_message_id": row.last_message_id,
                "status": row.status,
            }

    def start_dify_invocation(
        self,
        task_id: str,
        run_id: str,
        node_name: str,
        app_name: str,
        conversation_key: str | None,
        conversation_id: str | None,
        request: dict[str, Any],
        prompt_version: str | None = None,
    ) -> str:
        """Persist a Dify application turn before the external streaming request starts."""

        started = utcnow()
        with self.session_factory.begin() as db:
            sequence = int(
                db.scalar(
                    select(func.count(DifyInvocation.id)).where(
                        DifyInvocation.run_id == run_id,
                        DifyInvocation.node_name == node_name,
                    )
                )
                or 0
            ) + 1
            row = DifyInvocation(
                task_id=task_id,
                run_id=run_id,
                node_name=node_name,
                invocation_key=f"dify:{run_id}:{node_name}:{sequence}",
                conversation_key=conversation_key or "__stateless__",
                external_conversation_id=conversation_id,
                app_name=app_name,
                prompt_version=prompt_version,
                stream=True,
                status="RUNNING",
                request_text=json.dumps(request, ensure_ascii=False, default=str),
                started_at=started,
            )
            execution = db.scalar(
                select(WorkflowNodeExecution)
                .where(
                    WorkflowNodeExecution.run_id == run_id,
                    WorkflowNodeExecution.node_name == node_name,
                    WorkflowNodeExecution.status == "RUNNING",
                )
                .order_by(WorkflowNodeExecution.attempt.desc())
                .limit(1)
            )
            if execution is not None:
                execution.model_name = f"dify:{app_name}"
                execution.prompt_version = prompt_version
            db.add(row)
            db.flush()
            return row.id

    def finish_dify_invocation(
        self,
        invocation_id: str,
        *,
        task_id: str,
        app_name: str,
        conversation_key: str | None,
        conversation_id: str | None = None,
        message_id: str | None = None,
        response: str | None = None,
        reasoning: str | None = None,
        usage: dict[str, Any] | None = None,
        error: Exception | None = None,
    ) -> None:
        """Complete audit and atomically create/update the Dify conversation mapping."""

        finished = utcnow()
        with self.session_factory.begin() as db:
            row = db.get(DifyInvocation, invocation_id)
            if row is None:
                return
            row.finished_at = finished
            row.duration_ms = int((finished - row.started_at).total_seconds() * 1000)
            if error is not None:
                row.status = "FAILED"
                row.error_json = {"type": type(error).__name__, "message": str(error)}
                return
            row.status = "SUCCEEDED"
            row.external_conversation_id = conversation_id
            row.message_id = message_id
            row.response_text = response
            row.reasoning_text = reasoning
            row.usage = usage
            if conversation_key is None:
                # Keep the external conversation_id on the invocation for audit, but do not create
                # an auto-reusable mapping when the caller selected stateless mode.
                return
            if not conversation_id:
                raise ValueError("history-enabled Dify chat requires conversation_id")
            conversation = db.scalar(
                select(DifyConversation).where(
                    DifyConversation.task_id == task_id,
                    DifyConversation.app_name == app_name,
                    DifyConversation.conversation_key == conversation_key,
                )
            )
            if conversation is None:
                conversation = DifyConversation(
                    task_id=task_id,
                    app_name=app_name,
                    conversation_key=conversation_key,
                    external_conversation_id=conversation_id,
                    last_message_id=message_id,
                    status="ACTIVE",
                )
                db.add(conversation)
            elif conversation.external_conversation_id != conversation_id:
                # A logical conversation must never silently jump to another Dify history.  This
                # usually indicates concurrent first turns using the same conversation_key.
                raise RuntimeError(
                    "Dify conversation_id changed for an existing conversation_key; "
                    "use distinct keys for parallel branches"
                )
            else:
                conversation.last_message_id = message_id
                conversation.status = "ACTIVE"

    def dify_invocations(self, task_id: str) -> list[dict[str, Any]]:
        """Return Dify audit rows without mixing them into stateless model invocations."""

        with self.session_factory() as db:
            rows = db.scalars(
                select(DifyInvocation)
                .where(DifyInvocation.task_id == task_id)
                .order_by(DifyInvocation.started_at)
            ).all()
            return [
                {
                    "invocation_id": row.id,
                    "node_name": row.node_name,
                    "app_name": row.app_name,
                    "conversation_key": (
                        None if row.conversation_key == "__stateless__" else row.conversation_key
                    ),
                    "conversation_id": row.external_conversation_id,
                    "message_id": row.message_id,
                    "prompt_version": row.prompt_version,
                    "stream": row.stream,
                    "status": row.status,
                    "request": json.loads(row.request_text),
                    "response": row.response_text,
                    "reasoning": row.reasoning_text,
                    "usage": row.usage,
                    "duration_ms": row.duration_ms,
                    "error": row.error_json,
                }
                for row in rows
            ]

    @staticmethod
    def _serialize_content(content: Any, content_type: str) -> str:
        if isinstance(content, str):
            return content
        return json.dumps(content, ensure_ascii=False, default=str, separators=(",", ":"))

    def persist_artifact(self, task_id: str, run_id: str, artifact: Artifact) -> str:
        """按任务和产物类型分配版本，计算 SHA-256，并可设置最终产物引用。"""
        text = self._serialize_content(artifact.content, artifact.content_type) if artifact.content is not None else None
        if text is None and not artifact.url:
            raise ValueError("artifact content and url cannot both be empty")
        hash_source = (text or artifact.url or "").encode("utf-8")
        with self.session_factory.begin() as db:
            version = int(db.scalar(select(func.max(WorkflowArtifact.version)).where(
                WorkflowArtifact.task_id == task_id,
                WorkflowArtifact.artifact_type == artifact.type,
            )) or 0) + 1
            row = WorkflowArtifact(
                task_id=task_id,
                run_id=run_id,
                artifact_type=artifact.type,
                version=version,
                content_type=artifact.content_type,
                content_text=text,
                url=artifact.url,
                content_hash=hashlib.sha256(hash_source).hexdigest(),
                size_bytes=len(hash_source),
            )
            db.add(row)
            db.flush()
            artifact_id = row.id
            if artifact.final:
                task = db.get(WorkflowTask, task_id)
                if task is not None:
                    task.final_artifact_id = artifact_id
        return artifact_id

    def get_artifact(self, artifact_id: str) -> dict[str, Any] | None:
        with self.session_factory() as db:
            row = db.get(WorkflowArtifact, artifact_id)
            if row is None:
                return None
            content: Any = row.content_text
            if row.content_type == "application/json" and row.content_text:
                try:
                    content = json.loads(row.content_text)
                except json.JSONDecodeError:
                    pass
            return {
                "artifact_id": row.id,
                "artifact_type": row.artifact_type,
                "version": row.version,
                "content_type": row.content_type,
                "content_hash": row.content_hash,
                "content": content,
                "url": row.url,
            }

    def append_event(
        self,
        task_id: str,
        run_id: str,
        event_type: str,
        *,
        node_name: str | None = None,
        status: str | None = None,
        artifact_id: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> int:
        """在任务行锁保护下分配严格递增事件序号并写入持久事件表。"""
        with self.session_factory.begin() as db:
            # MySQL/TiDB use the task row as the per-task sequence allocation lock.
            # SQLite serializes the write transaction for the starter project.
            db.scalar(select(WorkflowTask.id).where(WorkflowTask.id == task_id).with_for_update())
            sequence = int(db.scalar(select(func.max(WorkflowEvent.event_seq)).where(WorkflowEvent.task_id == task_id)) or 0) + 1
            db.add(WorkflowEvent(
                task_id=task_id,
                run_id=run_id,
                event_seq=sequence,
                event_type=event_type,
                node_name=node_name,
                status=status,
                artifact_id=artifact_id,
                payload=payload or {},
                occurred_at=utcnow(),
            ))
            return sequence

    def trace(self, task_id: str) -> dict[str, Any]:
        """聚合最新 Run 的节点执行和任务全量事件，供排障与审计查询。"""
        with self.session_factory() as db:
            run = db.scalar(select(WorkflowRun).where(WorkflowRun.task_id == task_id).order_by(WorkflowRun.created_at.desc()))
            if run is None:
                return {"events": [], "nodes": []}
            nodes = db.scalars(select(WorkflowNodeExecution).where(WorkflowNodeExecution.run_id == run.id).order_by(WorkflowNodeExecution.started_at)).all()
            events = db.scalars(select(WorkflowEvent).where(WorkflowEvent.task_id == task_id).order_by(WorkflowEvent.event_seq)).all()
            return {
                "nodes": [{
                    "node_name": n.node_name,
                    "attempt": n.attempt,
                    "status": n.status,
                    "started_at": n.started_at.isoformat() if n.started_at else None,
                    "finished_at": n.finished_at.isoformat() if n.finished_at else None,
                    "output_artifact_id": n.output_artifact_id,
                    "model_name": n.model_name,
                    "prompt_version": n.prompt_version,
                    "error": n.error_json,
                } for n in nodes],
                "events": [{
                    "event_seq": e.event_seq,
                    "event_type": e.event_type,
                    "node_name": e.node_name,
                    "status": e.status,
                    "artifact_id": e.artifact_id,
                    "payload": e.payload,
                    "occurred_at": e.occurred_at.isoformat(),
                } for e in events],
            }

    def previous_successful_node(self, run_id: str, exclude_node: str) -> tuple[str, int] | None:
        """返回本 run 内上一个已成功节点执行（排除自身），用于动态步骤 dependsOn。"""
        with self.session_factory() as db:
            row = db.scalar(select(WorkflowNodeExecution).where(
                WorkflowNodeExecution.run_id == run_id,
                WorkflowNodeExecution.status == "SUCCEEDED",
                WorkflowNodeExecution.node_name != exclude_node,
            ).order_by(WorkflowNodeExecution.finished_at.desc()).limit(1))
            if row is None:
                return None
            return row.node_name, int(row.attempt)

    def successful_node_codes(self, run_id: str) -> set[str]:
        """返回该 Run 已成功的节点 code 集合，用于对账远端分支步骤的 Skipped 状态。"""
        with self.session_factory() as db:
            return set(db.scalars(select(WorkflowNodeExecution.node_name).where(
                WorkflowNodeExecution.run_id == run_id,
                WorkflowNodeExecution.status == "SUCCEEDED",
            )).all())

    def execution_bindings(self, task_id: str) -> list[dict[str, Any]]:
        """Return the complete local-run to remote-task retry history."""
        with self.session_factory() as db:
            rows = db.scalars(select(ExecutionBinding).where(
                ExecutionBinding.local_task_id == task_id
            ).order_by(ExecutionBinding.retry_seq)).all()
            return [{
                "binding_id": row.id,
                "local_task_id": row.local_task_id,
                "local_run_id": row.local_run_id,
                "retry_seq": row.retry_seq,
                "parent_binding_id": row.parent_binding_id,
                "external_task_id": row.external_task_id,
                "external_task_code": row.external_task_code,
                "binding_status": row.binding_status,
                "external_status": row.external_status,
                "last_error": row.last_error,
            } for row in rows]


__all__ = [
    "SQLAlchemyWorkflowStorage",
    "WorkflowArtifact",
    "WorkflowCheckpoint",
    "WorkflowDecision",
    "WorkflowEvent",
    "WorkflowNodeExecution",
    "WorkflowRun",
    "WorkflowTask",
]
