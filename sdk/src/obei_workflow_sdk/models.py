from __future__ import annotations

from datetime import datetime, timezone

import ulid
from sqlalchemy import BigInteger, Boolean, DateTime, Index, Integer, JSON, String, Text, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.dialects.mysql import LONGTEXT, MEDIUMTEXT


def new_id() -> str:
    return str(ulid.new())


def utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


class Base(DeclarativeBase):
    pass


class IdMixin:
    id: Mapped[str] = mapped_column(String(26), primary_key=True, default=new_id)


class TimestampMixin:
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, onupdate=utcnow, nullable=False)


class WorkflowTask(Base, IdMixin, TimestampMixin):
    __tablename__ = "obei_workshop_task"
    __table_args__ = (Index("ix_sdk_task_actor", "actor_id"), Index("ix_sdk_task_status", "status"))

    actor_id: Mapped[str] = mapped_column(String(128), nullable=False)
    workflow_type: Mapped[str] = mapped_column(String(64), nullable=False)
    input_text: Mapped[str] = mapped_column(Text().with_variant(MEDIUMTEXT(), "mysql"), nullable=False)
    input_payload: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    status: Mapped[str] = mapped_column(String(32), default="QUEUED", nullable=False)
    current_node: Mapped[str | None] = mapped_column(String(128))
    final_artifact_id: Mapped[str | None] = mapped_column(String(26))
    cancel_requested: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)


class WorkflowRun(Base, IdMixin, TimestampMixin):
    __tablename__ = "obei_workshop_task_run"
    __table_args__ = (UniqueConstraint("external_run_key", name="uq_sdk_run_external_key"),)

    task_id: Mapped[str] = mapped_column(String(26), nullable=False, index=True)
    external_run_key: Mapped[str] = mapped_column(String(128), nullable=False)
    status: Mapped[str] = mapped_column(String(32), default="QUEUED", nullable=False)
    current_node: Mapped[str | None] = mapped_column(String(128))
    state_seq: Mapped[int] = mapped_column(BigInteger, default=0, nullable=False)
    version: Mapped[int] = mapped_column(BigInteger, default=0, nullable=False)


class WorkflowCheckpoint(Base, IdMixin, TimestampMixin):
    __tablename__ = "obei_workshop_task_checkpoint"
    __table_args__ = (
        UniqueConstraint("run_id", "state_seq", name="uq_sdk_checkpoint_seq"),
        UniqueConstraint("run_id", "state_hash", name="uq_sdk_checkpoint_hash"),
    )

    run_id: Mapped[str] = mapped_column(String(26), nullable=False, index=True)
    state_seq: Mapped[int] = mapped_column(BigInteger, nullable=False)
    node_name: Mapped[str] = mapped_column(String(128), nullable=False)
    state_text: Mapped[str] = mapped_column(Text().with_variant(LONGTEXT(), "mysql"), nullable=False)
    state_format: Mapped[str] = mapped_column(String(32), default="json", nullable=False)
    state_hash: Mapped[str] = mapped_column(String(64), nullable=False)


class WorkflowNodeExecution(Base, IdMixin, TimestampMixin):
    __tablename__ = "obei_workshop_task_node_execution"
    __table_args__ = (
        UniqueConstraint("idempotency_key", name="uq_sdk_node_execution_key"),
        Index("ix_sdk_node_execution_run_node", "run_id", "node_name"),
    )

    run_id: Mapped[str] = mapped_column(String(26), nullable=False)
    node_name: Mapped[str] = mapped_column(String(128), nullable=False)
    node_version: Mapped[str] = mapped_column(String(64), default="1", nullable=False)
    attempt: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(256), nullable=False)
    input_refs: Mapped[list] = mapped_column(JSON, default=list, nullable=False)
    output_artifact_id: Mapped[str | None] = mapped_column(String(26))
    prompt_version: Mapped[str | None] = mapped_column(String(64))
    model_name: Mapped[str | None] = mapped_column(String(256))
    error_json: Mapped[dict | None] = mapped_column(JSON)
    started_at: Mapped[datetime | None] = mapped_column(DateTime)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime)


class WorkflowArtifact(Base, IdMixin, TimestampMixin):
    __tablename__ = "obei_workshop_task_artifact"
    __table_args__ = (
        UniqueConstraint("task_id", "artifact_type", "version", name="uq_sdk_artifact_version"),
        Index("ix_sdk_artifact_task_type", "task_id", "artifact_type"),
    )

    task_id: Mapped[str] = mapped_column(String(26), nullable=False)
    run_id: Mapped[str] = mapped_column(String(26), nullable=False)
    artifact_type: Mapped[str] = mapped_column(String(64), nullable=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    content_type: Mapped[str] = mapped_column(String(128), nullable=False)
    content_text: Mapped[str | None] = mapped_column(Text().with_variant(LONGTEXT(), "mysql"))
    url: Mapped[str | None] = mapped_column(String(2048))
    content_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)


class WorkflowEvent(Base, IdMixin):
    __tablename__ = "obei_workshop_task_event"
    __table_args__ = (
        UniqueConstraint("task_id", "event_seq", name="uq_sdk_event_seq"),
        Index("ix_sdk_event_task_seq", "task_id", "event_seq"),
    )

    task_id: Mapped[str] = mapped_column(String(26), nullable=False)
    run_id: Mapped[str] = mapped_column(String(26), nullable=False)
    event_seq: Mapped[int] = mapped_column(BigInteger, nullable=False)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    node_name: Mapped[str | None] = mapped_column(String(128))
    status: Mapped[str | None] = mapped_column(String(32))
    artifact_id: Mapped[str | None] = mapped_column(String(26))
    payload: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)


class WorkflowDecision(Base, IdMixin, TimestampMixin):
    __tablename__ = "obei_workshop_task_decision"
    __table_args__ = (UniqueConstraint("decision_key", name="uq_sdk_decision_key"),)

    task_id: Mapped[str] = mapped_column(String(26), nullable=False)
    run_id: Mapped[str] = mapped_column(String(26), nullable=False)
    node_name: Mapped[str] = mapped_column(String(128), nullable=False)
    decision_key: Mapped[str] = mapped_column(String(128), nullable=False)
    artifact_id: Mapped[str | None] = mapped_column(String(26))
    artifact_version: Mapped[int | None] = mapped_column(Integer)
    content_hash: Mapped[str | None] = mapped_column(String(64))
    decision: Mapped[str] = mapped_column(String(32), nullable=False)
    feedback: Mapped[str | None] = mapped_column(Text().with_variant(LONGTEXT(), "mysql"))
    actor_id: Mapped[str] = mapped_column(String(128), nullable=False)
    decided_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)


class ExecutionBinding(Base, IdMixin, TimestampMixin):
    """Technical table binding a local task to the real execution-task service."""

    __tablename__ = "obei_workshop_execution_binding"
    __table_args__ = (
        UniqueConstraint("local_task_id", "retry_seq", name="uq_sdk_binding_retry"),
        UniqueConstraint("external_task_id", name="uq_sdk_binding_external"),
        UniqueConstraint("idempotency_key", name="uq_sdk_binding_idempotency"),
    )
    local_task_id: Mapped[str] = mapped_column(String(26), nullable=False, index=True)
    local_run_id: Mapped[str] = mapped_column(String(26), nullable=False)
    external_task_id: Mapped[str | None] = mapped_column(String(128))
    external_task_code: Mapped[str | None] = mapped_column(String(128))
    idempotency_key: Mapped[str] = mapped_column(String(256), nullable=False)
    retry_seq: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    parent_binding_id: Mapped[str | None] = mapped_column(String(26))
    binding_status: Mapped[str] = mapped_column(String(32), default="PENDING", nullable=False)
    external_status: Mapped[str | None] = mapped_column(String(32))
    last_error: Mapped[dict | None] = mapped_column(JSON)


class TaskSystemOutbox(Base, IdMixin, TimestampMixin):
    """Durable ordered Outbox; external HTTP is never performed inside a node transaction."""

    __tablename__ = "obei_workshop_task_system_outbox"
    __table_args__ = (
        UniqueConstraint("idempotency_key", name="uq_sdk_task_outbox_idempotency"),
        Index("ix_sdk_task_outbox_task_seq", "local_task_id", "event_seq"),
    )
    local_task_id: Mapped[str] = mapped_column(String(26), nullable=False)
    local_run_id: Mapped[str] = mapped_column(String(26), nullable=False)
    binding_id: Mapped[str] = mapped_column(String(26), nullable=False)
    event_seq: Mapped[int] = mapped_column(BigInteger, nullable=False)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    step_code: Mapped[str | None] = mapped_column(String(128))
    payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(256), nullable=False)
    status: Mapped[str] = mapped_column(String(32), default="PENDING", nullable=False)
    attempts: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    next_attempt_at: Mapped[datetime | None] = mapped_column(DateTime)
    last_error: Mapped[dict | None] = mapped_column(JSON)


class LLMInvocation(Base, IdMixin, TimestampMixin):
    """Complete audited model invocation; streaming chunks remain transient events."""

    __tablename__ = "obei_workshop_llm_invocation"
    __table_args__ = (
        UniqueConstraint("invocation_key", name="uq_sdk_llm_invocation_key"),
        Index("ix_sdk_llm_invocation_run_node", "run_id", "node_name"),
    )
    task_id: Mapped[str] = mapped_column(String(26), nullable=False)
    run_id: Mapped[str] = mapped_column(String(26), nullable=False)
    node_name: Mapped[str] = mapped_column(String(128), nullable=False)
    invocation_key: Mapped[str] = mapped_column(String(256), nullable=False)
    provider: Mapped[str] = mapped_column(String(32), nullable=False)
    model: Mapped[str | None] = mapped_column(String(256))
    prompt_version: Mapped[str | None] = mapped_column(String(64))
    stream: Mapped[bool] = mapped_column(Boolean, nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    request_text: Mapped[str] = mapped_column(Text().with_variant(LONGTEXT(), "mysql"), nullable=False)
    response_text: Mapped[str | None] = mapped_column(Text().with_variant(LONGTEXT(), "mysql"))
    reasoning_text: Mapped[str | None] = mapped_column(Text().with_variant(LONGTEXT(), "mysql"))
    usage: Mapped[dict | None] = mapped_column(JSON)
    provider_request_id: Mapped[str | None] = mapped_column(String(256))
    error_json: Mapped[dict | None] = mapped_column(JSON)
    started_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime)
    duration_ms: Mapped[int | None] = mapped_column(BigInteger)


class DifyConversation(Base, IdMixin, TimestampMixin):
    """Task-scoped mapping to the conversation history owned by a Dify Chat App."""

    __tablename__ = "obei_workshop_dify_conversation"
    __table_args__ = (
        UniqueConstraint("task_id", "app_name", "conversation_key", name="uq_sdk_dify_conversation"),
        Index("ix_sdk_dify_conversation_task", "task_id"),
    )
    task_id: Mapped[str] = mapped_column(String(26), nullable=False)
    app_name: Mapped[str] = mapped_column(String(128), nullable=False)
    conversation_key: Mapped[str] = mapped_column(String(128), nullable=False)
    external_conversation_id: Mapped[str] = mapped_column(String(256), nullable=False)
    last_message_id: Mapped[str | None] = mapped_column(String(256))
    status: Mapped[str] = mapped_column(String(32), default="ACTIVE", nullable=False)


class DifyInvocation(Base, IdMixin, TimestampMixin):
    """Audited Dify application turn, separate from stateless model invocations."""

    __tablename__ = "obei_workshop_dify_invocation"
    __table_args__ = (
        UniqueConstraint("invocation_key", name="uq_sdk_dify_invocation_key"),
        Index("ix_sdk_dify_invocation_run_node", "run_id", "node_name"),
    )
    task_id: Mapped[str] = mapped_column(String(26), nullable=False)
    run_id: Mapped[str] = mapped_column(String(26), nullable=False)
    node_name: Mapped[str] = mapped_column(String(128), nullable=False)
    invocation_key: Mapped[str] = mapped_column(String(256), nullable=False)
    conversation_key: Mapped[str] = mapped_column(String(128), nullable=False)
    external_conversation_id: Mapped[str | None] = mapped_column(String(256))
    message_id: Mapped[str | None] = mapped_column(String(256))
    app_name: Mapped[str] = mapped_column(String(128), nullable=False)
    prompt_version: Mapped[str | None] = mapped_column(String(64))
    stream: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    request_text: Mapped[str] = mapped_column(Text().with_variant(LONGTEXT(), "mysql"), nullable=False)
    response_text: Mapped[str | None] = mapped_column(Text().with_variant(LONGTEXT(), "mysql"))
    reasoning_text: Mapped[str | None] = mapped_column(Text().with_variant(LONGTEXT(), "mysql"))
    usage: Mapped[dict | None] = mapped_column(JSON)
    error_json: Mapped[dict | None] = mapped_column(JSON)
    started_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime)
    duration_ms: Mapped[int | None] = mapped_column(BigInteger)
