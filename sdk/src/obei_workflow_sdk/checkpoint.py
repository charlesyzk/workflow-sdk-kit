from __future__ import annotations

import asyncio
import base64
import json
from collections.abc import AsyncIterator, Iterator, Sequence
from typing import Any

from langgraph.checkpoint.base import WRITES_IDX_MAP, BaseCheckpointSaver, CheckpointTuple, get_checkpoint_id
from sqlalchemy import select

from .models import WorkflowCheckpoint, WorkflowRun


def _pack(serializer: Any, value: Any) -> dict[str, str]:
    """保留 LangGraph 类型标记并将二进制载荷编码为可存储 JSON。"""
    kind, data = serializer.dumps_typed(value)
    return {"kind": kind, "data": base64.b64encode(data).decode("ascii")}


def _unpack(serializer: Any, value: dict[str, str]) -> Any:
    """还原 ``_pack`` 生成的类型化 Checkpoint 值。"""
    return serializer.loads_typed((value["kind"], base64.b64decode(value["data"])))


class SQLAlchemyCheckpointSaver(BaseCheckpointSaver):
    """LangGraph checkpoint adapter backed by the SDK checkpoint table."""

    def __init__(self, storage: Any):
        super().__init__()
        self.storage = storage

    def _run(self, thread_id: str) -> WorkflowRun:
        """把 LangGraph thread_id 映射回 SDK 的持久 Run。"""
        with self.storage.session_factory() as db:
            run = db.scalar(select(WorkflowRun).where(WorkflowRun.external_run_key == thread_id))
            if run is None:
                raise ValueError(f"workflow run not found for thread: {thread_id}")
            db.expunge(run)
            return run

    def get_tuple(self, config: dict) -> CheckpointTuple | None:
        """读取指定 Checkpoint；未指定 id 时返回该 Run 最新状态。"""
        thread_id = config["configurable"]["thread_id"]
        checkpoint_id = get_checkpoint_id(config)
        run = self._run(thread_id)
        with self.storage.session_factory() as db:
            query = select(WorkflowCheckpoint).where(WorkflowCheckpoint.run_id == run.id)
            query = query.where(WorkflowCheckpoint.state_hash == checkpoint_id) if checkpoint_id else query.order_by(WorkflowCheckpoint.state_seq.desc()).limit(1)
            row = db.scalar(query)
            if row is None:
                return None
            envelope = json.loads(row.state_text)
            checkpoint = _unpack(self.serde, envelope["checkpoint"])
            metadata = _unpack(self.serde, envelope["metadata"])
            namespace = envelope.get("checkpoint_ns", "")
            result_config = {"configurable": {"thread_id": thread_id, "checkpoint_ns": namespace, "checkpoint_id": row.state_hash}}
            parent_id = envelope.get("parent_checkpoint_id")
            parent_config = {"configurable": {"thread_id": thread_id, "checkpoint_ns": namespace, "checkpoint_id": parent_id}} if parent_id else None
            writes = [(item["task_id"], item["channel"], _unpack(self.serde, item["value"])) for item in envelope.get("pending_writes", [])]
            return CheckpointTuple(result_config, checkpoint, metadata, parent_config, writes)

    def list(self, config: dict | None, *, filter: dict | None = None, before: dict | None = None, limit: int | None = None) -> Iterator[CheckpointTuple]:
        if config is None:
            return
        current = self.get_tuple(config)
        if current is not None:
            yield current

    def put(self, config: dict, checkpoint: dict, metadata: dict, new_versions: dict) -> dict:
        """幂等追加 Checkpoint，并在同事务递增 Run 的状态序号。"""
        thread_id = config["configurable"]["thread_id"]
        namespace = config["configurable"].get("checkpoint_ns", "")
        checkpoint_id = str(checkpoint["id"])
        run = self._run(thread_id)
        envelope = {
            "checkpoint": _pack(self.serde, checkpoint),
            "metadata": _pack(self.serde, metadata),
            "checkpoint_ns": namespace,
            "parent_checkpoint_id": get_checkpoint_id(config),
            "pending_writes": [],
        }
        with self.storage.session_factory.begin() as db:
            existing = db.scalar(select(WorkflowCheckpoint).where(
                WorkflowCheckpoint.run_id == run.id,
                WorkflowCheckpoint.state_hash == checkpoint_id,
            ))
            if existing is None:
                tracked = db.get(WorkflowRun, run.id)
                sequence = int(tracked.state_seq) + 1
                db.add(WorkflowCheckpoint(
                    run_id=run.id,
                    state_seq=sequence,
                    node_name=str(metadata.get("source", "langgraph")),
                    state_text=json.dumps(envelope, ensure_ascii=False, separators=(",", ":")),
                    state_format="langgraph-json",
                    state_hash=checkpoint_id,
                ))
                tracked.state_seq = sequence
        return {"configurable": {"thread_id": thread_id, "checkpoint_ns": namespace, "checkpoint_id": checkpoint_id}}

    def put_writes(self, config: dict, writes: Sequence[tuple[str, Any]], task_id: str, task_path: str = "") -> None:
        """保存 LangGraph pending writes，支持 interrupt 后的精确恢复。"""
        checkpoint_id = get_checkpoint_id(config)
        if not checkpoint_id:
            return
        run = self._run(config["configurable"]["thread_id"])
        with self.storage.session_factory.begin() as db:
            row = db.scalar(select(WorkflowCheckpoint).where(
                WorkflowCheckpoint.run_id == run.id,
                WorkflowCheckpoint.state_hash == checkpoint_id,
            ))
            if row is None:
                return
            envelope = json.loads(row.state_text)
            existing = {(str(item["task_id"]), int(item["write_index"])): item for item in envelope.get("pending_writes", [])}
            for position, (channel, value) in enumerate(writes):
                write_index = int(WRITES_IDX_MAP.get(channel, position))
                key = (task_id, write_index)
                if write_index >= 0 and key in existing:
                    continue
                existing[key] = {
                    "task_id": task_id,
                    "task_path": task_path,
                    "write_index": write_index,
                    "channel": channel,
                    "value": _pack(self.serde, value),
                }
            envelope["pending_writes"] = list(existing.values())
            row.state_text = json.dumps(envelope, ensure_ascii=False, separators=(",", ":"))

    def delete_thread(self, thread_id: str) -> None:
        """审计 Checkpoint 不允许通过 LangGraph 接口物理删除。"""
        raise NotImplementedError("workflow audit checkpoints are append-only")

    async def aget_tuple(self, config: dict) -> CheckpointTuple | None:
        return await asyncio.to_thread(self.get_tuple, config)

    async def alist(self, config: dict | None, *, filter: dict | None = None, before: dict | None = None, limit: int | None = None) -> AsyncIterator[CheckpointTuple]:
        items = await asyncio.to_thread(lambda: list(self.list(config, filter=filter, before=before, limit=limit)))
        for item in items:
            yield item

    async def aput(self, config: dict, checkpoint: dict, metadata: dict, new_versions: dict) -> dict:
        return await asyncio.to_thread(self.put, config, checkpoint, metadata, new_versions)

    async def aput_writes(self, config: dict, writes: Sequence[tuple[str, Any]], task_id: str, task_path: str = "") -> None:
        await asyncio.to_thread(self.put_writes, config, writes, task_id, task_path)
