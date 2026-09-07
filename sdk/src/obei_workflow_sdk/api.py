from __future__ import annotations

from typing import Any

import asyncio
import json

from fastapi import APIRouter, Header, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field


class SubmitWorkflowRequest(BaseModel):
    workflow_type: str = Field(min_length=1, max_length=64)
    actor_id: str = Field(default="demo-user", min_length=1, max_length=128)
    input: dict[str, Any]


class DecisionRequest(BaseModel):
    decision_key: str
    decision: str
    feedback: str = ""
    actor_id: str = Field(min_length=1, max_length=128)
    artifact_id: str | None = None
    artifact_version: int | None = Field(default=None, ge=1)
    content_hash: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")


def create_workflow_router(runtime: Any, *, prefix: str = "/api/v1") -> APIRouter:
    """创建可挂载到宿主 FastAPI 的工作流路由；路由本身不持有全局状态。"""
    router = APIRouter(prefix=prefix)

    @router.post("/tasks", status_code=202)
    def submit(request: SubmitWorkflowRequest):
        """同步校验请求并返回 202；真正执行由 ARQ Worker 完成。"""
        try:
            return runtime.submit(request.workflow_type, request.input, request.actor_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    @router.get("/tasks/{task_id}")
    def get_task(task_id: str):
        task = runtime.storage.get_task(task_id)
        if task is None:
            raise HTTPException(status_code=404, detail="task not found")
        return task

    @router.get("/tasks/{task_id}/trace")
    def get_trace(task_id: str):
        task = runtime.storage.get_task(task_id)
        if task is None:
            raise HTTPException(status_code=404, detail="task not found")
        return runtime.storage.trace(task_id)

    @router.get("/artifacts/{artifact_id}")
    def get_artifact(artifact_id: str):
        artifact = runtime.storage.get_artifact(artifact_id)
        if artifact is None:
            raise HTTPException(status_code=404, detail="artifact not found")
        return artifact

    @router.get("/tasks/{task_id}/llm-invocations")
    def get_llm_invocations(task_id: str):
        if runtime.storage.get_task(task_id) is None:
            raise HTTPException(status_code=404, detail="task not found")
        return {"items": runtime.storage.llm_invocations(task_id)}

    @router.get("/tasks/{task_id}/dify-invocations")
    def get_dify_invocations(task_id: str):
        """Return stateful Dify application turns separately from direct model calls."""

        if runtime.storage.get_task(task_id) is None:
            raise HTTPException(status_code=404, detail="task not found")
        return {"items": runtime.storage.dify_invocations(task_id)}

    @router.post("/tasks/{task_id}/cancel", status_code=202)
    def cancel(task_id: str):
        try:
            return runtime.cancel(task_id)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail="task not found")

    @router.post("/tasks/{task_id}/decisions", status_code=202)
    def decide(task_id: str, request: DecisionRequest):
        try:
            return runtime.decide(task_id, request.decision_key, request.decision, request.feedback, request.actor_id, request.artifact_id, request.artifact_version, request.content_hash)
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @router.post("/tasks/{task_id}/retry", status_code=202)
    def retry(task_id: str):
        try:
            return runtime.retry(task_id)
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @router.get("/tasks/{task_id}/events")
    async def events(task_id: str, last_event_id: str = Header(default="", alias="Last-Event-ID")):
        """先回放数据库事件，再转入 Redis 实时流，并按序号消除两者重叠。"""
        if runtime.storage.get_task(task_id) is None:
            raise HTTPException(status_code=404, detail="task not found")
        bus = runtime.services.event_bus
        if bus is None:
            raise HTTPException(status_code=503, detail="Redis event bus is not configured")
        try:
            db_cursor = int(last_event_id or 0)
        except ValueError:
            db_cursor = 0
        stream_cursor = await bus.tail_id(task_id)

        async def stream():
            """持续产生 SSE frame；空闲时发送注释心跳防止代理关闭连接。"""
            nonlocal db_cursor, stream_cursor
            while True:
                rows = await asyncio.to_thread(runtime.storage.events_after, task_id, db_cursor)
                for row in rows:
                    db_cursor = row["event_seq"]
                    yield f"id: {db_cursor}\nevent: {row['event_type']}\ndata: {json.dumps(row, ensure_ascii=False)}\n\n"
                messages = await bus.read(task_id, stream_cursor, runtime.settings.event_stream_block_ms)
                if not messages:
                    yield ": heartbeat\n\n"
                for message_id, payload in messages:
                    stream_cursor = message_id
                    if "db_event_seq" not in payload:
                        event_type = payload.get("type", "workflow_stream")
                        yield f"event: {event_type}\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n"
                        continue
                    sequence = int(payload.get("db_event_seq") or 0)
                    if sequence <= db_cursor:
                        continue
                    db_cursor = sequence
                    event_type = payload.get("type", "workflow_event")
                    yield f"id: {db_cursor}\nevent: {event_type}\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n"

        return StreamingResponse(stream(), media_type="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

    @router.get("/workflow-types")
    def workflow_types():
        return {"workflow_types": runtime.registry.types()}

    @router.get("/dify-apps")
    def dify_apps():
        """List configured aliases and modes; never expose app URLs or API Keys."""
        registry = runtime.services.dify_registry
        return {"items": registry.descriptions() if registry is not None else []}

    @router.get("/workflow-types/{workflow_type}/registration-definition")
    def registration_definition(workflow_type: str):
        """生成可直接提交给远端注册接口的步骤 JSON 数组。

        该接口只读取代码中的工作流定义，不访问任务数据，也不会自动向远端注册或
        签发 Key。宿主如果不希望普通用户看到内部流程结构，应在挂载 Router 时由
        自己的认证/授权中间件保护该路径。
        """
        try:
            return runtime.registry.registration_definition(workflow_type)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    return router
