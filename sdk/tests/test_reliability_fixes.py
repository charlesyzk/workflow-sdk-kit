import asyncio
import json
from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy import select

from obei_workflow_sdk import (
    LLMRequest,
    LLMResponse,
    LLMRunHandle,
    RunStreamAdapter,
    SQLAlchemyWorkflowStorage,
    create_workflow_router,
)
from obei_workflow_sdk.dify import DifyAppClient, DifyAppConfig, DifyRequest
from obei_workflow_sdk.execution_task import ExecutionTaskAdapter, pending_execution_task_ids
from obei_workflow_sdk.models import ExecutionBinding, TaskSystemOutbox


def _settings():
    return SimpleNamespace(
        redis_url="redis://unused",
        arq_execution_sync_queue="sync",
        execution_task_execution_mode="RecordOnly",
        execution_task_output_max_bytes=8192,
    )


def test_task_status_reentry_and_checkpoint_steps_are_preserved_on_new_binding(tmp_path):
    storage = SQLAlchemyWorkflowStorage(f"sqlite:///{tmp_path / 'task.db'}", create_tables=True)
    task_id, run_id = storage.create_task_run("flow", "actor", {})
    adapter = ExecutionTaskAdapter(storage, _settings())
    adapter._publish = lambda task_id: None

    for status in ("QUEUED", "RUNNING", "WAITING_USER", "RUNNING"):
        adapter.task_status(task_id, status)
    execution_id, attempt = storage.start_node(run_id, "first", "1")
    adapter.step_status(task_id, run_id, "first", "First", "RUNNING", attempt)
    storage.finish_node(execution_id)
    adapter.step_status(task_id, run_id, "first", "First", "SUCCEEDED", attempt)
    with storage.session_factory.begin() as db:
        for row in db.scalars(select(TaskSystemOutbox)).all():
            row.status = "SUCCEEDED"

    adapter.retry(task_id, run_id)
    with storage.session_factory() as db:
        bindings = db.scalars(select(ExecutionBinding).order_by(ExecutionBinding.retry_seq)).all()
        status_rows = db.scalars(select(TaskSystemOutbox).where(
            TaskSystemOutbox.binding_id == bindings[0].id,
            TaskSystemOutbox.event_type == "TASK_STATUS",
        )).all()
        replay = db.scalars(select(TaskSystemOutbox).where(
            TaskSystemOutbox.binding_id == bindings[1].id,
        ).order_by(TaskSystemOutbox.event_seq)).all()
        assert len(status_rows) == 3
        assert [row.event_type for row in replay] == ["TASK_CREATE", "TASK_STATUS", "STEP_START", "STEP_STATUS"]
        assert replay[-1].payload["status"] == "Success"
    assert task_id in pending_execution_task_ids(storage)
    history = storage.execution_bindings(task_id)
    assert [item["retry_seq"] for item in history] == [0, 1]
    assert history[1]["parent_binding_id"] == history[0]["binding_id"]


@pytest.mark.parametrize("event,operation,app_type", [
    ({"event": "error", "message": "provider failed"}, "chat", "chat"),
    ({"event": "workflow_finished", "data": {"status": "failed", "error": "node failed", "outputs": {}}}, "workflow", "workflow"),
    ({"event": "message", "answer": "partial"}, "chat", "chat"),
])
def test_dify_rejects_error_failed_and_truncated_streams(event, operation, app_type):
    body = f"data: {json.dumps(event)}\n\n"
    transport = httpx.MockTransport(lambda request: httpx.Response(200, text=body))
    client = DifyAppClient(DifyAppConfig("app", "https://dify/v1", "key", app_type), transport=transport)
    request = DifyRequest(operation, query="hello" if operation == "chat" else None)
    with pytest.raises(RuntimeError):
        asyncio.run(client.invoke(request))


def test_sse_drains_all_database_pages_before_the_redis_buffer():
    historical = [{"event_seq": seq, "event_type": "history"} for seq in range(1, 1002)]
    calls = []

    class Storage:
        def get_task(self, task_id): return {"task_id": task_id}
        def events_after(self, task_id, seq, limit=1000):
            calls.append(seq)
            return [event for event in historical if event["event_seq"] > seq][:limit]

    class Bus:
        async def tail_id(self, task_id): return "1001-0"
        async def read(self, task_id, cursor, block):
            return [("1002-0", {"db_event_seq": 1002, "type": "live"})]

    runtime = SimpleNamespace(
        storage=Storage(), services=SimpleNamespace(event_bus=Bus()),
        settings=SimpleNamespace(event_stream_block_ms=1), registry=SimpleNamespace(),
    )
    app = FastAPI()
    app.include_router(create_workflow_router(runtime))
    route = next(route for route in app.routes if route.path.endswith("/events"))

    async def consume():
        response = await route.endpoint("task", "")
        frames = [await anext(response.body_iterator) for _ in range(1002)]
        await response.body_iterator.aclose()
        return [int(frame.splitlines()[0][4:]) for frame in frames if frame.startswith("id:")]

    ids = asyncio.run(consume())
    assert ids == list(range(1, 1003))
    assert calls[:2] == [0, 1000]


def test_run_stream_adapter_is_registry_compatible():
    class Adapter(RunStreamAdapter):
        async def run(self, request): return LLMRunHandle("run-1")
        async def stream(self, handle, on_chunk=None): return LLMResponse(content="done")

    response = asyncio.run(Adapter("runs", "model").generate(
        LLMRequest(messages=[{"role": "user", "content": "x"}], stream=True)
    ))
    assert (response.content, response.provider_request_id, response.provider) == ("done", "run-1", "runs")
