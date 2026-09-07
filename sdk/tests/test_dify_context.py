from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
import httpx

from obei_workflow_sdk.dify import BoundDifyClient, DifyAppClient, DifyAppConfig, DifyChunk, DifyNodeConfig, DifyRequest, DifyResponse, create_dify_registry
from obei_workflow_sdk.graph import RuntimeServices
from obei_workflow_sdk.models import DifyConversation, DifyInvocation
from obei_workflow_sdk.storage import SQLAlchemyWorkflowStorage
from obei_workflow_sdk.llm import BoundLLMClient, LLMNodeConfig, LLMResponse


class MemoryStorage:
    def __init__(self) -> None:
        self.conversation_id = None
        self.started = []
        self.finished = []

    def get_dify_conversation(self, task_id, app_name, conversation_key):
        if self.conversation_id is None:
            return None
        return {"conversation_id": self.conversation_id, "last_message_id": "previous"}

    def start_dify_invocation(self, *args):
        self.started.append(args)
        return f"invocation-{len(self.started)}"

    def finish_dify_invocation(self, invocation_id, **values):
        self.finished.append((invocation_id, values))
        if values.get("conversation_id"):
            self.conversation_id = values["conversation_id"]


class FakeDifyClient:
    def __init__(self) -> None:
        self.requests = []
        self.config = SimpleNamespace(app_type="chat")

    async def invoke(self, request, on_chunk):
        self.requests.append(request)
        await on_chunk(DifyChunk("content", "增量"))
        return DifyResponse(
            content="完整回答",
            conversation_id=request.conversation_id or "conversation-1",
            message_id=f"message-{len(self.requests)}",
        )


class FakeDifyRegistry:
    def __init__(self, client):
        self.client = client

    def get(self, name):
        return self.client


def test_dify_uses_dedicated_table_prefixes():
    assert DifyConversation.__tablename__ == "obei_workshop_dify_conversation"
    assert DifyInvocation.__tablename__ == "obei_workshop_dify_invocation"


def test_dify_config_is_streaming_by_contract():
    config = DifyNodeConfig(conversation_key="main")
    assert not hasattr(config, "stream")
    assert config.use_history is False


def test_bound_dify_is_stateless_by_default_and_history_is_opt_in():
    class Services:
        async def invoke_dify(self, context, request, **options):
            self.calls.append((request, options))
            return DifyResponse(content="answer", conversation_id="returned-id")

    services = Services()
    services.calls = []
    client = BoundDifyClient(services, SimpleNamespace(), DifyNodeConfig())

    async def run():
        await client.chat("无上下文")
        await client.chat("延续上下文", use_history=True, conversation_key="main")
        await client.chat("显式会话", conversation_id="caller-id")

    asyncio.run(run())
    assert services.calls[0][1]["conversation_key"] is None
    assert services.calls[0][0].conversation_id is None
    assert services.calls[1][1]["conversation_key"] == "main"
    assert services.calls[2][1]["conversation_key"] is None
    assert services.calls[2][0].conversation_id == "caller-id"


def test_second_dify_turn_reuses_conversation_and_emits_common_events():
    storage = MemoryStorage()
    client = FakeDifyClient()
    events = []
    services = RuntimeServices(storage, SimpleNamespace(), dify_registry=FakeDifyRegistry(client))
    services.transient_event = lambda task_id, event_type, **payload: events.append(
        (event_type, payload)
    )
    context = SimpleNamespace(
        task_id="task-1", run_id="run-1", node_code="chat", attempt=1
    )

    async def run():
        from obei_workflow_sdk.dify import DifyRequest

        await services.invoke_dify(
            context,
            DifyRequest("chat", query="第一问"),
            conversation_key="main",
            app_name="assistant",
        )
        await services.invoke_dify(
            context,
            DifyRequest("chat", query="继续"),
            conversation_key="main",
            app_name="assistant",
        )

    asyncio.run(run())
    assert client.requests[0].conversation_id is None
    assert client.requests[1].conversation_id == "conversation-1"
    assert [name for name, _ in events] == [
        "llm_start",
        "llm_token",
        "llm_end",
        "llm_start",
        "llm_token",
        "llm_end",
    ]
    assert all(payload["source"] == "dify" for _, payload in events)
    assert all(payload["provider"] == "dify" for _, payload in events)
    assert len(storage.finished) == 2


def test_sqlite_persists_and_reuses_dify_conversation(tmp_path):
    storage = SQLAlchemyWorkflowStorage(
        f"sqlite:///{tmp_path / 'workflow.db'}", create_tables=True
    )
    client = FakeDifyClient()
    services = RuntimeServices(storage, SimpleNamespace(), dify_registry=FakeDifyRegistry(client))
    context = SimpleNamespace(
        task_id="task-1", run_id="run-1", node_code="chat", attempt=1
    )

    async def run():
        from obei_workflow_sdk.dify import DifyRequest

        for query in ("第一问", "继续"):
            await services.invoke_dify(
                context,
                DifyRequest("chat", query=query),
                conversation_key="main",
                app_name="assistant",
            )

    asyncio.run(run())
    assert client.requests[1].conversation_id == "conversation-1"
    assert storage.get_dify_conversation("task-1", "assistant", "main") == {
        "conversation_id": "conversation-1",
        "last_message_id": "message-2",
        "status": "ACTIVE",
    }
    rows = storage.dify_invocations("task-1")
    assert [row["status"] for row in rows] == ["SUCCEEDED", "SUCCEEDED"]
    assert all(row["stream"] is True for row in rows)


def test_stateless_dify_turn_is_audited_but_not_reused(tmp_path):
    storage = SQLAlchemyWorkflowStorage(
        f"sqlite:///{tmp_path / 'stateless.db'}", create_tables=True
    )
    client = FakeDifyClient()
    services = RuntimeServices(storage, SimpleNamespace(), dify_registry=FakeDifyRegistry(client))
    context = SimpleNamespace(
        task_id="task-1", run_id="run-1", node_code="chat", attempt=1
    )

    async def run():
        from obei_workflow_sdk.dify import DifyRequest

        for query in ("第一问", "第二问"):
            await services.invoke_dify(
                context,
                DifyRequest("chat", query=query),
                conversation_key=None,
                app_name="assistant",
            )

    asyncio.run(run())
    assert [request.conversation_id for request in client.requests] == [None, None]
    assert storage.get_dify_conversation("task-1", "assistant", "main") is None
    rows = storage.dify_invocations("task-1")
    assert len(rows) == 2
    assert all(row["conversation_key"] is None for row in rows)


def test_direct_model_chat_forwards_caller_managed_history():
    class Services:
        async def invoke_llm(self, context, adapter, request, stage=None):
            self.request = request
            return LLMResponse(content="answer")

    services = Services()
    client = BoundLLMClient(
        services,
        SimpleNamespace(),
        LLMNodeConfig(adapter="openai", stream=True),
    )
    messages = [
        {"role": "system", "content": "system"},
        {"role": "assistant", "content": "previous answer"},
        {"role": "user", "content": "continue"},
    ]
    assert asyncio.run(client.chat(messages)) == "answer"
    assert services.request.messages == messages
    assert services.request.stream is True


def test_six_dify_modes_select_the_correct_protocol():
    for mode in ("chat", "agent-chat", "agent", "advanced-chat"):
        client = DifyAppClient(DifyAppConfig(mode, "https://dify/v1", "key", mode))
        url, body = client._endpoint_and_body(DifyRequest("chat", query="hello"))
        assert url.endswith("/chat-messages")
        assert body["response_mode"] == "streaming"
    completion = DifyAppClient(DifyAppConfig("writer", "https://dify/v1", "key", "completion"))
    assert completion._endpoint_and_body(DifyRequest("completion", {"topic": "x"}))[0].endswith("/completion-messages")
    workflow = DifyAppClient(DifyAppConfig("flow", "https://dify/v1", "key", "workflow"))
    assert workflow._endpoint_and_body(DifyRequest("workflow", {"id": 1}))[0].endswith("/workflows/run")


def test_agent_stream_does_not_duplicate_closing_message():
    events = [
        {"event": "agent_message", "answer": "增量", "conversation_id": "c1", "message_id": "m1"},
        {"event": "message", "answer": "增量", "conversation_id": "c1", "message_id": "m1"},
        {"event": "message_end", "metadata": {"usage": {"total_tokens": 3}}},
    ]
    body = "".join(f"data: {json.dumps(item, ensure_ascii=False)}\n\n" for item in events)
    transport = httpx.MockTransport(lambda request: httpx.Response(200, text=body))
    client = DifyAppClient(DifyAppConfig("agent", "https://dify/v1", "key", "agent"), transport=transport)
    response = asyncio.run(client.invoke(DifyRequest("chat", query="hello")))
    assert response.content == "增量"
    assert response.conversation_id == "c1"


def test_workflow_stream_returns_outputs_without_conversation_id():
    events = [
        {"event": "workflow_started", "task_id": "t1", "workflow_run_id": "r1", "data": {"id": "r1"}},
        {"event": "node_finished", "task_id": "t1", "workflow_run_id": "r1", "data": {"title": "node"}},
        {"event": "workflow_finished", "task_id": "t1", "workflow_run_id": "r1", "data": {"outputs": {"report": "done"}}},
    ]
    body = "".join(f"data: {json.dumps(item)}\n\n" for item in events)
    transport = httpx.MockTransport(lambda request: httpx.Response(200, text=body))
    client = DifyAppClient(DifyAppConfig("flow", "https://dify/v1", "key", "workflow"), transport=transport)
    response = asyncio.run(client.invoke(DifyRequest("workflow", {"order": "1"})))
    assert response.outputs == {"report": "done"}
    assert response.conversation_id is None
    assert response.workflow_run_id == "r1"


def test_multi_app_registry_resolves_each_key_from_its_environment(monkeypatch):
    monkeypatch.setenv("DIFY_AGENT_KEY", "agent-secret")
    monkeypatch.setenv("DIFY_FLOW_KEY", "flow-secret")
    settings = SimpleNamespace(
        dify_apps_json=json.dumps([
            {"name": "analysis", "base_url": "https://dify/v1", "app_type": "agent", "api_key_env": "DIFY_AGENT_KEY"},
            {"name": "report", "base_url": "https://dify/v1", "app_type": "workflow", "api_key_env": "DIFY_FLOW_KEY"},
        ]),
        dify_timeout_seconds=120,
        dify_user="sdk",
        resolved_dify_api_key="",
    )
    registry = create_dify_registry(settings)
    assert registry.names() == ["analysis", "report"]
    assert registry.get("analysis").config.api_key == "agent-secret"
    assert registry.get("report").config.app_type == "workflow"
