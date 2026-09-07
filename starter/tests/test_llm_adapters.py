from pathlib import Path
import asyncio
from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient
from obei_workflow_sdk import DifyAdapter, LLMRegistry, LLMRequest, LLMResponse, OpenAICompatibleAdapter, SQLAlchemyWorkflowStorage, WorkflowRegistry, WorkflowRuntime, create_workflow_router, export_workflow_definition
from obei_workflow_sdk.llm import LLMChunk
from obei_workflow_sdk.testing import InlineDispatcher, NullTaskSystemAdapter
from starter_app.workflows import DifyWriterWorkflow, OpenAIWriterWorkflow, TaskSystemSmokeWorkflow


class FakeAdapter:
    def __init__(self, name: str):
        self.name = name
        self.default_model = f"{name}-model"
        self.requests = []
        self.handlers = []

    async def generate(self, request, on_chunk=None):
        self.requests.append(request)
        self.handlers.append(on_chunk)
        if on_chunk:
            await on_chunk(LLMChunk("reasoning", "思考"))
            await on_chunk(LLMChunk("content", "回答"))
        return LLMResponse(content="完整回答", reasoning="思考", usage={"total_tokens": 9}, provider=self.name, model=self.default_model, provider_request_id="provider-1")


def make_runtime(tmp_path: Path):
    storage = SQLAlchemyWorkflowStorage(f"sqlite:///{tmp_path / 'llm.db'}", create_tables=True)
    workflows = WorkflowRegistry(); workflows.register(OpenAIWriterWorkflow()); workflows.register(DifyWriterWorkflow())
    llms = LLMRegistry(); openai = FakeAdapter("openai"); dify = FakeAdapter("dify")
    llms.register(openai); llms.register(dify)
    class EventBus:
        def __init__(self): self.events = []
        def publish(self, task_id, payload): self.events.append((task_id, payload)); return "1-0"
    event_bus = EventBus()
    runtime = WorkflowRuntime(storage, workflows, InlineDispatcher(), NullTaskSystemAdapter(), llm_registry=llms, event_bus=event_bus)
    return runtime, storage, openai, dify, event_bus


def test_llm_nodes_explicitly_control_streaming_and_audit_io(tmp_path: Path):
    runtime, storage, openai, dify, event_bus = make_runtime(tmp_path)

    first = runtime.submit("openai_stream_writer", {"topic": "供应链"}, "tester")
    second = runtime.submit("dify_blocking_writer", {"topic": "采购"}, "tester")

    assert openai.requests[0].stream is True
    assert openai.handlers[0] is not None
    assert dify.requests[0].stream is False
    assert dify.handlers[0] is None

    streaming_audit = storage.llm_invocations(first["task_id"])[0]
    blocking_audit = storage.llm_invocations(second["task_id"])[0]
    assert streaming_audit["request"]["messages"][1]["content"] == '{"topic": "供应链"}'
    assert streaming_audit["response"] == "完整回答"
    assert streaming_audit["reasoning"] == "思考"
    assert streaming_audit["usage"] == {"total_tokens": 9}
    assert streaming_audit["provider_request_id"] == "provider-1"
    assert streaming_audit["prompt_version"] == "writer-v1"
    assert streaming_audit["stream"] is True
    assert blocking_audit["stream"] is False
    assert storage.trace(first["task_id"])["nodes"][0]["model_name"] == "openai:openai-model"
    openai_types = [payload["type"] for task_id, payload in event_bus.events if task_id == first["task_id"]]
    dify_types = [payload["type"] for task_id, payload in event_bus.events if task_id == second["task_id"]]
    assert "llm_token" in openai_types and "llm_reasoning_token" in openai_types
    assert "llm_token" not in dify_types and "llm_reasoning_token" not in dify_types


def test_adapter_request_formats_are_provider_correct():
    request_stream = LLMRequest(messages=[{"role": "user", "content": "hello"}], stream=True, response_format="json_object")
    openai = OpenAICompatibleAdapter("https://model.example/v1", "key", "model-x")
    openai_body = openai._body(request_stream)
    assert openai_body["model"] == "model-x"
    assert openai_body["stream"] is True
    assert openai_body["response_format"] == {"type": "json_object"}
    assert openai_body["messages"][0]["role"] == "user"

    dify = DifyAdapter("https://dify.example/v1", "app-key")
    dify_body = dify._body(LLMRequest(messages=[{"role": "user", "content": "hello"}], stream=False))
    assert dify_body["response_mode"] == "blocking"
    assert dify_body["query"] == "[user]\nhello"
    assert dify_body["inputs"] == {}


def test_task_system_smoke_workflow_uses_predefined_step_code(tmp_path: Path):
    storage = SQLAlchemyWorkflowStorage(f"sqlite:///{tmp_path / 'smoke.db'}", create_tables=True)
    workflows = WorkflowRegistry(); workflows.register(TaskSystemSmokeWorkflow())
    llms = LLMRegistry(); llms.register(FakeAdapter("openai"))
    runtime = WorkflowRuntime(storage, workflows, InlineDispatcher(), NullTaskSystemAdapter(), llm_registry=llms)
    accepted = runtime.submit("task_system_smoke", {"topic": "测试"}, "tester")
    trace = storage.trace(accepted["task_id"])
    assert trace["nodes"][0]["node_name"] == "intent_recognition"
    assert len(trace["nodes"]) == 15
    assert trace["nodes"][-1]["node_name"] == "html_report_generation"
    assert all(node["status"] == "SUCCEEDED" for node in trace["nodes"])


def test_workflow_definition_export_matches_remote_registration_contract():
    definition = export_workflow_definition(TaskSystemSmokeWorkflow())
    assert len(definition) == 15
    assert definition[0] == {
        "stepCode": "intent_recognition",
        "name": "意图识别",
        "stepType": "Reasoning",
        "dependsOn": [],
        "needConfirmation": False,
        "exceptionStrategy": None,
    }
    assert definition[1]["dependsOn"] == ["intent_recognition"]
    assert definition[-1]["dependsOn"] == ["report_generation"]
    assert [item["stepCode"] for item in definition if item["needConfirmation"]] == [
        "confirm_rewrite",
        "confirm_discovery",
        "confirm_sql",
        "confirm_summary",
    ]


def test_registration_definition_is_available_over_http():
    registry = WorkflowRegistry(); registry.register(TaskSystemSmokeWorkflow())
    app = FastAPI()
    app.include_router(create_workflow_router(SimpleNamespace(registry=registry)))
    response = TestClient(app).get(
        "/api/v1/workflow-types/task_system_smoke/registration-definition"
    )
    assert response.status_code == 200
    assert len(response.json()) == 15
    assert response.json()[2]["needConfirmation"] is True


def test_openai_stream_parser_collects_content_reasoning_and_usage(monkeypatch):
    lines = [
        'data: {"id":"req-1","model":"m1","choices":[{"delta":{"reasoning_content":"思"}}]}',
        'data: {"id":"req-1","model":"m1","choices":[{"delta":{"content":"答"}}]}',
        'data: {"id":"req-1","model":"m1","choices":[],"usage":{"total_tokens":2}}',
        "data: [DONE]",
    ]

    class Response:
        def raise_for_status(self): pass
        async def aiter_lines(self):
            for line in lines: yield line

    class Stream:
        async def __aenter__(self): return Response()
        async def __aexit__(self, *args): pass

    class Client:
        async def __aenter__(self): return self
        async def __aexit__(self, *args): pass
        def stream(self, *args, **kwargs): return Stream()

    monkeypatch.setattr("obei_workflow_sdk.llm.httpx.AsyncClient", lambda **kwargs: Client())
    chunks = []
    adapter = OpenAICompatibleAdapter("https://model.example/v1", "key", "m1")
    response = asyncio.run(adapter.generate(
        LLMRequest(messages=[{"role": "user", "content": "x"}], stream=True),
        on_chunk=chunks.append,
    ))
    assert response.content == "答"
    assert response.reasoning == "思"
    assert response.usage == {"total_tokens": 2}
    assert response.provider_request_id == "req-1"
    assert [(item.kind, item.content) for item in chunks] == [("reasoning", "思"), ("content", "答")]


def test_dify_blocking_parser_records_answer_and_usage(monkeypatch):
    class Response:
        def raise_for_status(self): pass
        def json(self):
            return {"message_id": "dify-1", "answer": "完整回答", "metadata": {"usage": {"total_tokens": 8}}}

    class Client:
        async def __aenter__(self): return self
        async def __aexit__(self, *args): pass
        async def post(self, *args, **kwargs): return Response()

    monkeypatch.setattr("obei_workflow_sdk.llm.httpx.AsyncClient", lambda **kwargs: Client())
    adapter = DifyAdapter("https://dify.example/v1", "app-key")
    response = asyncio.run(adapter.generate(LLMRequest(messages=[{"role": "user", "content": "x"}], stream=False)))
    assert response.content == "完整回答"
    assert response.usage == {"total_tokens": 8}
    assert response.provider_request_id == "dify-1"
