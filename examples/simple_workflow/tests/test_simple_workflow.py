"""教程示例的基础设施无关测试。"""

from pathlib import Path

from obei_workflow_sdk import (
    LLMRegistry,
    LLMResponse,
    SQLAlchemyWorkflowStorage,
    WorkflowRegistry,
    WorkflowRuntime,
    export_workflow_definition,
)
from obei_workflow_sdk.llm import LLMChunk
from obei_workflow_sdk.testing import InlineDispatcher, NullTaskSystemAdapter

from simple_app.workflow import SimpleLLMWorkflow


class FakeOpenAIAdapter:
    name = "openai"
    default_model = "fake-model"

    def __init__(self):
        self.stream_flags = []
        self.provider_options = []

    async def generate(self, request, on_chunk=None):
        self.stream_flags.append(request.stream)
        self.provider_options.append(request.provider_options)
        if on_chunk:
            await on_chunk(LLMChunk("content", "测试"))
            await on_chunk(LLMChunk("content", "成功"))
        return LLMResponse(
            content="测试成功",
            usage={"total_tokens": 4},
            provider="openai",
            model=self.default_model,
            provider_request_id="fake-request-1",
        )


class RecordingEventBus:
    def __init__(self):
        self.items = []

    def publish(self, task_id, payload):
        self.items.append((task_id, payload))
        return "1-0"


def test_simple_workflow_executes_and_exports_registration_json(tmp_path: Path):
    storage = SQLAlchemyWorkflowStorage(f"sqlite:///{tmp_path / 'simple.db'}", create_tables=True)
    registry = WorkflowRegistry(); registry.register(SimpleLLMWorkflow())
    adapter = FakeOpenAIAdapter()
    llms = LLMRegistry(); llms.register(adapter)
    events = RecordingEventBus()
    runtime = WorkflowRuntime(
        storage,
        registry,
        InlineDispatcher(),
        NullTaskSystemAdapter(),
        llm_registry=llms,
        event_bus=events,
    )

    accepted = runtime.submit(
        "simple_llm_workflow",
        {"question": "  SDK   是否正常？  "},
        "tester",
    )

    task = storage.get_task(accepted["task_id"])
    assert task is not None and task["status"] == "SUCCEEDED"
    trace = storage.trace(accepted["task_id"])
    assert [node["node_name"] for node in trace["nodes"]] == [
        "validate_input",
        "prepare_context",
        "generate_draft",
        "polish_answer",
    ]
    event_types = [payload["type"] for _, payload in events.items]
    assert event_types.count("context_token") == 2
    assert event_types.count("llm_token") == 2
    assert adapter.stream_flags == [True, False]
    assert adapter.provider_options == [
        {},
        {"enable_thinking": False, "max_tokens": 512},
    ]
    invocations = storage.llm_invocations(accepted["task_id"])
    assert [item["stream"] for item in invocations] == [True, False]
    assert all(item["status"] == "SUCCEEDED" for item in invocations)

    definition = export_workflow_definition(SimpleLLMWorkflow())
    assert definition == [
        {
            "stepCode": "validate_input",
            "name": "校验输入",
            "stepType": "Reasoning",
            "dependsOn": [],
            "needConfirmation": False,
            "exceptionStrategy": None,
        },
        {
            "stepCode": "prepare_context",
            "name": "准备上下文",
            "stepType": "Reasoning",
            "dependsOn": ["validate_input"],
            "needConfirmation": False,
            "exceptionStrategy": None,
        },
        {
            "stepCode": "generate_draft",
            "name": "流式生成草稿",
            "stepType": "Reasoning",
            "dependsOn": ["prepare_context"],
            "needConfirmation": False,
            "exceptionStrategy": None,
        },
        {
            "stepCode": "polish_answer",
            "name": "非流式润色回答",
            "stepType": "Reasoning",
            "dependsOn": ["generate_draft"],
            "needConfirmation": False,
            "exceptionStrategy": None,
        },
    ]
