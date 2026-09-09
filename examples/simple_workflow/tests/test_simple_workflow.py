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


class EmptyContentAdapter:
    """模拟推理型模型：正文为空、推理非空（max_tokens 被 reasoning 吃光）。"""

    name = "openai"
    default_model = "fake-model"

    async def generate(self, request, on_chunk=None):
        if on_chunk:
            await on_chunk(LLMChunk("reasoning", "思考中"))
        return LLMResponse(
            content="",
            reasoning="思考中",
            usage={"total_tokens": 8},
            provider="openai",
            model=self.default_model,
            provider_request_id="fake-empty",
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
        {"max_tokens": 2048},
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


def test_streaming_node_emits_tokens_non_streaming_does_not(tmp_path: Path):
    """显式验证：流式 LLM 节点发 llm_token，非流式节点零 token，两者共存不串台。"""
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
        {"question": "流式与非流式是否兼容？"},
        "tester",
    )
    task = storage.get_task(accepted["task_id"])
    assert task["status"] == "SUCCEEDED"

    tokens = [p for _, p in events.items if p["type"] == "llm_token"]
    # 只有流式节点 generate_draft 会产生 token
    assert tokens, "streaming node should emit llm_token events"
    assert {p["node_name"] for p in tokens} == {"generate_draft"}
    # 非流式节点 polish_answer 一个 token 都不产生
    polish_tokens = [p for _, p in events.items if p["type"] == "llm_token" and p["node_name"] == "polish_answer"]
    assert polish_tokens == []
    # 适配器确实分别以 stream=True / stream=False 被调用
    assert adapter.stream_flags == [True, False]
    # LLM 审计表记录了流式标记
    assert [i["stream"] for i in storage.llm_invocations(accepted["task_id"])] == [True, False]


def test_empty_content_emits_warning_event(tmp_path: Path):
    """正文为空但推理非空时，SDK 只发 llm_empty_content 警告事件，不做自动回退。"""
    storage = SQLAlchemyWorkflowStorage(f"sqlite:///{tmp_path / 'simple.db'}", create_tables=True)
    registry = WorkflowRegistry(); registry.register(SimpleLLMWorkflow())
    llms = LLMRegistry(); llms.register(EmptyContentAdapter())
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
        {"question": "测试空正文警告"},
        "tester",
    )
    task = storage.get_task(accepted["task_id"])
    assert task["status"] == "SUCCEEDED"

    warnings = [p for _, p in events.items if p["type"] == "llm_empty_content"]
    assert {w["node_name"] for w in warnings} == {"generate_draft", "polish_answer"}
    assert all("empty content" in w["message"] for w in warnings)
