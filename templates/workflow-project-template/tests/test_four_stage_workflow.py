"""四环节模板的基础设施无关回归测试。

测试使用 SQLite、内联执行器和假的 LLM Adapter，因此不需要真实数据库、Redis、
模型 Key 或远端任务系统。它验证的是模板开发者最容易改坏的契约：节点顺序、两
种流式模式、Artifact、审计记录以及远端注册 JSON 的依赖。
"""

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

from workflow_app.workflows.four_stage import FourStageWorkflow


class FakeOpenAIAdapter:
    """记录请求模式并为流式调用模拟两个 token。"""

    name = "openai"
    default_model = "fake-model"

    def __init__(self) -> None:
        self.stream_flags: list[bool] = []
        self.provider_options: list[dict] = []

    async def generate(self, request, on_chunk=None):
        self.stream_flags.append(request.stream)
        self.provider_options.append(request.provider_options)
        if on_chunk is not None:
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
    """只记录发布内容，不连接 Redis。"""

    def __init__(self) -> None:
        self.items: list[tuple[str, dict]] = []

    def publish(self, task_id: str, payload: dict) -> str:
        self.items.append((task_id, payload))
        return "1-0"


def test_four_stage_workflow_and_registration_definition(tmp_path: Path) -> None:
    storage = SQLAlchemyWorkflowStorage(
        f"sqlite:///{tmp_path / 'template.db'}",
        create_tables=True,
    )
    registry = WorkflowRegistry()
    registry.register(FourStageWorkflow())

    adapter = FakeOpenAIAdapter()
    llm_registry = LLMRegistry()
    llm_registry.register(adapter)
    events = RecordingEventBus()

    runtime = WorkflowRuntime(
        storage,
        registry,
        InlineDispatcher(),
        NullTaskSystemAdapter(),
        llm_registry=llm_registry,
        event_bus=events,
    )
    accepted = runtime.submit(
        "simple_llm_workflow",
        {"question": "  模板   是否正常？  "},
        "template-test",
    )

    task = storage.get_task(accepted["task_id"])
    assert task is not None
    assert task["status"] == "SUCCEEDED"
    assert task["final_artifact_id"] is not None

    trace = storage.trace(accepted["task_id"])
    assert [node["node_name"] for node in trace["nodes"]] == [
        "validate_input",
        "prepare_context",
        "generate_draft",
        "polish_answer",
    ]
    assert all(node["status"] == "SUCCEEDED" for node in trace["nodes"])

    event_types = [payload["type"] for _, payload in events.items]
    assert event_types.count("context_token") == 2
    # Fake Adapter 只在 stream=True 时收到 on_chunk，因此第四步不会增加 token。
    assert event_types.count("llm_token") == 2
    assert adapter.stream_flags == [True, False]
    assert adapter.provider_options == [
        {},
        {"enable_thinking": False, "max_tokens": 512},
    ]

    invocations = storage.llm_invocations(accepted["task_id"])
    assert [item["stream"] for item in invocations] == [True, False]
    assert all(item["status"] == "SUCCEEDED" for item in invocations)

    assert export_workflow_definition(FourStageWorkflow()) == [
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
