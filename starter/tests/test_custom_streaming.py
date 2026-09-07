"""节点自定义 yield 流式事件的行为测试。"""

from pathlib import Path
from typing import AsyncIterator, TypedDict

from pydantic import BaseModel

from obei_workflow_sdk import (
    NodeOutput,
    NodeStreamChunk,
    SQLAlchemyWorkflowStorage,
    StateField,
    TaskStep,
    Workflow,
    WorkflowGraph,
    WorkflowInput,
    WorkflowNode,
    WorkflowRegistry,
    WorkflowRuntime,
)
from obei_workflow_sdk.testing import InlineDispatcher, NullTaskSystemAdapter


class StreamRequest(WorkflowInput):
    text: str


class StreamState(TypedDict, total=False):
    task_id: str
    run_id: str
    actor_id: str
    workflow_type: str
    text: str
    normalized: str
    current_node: str


class StreamInput(BaseModel):
    text: str


class StreamingNode(WorkflowNode[StreamInput, dict]):
    step = TaskStep(code="stream", name="自定义流")
    input_model = StreamInput
    input_fields = {"text": StateField("text")}

    async def execute(self, ctx, node_input) -> AsyncIterator[NodeStreamChunk | NodeOutput[dict]]:
        yield NodeStreamChunk("第一段", event_type="business_token", payload={"index": 0})
        yield NodeStreamChunk("关键阶段完成", event_type="business_milestone", persist=True)
        yield NodeOutput(data={"normalized": node_input.text.strip()})


class StreamWorkflow(Workflow):
    workflow_type = "custom_stream"
    input_model = StreamRequest
    state_model = StreamState

    def build(self, graph: WorkflowGraph):
        graph.add_node("stream", StreamingNode())
        graph.set_entry_point("stream").set_finish_point("stream")


class RecordingEventBus:
    def __init__(self):
        self.items = []

    def publish(self, task_id, payload):
        self.items.append((task_id, payload))
        return "1-0"


def test_async_generator_yields_transient_and_persistent_events(tmp_path: Path):
    storage = SQLAlchemyWorkflowStorage(f"sqlite:///{tmp_path / 'stream.db'}", create_tables=True)
    registry = WorkflowRegistry(); registry.register(StreamWorkflow())
    event_bus = RecordingEventBus()
    runtime = WorkflowRuntime(
        storage,
        registry,
        InlineDispatcher(),
        NullTaskSystemAdapter(),
        event_bus=event_bus,
    )

    accepted = runtime.submit("custom_stream", {"text": "  hello  "}, "tester")

    task = storage.get_task(accepted["task_id"])
    assert task is not None and task["status"] == "SUCCEEDED"
    transient = next(payload for _, payload in event_bus.items if payload["type"] == "business_token")
    assert transient == {
        "type": "business_token",
        "node_name": "stream",
        "index": 0,
        "content": "第一段",
    }
    trace = storage.trace(accepted["task_id"])
    milestone = next(event for event in trace["events"] if event["event_type"] == "business_milestone")
    assert milestone["payload"] == {"content": "关键阶段完成"}
