from pathlib import Path
from typing import TypedDict

from pydantic import BaseModel

from obei_workflow_sdk import HumanGateNode, SQLAlchemyWorkflowStorage, TaskStep, Workflow, WorkflowGraph, WorkflowInput, WorkflowRegistry, WorkflowRuntime
from obei_workflow_sdk.testing import InlineDispatcher, NullTaskSystemAdapter


class Request(WorkflowInput):
    value: int


class State(TypedDict, total=False):
    task_id: str
    run_id: str
    actor_id: str
    workflow_type: str
    value: int
    last_decision: dict
    current_node: str


class GateInput(BaseModel):
    value: int


class ApprovalGate(HumanGateNode):
    step = TaskStep(code="approve", name="人工确认")
    input_model = GateInput


class ApprovalWorkflow(Workflow):
    workflow_type = "approval"
    input_model = Request
    state_model = State

    def build(self, graph: WorkflowGraph):
        graph.add_node("approve", ApprovalGate())
        graph.set_entry_point("approve")
        graph.set_finish_point("approve")


def test_interrupt_decision_and_resume(tmp_path: Path):
    storage = SQLAlchemyWorkflowStorage(f"sqlite:///{tmp_path / 'gate.db'}", create_tables=True)
    registry = WorkflowRegistry(); registry.register(ApprovalWorkflow())
    runtime = WorkflowRuntime(storage, registry, InlineDispatcher(), NullTaskSystemAdapter())
    accepted = runtime.submit("approval", {"value": 7}, "tester")
    task = storage.get_task(accepted["task_id"])
    assert task["status"] == "WAITING_USER"
    event = next(item for item in storage.trace(task["task_id"])["events"] if item["event_type"] == "workflow_waiting_user")
    result = runtime.decide(task["task_id"], event["payload"]["decision_key"], "CONFIRM", "ok", "tester")
    assert result["status"] == "QUEUED"
    assert storage.get_task(task["task_id"])["status"] == "SUCCEEDED"
