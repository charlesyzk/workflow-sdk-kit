from __future__ import annotations

from pathlib import Path

from obei_workflow_sdk import SQLAlchemyWorkflowStorage, WorkflowRegistry, WorkflowRuntime
from obei_workflow_sdk.testing import InlineDispatcher, NullTaskSystemAdapter
from obei_workflow_sdk.execution_task import ExecutionTaskAdapter
from obei_workflow_sdk.models import ExecutionBinding, TaskSystemOutbox
from sqlalchemy import select
from types import SimpleNamespace
from starter_app.workflows import NumberAnalysisWorkflow


def test_number_analysis_writes_seven_table_records(tmp_path: Path):
    storage = SQLAlchemyWorkflowStorage(f"sqlite:///{tmp_path / 'test.db'}", create_tables=True)
    registry = WorkflowRegistry()
    registry.register(NumberAnalysisWorkflow())
    runtime = WorkflowRuntime(storage, registry, InlineDispatcher(), NullTaskSystemAdapter())

    accepted = runtime.submit("number_analysis", {
        "numbers": [1, 2, 3],
        "multiplier": 2,
        "delay_seconds": 0,
    }, "tester")

    task = storage.get_task(accepted["task_id"])
    assert task is not None
    assert task["status"] == "SUCCEEDED"
    assert task["final_artifact_id"]
    artifact = storage.get_artifact(task["final_artifact_id"])
    assert artifact is not None
    assert "平均值：4.0" in artifact["content"]

    with storage.session_factory() as db:
        from obei_workflow_sdk.models import WorkflowTask
        persisted_task = db.get(WorkflowTask, accepted["task_id"])
        assert persisted_task is not None
        assert persisted_task.input_text == '{"numbers":[1.0,2.0,3.0],"multiplier":2.0,"delay_seconds":0.0}'

    trace = storage.trace(accepted["task_id"])
    assert [node["node_name"] for node in trace["nodes"]] == [
        "normalize_numbers",
        "calculate_statistics",
        "generate_report",
    ]
    assert all(node["status"] == "SUCCEEDED" for node in trace["nodes"])
    assert any(event["event_type"] == "workflow_finished" for event in trace["events"])


def test_execution_task_adapter_writes_durable_ordered_outbox(tmp_path: Path):
    storage = SQLAlchemyWorkflowStorage(f"sqlite:///{tmp_path / 'outbox.db'}", create_tables=True)
    task_id, run_id = storage.create_task_run("number_analysis", "tester", {"numbers": [1], "multiplier": 1, "delay_seconds": 0})
    settings = SimpleNamespace(
        redis_url="redis://unused:6379/0",
        execution_task_execution_mode="RecordOnly",
        execution_task_output_max_bytes=8192,
    )
    adapter = ExecutionTaskAdapter(storage, settings)
    published = []
    adapter._publish = published.append
    adapter.task_status(task_id, "QUEUED", {"run_id": run_id})
    adapter.step_status(task_id, run_id, "calculate", "计算", "RUNNING", 1)
    with storage.session_factory() as db:
        binding = db.scalar(select(ExecutionBinding).where(ExecutionBinding.local_task_id == task_id))
        rows = db.scalars(select(TaskSystemOutbox).where(TaskSystemOutbox.local_task_id == task_id).order_by(TaskSystemOutbox.event_seq)).all()
    assert binding is not None and binding.binding_status == "PENDING"
    assert [row.event_type for row in rows] == ["TASK_CREATE", "STEP_START"]
    assert [row.event_seq for row in rows] == [1, 2]
    assert published == [task_id, task_id]
    adapter.retry(task_id, run_id)
    with storage.session_factory() as db:
        bindings = db.scalars(select(ExecutionBinding).where(ExecutionBinding.local_task_id == task_id).order_by(ExecutionBinding.retry_seq)).all()
    assert [binding.retry_seq for binding in bindings] == [0, 1]
    assert bindings[1].parent_binding_id == bindings[0].id
