"""内容复核示例的基础设施无关测试：决策路径 + 回环重跑 + 注册 JSON 导出。"""

from pathlib import Path

import pytest

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

from review_app.workflow import ContentReviewWorkflow


class FakeOpenAIAdapter:
    name = "openai"
    default_model = "fake-model"

    def __init__(self):
        self.stream_flags = []

    async def generate(self, request, on_chunk=None):
        self.stream_flags.append(request.stream)
        if on_chunk:
            await on_chunk(LLMChunk("content", "测试"))
            await on_chunk(LLMChunk("content", "内容"))
        return LLMResponse(
            content="测试内容",
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


class RecordingTaskSystem:
    """按调用顺序记录 task_status/step_status，用于断言 SKIPPED 对账。"""

    def __init__(self):
        self.calls = []

    def task_status(self, task_id, status, payload=None):
        self.calls.append(("task", status))
        return None

    def step_status(self, task_id, run_id, step_code, step_name, status, attempt, payload=None, definition=None):
        self.calls.append(("step", step_code, status, definition))
        return None


def build_runtime(tmp_path: Path):
    storage = SQLAlchemyWorkflowStorage(f"sqlite:///{tmp_path / 'review.db'}", create_tables=True)
    registry = WorkflowRegistry()
    registry.register(ContentReviewWorkflow())
    llms = LLMRegistry()
    llms.register(FakeOpenAIAdapter())
    events = RecordingEventBus()
    runtime = WorkflowRuntime(
        storage,
        registry,
        InlineDispatcher(),
        NullTaskSystemAdapter(),
        llm_registry=llms,
        event_bus=events,
    )
    return runtime, storage, events


def decision_key_for(storage, task_id: str) -> dict:
    event = next(
        item
        for item in storage.trace(task_id)["events"]
        if item["event_type"] == "workflow_waiting_user"
    )
    return event["payload"]


def latest_waiting_payload(storage, task_id: str) -> dict:
    """回环重跑会产生多个 waiting_user 事件，取最新一轮。"""
    events = [
        item
        for item in storage.trace(task_id)["events"]
        if item["event_type"] == "workflow_waiting_user"
    ]
    return events[-1]["payload"]


def submit_and_wait(runtime, storage):
    accepted = runtime.submit(
        "content_review",
        {"topic": "SDK 学习建议", "requirements": "200 字以内"},
        "tester",
    )
    task = storage.get_task(accepted["task_id"])
    assert task["status"] == "WAITING_USER"
    payload = decision_key_for(storage, accepted["task_id"])
    # 人工门应携带初稿 Artifact 引用，复核人凭它查看全文。
    assert payload["artifact_ref"], "decision payload must reference the draft artifact"
    return accepted["task_id"], payload


def decide_with_artifact(runtime, storage, task_id, payload, decision, feedback):
    """审批决策必须引用复核人实际审阅的 Artifact（id + version + content_hash）。"""
    artifact = storage.get_artifact(payload["artifact_ref"])
    return runtime.decide(
        task_id,
        payload["decision_key"],
        decision,
        feedback,
        "tester",
        artifact_id=artifact["artifact_id"],
        artifact_version=artifact["version"],
        content_hash=artifact["content_hash"],
    )


def test_confirm_path_succeeds(tmp_path: Path):
    runtime, storage, _ = build_runtime(tmp_path)
    task_id, payload = submit_and_wait(runtime, storage)
    result = decide_with_artifact(runtime, storage, task_id, payload, "CONFIRM", "通过")
    assert result["status"] == "QUEUED"
    task = storage.get_task(task_id)
    assert task["status"] == "SUCCEEDED"
    artifact = storage.get_artifact(task["final_artifact_id"])
    assert artifact["artifact_type"] == "answer"


def test_revise_path_regenerates_with_feedback(tmp_path: Path):
    runtime, storage, _ = build_runtime(tmp_path)
    task_id, payload = submit_and_wait(runtime, storage)
    decide_with_artifact(runtime, storage, task_id, payload, "REVISE", "语气太生硬")
    task = storage.get_task(task_id)
    assert task["status"] == "SUCCEEDED"
    artifact = storage.get_artifact(task["final_artifact_id"])
    assert artifact["artifact_type"] == "revised_answer"


def test_reject_loops_back_and_regenerates(tmp_path: Path):
    """REJECT 回环重跑初稿：round+1，重新进入人工复核，第二轮通过后定稿。"""
    runtime, storage, _ = build_runtime(tmp_path)
    task_id, payload = submit_and_wait(runtime, storage)
    assert payload["round"] == 1
    assert payload["allowed_decisions"] == ["CONFIRM", "REJECT", "REVISE", "CLOSE"]

    decide_with_artifact(runtime, storage, task_id, payload, "REJECT", "方向不对，重写")
    task = storage.get_task(task_id)
    assert task["status"] == "WAITING_USER", "驳回后应回环并再次暂停在人工复核"
    round2 = latest_waiting_payload(storage, task_id)
    assert round2["round"] == 2

    decide_with_artifact(runtime, storage, task_id, round2, "CONFIRM", "这版可以")
    task = storage.get_task(task_id)
    assert task["status"] == "SUCCEEDED"
    artifact = storage.get_artifact(task["final_artifact_id"])
    assert artifact["artifact_type"] == "answer"


def test_close_decision_closes_with_notice(tmp_path: Path):
    runtime, storage, _ = build_runtime(tmp_path)
    task_id, payload = submit_and_wait(runtime, storage)
    decide_with_artifact(runtime, storage, task_id, payload, "CLOSE", "方向不对，不做了")
    task = storage.get_task(task_id)
    assert task["status"] == "SUCCEEDED"
    artifact = storage.get_artifact(task["final_artifact_id"])
    assert artifact["artifact_type"] == "rejection_notice"


def test_max_rounds_stops_offering_reject(tmp_path: Path):
    """第 3 轮起不再提供 REJECT，decide(REJECT) 被 SDK 校验拒绝，防止无限回环。"""
    runtime, storage, _ = build_runtime(tmp_path)
    task_id, payload = submit_and_wait(runtime, storage)
    decide_with_artifact(runtime, storage, task_id, payload, "REJECT", "再写")
    round2 = latest_waiting_payload(storage, task_id)
    assert round2["round"] == 2
    assert "REJECT" in round2["allowed_decisions"]

    decide_with_artifact(runtime, storage, task_id, round2, "REJECT", "还不行")
    round3 = latest_waiting_payload(storage, task_id)
    assert round3["round"] == 3
    assert "REJECT" not in round3["allowed_decisions"]

    with pytest.raises(ValueError):
        decide_with_artifact(runtime, storage, task_id, round3, "REJECT", "超限")
    decide_with_artifact(runtime, storage, task_id, round3, "CLOSE", "结束")
    assert storage.get_task(task_id)["status"] == "SUCCEEDED"


def test_reject_round_reported_as_dynamic_steps(tmp_path: Path):
    """回环重跑时，远端上报 _r2 后缀的动态步骤 + 正确的 definition。"""
    storage = SQLAlchemyWorkflowStorage(f"sqlite:///{tmp_path / 'review.db'}", create_tables=True)
    registry = WorkflowRegistry()
    registry.register(ContentReviewWorkflow())
    llms = LLMRegistry()
    llms.register(FakeOpenAIAdapter())
    task_system = RecordingTaskSystem()
    runtime = WorkflowRuntime(
        storage,
        registry,
        InlineDispatcher(),
        task_system,
        llm_registry=llms,
        event_bus=RecordingEventBus(),
    )
    task_id, payload = submit_and_wait(runtime, storage)
    decide_with_artifact(runtime, storage, task_id, payload, "REJECT", "重写")
    round2 = latest_waiting_payload(storage, task_id)
    decide_with_artifact(runtime, storage, task_id, round2, "CONFIRM", "可以")
    assert storage.get_task(task_id)["status"] == "SUCCEEDED"

    codes = {c[1] for c in task_system.calls if c[0] == "step"}
    # 步骤码集合必须精确：一次回环 = 静态 6 步 + r2 两个动态步骤；
    # 人工门恢复复用执行记录，不允许出现恢复误报的幽灵轮次步骤。
    assert codes == {
        "analyze_request", "generate_draft", "human_review",
        "generate_draft_r2", "human_review_r2",
        "polish_final", "revise_draft", "reject_close",
    }

    gd2_defs = [c[3] for c in task_system.calls if c[0] == "step" and c[1] == "generate_draft_r2" and c[3] is not None]
    assert gd2_defs and gd2_defs[0]["name"] == "生成初稿（第2轮）"
    assert gd2_defs[0]["needConfirmation"] is False
    assert gd2_defs[0]["dependsOn"] == ["human_review"]
    hr2_defs = [c[3] for c in task_system.calls if c[0] == "step" and c[1] == "human_review_r2" and c[3] is not None]
    assert hr2_defs and hr2_defs[0]["needConfirmation"] is True
    assert hr2_defs[0]["dependsOn"] == ["generate_draft_r2"]
    # 第 1 轮静态步骤不携带 definition
    round1_defs = [c[3] for c in task_system.calls if c[0] == "step" and c[1] == "generate_draft" and c[3] is not None]
    assert round1_defs == []


def test_dynamic_step_outbox_carries_definition_verbatim(tmp_path: Path):
    """ExecutionTaskAdapter 原样透传动态步骤 definition（含 dependsOn）。"""
    from types import SimpleNamespace

    from sqlalchemy import select

    from obei_workflow_sdk.execution_task import ExecutionTaskAdapter
    from obei_workflow_sdk.models import TaskSystemOutbox

    storage = SQLAlchemyWorkflowStorage(f"sqlite:///{tmp_path / 'adapter.db'}", create_tables=True)
    task_id, run_id = storage.create_task_run("content_review", "tester", {"topic": "x", "requirements": ""})

    settings = SimpleNamespace(
        redis_url="redis://unused:6379/0",
        execution_task_execution_mode="RecordOnly",
        execution_task_output_max_bytes=8192,
    )
    adapter = ExecutionTaskAdapter(storage, settings)
    adapter._publish = lambda _task_id: None  # type: ignore[assignment]

    adapter.step_status(
        task_id, run_id, "generate_draft_r2", "生成初稿（第2轮）", "RUNNING", 2,
        definition={
            "name": "生成初稿（第2轮）",
            "stepType": "Reasoning",
            "needConfirmation": False,
            "exceptionStrategy": None,
            "dependsOn": ["human_review"],
        },
    )

    with storage.session_factory() as db:
        row = db.scalar(select(TaskSystemOutbox).where(
            TaskSystemOutbox.local_task_id == task_id,
            TaskSystemOutbox.event_type == "STEP_START",
        ))
    assert row is not None
    assert row.step_code == "generate_draft_r2"
    definition = row.payload["definition"]
    assert definition["name"] == "生成初稿（第2轮）"
    assert definition["dependsOn"] == ["human_review"]


def test_export_registration_json(tmp_path: Path):
    definition = export_workflow_definition(ContentReviewWorkflow())
    steps = {item["stepCode"]: item for item in definition}
    assert list(steps) == [
        "analyze_request",
        "generate_draft",
        "human_review",
        "polish_final",
        "revise_draft",
        "reject_close",
    ]
    assert steps["human_review"]["needConfirmation"] is True
    assert steps["polish_final"]["dependsOn"] == ["human_review"]
    assert steps["revise_draft"]["dependsOn"] == ["human_review"]
    assert steps["reject_close"]["dependsOn"] == ["human_review"]


def test_confirm_marks_untaken_branches_skipped(tmp_path: Path):
    """CONFIRM 分支走完后，未走到的 revise_draft/reject_close 必须先标 SKIPPED。"""
    storage = SQLAlchemyWorkflowStorage(f"sqlite:///{tmp_path / 'review.db'}", create_tables=True)
    registry = WorkflowRegistry()
    registry.register(ContentReviewWorkflow())
    llms = LLMRegistry()
    llms.register(FakeOpenAIAdapter())
    task_system = RecordingTaskSystem()
    runtime = WorkflowRuntime(
        storage,
        registry,
        InlineDispatcher(),
        task_system,
        llm_registry=llms,
        event_bus=RecordingEventBus(),
    )
    task_id, payload = submit_and_wait(runtime, storage)
    decide_with_artifact(runtime, storage, task_id, payload, "CONFIRM", "通过")
    assert storage.get_task(task_id)["status"] == "SUCCEEDED"

    skipped = [(c[1], i) for i, c in enumerate(task_system.calls) if c[0] == "step" and c[2] == "SKIPPED"]
    assert [code for code, _ in skipped] == ["revise_draft", "reject_close"]

    success = next(i for i, c in enumerate(task_system.calls) if c[0] == "task" and c[1] == "SUCCEEDED")
    assert all(i < success for _, i in skipped)
