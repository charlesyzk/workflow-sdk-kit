"""内容复核示例的基础设施无关测试：三条决策路径 + 注册 JSON 导出。"""

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

    def step_status(self, task_id, run_id, step_code, step_name, status, attempt, payload=None):
        self.calls.append(("step", step_code, status))
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


def test_reject_path_closes_with_notice(tmp_path: Path):
    runtime, storage, _ = build_runtime(tmp_path)
    task_id, payload = submit_and_wait(runtime, storage)
    decide_with_artifact(runtime, storage, task_id, payload, "REJECT", "方向不对")
    task = storage.get_task(task_id)
    assert task["status"] == "SUCCEEDED"
    artifact = storage.get_artifact(task["final_artifact_id"])
    assert artifact["artifact_type"] == "rejection_notice"


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
