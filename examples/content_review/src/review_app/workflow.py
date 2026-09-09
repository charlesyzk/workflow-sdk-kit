"""内容生成与人工复核示例工作流（展示流式 LLM + HumanGate + 三路条件分支）。

执行链路：
    analyze_request → generate_draft(流式) → human_review(人工暂停)
        ├─ CONFIRM → polish_final（非流式定稿）
        ├─ REJECT  → reject_close（记录驳回并关闭）
        └─ REVISE  → revise_draft（带反馈流式修订）

远端注册 JSON 由同一份 build() 生成：human_review 自动 needConfirmation=true，
三条分支的 dependsOn 都来自真实条件边。
"""

from __future__ import annotations

from typing import AsyncIterator, TypedDict

from pydantic import BaseModel, Field

from obei_workflow_sdk import (
    Artifact,
    HumanGateNode,
    LLMNodeConfig,
    NodeContext,
    NodeOutput,
    NodeStreamChunk,
    StateField,
    TaskNotification,
    TaskStep,
    Workflow,
    WorkflowGraph,
    WorkflowInput,
    WorkflowNode,
)


class ReviewRequest(WorkflowInput):
    """HTTP 提交参数。"""

    topic: str = Field(min_length=1, max_length=500, description="内容主题")
    requirements: str = Field(default="", max_length=2000, description="附加要求（可选）")


class ReviewState(TypedDict, total=False):
    """工作流共享状态。"""

    task_id: str
    run_id: str
    actor_id: str
    workflow_type: str
    current_node: str
    topic: str
    requirements: str
    analysis: str
    draft: str
    draft_ref: str
    last_decision: dict
    feedback: str
    answer: str
    reason: str


class AnalyzeInput(BaseModel):
    topic: str
    requirements: str


class AnalyzeOutput(BaseModel):
    analysis: str


class AnalyzeRequestNode(WorkflowNode[AnalyzeInput, AnalyzeOutput]):
    """第一环节：纯函数节点，用自定义流展示上下文构建过程。"""

    step = TaskStep(code="analyze_request", name="分析需求", step_type="Reasoning")
    input_model = AnalyzeInput
    input_fields = {
        "topic": StateField("topic"),
        "requirements": StateField("requirements"),
    }
    output_fields = {"analysis": StateField("analysis")}

    async def execute(
        self,
        ctx: NodeContext,
        node_input: AnalyzeInput,
    ) -> AsyncIterator[NodeStreamChunk | NodeOutput[AnalyzeOutput]]:
        yield NodeStreamChunk(
            "正在分析内容需求",
            event_type="context_token",
            payload={"stage": "start"},
        )
        ctx.raise_if_cancelled()
        analysis = f"内容主题：{node_input.topic}"
        if node_input.requirements:
            analysis += f"\n附加要求：{node_input.requirements}"
        yield NodeStreamChunk(
            "需求分析完成",
            event_type="context_token",
            payload={"stage": "done"},
        )
        yield NodeOutput(data=AnalyzeOutput(analysis=analysis))


class DraftInput(BaseModel):
    analysis: str


class DraftOutput(BaseModel):
    draft: str


class GenerateDraftNode(WorkflowNode[DraftInput, DraftOutput]):
    """第二环节：调用生产模型流式生成初稿，SDK 自动转发逐字 token。"""

    step = TaskStep(code="generate_draft", name="生成初稿", step_type="Reasoning")
    llm_config = LLMNodeConfig(
        adapter="openai",
        stream=True,
        temperature=0.3,
        prompt_version="content-review-draft-v1",
    )
    input_model = DraftInput
    input_fields = {"analysis": StateField("analysis")}
    output_fields = {"draft": StateField("draft")}

    async def execute(
        self,
        ctx: NodeContext,
        node_input: DraftInput,
    ) -> NodeOutput[DraftOutput]:
        draft = await ctx.llm.complete(
            "你是内容创作助手。请根据需求分析生成一篇中文初稿，语言自然、结构清晰。",
            {"analysis": node_input.analysis},
            stage="draft_generation",
        )
        return NodeOutput(
            data=DraftOutput(draft=draft),
            artifacts=[
                Artifact(type="draft", content=draft, content_type="text/markdown")
            ],
            notification=TaskNotification(
                summary="初稿生成完成，等待人工复核",
                output={"draft": draft},
            ),
        )


class ReviewGateInput(BaseModel):
    draft: str


class ReviewGate(HumanGateNode):
    """第三环节：人工复核。任务在此暂停，等待 API 提交决策后恢复。

    决策值：CONFIRM（通过）/ REJECT（驳回）/ REVISE（修订）。
    复核人可通过 draft_ref 引用的 Artifact 查看初稿全文。
    """

    step = TaskStep(code="human_review", name="人工复核初稿", step_type="Reasoning")
    input_model = ReviewGateInput
    input_fields = {"draft": StateField("draft")}
    artifact_ref_field = "draft_ref"
    allowed_decisions = ("CONFIRM", "REJECT", "REVISE")

    def execute(
        self,
        ctx: NodeContext,
        node_input: ReviewGateInput,
    ) -> NodeOutput[dict]:
        outcome = super().execute(ctx, node_input)
        decision = (outcome.state_update or {}).get("last_decision") or {}
        # 把反馈提升为顶层状态键，便于后续节点用 StateField 声明式读取。
        return NodeOutput(
            state_update={
                "last_decision": decision,
                "feedback": decision.get("feedback", ""),
            }
        )


def route_after_review(state: ReviewState) -> str:
    """按人工决策返回路由键；path_map 负责把键映射到目标节点。"""
    decision = (state.get("last_decision") or {}).get("decision", "REJECT")
    if decision not in ("CONFIRM", "REJECT", "REVISE"):
        return "REJECT"
    return decision


class PolishInput(BaseModel):
    draft: str


class PolishOutput(BaseModel):
    answer: str


class PolishFinalNode(WorkflowNode[PolishInput, PolishOutput]):
    """CONFIRM 分支：非流式润色定稿。"""

    step = TaskStep(code="polish_final", name="润色定稿", step_type="Reasoning")
    llm_config = LLMNodeConfig(
        adapter="openai",
        stream=False,
        temperature=0.1,
        prompt_version="content-review-polish-v1",
    )
    input_model = PolishInput
    input_fields = {"draft": StateField("draft")}
    output_fields = {"answer": StateField("answer")}

    async def execute(
        self,
        ctx: NodeContext,
        node_input: PolishInput,
    ) -> NodeOutput[PolishOutput]:
        answer = await ctx.llm.complete(
            "请润色定稿：保持事实不变，输出简洁、完整的最终中文内容。",
            {"draft": node_input.draft},
            stage="final_polish",
            provider_options={"enable_thinking": False, "max_tokens": 1024},
        )
        return NodeOutput(
            data=PolishOutput(answer=answer),
            artifacts=[
                Artifact(
                    type="answer",
                    content=answer,
                    content_type="text/markdown",
                    final=True,
                )
            ],
            notification=TaskNotification(
                summary="复核通过，定稿完成",
                output={"answer": answer},
            ),
        )


class ReviseInput(BaseModel):
    draft: str
    feedback: str


class ReviseOutput(BaseModel):
    answer: str


class ReviseDraftNode(WorkflowNode[ReviseInput, ReviseOutput]):
    """REVISE 分支：携带复核反馈流式修订，产出修订版作为最终内容。"""

    step = TaskStep(code="revise_draft", name="按反馈修订", step_type="Reasoning")
    llm_config = LLMNodeConfig(
        adapter="openai",
        stream=True,
        temperature=0.2,
        prompt_version="content-review-revise-v1",
    )
    input_model = ReviseInput
    input_fields = {
        "draft": StateField("draft"),
        "feedback": StateField("feedback"),
    }
    output_fields = {"answer": StateField("answer")}

    async def execute(
        self,
        ctx: NodeContext,
        node_input: ReviseInput,
    ) -> NodeOutput[ReviseOutput]:
        revised = await ctx.llm.complete(
            "请根据复核反馈修订初稿。必须逐条回应反馈意见，输出修订后的完整中文内容。",
            {"draft": node_input.draft, "feedback": node_input.feedback or "（无具体反馈）"},
            stage="revision",
        )
        return NodeOutput(
            data=ReviseOutput(answer=revised),
            artifacts=[
                Artifact(
                    type="revised_answer",
                    content=revised,
                    content_type="text/markdown",
                    final=True,
                )
            ],
            notification=TaskNotification(
                summary="已按反馈完成修订",
                output={"answer": revised},
            ),
        )


class RejectInput(BaseModel):
    draft: str
    feedback: str


class RejectOutput(BaseModel):
    reason: str


class RejectCloseNode(WorkflowNode[RejectInput, RejectOutput]):
    """REJECT 分支：纯函数节点，记录驳回说明并关闭任务。"""

    step = TaskStep(code="reject_close", name="驳回关闭", step_type="Reasoning")
    input_model = RejectInput
    input_fields = {
        "draft": StateField("draft"),
        "feedback": StateField("feedback"),
    }
    output_fields = {"reason": StateField("reason")}

    def execute(
        self,
        ctx: NodeContext,
        node_input: RejectInput,
    ) -> NodeOutput[RejectOutput]:
        ctx.raise_if_cancelled()
        reason = f"初稿被人工驳回。反馈：{node_input.feedback or '（未填写）'}"
        return NodeOutput(
            data=RejectOutput(reason=reason),
            artifacts=[
                Artifact(
                    type="rejection_notice",
                    content=reason,
                    content_type="text/plain",
                    final=True,
                )
            ],
            notification=TaskNotification(
                summary="初稿被驳回，任务关闭",
                output={"reason": reason},
            ),
        )


class ContentReviewWorkflow(Workflow):
    """六节点图：流式生成 + 人工门 + 三路决策分支。"""

    workflow_type = "content_review"
    version = "1.0"
    input_model = ReviewRequest
    state_model = ReviewState

    def build(self, graph: WorkflowGraph) -> None:
        graph.add_node("analyze_request", AnalyzeRequestNode())
        graph.add_node("generate_draft", GenerateDraftNode())
        graph.add_node("human_review", ReviewGate())
        graph.add_node("polish_final", PolishFinalNode())
        graph.add_node("revise_draft", ReviseDraftNode())
        graph.add_node("reject_close", RejectCloseNode())
        graph.set_entry_point("analyze_request")
        graph.add_edge("analyze_request", "generate_draft")
        graph.add_edge("generate_draft", "human_review")
        graph.add_conditional_edges(
            "human_review",
            route_after_review,
            path_map={
                "CONFIRM": "polish_final",
                "REJECT": "reject_close",
                "REVISE": "revise_draft",
            },
        )
        graph.set_finish_point("polish_final")
        graph.set_finish_point("revise_draft")
        graph.set_finish_point("reject_close")
