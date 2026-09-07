"""覆盖四种节点执行方式的完整示例工作流。"""

from __future__ import annotations

from typing import AsyncIterator, TypedDict

from pydantic import BaseModel, Field

from obei_workflow_sdk import (
    Artifact,
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


class SimpleWorkflowRequest(WorkflowInput):
    """HTTP 提交参数；extra=forbid 由 WorkflowInput 统一控制。"""

    question: str = Field(min_length=1, max_length=2000)


class SimpleWorkflowState(TypedDict, total=False):
    """工作流共享状态；SDK 公共字段和业务字段放在同一个 TypedDict 中。"""

    task_id: str
    run_id: str
    actor_id: str
    workflow_type: str
    current_node: str
    question: str
    validated_question: str
    prepared_context: str
    draft: str
    answer: str
    answer_ref: str


class ValidateInput(BaseModel):
    question: str


class ValidateOutput(BaseModel):
    validated_question: str


class ValidateInputNode(WorkflowNode[ValidateInput, ValidateOutput]):
    """第一环节：不调用 LLM、不流式输出的同步纯函数节点。"""

    step = TaskStep(
        code="validate_input",
        name="校验输入",
        step_type="Reasoning",
    )
    input_model = ValidateInput
    input_fields = {"question": StateField("question")}
    output_fields = {"validated_question": StateField("validated_question")}

    def execute(
        self,
        ctx: NodeContext,
        node_input: ValidateInput,
    ) -> NodeOutput[ValidateOutput]:
        ctx.raise_if_cancelled()
        normalized = " ".join(node_input.question.split())
        if not normalized:
            raise ValueError("question cannot be blank")
        return NodeOutput(data=ValidateOutput(validated_question=normalized))


class ContextInput(BaseModel):
    validated_question: str


class ContextOutput(BaseModel):
    prepared_context: str


class PrepareContextNode(WorkflowNode[ContextInput, ContextOutput]):
    """第二环节：不调用 LLM、使用 yield 自定义流的纯函数节点。"""

    step = TaskStep(
        code="prepare_context",
        name="准备上下文",
        step_type="Reasoning",
    )
    input_model = ContextInput
    input_fields = {"validated_question": StateField("validated_question")}
    output_fields = {"prepared_context": StateField("prepared_context")}

    async def execute(
        self,
        ctx: NodeContext,
        node_input: ContextInput,
    ) -> AsyncIterator[NodeStreamChunk | NodeOutput[ContextOutput]]:
        # 默认 persist=False：高频片段只进 Redis Stream/SSE，不逐条写 TiDB。
        yield NodeStreamChunk(
            "正在读取已校验问题",
            event_type="context_token",
            payload={"stage": "read"},
        )

        ctx.raise_if_cancelled()
        prepared_context = (
            f"用户问题：{node_input.validated_question}\n"
            "回答要求：内容准确、语言简洁，并给出一个明确结论。"
        )

        yield NodeStreamChunk(
            "模型上下文准备完成",
            event_type="context_token",
            payload={"stage": "complete"},
        )

        # async generator 不能 return NodeOutput，必须把唯一最终结果作为最后一项 yield。
        yield NodeOutput(data=ContextOutput(prepared_context=prepared_context))


class DraftInput(BaseModel):
    prepared_context: str


class DraftOutput(BaseModel):
    draft: str


class GenerateDraftNode(WorkflowNode[DraftInput, DraftOutput]):
    """第三环节：调用 LLM，并由 SDK 自动转发模型 token 流。"""

    step = TaskStep(
        code="generate_draft",
        name="流式生成草稿",
        step_type="Reasoning",
    )
    llm_config = LLMNodeConfig(
        adapter="openai",
        stream=True,
        temperature=0.2,
        prompt_version="simple-draft-v1",
    )
    input_model = DraftInput
    input_fields = {
        "prepared_context": StateField("prepared_context"),
    }
    output_fields = {"draft": StateField("draft")}

    async def execute(
        self,
        ctx: NodeContext,
        node_input: DraftInput,
    ) -> NodeOutput[DraftOutput]:
        # stream=True 时 Adapter 使用 SSE；正文发布为 llm_token，推理内容发布为
        # llm_reasoning_token。complete() 仍返回完整正文，便于持久化最终结果。
        draft = await ctx.llm.complete(
            "你是企业助手。请根据上下文生成一版中文回答草稿。",
            {"context": node_input.prepared_context},
            stage="draft_generation",
        )
        return NodeOutput(
            data=DraftOutput(draft=draft),
            artifacts=[
                Artifact(
                    type="draft",
                    content=draft,
                    content_type="text/markdown",
                )
            ],
            notification=TaskNotification(
                summary="回答草稿生成完成",
                output={"draft": draft},
            ),
        )


class PolishInput(BaseModel):
    draft: str


class PolishOutput(BaseModel):
    answer: str


class PolishAnswerNode(WorkflowNode[PolishInput, PolishOutput]):
    """第四环节：调用同一 LLM，但关闭流式并等待完整最终回答。"""

    step = TaskStep(
        code="polish_answer",
        name="非流式润色回答",
        step_type="Reasoning",
    )
    llm_config = LLMNodeConfig(
        adapter="openai",
        stream=False,
        temperature=0.1,
        prompt_version="simple-polish-v1",
    )
    input_model = PolishInput
    input_fields = {"draft": StateField("draft")}
    output_fields = {"answer": StateField("answer")}

    async def execute(
        self,
        ctx: NodeContext,
        node_input: PolishInput,
    ) -> NodeOutput[PolishOutput]:
        # stream=False 时不会产生 llm_token/llm_reasoning_token，调用会在供应商返回
        # 完整响应后继续。最终调用仍完整写入 LLM 审计表。
        answer = await ctx.llm.complete(
            "请润色草稿，保持事实不变，输出简洁、完整的最终中文回答。",
            {"draft": node_input.draft},
            stage="answer_polish",
            # 这是供应商透传参数，不会改变 SDK 的 stream=False 语义。
            # qwen3 系列默认可能先执行较长时间的深度思考；对于只做文字润色的
            # 演示节点没有必要启用该能力。关闭思考并限制输出长度，可以让阻塞式
            # 请求稳定地在网关超时前返回。其他 OpenAI-compatible 模型如果不支持
            # enable_thinking，应删除该字段，只保留模型支持的参数。
            provider_options={"enable_thinking": False, "max_tokens": 512},
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
                summary="最终回答润色完成",
                output={"answer": answer},
            ),
        )


class SimpleLLMWorkflow(Workflow):
    """四节点线性图；覆盖普通/流式纯函数和流式/非流式 LLM。"""

    workflow_type = "simple_llm_workflow"
    version = "1.0"
    input_model = SimpleWorkflowRequest
    state_model = SimpleWorkflowState

    def build(self, graph: WorkflowGraph) -> None:
        graph.add_node("validate_input", ValidateInputNode())
        graph.add_node("prepare_context", PrepareContextNode())
        graph.add_node("generate_draft", GenerateDraftNode())
        graph.add_node("polish_answer", PolishAnswerNode())
        graph.set_entry_point("validate_input")
        graph.add_edge("validate_input", "prepare_context")
        graph.add_edge("prepare_context", "generate_draft")
        graph.add_edge("generate_draft", "polish_answer")
        graph.set_finish_point("polish_answer")
