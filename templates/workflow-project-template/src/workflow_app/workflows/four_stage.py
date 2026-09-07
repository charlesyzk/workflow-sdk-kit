"""模板自带的四环节工作流。

这个文件不是只能运行一次的演示脚本，而是业务团队复制后继续开发的参考实现。
它刻意把四种最常见的节点形态放在同一条线性工作流中：

1. 同步纯函数节点；
2. 不调用 LLM、但通过 yield 主动输出业务流的节点；
3. 调用 LLM 并使用供应商流式接口的节点；
4. 调用 LLM、等待完整响应并持久化最终 Artifact 的节点。

使用者可以先运行测试确认模板正常，再逐个替换节点的输入模型、业务逻辑、Prompt
和边。注册 JSON 始终从本文件的真实图生成，因此不要另外手工维护一份依赖配置。
"""

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


class FourStageRequest(WorkflowInput):
    """POST /api/v1/tasks 中 input 字段对应的业务请求。"""

    question: str = Field(
        min_length=1,
        max_length=2000,
        description="需要工作流回答的问题",
    )


class FourStageState(TypedDict, total=False):
    """LangGraph 节点共享状态。

    ``total=False`` 很重要：初始状态只有请求字段，后续字段由各节点逐步写入。前五
    个字段由 SDK 维护；从 question 开始是业务字段。业务节点应通过 StateField
    声明读写关系，不要依赖整份 State 的隐式结构。
    """

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


# ---------------------------------------------------------------------------
# 第一环节：同步纯函数，不调用 LLM，不流式
# ---------------------------------------------------------------------------


class ValidateInput(BaseModel):
    question: str


class ValidateOutput(BaseModel):
    validated_question: str


class ValidateInputNode(WorkflowNode[ValidateInput, ValidateOutput]):
    """整理输入的同步纯函数节点。

    短时间、无 I/O 的转换适合普通 ``def``。网络请求、文件 I/O 或长时间等待应
    改用 ``async def``，避免阻塞 ARQ Worker 的事件循环。
    """

    step = TaskStep(
        # code 同时是图节点名、数据库 node_name、远端 stepCode 和 Outbox 标识；
        # 一旦登记到远端，就应把它当成稳定协议字段。
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
        # 协作式取消需要在循环或外部调用边界主动检查；SDK 不会强行终止线程。
        ctx.raise_if_cancelled()
        normalized = " ".join(node_input.question.split())
        if not normalized:
            raise ValueError("question cannot be blank")
        return NodeOutput(data=ValidateOutput(validated_question=normalized))


# ---------------------------------------------------------------------------
# 第二环节：普通业务节点，自定义 yield 流，不调用 LLM
# ---------------------------------------------------------------------------


class PrepareContextInput(BaseModel):
    validated_question: str


class PrepareContextOutput(BaseModel):
    prepared_context: str


class PrepareContextNode(
    WorkflowNode[PrepareContextInput, PrepareContextOutput]
):
    """构造模型上下文，并让前端实时看到业务处理片段。"""

    step = TaskStep(
        code="prepare_context",
        name="准备上下文",
        step_type="Reasoning",
    )
    input_model = PrepareContextInput
    input_fields = {
        "validated_question": StateField("validated_question"),
    }
    output_fields = {
        "prepared_context": StateField("prepared_context"),
    }

    async def execute(
        self,
        ctx: NodeContext,
        node_input: PrepareContextInput,
    ) -> AsyncIterator[NodeStreamChunk | NodeOutput[PrepareContextOutput]]:
        # NodeStreamChunk 默认 persist=False，只发送到 Redis Stream/SSE，适合高频
        # 片段。设置 persist=True 会额外写入数据库事件表，只应用于低频关键事实。
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

        # async generator 不能 ``return NodeOutput``。SDK 约定生成器必须且只能
        # yield 一个最终 NodeOutput；缺失、重复或 yield 普通字符串都会明确报错。
        yield NodeOutput(
            data=PrepareContextOutput(prepared_context=prepared_context)
        )


# ---------------------------------------------------------------------------
# 第三环节：流式 LLM，SDK 自动发布 token
# ---------------------------------------------------------------------------


class GenerateDraftInput(BaseModel):
    prepared_context: str


class GenerateDraftOutput(BaseModel):
    draft: str


class GenerateDraftNode(WorkflowNode[GenerateDraftInput, GenerateDraftOutput]):
    """使用 OpenAI-compatible 流式接口生成草稿。"""

    step = TaskStep(
        code="generate_draft",
        name="流式生成草稿",
        step_type="Reasoning",
    )
    llm_config = LLMNodeConfig(
        # adapter 名称对应宿主注册的 LLM Adapter；模板默认由环境变量创建 openai。
        adapter="openai",
        # True 表示调用供应商 SSE 接口，正文和思考内容会分别形成 llm_token 与
        # llm_reasoning_token；最终完整响应仍会返回给节点并写入审计表。
        stream=True,
        temperature=0.2,
        prompt_version="template-draft-v1",
    )
    input_model = GenerateDraftInput
    input_fields = {
        "prepared_context": StateField("prepared_context"),
    }
    output_fields = {"draft": StateField("draft")}

    async def execute(
        self,
        ctx: NodeContext,
        node_input: GenerateDraftInput,
    ) -> NodeOutput[GenerateDraftOutput]:
        draft = await ctx.llm.complete(
            "你是企业助手。请根据上下文生成一版中文回答草稿。",
            {"context": node_input.prepared_context},
            stage="draft_generation",
        )
        return NodeOutput(
            data=GenerateDraftOutput(draft=draft),
            # 草稿是可追踪的中间产物，但 final=False，不会成为任务最终结果。
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


# ---------------------------------------------------------------------------
# 第四环节：非流式 LLM，等待完整响应
# ---------------------------------------------------------------------------


class PolishAnswerInput(BaseModel):
    draft: str


class PolishAnswerOutput(BaseModel):
    answer: str


class PolishAnswerNode(WorkflowNode[PolishAnswerInput, PolishAnswerOutput]):
    """关闭模型流式接口，生成并持久化最终回答。"""

    step = TaskStep(
        code="polish_answer",
        name="非流式润色回答",
        step_type="Reasoning",
    )
    llm_config = LLMNodeConfig(
        adapter="openai",
        # False 是显式契约：该调用不会发布 llm_token，只有完整响应返回后才继续。
        stream=False,
        temperature=0.1,
        prompt_version="template-polish-v1",
    )
    input_model = PolishAnswerInput
    input_fields = {"draft": StateField("draft")}
    output_fields = {"answer": StateField("answer")}

    async def execute(
        self,
        ctx: NodeContext,
        node_input: PolishAnswerInput,
    ) -> NodeOutput[PolishAnswerOutput]:
        answer = await ctx.llm.complete(
            "请润色草稿，保持事实不变，输出简洁、完整的最终中文回答。",
            {"draft": node_input.draft},
            stage="answer_polish",
            # provider_options 会原样并入供应商请求体。qwen3 系列在简单润色场景
            # 可关闭深度思考以降低阻塞式调用耗时；其他模型不支持该参数时请删除。
            provider_options={"enable_thinking": False, "max_tokens": 512},
        )
        return NodeOutput(
            data=PolishAnswerOutput(answer=answer),
            artifacts=[
                Artifact(
                    type="answer",
                    content=answer,
                    content_type="text/markdown",
                    # final=True 会把 Artifact ID 写入 Task.final_artifact_id。
                    final=True,
                )
            ],
            notification=TaskNotification(
                summary="最终回答润色完成",
                output={"answer": answer},
            ),
        )


class FourStageWorkflow(Workflow):
    """把四个节点编排为一条线性工作流。"""

    # workflow_type 会出现在 HTTP 请求、数据库、ARQ 消息和远端登记中。登记后
    # 不应随意修改；复制模板开发新流程时，应在第一次登记前换成业务稳定标识。
    workflow_type = "simple_llm_workflow"
    version = "1.0"
    input_model = FourStageRequest
    state_model = FourStageState

    def build(self, graph: WorkflowGraph) -> None:
        # add_node 的名称必须与 node.step.code 完全一致，SDK 会在构图时校验。
        graph.add_node("validate_input", ValidateInputNode())
        graph.add_node("prepare_context", PrepareContextNode())
        graph.add_node("generate_draft", GenerateDraftNode())
        graph.add_node("polish_answer", PolishAnswerNode())

        graph.set_entry_point("validate_input")
        graph.add_edge("validate_input", "prepare_context")
        graph.add_edge("prepare_context", "generate_draft")
        graph.add_edge("generate_draft", "polish_answer")
        graph.set_finish_point("polish_answer")
