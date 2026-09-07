"""LLM 工作流、远端注册定义和十五步任务系统契约的完整示例。

本文件同时展示三种典型节点：OpenAI-compatible 流式节点、Dify 阻塞节点、
以及不调用外部服务的确定性透传节点。``TaskSystemSmokeWorkflow`` 的十五步图
与远端预置模板保持一致，可用于注册 JSON 导出和真实集成冒烟测试。
"""

from typing import TypedDict

from pydantic import BaseModel, Field

from obei_workflow_sdk import Artifact, LLMNodeConfig, NodeContext, NodeOutput, StateField, TaskNotification, TaskStep, Workflow, WorkflowGraph, WorkflowInput, WorkflowNode


class WriterRequest(WorkflowInput):
    topic: str = Field(min_length=1, max_length=2000)


class WriterState(TypedDict, total=False):
    """LangGraph 在节点间传递的状态。

    total=False 允许工作流逐步补齐字段；task_id/run_id 等公共字段由 SDK 的
    ``Workflow.initial_state`` 注入，业务请求只需要提供 topic。
    """
    task_id: str
    run_id: str
    actor_id: str
    workflow_type: str
    topic: str
    answer: str
    answer_ref: str
    current_node: str


class WriterInput(BaseModel):
    topic: str


class WriterOutput(BaseModel):
    answer: str


class OpenAIStreamingWriterNode(WorkflowNode[WriterInput, WriterOutput]):
    """通过 OpenAI-compatible Adapter 流式生成文本并保存最终 Artifact。"""

    step = TaskStep(code="openai_write", name="OpenAI 兼容模型流式写作")
    # 显式声明：使用标准 OpenAI Adapter，并向 SSE 流式发送 token。
    llm_config = LLMNodeConfig(adapter="openai", stream=True, temperature=0.2, prompt_version="writer-v1")
    input_model = WriterInput
    input_fields = {"topic": StateField("topic")}
    output_fields = {"answer": StateField("answer")}

    async def execute(self, ctx: NodeContext, node_input: WriterInput) -> NodeOutput[WriterOutput]:
        answer = await ctx.llm.complete(
            "你是企业写作助手。请用简洁中文回答用户主题。",
            {"topic": node_input.topic},
            stage="writing",
        )
        return NodeOutput(
            data=WriterOutput(answer=answer),
            artifacts=[Artifact(type="answer", content=answer, content_type="text/markdown", final=True)],
            notification=TaskNotification(summary="OpenAI 兼容模型写作完成"),
        )


class DifyBlockingWriterNode(WorkflowNode[WriterInput, WriterOutput]):
    """通过 Dify Chat App 阻塞接口生成文本，不发送逐 token SSE。"""

    step = TaskStep(code="dify_write", name="Dify 非流式写作")
    # 显式声明：使用 Dify Adapter，等待 blocking 完整响应，不发送 token。
    llm_config = LLMNodeConfig(adapter="dify", stream=False)
    input_model = WriterInput
    input_fields = {"topic": StateField("topic")}
    output_fields = {"answer": StateField("answer")}

    async def execute(self, ctx: NodeContext, node_input: WriterInput) -> NodeOutput[WriterOutput]:
        answer = await ctx.llm.complete(
            "你是企业写作助手。请用简洁中文回答用户主题。",
            {"topic": node_input.topic},
            stage="writing",
        )
        return NodeOutput(
            data=WriterOutput(answer=answer),
            artifacts=[Artifact(type="answer", content=answer, content_type="text/markdown", final=True)],
            notification=TaskNotification(summary="Dify 写作完成"),
        )


class IntentRecognitionNode(OpenAIStreamingWriterNode):
    """使用远端预定义 stepCode 的真实模型节点。

    继承写作节点以复用输入/输出字段映射，但覆盖提示词和 TaskStep。远端注册后，
    stepCode 必须精确等于 ``intent_recognition``，否则步骤开始接口会返回 404。
    """

    step = TaskStep(code="intent_recognition", name="意图识别")
    llm_config = LLMNodeConfig(
        adapter="openai",
        stream=True,
        temperature=0.0,
        prompt_version="task-system-smoke-v1",
    )

    async def execute(self, ctx: NodeContext, node_input: WriterInput) -> NodeOutput[WriterOutput]:
        answer = await ctx.llm.complete(
            "识别用户输入的主要意图，并用一句简洁中文描述。",
            {"text": node_input.topic},
            stage="intent_recognition",
        )
        return NodeOutput(
            data=WriterOutput(answer=answer),
            artifacts=[Artifact(type="intent", content=answer, content_type="text/plain", final=True)],
            notification=TaskNotification(summary="意图识别完成", output={"intent": answer}),
        )


class TaskSystemPassThroughNode(WorkflowNode[WriterInput, dict]):
    """用于验证远端十五步契约的最小确定性节点。

    这些节点不伪造新的业务结果，只验证节点生命周期、数据库记录和远端步骤状态
    是否按依赖顺序正常推进。实际项目应将相同 stepCode 替换为自己的业务实现。
    ``need_confirmation`` 在此冒烟图中只作为远端注册元数据；真正需要暂停等待用户
    时应继承 HumanGateNode，让 SDK 写 Decision 并通过 Checkpoint 恢复。
    """

    input_model = WriterInput
    input_fields = {"topic": StateField("topic")}

    def __init__(self, code: str, name: str, need_confirmation: bool = False):
        self.step = TaskStep(code=code, name=name, need_confirmation=need_confirmation)

    def execute(self, ctx: NodeContext, node_input: WriterInput) -> NodeOutput[dict]:
        return NodeOutput(
            data={},
            notification=TaskNotification(
                summary=f"{self.step.name}冒烟验证完成",
                output={"verified": True},
            ),
        )


# 元组顺序就是线性 DAG 的拓扑顺序，三项依次为：稳定步骤码、展示名称、是否需要
# 远端确认。导出器不会读取这个常量，而是读取下面 build() 真正注册到图里的节点
# 和边；因此即使未来改成分支图，注册 JSON 仍以运行图为准。
TASK_SYSTEM_SMOKE_STEPS = (
    ("intent_recognition", "意图识别", False),
    ("query_rewrite", "查询改写", False),
    ("confirm_rewrite", "查询改写确认", True),
    ("data_exploration", "数据探索", False),
    ("confirm_discovery", "探索方案确认", True),
    ("sql_generation", "SQL生成", False),
    ("sql_validation", "SQL静态校验", False),
    ("sql_review", "SQL审核", False),
    ("confirm_sql", "SQL确认", True),
    ("sql_execution", "SQL执行", False),
    ("data_summary", "数据总结", False),
    ("confirm_summary", "总结确认", True),
    ("capture_confirmed_case", "案例沉淀", False),
    ("report_generation", "分析报告生成", False),
    ("html_report_generation", "HTML报告生成", False),
)


class OpenAIWriterWorkflow(Workflow):
    """单节点 OpenAI 流式工作流，适合验证模型、SSE 和审计表。"""

    workflow_type = "openai_stream_writer"
    input_model = WriterRequest
    state_model = WriterState

    def build(self, graph: WorkflowGraph):
        graph.add_node("openai_write", OpenAIStreamingWriterNode())
        graph.set_entry_point("openai_write").set_finish_point("openai_write")


class DifyWriterWorkflow(Workflow):
    """单节点 Dify 阻塞工作流，适合验证多 Adapter 切换。"""

    workflow_type = "dify_blocking_writer"
    input_model = WriterRequest
    state_model = WriterState

    def build(self, graph: WorkflowGraph):
        graph.add_node("dify_write", DifyBlockingWriterNode())
        graph.set_entry_point("dify_write").set_finish_point("dify_write")


class TaskSystemSmokeWorkflow(Workflow):
    """与远端固定模板一致的十五步线性集成工作流。

    第一个节点真实调用模型，其余节点执行最小透传。每条边均通过 WorkflowGraph
    声明，所以同一份 build() 同时控制 LangGraph 运行顺序和注册 JSON dependsOn。
    """
    workflow_type = "task_system_smoke"
    input_model = WriterRequest
    state_model = WriterState

    def build(self, graph: WorkflowGraph):
        # 首节点需要真实 LLM 行为，因此使用专用实现；其余节点可由元数据批量构造。
        graph.add_node("intent_recognition", IntentRecognitionNode())
        for code, name, need_confirmation in TASK_SYSTEM_SMOKE_STEPS[1:]:
            graph.add_node(code, TaskSystemPassThroughNode(code, name, need_confirmation))
        # 相邻元组两两连接，形成与远端 dependsOn 完全一致的线性 DAG。
        for (source, _, _), (target, _, _) in zip(TASK_SYSTEM_SMOKE_STEPS, TASK_SYSTEM_SMOKE_STEPS[1:]):
            graph.add_edge(source, target)
        # START/END 只控制 LangGraph 执行，不会出现在远端注册清单中。
        graph.set_entry_point(TASK_SYSTEM_SMOKE_STEPS[0][0])
        graph.set_finish_point(TASK_SYSTEM_SMOKE_STEPS[-1][0])
