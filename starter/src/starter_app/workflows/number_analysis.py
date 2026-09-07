from __future__ import annotations

import time
from typing import TypedDict

from pydantic import BaseModel, Field

from obei_workflow_sdk import (
    Artifact,
    NodeContext,
    NodeOutput,
    StateField,
    TaskNotification,
    TaskStep,
    Workflow,
    WorkflowGraph,
    WorkflowInput,
    WorkflowNode,
)


class NumberAnalysisRequest(WorkflowInput):
    numbers: list[float] = Field(min_length=1, max_length=1000)
    multiplier: float = 1.0
    delay_seconds: float = Field(default=1.0, ge=0, le=10)


class NumberAnalysisState(TypedDict, total=False):
    task_id: str
    run_id: str
    actor_id: str
    workflow_type: str
    numbers: list[float]
    multiplier: float
    delay_seconds: float
    normalized_numbers: list[float]
    count: int
    total: float
    average: float
    analysis_result_ref: str
    report_ref: str
    current_node: str


class NormalizeInput(BaseModel):
    numbers: list[float]
    multiplier: float


class NormalizeOutput(BaseModel):
    normalized_numbers: list[float]


class NormalizeNumbersNode(WorkflowNode[NormalizeInput, NormalizeOutput]):
    step = TaskStep(code="normalize_numbers", name="标准化数字")
    input_model = NormalizeInput
    input_fields = {
        "numbers": StateField("numbers"),
        "multiplier": StateField("multiplier"),
    }
    output_fields = {"normalized_numbers": StateField("normalized_numbers")}

    def execute(self, ctx: NodeContext, node_input: NormalizeInput) -> NodeOutput[NormalizeOutput]:
        ctx.progress(20, "正在应用乘数")
        values = [round(value * node_input.multiplier, 4) for value in node_input.numbers]
        return NodeOutput(data=NormalizeOutput(normalized_numbers=values))


class CalculateInput(BaseModel):
    normalized_numbers: list[float]
    delay_seconds: float


class CalculateOutput(BaseModel):
    count: int
    total: float
    average: float


class CalculateStatisticsNode(WorkflowNode[CalculateInput, CalculateOutput]):
    step = TaskStep(code="calculate_statistics", name="计算统计结果")
    input_model = CalculateInput
    input_fields = {
        "normalized_numbers": StateField("normalized_numbers"),
        "delay_seconds": StateField("delay_seconds"),
    }
    output_fields = {
        "count": StateField("count"),
        "total": StateField("total"),
        "average": StateField("average"),
    }

    def execute(self, ctx: NodeContext, node_input: CalculateInput) -> NodeOutput[CalculateOutput]:
        # 纯代码节点也在后台 Worker/线程执行。延迟只用于演示 HTTP 已经先返回 202。
        time.sleep(node_input.delay_seconds)
        ctx.raise_if_cancelled()
        count = len(node_input.normalized_numbers)
        total = round(sum(node_input.normalized_numbers), 4)
        average = round(total / count, 4)
        result = CalculateOutput(count=count, total=total, average=average)
        return NodeOutput(
            data=result,
            artifacts=[Artifact(type="analysis_result", content=result.model_dump())],
            notification=TaskNotification(
                summary=f"完成 {count} 个数字的统计",
                output=result.model_dump(),
            ),
        )


class ReportInput(BaseModel):
    count: int
    total: float
    average: float


class GenerateReportNode(WorkflowNode[ReportInput, dict]):
    step = TaskStep(code="generate_report", name="生成最终报告")
    input_model = ReportInput
    input_fields = {
        "count": StateField("count"),
        "total": StateField("total"),
        "average": StateField("average"),
    }

    def execute(self, ctx: NodeContext, node_input: ReportInput) -> NodeOutput[dict]:
        report = (
            "# 数字分析报告\n\n"
            f"- 数量：{node_input.count}\n"
            f"- 总和：{node_input.total}\n"
            f"- 平均值：{node_input.average}\n"
        )
        return NodeOutput(
            artifacts=[Artifact(
                type="report",
                content=report,
                content_type="text/markdown",
                final=True,
            )],
            notification=TaskNotification(summary="最终报告已生成"),
        )


class NumberAnalysisWorkflow(Workflow):
    workflow_type = "number_analysis"
    version = "1.0"
    input_model = NumberAnalysisRequest
    state_model = NumberAnalysisState

    def build(self, graph: WorkflowGraph) -> None:
        graph.add_node("normalize_numbers", NormalizeNumbersNode())
        graph.add_node("calculate_statistics", CalculateStatisticsNode())
        graph.add_node("generate_report", GenerateReportNode())
        graph.set_entry_point("normalize_numbers")
        graph.add_edge("normalize_numbers", "calculate_statistics")
        graph.add_edge("calculate_statistics", "generate_report")
        graph.set_finish_point("generate_report")

