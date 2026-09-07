from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Generic, TypeVar

from pydantic import BaseModel, ConfigDict


InputT = TypeVar("InputT", bound=BaseModel)
OutputT = TypeVar("OutputT")


class WorkflowInput(BaseModel):
    """Base class for workflow request models."""

    model_config = ConfigDict(extra="forbid")


@dataclass(frozen=True)
class StateField:
    """Maps one node input/output field to a LangGraph state key."""

    key: str


@dataclass(frozen=True)
class TaskStep:
    """一个业务节点在本地运行时和远端任务系统中的稳定描述。

    ``code`` 是最重要的契约字段：它同时用于 LangGraph 节点名、数据库
    ``node_name``、Outbox ``step_code`` 以及远端注册 JSON 的 ``stepCode``。
    已经注册到远端以后不要随意修改，否则历史 Checkpoint 和远端步骤定义将
    无法与新代码对应。

    前七个字段控制本地运行和展示；最后三个字段专门描述远端注册信息。把两类
    信息放在同一个不可变对象中，可以保证运行图与注册 JSON 来自同一事实源，
    避免维护两份流程定义后逐渐产生偏差。
    """

    # 稳定、唯一、机器可读的节点标识；建议使用小写 snake_case。
    code: str
    # 面向用户和任务系统界面的中文名称。
    name: str
    # 可选的详细说明，供宿主 UI、文档或后续注册协议扩展使用。
    description: str | None = None
    # False 表示节点只在本地执行，不产生远端步骤 Outbox。
    notify_task_system: bool = True
    # False 表示内部实现节点；注册 JSON 导出时会隐藏并折叠其前后依赖。
    visible: bool = True
    # 节点级超时提示。目前由宿主执行策略消费，不会擅自中断业务协程。
    timeout_seconds: int | None = None
    # 远端步骤分类，默认与当前任务系统协议一致使用 Reasoning。
    step_type: str = "Reasoning"
    # 普通节点可显式声明；HumanGateNode 即使未声明也会自动导出为 True。
    need_confirmation: bool = False
    # 原样写入远端 exceptionStrategy；None 会序列化为 JSON null。
    exception_strategy: Any = None


@dataclass(frozen=True)
class Artifact:
    type: str
    content: Any = None
    content_type: str = "application/json"
    url: str | None = None
    expose_as: str | None = None
    final: bool = False


@dataclass(frozen=True)
class TaskNotification:
    summary: str | None = None
    output: dict[str, Any] = field(default_factory=dict)


@dataclass
class NodeOutput(Generic[OutputT]):
    data: OutputT | None = None
    state_update: dict[str, Any] = field(default_factory=dict)
    artifacts: list[Artifact] = field(default_factory=list)
    notification: TaskNotification | None = None


@dataclass(frozen=True)
class NodeStreamChunk:
    """节点通过 ``yield`` 主动发送的流式事件。

    ``content`` 是前端最常消费的增量文本；``event_type`` 会成为 SSE 的 event
    名称。默认只发布到 Redis Stream，适合 token、进度片段等高频数据；确实需要
    断线后从数据库回放的低频业务事件可以设置 ``persist=True``。

    使用该类型而不是直接 yield 字符串，可以避免把业务最终返回值和流式片段混在
    一起，并给未来扩展序号、媒体类型等字段保留稳定契约。
    """

    content: str
    event_type: str = "node_token"
    payload: dict[str, Any] = field(default_factory=dict)
    persist: bool = False

    def __post_init__(self) -> None:
        if not self.event_type:
            raise ValueError("NodeStreamChunk.event_type cannot be empty")


class NodeContext:
    """Stable capabilities available to business nodes."""

    def __init__(self, services: Any, state: dict[str, Any], node_code: str, attempt: int, llm_config: Any = None, dify_config: Any = None):
        self._services = services
        self._state = state
        self.task_id = str(state["task_id"])
        self.run_id = str(state["run_id"])
        self.actor_id = str(state.get("actor_id", ""))
        self.workflow_type = str(state.get("workflow_type", ""))
        self.node_code = node_code
        self.attempt = attempt
        self._llm_config = llm_config
        self._dify_config = dify_config

    @property
    def llm(self):
        if self._llm_config is None:
            raise RuntimeError("this node did not explicitly declare LLMNodeConfig(adapter=..., stream=...)")
        if self._services.llm_registry is None:
            raise RuntimeError("LLM registry is not configured")
        from .llm import BoundLLMClient
        return BoundLLMClient(self._services, self, self._llm_config)

    @property
    def dify(self):
        """Return the stateful Dify Chat App facade declared by this node."""

        if self._dify_config is None:
            raise RuntimeError("this node did not explicitly declare DifyNodeConfig(...)")
        if self._services.dify_registry is None:
            raise RuntimeError("Dify app registry is not configured")
        from .dify import BoundDifyClient

        return BoundDifyClient(self._services, self, self._dify_config)

    def progress(self, percent: int, message: str, **payload: Any) -> None:
        self._services.event(
            self.task_id,
            self.run_id,
            "node_progress",
            node_name=self.node_code,
            status="RUNNING",
            payload={"percent": percent, "message": message, **payload},
        )

    def emit(self, event_type: str, **payload: Any) -> None:
        self._services.event(
            self.task_id,
            self.run_id,
            event_type,
            node_name=self.node_code,
            payload=payload,
        )

    def artifact(self, artifact_id: str) -> dict[str, Any] | None:
        return self._services.storage.get_artifact(artifact_id)

    def raise_if_cancelled(self) -> None:
        if self._services.storage.is_cancel_requested(self.task_id):
            raise WorkflowCancelled("workflow cancellation requested")


class WorkflowCancelled(RuntimeError):
    pass
