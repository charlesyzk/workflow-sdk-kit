from __future__ import annotations

import inspect
import asyncio
import json
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Generic, TypeVar
from contextlib import asynccontextmanager, contextmanager
from contextvars import ContextVar

from langgraph.graph import END, START, StateGraph
from langgraph.errors import GraphInterrupt
from langgraph.types import interrupt
from pydantic import BaseModel

from .contracts import NodeContext, NodeOutput, NodeStreamChunk, StateField, TaskStep, WorkflowInput
from .llm import LLMNodeConfig, LLMRequest
from .dify import DifyNodeConfig, DifyRequest
from .run_lock import RedisDifyConversationLock, WorkflowRunLockLost


NodeInputT = TypeVar("NodeInputT", bound=BaseModel)
NodeDataT = TypeVar("NodeDataT")
_RUN_OWNERSHIP_GUARD: ContextVar[Any] = ContextVar("workflow_run_ownership_guard", default=None)


@dataclass
class RuntimeServices:
    """节点运行时可使用的基础设施门面，统一事件、LLM 审计与任务系统通知。"""
    storage: Any
    task_system: Any
    event_bus: Any = None
    llm_registry: Any = None
    dify_registry: Any = None
    settings: Any = None

    @contextmanager
    def run_ownership(self, guard: Any):
        token = _RUN_OWNERSHIP_GUARD.set(guard)
        try:
            yield
        finally:
            _RUN_OWNERSHIP_GUARD.reset(token)

    def ensure_run_owned(self) -> None:
        guard = _RUN_OWNERSHIP_GUARD.get()
        if guard is not None:
            guard.ensure_owned()

    def event(self, task_id: str, run_id: str, event_type: str, **kwargs: Any) -> int:
        """先持久化业务事件，再发布实时副本；数据库序号用于断线续传。"""
        sequence = self.storage.append_event(task_id, run_id, event_type, **kwargs)
        if self.event_bus is not None:
            self.event_bus.publish(task_id, {"db_event_seq": sequence, "type": event_type, **kwargs})
        return sequence

    def transient_event(self, task_id: str, event_type: str, **payload: Any) -> None:
        """发布无需落库的高频事件，主要用于流式模型 token。"""
        if self.event_bus is not None:
            self.event_bus.publish(task_id, {"type": event_type, **payload})

    async def invoke_llm(self, context: NodeContext, adapter_name: str, request: LLMRequest, *, stage: str | None = None):
        """执行 Adapter 调用，并保证成功、失败都完成 LLM 审计记录。"""
        self.ensure_run_owned()
        adapter = self.llm_registry.get(adapter_name)
        model = request.model or adapter.default_model
        invocation_id = self.storage.start_llm_invocation(
            context.task_id, context.run_id, context.node_code, adapter_name, model,
            request.stream,
            {"messages": request.messages, "temperature": request.temperature, "response_format": request.response_format, "provider_options": request.provider_options},
            context._llm_config.prompt_version,
        )
        self.transient_event(context.task_id, "llm_start", node_name=context.node_code, stage=stage, invocation_id=invocation_id, source="direct_model", provider=adapter_name, model=model, stream=request.stream)

        async def on_chunk(chunk):
            # Redis 客户端是同步实现，移到线程可避免阻塞 LangGraph 的事件循环。
            await asyncio.to_thread(
                self.transient_event,
                context.task_id,
                "llm_reasoning_token" if chunk.kind == "reasoning" else "llm_token",
                node_name=context.node_code, stage=stage, invocation_id=invocation_id, source="direct_model", provider=adapter_name, content=chunk.content,
            )

        try:
            response = await adapter.generate(request, on_chunk=on_chunk if request.stream else None)
            self.ensure_run_owned()
            self.storage.finish_llm_invocation(invocation_id, response=response.content, reasoning=response.reasoning, usage=response.usage, provider_request_id=response.provider_request_id)
            if not response.content and response.reasoning:
                # 推理型模型可能把 max_tokens 额度全花在 reasoning 上导致正文为空；
                # 这里只发警告事件提醒宿主排查（如放宽 max_tokens），不做自动回退。
                self.transient_event(
                    context.task_id,
                    "llm_empty_content",
                    node_name=context.node_code,
                    stage=stage,
                    invocation_id=invocation_id,
                    source="direct_model",
                    provider=adapter_name,
                    message="model returned empty content while reasoning is not empty",
                )
            self.transient_event(context.task_id, "llm_end", node_name=context.node_code, stage=stage, invocation_id=invocation_id, source="direct_model", provider=adapter_name, usage=response.usage)
            return response
        except Exception as exc:
            self.storage.finish_llm_invocation(invocation_id, error=exc)
            self.transient_event(context.task_id, "llm_error", node_name=context.node_code, stage=stage, invocation_id=invocation_id, source="direct_model", provider=adapter_name, error_type=type(exc).__name__, message=str(exc))
            raise

    async def invoke_dify(
        self,
        context: NodeContext,
        request: DifyRequest,
        *,
        conversation_key: str | None,
        app_name: str,
        prompt_version: str | None = None,
        stage: str | None = None,
    ):
        """Invoke one registered app; history calls are serialized by a Redis lease lock."""

        self.ensure_run_owned()
        if self.dify_registry is None:
            raise RuntimeError("Dify app registry is not configured")
        client = self.dify_registry.get(app_name)

        @asynccontextmanager
        async def no_lock():
            yield None

        lock = None
        if conversation_key is not None and self.settings is not None:
            identity = f"{context.task_id}:{app_name}:{conversation_key}"
            lock = RedisDifyConversationLock(
                self.settings.redis_url,
                identity,
                self.settings.dify_conversation_lock_ttl_seconds,
                self.settings.dify_conversation_lock_renew_seconds,
                self.settings.dify_conversation_lock_wait_seconds,
            )

        async with (lock.hold() if lock else no_lock()):
            conversation = (
                self.storage.get_dify_conversation(context.task_id, app_name, conversation_key)
                if conversation_key is not None else None
            )
            conversation_id = conversation["conversation_id"] if conversation else request.conversation_id
            effective_request = DifyRequest(
                request.operation, request.inputs, request.query, conversation_id, request.user
            )
            invocation_id = self.storage.start_dify_invocation(
                context.task_id, context.run_id, context.node_code, app_name,
                conversation_key, conversation_id,
                {"operation": request.operation, "query": request.query, "inputs": request.inputs, "conversation_id": conversation_id},
                prompt_version,
            )
            common = {
                "node_name": context.node_code, "stage": stage,
                "invocation_id": invocation_id, "source": "dify", "provider": "dify",
                "app_name": app_name, "app_type": client.config.app_type,
                "operation": request.operation, "conversation_key": conversation_key,
            }
            self.transient_event(context.task_id, "llm_start", **common, stream=True)

            async def on_chunk(chunk):
                if chunk.kind in {"content", "reasoning"}:
                    await asyncio.to_thread(
                        self.transient_event, context.task_id,
                        "llm_reasoning_token" if chunk.kind == "reasoning" else "llm_token",
                        **common, content=chunk.content, provider_event=chunk.event,
                    )
                else:
                    await asyncio.to_thread(
                        self.transient_event, context.task_id, "dify_event",
                        **common, kind=chunk.event or "provider_event", data=chunk.data,
                    )

            try:
                response = await client.invoke(effective_request, on_chunk=on_chunk)
                self.ensure_run_owned()
                response_text = response.content or (
                    json.dumps(response.outputs, ensure_ascii=False)
                    if response.outputs else ""
                )
                audit_usage = dict(response.usage)
                audit_usage["dify_identifiers"] = {
                    "task_id": response.task_id,
                    "workflow_run_id": response.workflow_run_id,
                }
                self.storage.finish_dify_invocation(
                    invocation_id, task_id=context.task_id, app_name=app_name,
                    conversation_key=conversation_key, conversation_id=response.conversation_id,
                    message_id=response.message_id or response.workflow_run_id or response.task_id,
                    response=response_text, reasoning=response.reasoning, usage=audit_usage,
                )
                self.transient_event(
                    context.task_id, "llm_end", **common,
                    conversation_id=response.conversation_id, message_id=response.message_id,
                    provider_task_id=response.task_id, workflow_run_id=response.workflow_run_id,
                    outputs=response.outputs, usage=response.usage,
                )
                return response
            except Exception as exc:
                self.storage.finish_dify_invocation(
                    invocation_id, task_id=context.task_id, app_name=app_name,
                    conversation_key=conversation_key, error=exc,
                )
                self.transient_event(
                    context.task_id, "llm_error", **common,
                    error_type=type(exc).__name__, message=str(exc),
                )
                raise

    def safe_task_status(self, *args: Any, **kwargs: Any) -> None:
        """隔离外部通知故障；可靠重试由已经落库的 SQL Outbox 接管。"""
        try:
            self.task_system.task_status(*args, **kwargs)
        except Exception:
            return None

    def safe_step_status(self, *args: Any, **kwargs: Any) -> None:
        """节点级通知的故障隔离入口，避免远端短暂异常打断本地执行。"""
        try:
            self.task_system.step_status(*args, **kwargs)
        except Exception:
            return None


class WorkflowNode(ABC, Generic[NodeInputT, NodeDataT]):
    """业务节点只实现 execute()；SDK 统一接管外层生命周期。

    ``execute`` 可以返回/await 一个 ``NodeOutput``，也可以实现为生成器并依次
    yield ``NodeStreamChunk``，最后 yield 唯一一个 ``NodeOutput``。后一种形式
    用于普通业务节点主动流式输出，不影响 LLM Adapter 自带的 token 流。
    """

    step: TaskStep
    input_model: type[NodeInputT]
    input_fields: dict[str, StateField] = {}
    output_fields: dict[str, StateField] = {}
    version: str = "1"
    llm_config: LLMNodeConfig | None = None
    dify_config: DifyNodeConfig | None = None

    @abstractmethod
    def execute(self, ctx: NodeContext, node_input: NodeInputT) -> NodeOutput[NodeDataT] | Any:
        raise NotImplementedError

    def _input(self, state: dict[str, Any]) -> NodeInputT:
        """按声明式 StateField 投影输入，并在业务代码运行前完成 Pydantic 校验。"""
        if self.input_fields:
            payload = {name: state.get(binding.key) for name, binding in self.input_fields.items()}
        else:
            payload = state
        return self.input_model.model_validate(payload)

    @staticmethod
    def _data_dict(data: Any) -> dict[str, Any]:
        if data is None:
            return {}
        if isinstance(data, BaseModel):
            return data.model_dump()
        if isinstance(data, dict):
            return data
        return {"result": data}

    def _remote_step(self, attempt: int) -> tuple[str, str, dict[str, Any] | None]:
        """折算远端上报用的步骤码/名称/动态定义；attempt>1 视为回环重跑。

        第 1 次执行沿用注册 stepCode；回环重跑用 ``{code}_r{attempt}`` 作为新
        stepCode 并携带 definition，远端会按「未知 stepCode + definition」自动
        创建动态步骤，使同一远端任务内可以承载多轮执行。
        """
        if attempt <= 1:
            return self.step.code, self.step.name, None
        name = f"{self.step.name}（第{attempt}轮）"
        definition = {
            "name": name,
            "stepType": self.step.step_type,
            "needConfirmation": self.step.need_confirmation or isinstance(self, HumanGateNode),
            "exceptionStrategy": self.step.exception_strategy,
        }
        return f"{self.step.code}_r{attempt}", name, definition

    def bind(self, services: RuntimeServices):
        """把业务 ``execute`` 包装成带完整生命周期观测的 LangGraph 节点。"""
        if not hasattr(self, "step") or not self.step.code:
            raise ValueError(f"{type(self).__name__} must declare a TaskStep")

        async def wrapped(state: dict[str, Any]) -> dict[str, Any]:
            # SDK 在调用业务代码前先记录 attempt 和 RUNNING，进程崩溃时仍可追踪。
            task_id, run_id = str(state["task_id"]), str(state["run_id"])
            services.ensure_run_owned()
            execution_id, attempt = services.storage.start_node(run_id, self.step.code, self.version)
            remote_code, remote_name, remote_definition = self._remote_step(attempt)
            if remote_definition is not None:
                # dependsOn 只在节点开始时算一次（此刻自身仍是 RUNNING，天然被排除），
                # START/STATUS 复用同一份，避免终态上报时取到自身造成自依赖。
                previous = services.storage.previous_successful_node(run_id, self.step.code)
                if previous is None:
                    remote_definition["dependsOn"] = []
                else:
                    prev_code, prev_attempt = previous
                    remote_definition["dependsOn"] = [
                        prev_code if prev_attempt <= 1 else f"{prev_code}_r{prev_attempt}"
                    ]
            services.storage.set_status(task_id, run_id, "RUNNING", self.step.code)
            services.event(task_id, run_id, "node_started", node_name=self.step.code, status="RUNNING", payload={"attempt": attempt, "name": self.step.name})
            if self.step.notify_task_system:
                services.safe_step_status(task_id, run_id, remote_code, remote_name, "RUNNING", attempt, definition=remote_definition)
            context = NodeContext(
                services,
                state,
                self.step.code,
                attempt,
                self.llm_config,
                self.dify_config,
            )
            try:
                context.raise_if_cancelled()
                node_input = self._input(state)
                if inspect.iscoroutinefunction(self.execute):
                    result = await self.execute(context, node_input)
                else:
                    # Legacy sync nodes run outside ARQ's event loop. I/O-heavy
                    # business nodes should still prefer async def execute().
                    result = await asyncio.to_thread(self.execute, context, node_input)
                if inspect.isawaitable(result):
                    result = await result
                if inspect.isasyncgen(result):
                    # Python 的 async generator 不能通过 ``return value`` 返回最终
                    # NodeOutput，因此约定最后显式 yield NodeOutput。SDK 在消费期间
                    # 立即发布 NodeStreamChunk，并在生成器结束后继续统一收口节点。
                    final_output: NodeOutput | None = None
                    async for item in result:
                        if isinstance(item, NodeStreamChunk):
                            await self._publish_stream_chunk(services, context, item)
                        elif isinstance(item, NodeOutput):
                            if final_output is not None:
                                raise TypeError("streaming node yielded more than one NodeOutput")
                            final_output = item
                        else:
                            raise TypeError(
                                "streaming node may only yield NodeStreamChunk or NodeOutput, "
                                f"got {type(item).__name__}"
                            )
                    if final_output is None:
                        raise TypeError("streaming node must yield one final NodeOutput")
                    result = final_output
                elif inspect.isgenerator(result):
                    # Pull every sync-generator item in a worker thread so a slow
                    # producer cannot block unrelated ARQ jobs.
                    final_output = None
                    sentinel = object()
                    def next_item():
                        try:
                            return next(result)
                        except StopIteration:
                            return sentinel
                    while True:
                        item = await asyncio.to_thread(next_item)
                        if item is sentinel:
                            break
                        if isinstance(item, NodeStreamChunk):
                            await self._publish_stream_chunk(services, context, item)
                        elif isinstance(item, NodeOutput):
                            if final_output is not None:
                                raise TypeError("streaming node yielded more than one NodeOutput")
                            final_output = item
                        else:
                            raise TypeError(
                                "streaming node may only yield NodeStreamChunk or NodeOutput, "
                                f"got {type(item).__name__}"
                            )
                    if final_output is None:
                        raise TypeError("streaming node must yield one final NodeOutput")
                    result = final_output
                if not isinstance(result, NodeOutput):
                    result = NodeOutput(data=result)
                services.ensure_run_owned()
                update = dict(result.state_update)
                data = self._data_dict(result.data)
                if self.output_fields:
                    for field_name, binding in self.output_fields.items():
                        update[binding.key] = data.get(field_name)
                else:
                    update.update(data)
                primary_artifact: str | None = None
                artifact_ids: list[str] = []
                for artifact in result.artifacts:
                    # Artifact 先持久化再暴露引用，审批节点永远只面向稳定版本。
                    artifact_id = services.storage.persist_artifact(task_id, run_id, artifact)
                    primary_artifact = primary_artifact or artifact_id
                    artifact_ids.append(artifact_id)
                    update[artifact.expose_as or f"{artifact.type}_ref"] = artifact_id
                    services.event(task_id, run_id, "artifact_created", node_name=self.step.code, artifact_id=artifact_id, payload={"artifact_type": artifact.type})
                update["current_node"] = self.step.code
                services.storage.finish_node(execution_id, artifact_id=primary_artifact)
                notification = result.notification
                payload = {"artifacts": artifact_ids}
                if notification:
                    payload.update({"summary": notification.summary, "output": notification.output})
                services.event(task_id, run_id, "node_succeeded", node_name=self.step.code, status="SUCCEEDED", artifact_id=primary_artifact, payload=payload)
                if self.step.notify_task_system:
                    services.safe_step_status(task_id, run_id, remote_code, remote_name, "SUCCEEDED", attempt, payload, definition=remote_definition)
                return update
            except GraphInterrupt:
                services.storage.wait_node(execution_id)
                services.event(task_id, run_id, "workflow_interrupt", node_name=self.step.code, status="WAITING_USER", payload={"name": self.step.name})
                raise
            except WorkflowRunLockLost:
                # A replacement worker now owns the run. The stale worker must not
                # write a misleading node failure or notify external systems.
                raise
            except Exception as exc:
                services.storage.finish_node(execution_id, error=exc)
                services.event(task_id, run_id, "node_failed", node_name=self.step.code, status="FAILED", payload={"type": type(exc).__name__, "message": str(exc)})
                if self.step.notify_task_system:
                    services.safe_step_status(task_id, run_id, remote_code, remote_name, "FAILED", attempt, {"message": str(exc)}, definition=remote_definition)
                raise

        return wrapped

    @staticmethod
    async def _publish_stream_chunk(
        services: RuntimeServices,
        context: NodeContext,
        chunk: NodeStreamChunk,
    ) -> None:
        """发布一个节点自定义流片段，并保持持久/瞬时事件语义明确。"""
        stream_payload = dict(chunk.payload)
        stream_payload["content"] = chunk.content
        if chunk.persist:
            # 持久事件进入数据库并由 EventBus 发布带 db_event_seq 的实时副本，适合
            # 阶段完成、关键告警等低频事实，不适合逐 token 数据。
            await asyncio.to_thread(
                services.event,
                context.task_id,
                context.run_id,
                chunk.event_type,
                node_name=context.node_code,
                payload=stream_payload,
            )
        else:
            # 高频片段只进 Redis Stream；NodeContext 信息放到事件顶层，前端可以用
            # SSE event_type 区分 LLM token 和业务自定义 token。
            await asyncio.to_thread(
                services.transient_event,
                context.task_id,
                chunk.event_type,
                node_name=context.node_code,
                **stream_payload,
            )


class HumanGateNode(WorkflowNode[NodeInputT, dict], ABC):
    """Production human gate backed by LangGraph interrupt and the decision table."""

    artifact_ref_field: str | None = None
    allowed_decisions: tuple[str, ...] = ("CONFIRM", "REJECT", "REVISE")

    def interrupt_payload(self, ctx: NodeContext, node_input: NodeInputT) -> dict[str, Any]:
        """构造 interrupt 载荷；子类可扩展 allowed_decisions 与附加字段。"""
        artifact_ref = ctx._state.get(self.artifact_ref_field) if self.artifact_ref_field else None
        return {
            "node_name": self.step.code,
            "artifact_ref": artifact_ref,
            "allowed_decisions": list(self.allowed_decisions),
        }

    def execute(self, ctx: NodeContext, node_input: NodeInputT) -> NodeOutput[dict]:
        value = interrupt(self.interrupt_payload(ctx, node_input))
        return NodeOutput(state_update={"last_decision": value})


class WorkflowGraph:
    """受控的 LangGraph 构建器，同时保存可导出的静态图元数据。

    业务方只能注册 ``WorkflowNode``，确保所有节点都经过生命周期包装。除了把
    节点和边交给 LangGraph，本类还保留原始节点对象、插入顺序和前驱关系；远端
    注册 JSON 因此直接来自实际运行图，不需要业务方额外维护一份 DAG。
    """

    def __init__(self, state_model: type, services: RuntimeServices):
        self._graph = StateGraph(state_model)
        self._services = services
        self._nodes: set[str] = set()
        # set 用于 O(1) 重复检测；list 保留声明顺序，作为拓扑排序中同层节点的
        # 稳定次序，确保重复导出不会因为 set/hash 顺序不同而产生无意义 diff。
        self._node_order: list[str] = []
        # 保存未 bind 的原始节点，导出时需要读取 TaskStep 和判断 HumanGateNode。
        self._node_definitions: dict[str, WorkflowNode] = {}
        # 只记录业务节点之间的反向边；START/END 是 LangGraph 控制节点，不应出现在
        # 远端 dependsOn 中。
        self._predecessors: dict[str, set[str]] = {}
        # 没有 path_map 的条件路由只能在运行时知道目标，静态导出无法可靠猜测。
        self._unresolved_conditional_sources: set[str] = set()

    def add_node(self, code: str, node: WorkflowNode) -> "WorkflowGraph":
        if not isinstance(node, WorkflowNode):
            raise TypeError("nodes must extend WorkflowNode so lifecycle instrumentation cannot be bypassed")
        if code != node.step.code:
            raise ValueError(f"graph node code {code!r} must equal TaskStep.code {node.step.code!r}")
        if code in self._nodes:
            raise ValueError(f"duplicate node: {code}")
        self._graph.add_node(code, node.bind(self._services))
        self._nodes.add(code)
        self._node_order.append(code)
        self._node_definitions[code] = node
        self._predecessors.setdefault(code, set())
        return self

    def add_edge(self, source: str, target: str, *, exclude_from_export: bool = False) -> "WorkflowGraph":
        self._graph.add_edge(source, target)
        if source != START and target != END and not exclude_from_export:
            self._predecessors.setdefault(target, set()).add(source)
        return self

    def add_conditional_edges(self, source: str, route: Any, path_map: Any = None, *, exclude_from_export: set[str] | None = None) -> "WorkflowGraph":
        self._graph.add_conditional_edges(source, route, path_map)
        # LangGraph 接受 dict（路由值 -> 节点名）或目标节点集合。只要目标明确，
        # 远端注册关心的是“可能到达的节点依赖 source”，不需要保存路由返回值。
        if isinstance(path_map, dict):
            targets = path_map.values()
        elif isinstance(path_map, (list, tuple, set)):
            targets = path_map
        else:
            targets = ()
            self._unresolved_conditional_sources.add(source)
        # 回环边（如驳回后回到前序节点）只作用于运行时 LangGraph，不进入注册依赖，
        # 否则注册清单会出现环，无法做拓扑排序。
        excluded = exclude_from_export or set()
        for target in targets:
            if target != END and str(target) not in excluded:
                self._predecessors.setdefault(str(target), set()).add(source)
        return self

    def set_entry_point(self, code: str) -> "WorkflowGraph":
        self._graph.add_edge(START, code)
        return self

    def set_finish_point(self, code: str) -> "WorkflowGraph":
        self._graph.add_edge(code, END)
        return self

    def compile(self, checkpointer: Any):
        return self._graph.compile(checkpointer=checkpointer)

    def registration_definition(self) -> list[dict[str, Any]]:
        """从真实运行图生成远端任务系统所需的步骤 JSON 数组。

        导出过程严格静态化且没有数据库/Redis 副作用。返回值只包含 JSON 原生类型，
        可以直接交给 FastAPI、``json.dumps`` 或远端注册客户端。

        规则：

        * 只导出 visible 且 notify_task_system 的节点；
        * 被隐藏的内部节点不会切断依赖，而是向上追溯最近的可见前驱；
        * 结果按稳定拓扑顺序排列，同层节点保持业务代码中的声明顺序；
        * HumanGateNode 自动标记 needConfirmation；
        * 无法静态确定目标的条件边明确报错，绝不生成可能错误的注册定义。
        """
        if self._unresolved_conditional_sources:
            sources = ", ".join(sorted(self._unresolved_conditional_sources))
            raise ValueError(
                "conditional edges require an explicit path_map before registration export: "
                f"{sources}"
            )

        # ``notify_task_system=False`` 的节点不会产生步骤状态 Outbox，如果仍将它
        # 注册到远端，该步骤将永远停在 Pending，所以必须与 invisible 节点一样
        # 从注册清单中排除。
        exported = {
            code
            for code, node in self._node_definitions.items()
            if node.step.visible and node.step.notify_task_system
        }

        def exported_predecessors(code: str, trail: frozenset[str] = frozenset()) -> set[str]:
            """返回 code 在“对外可见图”中的直接前驱。

            例如 A(可见) -> X(内部) -> B(可见) 会折叠成 A -> B。trail 用于在
            递归穿越多个内部节点时检测环，避免错误图导致无限递归。
            """
            if code in trail:
                raise ValueError(f"workflow graph contains a cycle at {code!r}")
            result: set[str] = set()
            for predecessor in self._predecessors.get(code, set()):
                if predecessor in exported:
                    result.add(predecessor)
                else:
                    result.update(exported_predecessors(predecessor, trail | {code}))
            return result

        # 先计算折叠后的依赖，再执行稳定版 Kahn 拓扑排序。使用声明顺序扫描候选
        # 节点，可以让导出的 JSON 适合纳入版本控制和人工审核。
        order_index = {code: index for index, code in enumerate(self._node_order)}
        dependencies = {code: exported_predecessors(code) for code in exported}
        ready = [code for code in self._node_order if code in exported and not dependencies[code]]
        ordered: list[str] = []
        while ready:
            code = ready.pop(0)
            ordered.append(code)
            for candidate in self._node_order:
                if candidate not in exported or candidate in ordered or candidate in ready:
                    continue
                if dependencies[candidate].issubset(ordered):
                    ready.append(candidate)
        if len(ordered) != len(exported):
            # 正常 DAG 最终一定会消费所有节点；剩余节点说明存在环或引用了无法
            # 满足的前驱，远端也无法注册，因此在本地给出明确错误。
            raise ValueError("workflow graph contains a cycle or unresolved dependency")

        result: list[dict[str, Any]] = []
        for code in ordered:
            node = self._node_definitions[code]
            step = node.step
            # 字段名严格匹配远端注册协议，保持 camelCase；None 会由 JSON 编码器
            # 转成 null，布尔值则保持真正的 JSON boolean。
            result.append({
                "stepCode": step.code,
                "name": step.name,
                "stepType": step.step_type,
                "dependsOn": sorted(dependencies[code], key=order_index.__getitem__),
                "needConfirmation": step.need_confirmation or isinstance(node, HumanGateNode),
                "exceptionStrategy": step.exception_strategy,
            })
        return result


class Workflow(ABC):
    workflow_type: str
    version: str = "1.0"
    input_model: type[WorkflowInput]
    state_model: type

    @abstractmethod
    def build(self, graph: WorkflowGraph) -> None:
        raise NotImplementedError

    def initial_state(self, task: Any, run: Any) -> dict[str, Any]:
        validated = self.input_model.model_validate(task.input_payload).model_dump()
        return {
            **validated,
            "task_id": task.id,
            "run_id": run.id,
            "actor_id": task.actor_id,
            "workflow_type": task.workflow_type,
            "current_node": None,
        }


class WorkflowRegistry:
    """工作流定义注册表；workflow_type 是 API 请求与代码定义之间的稳定契约。"""
    def __init__(self):
        self._definitions: dict[str, Workflow] = {}

    def register(self, workflow: Workflow) -> None:
        """注册一个唯一工作流类型，重复定义在启动阶段立即失败。"""
        if not workflow.workflow_type:
            raise ValueError("workflow_type is required")
        if workflow.workflow_type in self._definitions:
            raise ValueError(f"workflow already registered: {workflow.workflow_type}")
        self._definitions[workflow.workflow_type] = workflow

    def get(self, workflow_type: str) -> Workflow:
        try:
            return self._definitions[workflow_type]
        except KeyError as exc:
            raise KeyError(f"unknown workflow_type: {workflow_type}") from exc

    def types(self) -> list[str]:
        return sorted(self._definitions)

    def registration_definition(self, workflow_type: str) -> list[dict[str, Any]]:
        """构建并导出一个已注册工作流，不执行节点也不访问基础设施。

        RuntimeServices 使用空占位值是安全的，因为 ``Workflow.build`` 只声明图，
        ``node.bind`` 也只创建闭包；Storage/Redis/LLM 只有节点真正运行时才会访问。
        这使 CLI 可以在没有启动数据库和 Worker 的开发机上生成注册文件。
        """
        workflow = self.get(workflow_type)
        graph = WorkflowGraph(
            workflow.state_model,
            RuntimeServices(storage=None, task_system=None),
        )
        workflow.build(graph)
        return graph.registration_definition()


def export_workflow_definition(workflow: Workflow) -> list[dict[str, Any]]:
    """直接从一个 Workflow 实例生成远端注册清单。

    适合脚本、单元测试或尚未组装 Runtime 的项目。已经有 Runtime 时，优先调用
    ``runtime.registry.registration_definition(workflow_type)``，以确保导出的是宿主
    实际注册的那一个工作流版本。
    """
    registry = WorkflowRegistry()
    registry.register(workflow)
    return registry.registration_definition(workflow.workflow_type)
