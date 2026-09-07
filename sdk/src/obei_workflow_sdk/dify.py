"""Typed, streaming Dify application integration with optional durable conversations."""

from __future__ import annotations

from dataclasses import dataclass, field
import inspect
import json
import os
from typing import Any, Awaitable, Callable, Literal

import httpx
from dotenv import dotenv_values

DifyAppType = Literal["chat", "agent-chat", "agent", "advanced-chat", "completion", "workflow"]
CHAT_APP_TYPES = {"chat", "agent-chat", "agent", "advanced-chat"}


@dataclass(frozen=True)
class DifyAppConfig:
    """Connection and protocol identity for exactly one Dify application key."""
    name: str
    base_url: str
    api_key: str
    app_type: DifyAppType
    timeout_seconds: int = 120
    user: str = "obei-workflow-sdk"

    def __post_init__(self) -> None:
        if not self.name.strip() or not self.base_url.strip() or not self.api_key.strip():
            raise ValueError("Dify app name, base URL and API key are required")
        if self.app_type not in CHAT_APP_TYPES | {"completion", "workflow"}:
            raise ValueError(f"unsupported Dify app type: {self.app_type}")


@dataclass(frozen=True)
class DifyNodeConfig:
    """Node declaration selecting one registered Dify application."""
    app_name: str = "dify-chat-app"
    use_history: bool = False
    conversation_key: str = "main"
    prompt_version: str | None = None

    def __post_init__(self) -> None:
        if not self.app_name.strip():
            raise ValueError("DifyNodeConfig.app_name cannot be empty")
        if self.use_history and not self.conversation_key.strip():
            raise ValueError("conversation_key is required when Dify history is enabled")


@dataclass(frozen=True)
class DifyRequest:
    operation: Literal["chat", "completion", "workflow"]
    inputs: dict[str, Any] = field(default_factory=dict)
    query: str | None = None
    conversation_id: str | None = None
    user: str | None = None

    def __post_init__(self) -> None:
        if self.operation == "chat" and not (self.query or "").strip():
            raise ValueError("Dify chat query cannot be empty")


@dataclass
class DifyResponse:
    content: str = ""
    reasoning: str = ""
    outputs: dict[str, Any] = field(default_factory=dict)
    usage: dict[str, Any] = field(default_factory=dict)
    conversation_id: str | None = None
    message_id: str | None = None
    task_id: str | None = None
    workflow_run_id: str | None = None


@dataclass(frozen=True)
class DifyChunk:
    """Normalized stream item retaining the original event and data."""
    kind: Literal["content", "reasoning", "provider_event"]
    content: str = ""
    event: str | None = None
    data: dict[str, Any] = field(default_factory=dict)


ChunkHandler = Callable[[DifyChunk], Awaitable[None] | None]


async def _emit(handler: ChunkHandler | None, chunk: DifyChunk) -> None:
    if handler is None:
        return
    result = handler(chunk)
    if inspect.isawaitable(result):
        await result


class DifyAppClient:
    """Streaming HTTP client for one typed Dify app."""
    def __init__(self, config: DifyAppConfig, *, transport: httpx.AsyncBaseTransport | None = None) -> None:
        self.config = config
        self.base_url = config.base_url.rstrip("/")
        # Injectable transport keeps protocol tests fully local; production leaves it unset.
        self.transport = transport

    def _endpoint_and_body(self, request: DifyRequest) -> tuple[str, dict[str, Any]]:
        common = {"inputs": request.inputs, "response_mode": "streaming", "user": request.user or self.config.user}
        if request.operation == "chat":
            if self.config.app_type not in CHAT_APP_TYPES:
                raise TypeError(f"Dify app {self.config.name!r} ({self.config.app_type}) does not support chat")
            body = {**common, "query": request.query}
            if request.conversation_id:
                body["conversation_id"] = request.conversation_id
            return f"{self.base_url}/chat-messages", body
        if request.operation == "completion":
            if self.config.app_type != "completion":
                raise TypeError(f"Dify app {self.config.name!r} ({self.config.app_type}) is not a Text Generator")
            return f"{self.base_url}/completion-messages", common
        if self.config.app_type != "workflow":
            raise TypeError(f"Dify app {self.config.name!r} ({self.config.app_type}) is not a Workflow")
        return f"{self.base_url}/workflows/run", common

    async def invoke(self, request: DifyRequest, on_chunk: ChunkHandler | None = None) -> DifyResponse:
        """Execute one call and normalize all six mode-specific SSE contracts."""
        url, body = self._endpoint_and_body(request)
        headers = {"Authorization": f"Bearer {self.config.api_key}", "Content-Type": "application/json"}
        content_parts: list[str] = []
        reasoning_parts: list[str] = []
        outputs: dict[str, Any] = {}
        usage: dict[str, Any] = {}
        conversation_id = request.conversation_id
        message_id = task_id = workflow_run_id = None
        terminal_event: str | None = None
        timeout = httpx.Timeout(self.config.timeout_seconds, connect=20)
        async with httpx.AsyncClient(timeout=timeout, transport=self.transport) as http:
            async with http.stream("POST", url, headers=headers, json=body) as response:
                response.raise_for_status()
                async for line in response.aiter_lines():
                    if not line.startswith("data:"):
                        continue
                    raw = line[5:].strip()
                    if not raw or raw == "[DONE]":
                        continue
                    try:
                        item = json.loads(raw)
                    except json.JSONDecodeError:
                        continue
                    event = str(item.get("event") or "")
                    data = item.get("data") if isinstance(item.get("data"), dict) else {}
                    if event in {"error", "workflow_failed"}:
                        message = item.get("message") or data.get("error") or data.get("message") or event
                        raise RuntimeError(f"Dify stream failed: {message}")
                    conversation_id = item.get("conversation_id") or conversation_id
                    message_id = item.get("message_id") or message_id
                    task_id = item.get("task_id") or task_id
                    workflow_run_id = (
                        item.get("workflow_run_id")
                        or workflow_run_id
                        or (data.get("id") if event == "workflow_started" else None)
                    )
                    if event == "agent_thought":
                        thought = item.get("thought") or ""
                        if thought:
                            reasoning_parts.append(thought)
                            await _emit(on_chunk, DifyChunk("reasoning", thought, event, item))
                        await _emit(on_chunk, DifyChunk("provider_event", event=event, data=item))
                        continue
                    if event == "agent_message":
                        answer = item.get("answer") or ""
                        if answer:
                            content_parts.append(answer)
                            await _emit(on_chunk, DifyChunk("content", answer, event, item))
                        continue
                    if event == "message":
                        answer = item.get("answer") or ""
                        # New Agent closes with a full duplicate after agent_message deltas.
                        if not (self.config.app_type == "agent" and content_parts) and answer:
                            content_parts.append(answer)
                            await _emit(on_chunk, DifyChunk("content", answer, event, item))
                        continue
                    if event == "message_end":
                        terminal_event = event
                        usage = (item.get("metadata") or {}).get("usage") or usage
                    if event == "workflow_finished" and isinstance(data.get("outputs"), dict):
                        terminal_event = event
                        if str(data.get("status") or "").lower() in {"failed", "error", "stopped"}:
                            raise RuntimeError(
                                f"Dify workflow failed: {data.get('error') or data.get('message') or data.get('status')}"
                            )
                        outputs = data["outputs"]
                    await _emit(on_chunk, DifyChunk("provider_event", event=event, data=item))
        expected_terminal = "workflow_finished" if request.operation == "workflow" else "message_end"
        if terminal_event != expected_terminal:
            raise RuntimeError(
                f"Dify stream ended before {expected_terminal}; last terminal event was {terminal_event!r}"
            )
        return DifyResponse("".join(content_parts), "".join(reasoning_parts), outputs, usage, conversation_id, message_id, task_id, workflow_run_id)


class DifyAppRegistry:
    """Stable alias to typed app client and its dedicated credential."""
    def __init__(self) -> None:
        self._clients: dict[str, DifyAppClient] = {}

    def register(self, config: DifyAppConfig) -> None:
        if config.name in self._clients:
            raise ValueError(f"Dify app is already registered: {config.name}")
        self._clients[config.name] = DifyAppClient(config)

    def get(self, name: str) -> DifyAppClient:
        if name not in self._clients:
            raise KeyError(f"Dify app is not configured: {name}")
        return self._clients[name]

    def names(self) -> list[str]:
        return sorted(self._clients)

    def descriptions(self) -> list[dict[str, str]]:
        """Expose safe registry metadata without URLs or credentials."""
        return [
            {"name": name, "app_type": self._clients[name].config.app_type}
            for name in self.names()
        ]


def create_dify_registry(settings: Any) -> DifyAppRegistry:
    """Build a multi-app registry; secrets are referenced through ``api_key_env``."""
    registry = DifyAppRegistry()
    if settings.dify_apps_json:
        try:
            items = json.loads(settings.dify_apps_json)
        except json.JSONDecodeError as exc:
            raise ValueError("DIFY_APPS_JSON must be valid JSON") from exc
        if not isinstance(items, list):
            raise ValueError("DIFY_APPS_JSON must be a JSON array")
        env_file = settings.model_config.get("env_file", ".env") if hasattr(settings, "model_config") else ".env"
        file_values = dotenv_values(env_file) if env_file else {}
        for item in items:
            if not isinstance(item, dict):
                raise ValueError("each DIFY_APPS_JSON item must be an object")
            secret_name = str(item.get("api_key_env") or "")
            api_key = (
                os.environ.get(secret_name)
                or file_values.get(secret_name)
                or str(item.get("api_key") or "")
            ) if secret_name else str(item.get("api_key") or "")
            registry.register(DifyAppConfig(
                name=str(item.get("name") or ""), base_url=str(item.get("base_url") or ""),
                api_key=api_key, app_type=str(item.get("app_type") or ""),  # type: ignore[arg-type]
                timeout_seconds=int(item.get("timeout_seconds") or settings.dify_timeout_seconds),
                user=str(item.get("user") or settings.dify_user),
            ))
    elif settings.resolved_dify_api_key:
        registry.register(DifyAppConfig(
            settings.dify_default_app_name, settings.dify_base_url,
            settings.resolved_dify_api_key, settings.dify_default_app_type,
            settings.dify_timeout_seconds, settings.dify_user,
        ))
    return registry


class BoundDifyClient:
    """Node-bound facade; RuntimeServices owns persistence, locking and events."""
    def __init__(self, services: Any, context: Any, config: DifyNodeConfig) -> None:
        self.services, self.context, self.config = services, context, config

    async def chat(self, query: str, *, inputs: dict[str, Any] | None = None, conversation_key: str | None = None, conversation_id: str | None = None, use_history: bool | None = None, stage: str | None = None, user: str | None = None) -> str:
        history = self.config.use_history if use_history is None else use_history
        key = conversation_key or self.config.conversation_key
        if history and not key.strip():
            raise ValueError("conversation_key is required when Dify history is enabled")
        response = await self.services.invoke_dify(
            self.context, DifyRequest("chat", inputs or {}, query, conversation_id, user),
            conversation_key=key if history else None, app_name=self.config.app_name,
            prompt_version=self.config.prompt_version, stage=stage,
        )
        return response.content

    async def generate(self, inputs: dict[str, Any], *, stage: str | None = None, user: str | None = None) -> str:
        response = await self.services.invoke_dify(
            self.context, DifyRequest("completion", inputs, user=user), conversation_key=None,
            app_name=self.config.app_name, prompt_version=self.config.prompt_version, stage=stage,
        )
        return response.content

    async def run_workflow(self, inputs: dict[str, Any], *, stage: str | None = None, user: str | None = None) -> dict[str, Any]:
        response = await self.services.invoke_dify(
            self.context, DifyRequest("workflow", inputs, user=user), conversation_key=None,
            app_name=self.config.app_name, prompt_version=self.config.prompt_version, stage=stage,
        )
        return response.outputs
