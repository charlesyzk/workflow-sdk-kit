from __future__ import annotations

import inspect
import json
import re
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Literal, Protocol

import httpx


ChunkHandler = Callable[["LLMChunk"], Awaitable[None] | None]
JSON_BLOCK = re.compile(r"```(?:json)?\s*(.*?)```", re.DOTALL | re.IGNORECASE)


def parse_json_object(text: str) -> dict[str, Any]:
    """从纯 JSON 或 Markdown JSON 代码块中提取对象。"""
    match = JSON_BLOCK.search(text)
    source = match.group(1) if match else text
    starts = [index for index, char in enumerate(source) if char in "{["]
    if not starts:
        raise ValueError("model response does not contain JSON")
    value, _ = json.JSONDecoder().raw_decode(source[starts[0]:].lstrip())
    if not isinstance(value, dict):
        raise ValueError("model response must be a JSON object")
    return value["result"] if set(value) == {"result"} and isinstance(value["result"], dict) else value


@dataclass(frozen=True)
class LLMNodeConfig:
    """Every LLM node must explicitly choose both adapter and streaming behavior."""

    adapter: str
    stream: bool
    model: str | None = None
    temperature: float | None = None
    prompt_version: str | None = None

    def __post_init__(self):
        if not self.adapter.strip():
            raise ValueError("LLM adapter name is required")
        if type(self.stream) is not bool:
            raise TypeError("LLMNodeConfig.stream must be explicitly True or False")


@dataclass(frozen=True)
class LLMRequest:
    messages: list[dict[str, str]]
    stream: bool
    model: str | None = None
    temperature: float | None = None
    response_format: Literal["text", "json_object"] = "text"
    provider_options: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if type(self.stream) is not bool:
            raise TypeError("LLMRequest.stream must be explicitly True or False")
        if not self.messages:
            raise ValueError("LLMRequest.messages cannot be empty")


@dataclass(frozen=True)
class LLMChunk:
    kind: Literal["content", "reasoning"]
    content: str


@dataclass
class LLMResponse:
    content: str
    reasoning: str = ""
    usage: dict[str, Any] = field(default_factory=dict)
    provider: str = ""
    model: str | None = None
    provider_request_id: str | None = None

    def json(self) -> dict[str, Any]:
        return parse_json_object(self.content)


async def _emit(handler: ChunkHandler | None, chunk: LLMChunk) -> None:
    """兼容同步/异步 token 回调，并过滤空 token。"""
    if handler is None or not chunk.content:
        return
    result = handler(chunk)
    if inspect.isawaitable(result):
        await result


class LLMAdapter(Protocol):
    name: str
    default_model: str | None

    async def generate(self, request: LLMRequest, on_chunk: ChunkHandler | None = None) -> LLMResponse: ...


class OpenAICompatibleAdapter:
    """调用 OpenAI Chat Completions 兼容接口的真实 HTTP Adapter。"""
    name = "openai"

    def __init__(self, base_url: str, api_key: str, model: str, timeout_seconds: int = 120):
        if not base_url or not api_key or not model:
            raise ValueError("OpenAI-compatible base URL, API key and model are required")
        self.url = f"{base_url.rstrip('/')}/chat/completions"
        self.api_key, self.default_model, self.timeout_seconds = api_key, model, timeout_seconds

    def _body(self, request: LLMRequest) -> dict[str, Any]:
        """把供应商无关请求转换为 Chat Completions 请求体。"""
        body = {
            "model": request.model or self.default_model,
            "messages": request.messages,
            "stream": request.stream,
        }
        if request.temperature is not None:
            body["temperature"] = request.temperature
        if request.response_format == "json_object":
            body["response_format"] = {"type": "json_object"}
        body.update(request.provider_options)
        if request.stream and "stream_options" not in body:
            body["stream_options"] = {"include_usage": True}
        return body

    async def generate(self, request: LLMRequest, on_chunk: ChunkHandler | None = None) -> LLMResponse:
        """按 ``request.stream`` 选择普通响应或 SSE，并统一聚合完整结果。"""
        body = self._body(request)
        headers = {"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"}
        timeout = httpx.Timeout(self.timeout_seconds, connect=20)
        if not request.stream:
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.post(self.url, headers=headers, json=body)
                response.raise_for_status()
                data = response.json()
            choice = (data.get("choices") or [{}])[0]
            message = choice.get("message") or {}
            return LLMResponse(
                content=message.get("content") or "",
                reasoning=message.get("reasoning_content") or message.get("reasoning") or "",
                usage=data.get("usage") or {}, provider=self.name,
                model=data.get("model") or body["model"], provider_request_id=data.get("id"),
            )

        content_parts: list[str] = []
        reasoning_parts: list[str] = []
        usage: dict[str, Any] = {}
        response_id = None
        response_model = body["model"]
        async with httpx.AsyncClient(timeout=timeout) as client:
            async with client.stream("POST", self.url, headers=headers, json=body) as response:
                response.raise_for_status()
                async for line in response.aiter_lines():
                    if not line.startswith("data:"):
                        continue
                    raw = line[5:].strip()
                    if not raw or raw == "[DONE]":
                        continue
                    try:
                        event = json.loads(raw)
                    except json.JSONDecodeError:
                        continue
                    response_id = response_id or event.get("id")
                    response_model = event.get("model") or response_model
                    if event.get("usage"):
                        usage = event["usage"]
                    choices = event.get("choices") or []
                    if not choices:
                        continue
                    delta = choices[0].get("delta") or {}
                    reasoning = delta.get("reasoning_content") or delta.get("reasoning") or ""
                    content = delta.get("content") or ""
                    if reasoning:
                        reasoning_parts.append(reasoning); await _emit(on_chunk, LLMChunk("reasoning", reasoning))
                    if content:
                        content_parts.append(content); await _emit(on_chunk, LLMChunk("content", content))
        return LLMResponse("".join(content_parts), "".join(reasoning_parts), usage, self.name, response_model, response_id)


class DifyAdapter:
    """调用 Dify Chat App ``/chat-messages`` 的真实 HTTP Adapter。"""
    name = "dify"
    default_model = "dify-app"

    def __init__(self, base_url: str, api_key: str, timeout_seconds: int = 120, user: str = "obei-workflow-sdk"):
        if not base_url or not api_key:
            raise ValueError("Dify base URL and API key are required")
        self.url = f"{base_url.rstrip('/')}/chat-messages"
        self.api_key, self.timeout_seconds, self.user = api_key, timeout_seconds, user

    @staticmethod
    def _query(messages: list[dict[str, str]], json_output: bool) -> str:
        """把多角色消息压平成 Dify Chat API 的单个 query 字段。"""
        parts = [f"[{item.get('role', 'user')}]\n{item.get('content', '')}" for item in messages]
        if json_output:
            parts.append("请只输出一个 JSON 对象，不要输出 Markdown 代码块、解释或额外文字。")
        return "\n\n".join(parts)

    def _body(self, request: LLMRequest) -> dict[str, Any]:
        """构造 Dify 请求，并将显式流式选择映射为 response_mode。"""
        options = dict(request.provider_options)
        inputs = options.pop("inputs", {})
        body = {
            "inputs": inputs,
            "query": self._query(request.messages, request.response_format == "json_object"),
            "response_mode": "streaming" if request.stream else "blocking",
            "user": options.pop("user", self.user),
            **options,
        }
        return body

    async def generate(self, request: LLMRequest, on_chunk: ChunkHandler | None = None) -> LLMResponse:
        """解析 Dify blocking/streaming 两种响应并收集 usage 与思考内容。"""
        body = self._body(request)
        headers = {"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"}
        timeout = httpx.Timeout(self.timeout_seconds, connect=20)
        if not request.stream:
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.post(self.url, headers=headers, json=body)
                response.raise_for_status(); data = response.json()
            metadata = data.get("metadata") or {}
            return LLMResponse(
                content=data.get("answer") or "", usage=metadata.get("usage") or {},
                provider=self.name, model=self.default_model,
                provider_request_id=data.get("message_id") or data.get("id"),
            )

        content_parts: list[str] = []
        reasoning_parts: list[str] = []
        usage: dict[str, Any] = {}
        request_id = None
        in_reasoning = False
        async with httpx.AsyncClient(timeout=timeout) as client:
            async with client.stream("POST", self.url, headers=headers, json=body) as response:
                response.raise_for_status()
                async for line in response.aiter_lines():
                    if not line.startswith("data:"):
                        continue
                    raw = line[5:].strip()
                    if not raw or raw == "[DONE]":
                        continue
                    try:
                        event = json.loads(raw)
                    except json.JSONDecodeError:
                        continue
                    request_id = request_id or event.get("message_id") or event.get("id")
                    if event.get("event") == "message_end":
                        usage = (event.get("metadata") or {}).get("usage") or usage
                        continue
                    if event.get("event") == "agent_thought":
                        thought = event.get("thought") or ""
                        reasoning_parts.append(thought); await _emit(on_chunk, LLMChunk("reasoning", thought))
                        continue
                    token = event.get("answer") or ""
                    if not token:
                        continue
                    if "<think>" in token:
                        in_reasoning = True; token = token.replace("<think>", "")
                    if "</think>" in token:
                        thought, answer = token.split("</think>", 1)
                        if thought:
                            reasoning_parts.append(thought); await _emit(on_chunk, LLMChunk("reasoning", thought))
                        in_reasoning = False; token = answer
                    if token:
                        if in_reasoning:
                            reasoning_parts.append(token); await _emit(on_chunk, LLMChunk("reasoning", token))
                        else:
                            content_parts.append(token); await _emit(on_chunk, LLMChunk("content", token))
        return LLMResponse("".join(content_parts), "".join(reasoning_parts), usage, self.name, self.default_model, request_id)


class LLMRegistry:
    """宿主进程内的 Adapter 注册表；节点通过稳定名称选择供应商。"""
    def __init__(self):
        self.adapters: dict[str, LLMAdapter] = {}

    def register(self, adapter: LLMAdapter) -> None:
        if adapter.name in self.adapters:
            raise ValueError(f"LLM adapter already registered: {adapter.name}")
        self.adapters[adapter.name] = adapter

    def get(self, name: str) -> LLMAdapter:
        if name not in self.adapters:
            raise KeyError(f"LLM adapter is not configured: {name}")
        return self.adapters[name]


class BoundLLMClient:
    """Node-bound facade that automatically audits and streams one model call."""

    def __init__(self, services: Any, context: Any, config: LLMNodeConfig):
        self.services, self.context, self.config = services, context, config

    async def chat(
        self,
        messages: list[dict[str, str]],
        *,
        json_output: bool = False,
        stage: str | None = None,
        model: str | None = None,
        temperature: float | None = None,
        stream: bool | None = None,
        provider_options: dict[str, Any] | None = None,
    ) -> str:
        """Call a direct model with the complete caller-managed message history.

        Unlike Dify, Chat Completions does not own a conversation.  The workflow should keep the
        required messages or a summarized history in durable State and pass it here on every turn.
        """

        request = LLMRequest(
            messages=messages,
            stream=self.config.stream if stream is None else stream,
            model=model or self.config.model,
            temperature=self.config.temperature if temperature is None else temperature,
            response_format="json_object" if json_output else "text",
            provider_options=provider_options or {},
        )
        response = await self.services.invoke_llm(
            self.context, self.config.adapter, request, stage=stage
        )
        return response.content

    async def complete(self, system_prompt: str, payload: dict[str, Any], *, json_output: bool = False, stage: str | None = None, model: str | None = None, temperature: float | None = None, stream: bool | None = None, provider_options: dict[str, Any] | None = None) -> str:
        """发起一次受节点配置约束、自动审计且可选流式输出的模型调用。"""
        return await self.chat(
            [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
            ],
            json_output=json_output,
            stage=stage,
            model=model,
            temperature=temperature,
            stream=stream,
            provider_options=provider_options,
        )

    async def complete_json(self, system_prompt: str, payload: dict[str, Any], **kwargs: Any) -> dict[str, Any]:
        return parse_json_object(await self.complete(system_prompt, payload, json_output=True, **kwargs))


def create_llm_registry(settings: Any) -> LLMRegistry:
    """Register stateless direct-model adapters.

    Dify is assembled separately as ``DifyAppClient`` because it owns conversation state and is
    not substitutable for a Chat Completions model.  ``DifyAdapter`` remains importable only for
    backwards compatibility with existing hosts.
    """
    registry = LLMRegistry()
    if settings.openai_api_key:
        registry.register(OpenAICompatibleAdapter(settings.openai_base_url, settings.openai_api_key, settings.openai_model, settings.openai_timeout_seconds))
    return registry
