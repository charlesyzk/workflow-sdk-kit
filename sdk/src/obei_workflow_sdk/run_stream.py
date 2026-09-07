"""Extension contract for providers that create a run before streaming it."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field, replace
from typing import Any

from .llm import ChunkHandler, LLMRequest, LLMResponse


@dataclass(frozen=True)
class LLMRunHandle:
    run_id: str
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.run_id.strip():
            raise ValueError("provider run_id is required")


class RunStreamAdapter(ABC):
    """LLMRegistry-compatible base for a provider's run + stream protocol.

    Provider implementations must reject error events and premature EOF. This
    base deliberately does not retry ``run`` because doing so could create a
    second paid remote job; durable reattachment can be added once the concrete
    provider defines idempotency, status lookup and stream cursor semantics.
    """

    def __init__(self, name: str, default_model: str | None = None) -> None:
        if not name.strip():
            raise ValueError("LLM adapter name is required")
        self.name = name
        self.default_model = default_model

    @abstractmethod
    async def run(self, request: LLMRequest) -> LLMRunHandle:
        raise NotImplementedError

    @abstractmethod
    async def stream(
        self, handle: LLMRunHandle, on_chunk: ChunkHandler | None = None
    ) -> LLMResponse:
        raise NotImplementedError

    async def generate(
        self, request: LLMRequest, on_chunk: ChunkHandler | None = None
    ) -> LLMResponse:
        handle = await self.run(request)
        response = await self.stream(handle, on_chunk if request.stream else None)
        return replace(
            response,
            provider=response.provider or self.name,
            model=response.model or request.model or self.default_model,
            provider_request_id=response.provider_request_id or handle.run_id,
        )
