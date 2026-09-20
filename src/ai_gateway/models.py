from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


class RequestValidationError(ValueError):
    """Raised when an incoming API request is not valid."""


@dataclass(frozen=True)
class Message:
    role: str
    content: str | list[dict[str, Any]]

    @classmethod
    def from_dict(cls, value: Any) -> "Message":
        if not isinstance(value, dict):
            raise RequestValidationError("each message must be an object")
        role = value.get("role")
        content = value.get("content")
        if role not in {"system", "user", "assistant", "tool"}:
            raise RequestValidationError("message role must be system, user, assistant, or tool")
        if not isinstance(content, (str, list)):
            raise RequestValidationError("message content must be a string or content-part list")
        if isinstance(content, list) and not all(isinstance(part, dict) for part in content):
            raise RequestValidationError("message content parts must be objects")
        return cls(role, content)

    def as_dict(self) -> dict[str, Any]:
        return {"role": self.role, "content": self.content}


@dataclass(frozen=True)
class ChatRequest:
    model: str | None
    messages: tuple[Message, ...]
    temperature: float | None = None
    max_tokens: int | None = None
    stream: bool = False
    task: str | None = None
    extra: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, value: Any) -> "ChatRequest":
        if not isinstance(value, dict):
            raise RequestValidationError("request body must be a JSON object")
        raw_messages = value.get("messages")
        if not isinstance(raw_messages, list) or not raw_messages:
            raise RequestValidationError("messages must be a non-empty array")
        messages = tuple(Message.from_dict(item) for item in raw_messages)
        model = value.get("model")
        if model is not None and (not isinstance(model, str) or not model.strip()):
            raise RequestValidationError("model must be a non-empty string")
        temperature = value.get("temperature")
        if temperature is not None:
            if isinstance(temperature, bool) or not isinstance(temperature, (int, float)) or not 0 <= temperature <= 2:
                raise RequestValidationError("temperature must be between 0 and 2")
            temperature = float(temperature)
        max_tokens = value.get("max_tokens")
        if max_tokens is not None:
            if isinstance(max_tokens, bool) or not isinstance(max_tokens, int) or max_tokens < 1:
                raise RequestValidationError("max_tokens must be a positive integer")
        stream = value.get("stream", False)
        if not isinstance(stream, bool):
            raise RequestValidationError("stream must be a boolean")
        if stream:
            raise RequestValidationError("streaming responses are not supported yet")
        task = value.get("task")
        if task is not None and (not isinstance(task, str) or not task.strip()):
            raise RequestValidationError("task must be a non-empty string")
        known = {"model", "messages", "temperature", "max_tokens", "stream", "task"}
        extra = {key: item for key, item in value.items() if key not in known}
        return cls(model, messages, temperature, max_tokens, stream, task, extra)


@dataclass(frozen=True)
class Usage:
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0

    @classmethod
    def from_dict(cls, value: Any) -> "Usage":
        if not isinstance(value, dict):
            return cls()
        prompt = int(value.get("prompt_tokens", value.get("input_tokens", 0)) or 0)
        completion = int(value.get("completion_tokens", value.get("output_tokens", 0)) or 0)
        total = int(value.get("total_tokens", prompt + completion) or 0)
        return cls(prompt, completion, total)

    def as_dict(self) -> dict[str, int]:
        return {
            "prompt_tokens": self.prompt_tokens,
            "completion_tokens": self.completion_tokens,
            "total_tokens": self.total_tokens,
        }


@dataclass(frozen=True)
class ChatResponse:
    model: str
    content: str
    finish_reason: str = "stop"
    usage: Usage = field(default_factory=Usage)
    provider: str = "unknown"

    def as_openai(self, request_model: str | None = None) -> dict[str, Any]:
        import time

        return {
            "id": f"chatcmpl-{int(time.time() * 1000)}",
            "object": "chat.completion",
            "created": int(time.time()),
            "model": request_model or self.model,
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": self.content},
                "finish_reason": self.finish_reason,
            }],
            "usage": self.usage.as_dict(),
        }


@dataclass(frozen=True)
class ProviderConfig:
    name: str
    kind: str
    base_url: str
    model: str
    api_key: str | None = None
    timeout_seconds: float = 45.0
    priority: int = 0
    weight: int = 1
    headers: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class ServerConfig:
    host: str = "0.0.0.0"
    port: int = 8080
    api_key: str | None = None
    request_timeout_seconds: float = 45.0


@dataclass(frozen=True)
class RoutingConfig:
    default_model: str = "auto"
    max_retries: int = 2
    failure_cooldown_seconds: float = 30.0
    task_routes: dict[str, tuple[str, ...]] = field(default_factory=dict)


@dataclass(frozen=True)
class GatewayConfig:
    server: ServerConfig
    routing: RoutingConfig
    providers: tuple[ProviderConfig, ...]
