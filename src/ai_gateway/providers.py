from __future__ import annotations

import json
import urllib.error
import urllib.request
from abc import ABC, abstractmethod
from typing import Any

from .models import ChatRequest, ChatResponse, ProviderConfig, Usage


class ProviderError(RuntimeError):
    """A provider request failed and can be retried on another provider."""

    def __init__(self, message: str, *, status: int | None = None):
        super().__init__(message)
        self.status = status


class Provider(ABC):
    def __init__(self, config: ProviderConfig):
        self.config = config

    @abstractmethod
    def chat(self, request: ChatRequest) -> ChatResponse:
        raise NotImplementedError

    def health(self) -> bool:
        return True


class JsonHttpProvider(Provider):
    def _request(self, url: str, payload: dict[str, Any], headers: dict[str, str]) -> dict[str, Any]:
        body = json.dumps(payload).encode("utf-8")
        request = urllib.request.Request(url, data=body, headers={"content-type": "application/json", **headers}, method="POST")
        try:
            with urllib.request.urlopen(request, timeout=self.config.timeout_seconds) as response:
                raw = response.read()
                parsed = json.loads(raw.decode("utf-8"))
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")[:500]
            raise ProviderError(f"{self.config.name} returned HTTP {exc.code}: {detail}", status=exc.code) from exc
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            raise ProviderError(f"{self.config.name} request failed: {exc}") from exc
        if not isinstance(parsed, dict):
            raise ProviderError(f"{self.config.name} returned a non-object response")
        return parsed


class OpenAICompatibleProvider(JsonHttpProvider):
    def chat(self, request: ChatRequest) -> ChatResponse:
        headers = {**self.config.headers}
        if self.config.api_key:
            headers["authorization"] = f"Bearer {self.config.api_key}"
        payload: dict[str, Any] = {"model": self.config.model, "messages": [message.as_dict() for message in request.messages]}
        for key in ("temperature", "max_tokens", "stream"):
            value = getattr(request, key)
            if value is not None:
                payload[key] = value
        payload.update(request.extra)
        response = self._request(f"{self.config.base_url}/chat/completions", payload, headers)
        try:
            choice = response["choices"][0]
            message = choice["message"]
            content = message.get("content", "")
            if not isinstance(content, str):
                content = "".join(str(part.get("text", "")) for part in content if isinstance(part, dict) and part.get("text") is not None)
            return ChatResponse(self.config.model, content, choice.get("finish_reason") or "stop", Usage.from_dict(response.get("usage")), self.config.name)
        except (KeyError, IndexError, TypeError) as exc:
            raise ProviderError(f"{self.config.name} returned an invalid chat response") from exc


class AnthropicProvider(JsonHttpProvider):
    def chat(self, request: ChatRequest) -> ChatResponse:
        headers = {"x-api-key": self.config.api_key or "", "anthropic-version": "2023-06-01", **self.config.headers}
        system: list[str] = []
        messages: list[dict[str, Any]] = []
        for message in request.messages:
            if message.role == "system":
                system.append(message.content if isinstance(message.content, str) else json.dumps(message.content))
            else:
                messages.append(message.as_dict())
        payload: dict[str, Any] = {"model": self.config.model, "max_tokens": request.max_tokens or 1024, "messages": messages}
        if system:
            payload["system"] = "\n".join(system)
        if request.temperature is not None:
            payload["temperature"] = request.temperature
        payload.update(request.extra)
        response = self._request(f"{self.config.base_url}/v1/messages", payload, headers)
        try:
            content = "".join(str(part.get("text", "")) for part in response["content"] if isinstance(part, dict) and part.get("text") is not None)
            return ChatResponse(self.config.model, content, response.get("stop_reason") or "stop", Usage.from_dict(response.get("usage")), self.config.name)
        except (KeyError, TypeError) as exc:
            raise ProviderError(f"{self.config.name} returned an invalid messages response") from exc


class GeminiProvider(JsonHttpProvider):
    def chat(self, request: ChatRequest) -> ChatResponse:
        contents = []
        for message in request.messages:
            role = "model" if message.role == "assistant" else "user"
            text = message.content if isinstance(message.content, str) else json.dumps(message.content)
            contents.append({"role": role, "parts": [{"text": text}]})
        payload: dict[str, Any] = {"contents": contents}
        if request.temperature is not None:
            payload["generationConfig"] = {"temperature": request.temperature}
        headers = {**self.config.headers}
        url = f"{self.config.base_url}/v1beta/models/{self.config.model}:generateContent"
        if self.config.api_key:
            url += f"?key={self.config.api_key}"
        response = self._request(url, payload, headers)
        try:
            text = response["candidates"][0]["content"]["parts"][0]["text"]
            return ChatResponse(self.config.model, text, "stop", Usage.from_dict(response.get("usageMetadata")), self.config.name)
        except (KeyError, IndexError, TypeError) as exc:
            raise ProviderError(f"{self.config.name} returned an invalid generateContent response") from exc


class OllamaProvider(OpenAICompatibleProvider):
    def chat(self, request: ChatRequest) -> ChatResponse:
        config = self.config
        payload: dict[str, Any] = {
            "model": config.model,
            "messages": [message.as_dict() for message in request.messages],
            "stream": False,
        }
        if request.temperature is not None:
            payload["options"] = {"temperature": request.temperature}
        response = self._request(f"{config.base_url}/api/chat", payload, config.headers)
        try:
            message = response["message"]
            return ChatResponse(config.model, message.get("content", ""), "stop", Usage.from_dict({
                "prompt_tokens": response.get("prompt_eval_count", 0),
                "completion_tokens": response.get("eval_count", 0),
            }), config.name)
        except (KeyError, TypeError) as exc:
            raise ProviderError(f"{config.name} returned an invalid Ollama response") from exc


def build_provider(config: ProviderConfig) -> Provider:
    kinds = {
        "openai": OpenAICompatibleProvider,
        "openai-compatible": OpenAICompatibleProvider,
        "anthropic": AnthropicProvider,
        "gemini": GeminiProvider,
        "ollama": OllamaProvider,
    }
    try:
        return kinds[config.kind](config)
    except KeyError as exc:
        raise ValueError(f"unsupported provider kind: {config.kind}") from exc
