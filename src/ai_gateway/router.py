from __future__ import annotations

import threading
import time
from dataclasses import dataclass

from .models import ChatRequest, ChatResponse, GatewayConfig, ProviderConfig
from .providers import Provider, ProviderError, build_provider


@dataclass
class ProviderState:
    provider: Provider
    failures: int = 0
    unhealthy_until: float = 0.0
    requests: int = 0
    errors: int = 0
    latency_total: float = 0.0

    @property
    def available(self) -> bool:
        return time.monotonic() >= self.unhealthy_until


class Router:
    def __init__(self, config: GatewayConfig):
        self.config = config
        self._lock = threading.RLock()
        self.states = {item.name: ProviderState(build_provider(item)) for item in config.providers}

    def route(self, request: ChatRequest) -> ChatResponse:
        candidates = self._candidates(request)
        if not candidates:
            raise ProviderError("no healthy provider matches this request")
        attempts = min(len(candidates), self.config.routing.max_retries + 1)
        errors: list[str] = []
        for state in candidates[:attempts]:
            started = time.monotonic()
            with self._lock:
                state.requests += 1
            try:
                response = state.provider.chat(request)
            except ProviderError as exc:
                elapsed = time.monotonic() - started
                with self._lock:
                    state.errors += 1
                    state.failures += 1
                    state.latency_total += elapsed
                    if state.failures >= 2:
                        state.unhealthy_until = time.monotonic() + self.config.routing.failure_cooldown_seconds
                errors.append(f"{state.provider.config.name}: {exc}")
                continue
            with self._lock:
                state.failures = 0
                state.unhealthy_until = 0.0
                state.latency_total += time.monotonic() - started
            return response
        raise ProviderError("all providers failed: " + "; ".join(errors))

    def health(self) -> list[dict[str, object]]:
        with self._lock:
            return [
                {
                    "name": state.provider.config.name,
                    "kind": state.provider.config.kind,
                    "model": state.provider.config.model,
                    "healthy": state.available,
                    "failures": state.failures,
                    "requests": state.requests,
                    "errors": state.errors,
                    "average_latency_ms": round(state.latency_total / state.requests * 1000, 2) if state.requests else 0,
                }
                for state in self.states.values()
            ]

    def _candidates(self, request: ChatRequest) -> list[ProviderState]:
        route_names = self.config.routing.task_routes.get(request.task or "")
        states = list(self.states.values())
        if route_names:
            index = {name: position for position, name in enumerate(route_names)}
            states = [state for state in states if state.provider.config.name in index]
            states.sort(key=lambda state: index[state.provider.config.name])
        elif request.model and request.model not in {"auto", ""}:
            exact = [state for state in states if state.provider.config.model == request.model or state.provider.config.name == request.model]
            if exact:
                states = exact
        states = [state for state in states if state.available]
        if not route_names:
            states.sort(key=lambda state: (-state.provider.config.priority, state.provider.config.name))
        return states
