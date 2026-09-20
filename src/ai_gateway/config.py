from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from .models import GatewayConfig, ProviderConfig, RoutingConfig, ServerConfig


class ConfigError(ValueError):
    """Raised when gateway configuration is missing or invalid."""


def _number(value: Any, name: str, *, minimum: float = 0) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value < minimum:
        raise ConfigError(f"{name} must be a number >= {minimum}")
    return float(value)


def load_config(path: str | Path) -> GatewayConfig:
    try:
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
    except OSError as exc:
        raise ConfigError(f"cannot read config: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise ConfigError(f"invalid JSON config: {exc}") from exc
    if not isinstance(raw, dict):
        raise ConfigError("config must be a JSON object")

    server_raw = raw.get("server", {})
    routing_raw = raw.get("routing", {})
    providers_raw = raw.get("providers", [])
    if not isinstance(server_raw, dict) or not isinstance(routing_raw, dict):
        raise ConfigError("server and routing must be objects")
    if not isinstance(providers_raw, list) or not providers_raw:
        raise ConfigError("providers must be a non-empty array")

    server = ServerConfig(
        host=str(server_raw.get("host", "0.0.0.0")),
        port=int(server_raw.get("port", 8080)),
        api_key=_secret(server_raw.get("api_key"), server_raw.get("api_key_env")),
        request_timeout_seconds=_number(server_raw.get("request_timeout_seconds", 45), "server.request_timeout_seconds", minimum=0.1),
    )
    if not 1 <= server.port <= 65535:
        raise ConfigError("server.port must be between 1 and 65535")

    task_routes_raw = routing_raw.get("task_routes", {})
    if not isinstance(task_routes_raw, dict):
        raise ConfigError("routing.task_routes must be an object")
    task_routes: dict[str, tuple[str, ...]] = {}
    for task, names in task_routes_raw.items():
        if not isinstance(task, str) or not isinstance(names, list) or not all(isinstance(name, str) for name in names):
            raise ConfigError("routing.task_routes values must be arrays of provider names")
        task_routes[task] = tuple(names)
    routing = RoutingConfig(
        default_model=str(routing_raw.get("default_model", "auto")),
        max_retries=int(routing_raw.get("max_retries", 2)),
        failure_cooldown_seconds=_number(routing_raw.get("failure_cooldown_seconds", 30), "routing.failure_cooldown_seconds"),
        task_routes=task_routes,
    )
    if routing.max_retries < 0:
        raise ConfigError("routing.max_retries must be >= 0")

    providers: list[ProviderConfig] = []
    names: set[str] = set()
    for item in providers_raw:
        if not isinstance(item, dict):
            raise ConfigError("each provider must be an object")
        name = item.get("name")
        kind = item.get("kind")
        base_url = item.get("base_url")
        model = item.get("model")
        if not all(isinstance(value, str) and value.strip() for value in (name, kind, base_url, model)):
            raise ConfigError("provider name, kind, base_url, and model are required")
        if name in names:
            raise ConfigError(f"duplicate provider name: {name}")
        names.add(name)
        providers.append(ProviderConfig(
            name=name,
            kind=kind,
            base_url=base_url.rstrip("/"),
            model=model,
            api_key=_secret(item.get("api_key"), item.get("api_key_env")),
            timeout_seconds=_number(item.get("timeout_seconds", server.request_timeout_seconds), f"provider {name} timeout_seconds", minimum=0.1),
            priority=int(item.get("priority", 0)),
            weight=max(1, int(item.get("weight", 1))),
            headers={str(key): str(value) for key, value in (item.get("headers", {}) or {}).items()},
        ))
    unknown_routes = {name for route in task_routes.values() for name in route if name not in names}
    if unknown_routes:
        raise ConfigError(f"task route references unknown providers: {', '.join(sorted(unknown_routes))}")
    return GatewayConfig(server, routing, tuple(providers))


def _secret(value: Any, env_name: Any) -> str | None:
    if value is not None and env_name is not None:
        raise ConfigError("use either a literal secret or an environment variable, not both")
    if value is not None:
        if not isinstance(value, str):
            raise ConfigError("secret values must be strings")
        return value or None
    if env_name is not None:
        if not isinstance(env_name, str) or not env_name:
            raise ConfigError("secret environment variable names must be non-empty strings")
        return os.environ.get(env_name)
    return None
