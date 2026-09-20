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


def _integer(value: Any, name: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ConfigError(f"{name} must be an integer >= {minimum}")
    return value


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
        port=_integer(server_raw.get("port", 8080), "server.port", minimum=1),
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
        max_retries=_integer(routing_raw.get("max_retries", 2), "routing.max_retries"),
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
            priority=_integer(item.get("priority", 0), f"provider {name} priority"),
            weight=max(1, _integer(item.get("weight", 1), f"provider {name} weight", minimum=1)),
            headers=_headers(item.get("headers", {}), name),
        ))
    unknown_routes = {name for route in task_routes.values() for name in route if name not in names}
    if unknown_routes:
        raise ConfigError(f"task route references unknown providers: {', '.join(sorted(unknown_routes))}")
    return GatewayConfig(server, routing, tuple(providers))


def load_config_or_env(path: str | Path | None = None) -> GatewayConfig:
    """Load an explicit config, then the configured path, or use environment mode."""
    if path is not None:
        return load_config(path)
    configured_path = os.environ.get("AI_GATEWAY_CONFIG")
    if configured_path:
        return load_config(configured_path)
    default_path = Path("config.json")
    if default_path.exists():
        return load_config(default_path)
    return config_from_env()


def config_from_env() -> GatewayConfig:
    """Build a useful single-provider config without requiring a JSON file."""
    providers: list[ProviderConfig] = []
    _append_env_provider(
        providers,
        key_name="OPENAI_API_KEY",
        name="openai",
        kind="openai-compatible",
        base_url=os.environ.get("OPENAI_BASE_URL", "https://api.openai.com/v1"),
        model=os.environ.get("OPENAI_MODEL", "gpt-4o-mini"),
        priority=30,
    )
    _append_env_provider(
        providers,
        key_name="ANTHROPIC_API_KEY",
        name="anthropic",
        kind="anthropic",
        base_url=os.environ.get("ANTHROPIC_BASE_URL", "https://api.anthropic.com"),
        model=os.environ.get("ANTHROPIC_MODEL", "claude-3-5-haiku-latest"),
        priority=20,
    )
    _append_env_provider(
        providers,
        key_name="GEMINI_API_KEY",
        name="gemini",
        kind="gemini",
        base_url=os.environ.get("GEMINI_BASE_URL", "https://generativelanguage.googleapis.com"),
        model=os.environ.get("GEMINI_MODEL", "gemini-2.0-flash"),
        priority=20,
    )

    ollama_url = os.environ.get("OLLAMA_BASE_URL")
    ollama_model = os.environ.get("OLLAMA_MODEL")
    if ollama_url or ollama_model or not providers:
        providers.append(ProviderConfig(
            name="ollama",
            kind="ollama",
            base_url=(ollama_url or "http://127.0.0.1:11434").rstrip("/"),
            model=ollama_model or "llama3.2",
            priority=10,
        ))

    port = _env_integer("AI_GATEWAY_PORT", 8080, minimum=1)
    if port > 65535:
        raise ConfigError("AI_GATEWAY_PORT must be between 1 and 65535")
    return GatewayConfig(
        server=ServerConfig(
            host=os.environ.get("AI_GATEWAY_HOST", "0.0.0.0"),
            port=port,
            api_key=os.environ.get("AI_GATEWAY_API_KEY") or None,
            request_timeout_seconds=_env_number("AI_GATEWAY_TIMEOUT", 45.0, minimum=0.1),
        ),
        routing=RoutingConfig(
            max_retries=_env_integer("AI_GATEWAY_MAX_RETRIES", 2),
            failure_cooldown_seconds=_env_number("AI_GATEWAY_FAILURE_COOLDOWN", 30.0),
        ),
        providers=tuple(providers),
    )


def _append_env_provider(
    providers: list[ProviderConfig],
    *,
    key_name: str,
    name: str,
    kind: str,
    base_url: str,
    model: str,
    priority: int,
) -> None:
    api_key = os.environ.get(key_name)
    if not api_key:
        return
    providers.append(ProviderConfig(
        name=name,
        kind=kind,
        base_url=base_url.rstrip("/"),
        model=model,
        api_key=api_key,
        priority=priority,
    ))


def _env_integer(name: str, default: int, *, minimum: int = 0) -> int:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = int(raw, 10)
    except ValueError as exc:
        raise ConfigError(f"{name} must be an integer") from exc
    return _integer(value, name, minimum=minimum)


def _env_number(name: str, default: float, *, minimum: float = 0) -> float:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = float(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} must be a number") from exc
    return _number(value, name, minimum=minimum)


def _headers(value: Any, provider_name: str) -> dict[str, str]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ConfigError(f"provider {provider_name} headers must be an object")
    return {str(key): str(item) for key, item in value.items()}


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
