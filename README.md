# AI Gateway

`ai-gateway` is a small, dependency-free LLM gateway for teams that need one stable OpenAI-compatible endpoint in front of several model providers.

It keeps provider-specific request formats behind adapters and gives routing a single place to handle priority, task routes, retries, failure cooldowns, health reporting, and metrics.

## What it includes

- OpenAI-compatible providers, Anthropic Messages, Gemini `generateContent`, and Ollama
- model or task-based routing with provider priority
- bounded failover retries and a lightweight circuit cooldown after repeated failures
- environment-based secret loading; secrets are not required in the JSON config
- OpenAI-compatible `/v1/chat/completions` and `/v1/models` endpoints
- `/health`, `/v1/health`, and Prometheus-style `/metrics`
- standard-library-only runtime; no framework or SDK lock-in
- Docker image, Compose example, configuration validation, and tests

Streaming requests are intentionally rejected until an adapter can preserve provider-specific streaming semantics. Non-streaming chat completions are supported consistently across all adapters.

## Quick start

```sh
cp config.example.json config.json
export OPENAI_API_KEY=...
PYTHONPATH=src python -m ai_gateway check-config --config config.json
PYTHONPATH=src python -m ai_gateway serve --config config.json
```

The example configuration listens on port `8080` on all interfaces. Bind it to `127.0.0.1` for a local-only process or put authentication and a reverse proxy in front of a shared deployment.

```sh
curl http://127.0.0.1:8080/v1/chat/completions \
  -H 'content-type: application/json' \
  -d '{
    "model": "auto",
    "task": "chat",
    "messages": [{"role": "user", "content": "Give me one short idea for a Minecraft plugin."}]
  }'
```

Set `server.api_key_env` in the config to require a gateway bearer token:

```json
{"server": {"api_key_env": "AI_GATEWAY_API_KEY"}}
```

## Routing

Providers are tried in descending `priority` order. A request with `model` set to a configured provider name or model selects that provider. A request with `task` uses the ordered provider list under `routing.task_routes`.

```json
{
  "routing": {
    "max_retries": 2,
    "failure_cooldown_seconds": 30,
    "task_routes": {
      "code": ["openai", "local-ollama"],
      "chat": ["anthropic", "openai"]
    }
  }
}
```

After two consecutive failures a provider is temporarily removed from routing. A successful request resets its failure counter. `/v1/health` exposes provider state, request counts, failures, and average latency.

## Providers

| `kind` | API | Required fields |
| --- | --- | --- |
| `openai-compatible` | OpenAI and compatible gateways | `base_url`, `model`, optional `api_key_env` |
| `anthropic` | Anthropic Messages API | `base_url`, `model`, `api_key_env` |
| `gemini` | Gemini `generateContent` | `base_url`, `model`, `api_key_env` |
| `ollama` | Ollama `/api/chat` | `base_url`, `model` |

Provider secrets should be supplied through `api_key_env` rather than committed to a config file.

## Docker

```sh
export OPENAI_API_KEY=...
docker compose up --build
```

For production, mount a private config file instead of using `config.example.json` and set an API key for the gateway itself.

## Development

```sh
PYTHONPATH=src python -m unittest discover -s tests -v
python -m py_compile src/ai_gateway/*.py
```

The project deliberately uses the Python standard library so the routing and adapter behavior stays inspectable and easy to embed. A framework-specific deployment can wrap `GatewayService` without changing provider logic.

## License

MIT
