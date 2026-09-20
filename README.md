# AI Gateway

<p align="center">
  <strong>One endpoint for OpenAI, Anthropic, Gemini, Ollama, and compatible providers.</strong><br>
  Keep your app stable when models, vendors, or API keys change.
</p>

<p align="center">
  <a href="https://github.com/HP-network/ai-gateway/actions/workflows/ci.yml"><img src="https://github.com/HP-network/ai-gateway/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://github.com/HP-network/ai-gateway/releases"><img src="https://img.shields.io/github/v/release/HP-network/ai-gateway" alt="Release"></a>
  <a href="https://github.com/HP-network/ai-gateway/blob/main/LICENSE"><img src="https://img.shields.io/github/license/HP-network/ai-gateway" alt="License"></a>
</p>

AI Gateway is a self-hosted LLM gateway that exposes one OpenAI-compatible API in front of several providers. It routes requests, retries failures, reports provider health, tracks usage in SQLite, and keeps provider-specific formats out of your application.

## Start In 30 Seconds

You only need one provider key. No JSON file is required.

```bash
git clone https://github.com/HP-network/ai-gateway.git
cd ai-gateway

export OPENAI_API_KEY=sk-...
python -m venv .venv && source .venv/bin/activate
pip install .
ai-gateway
```

The local process listens on `127.0.0.1:8080` by default. Send the same request you already send to OpenAI:

```bash
curl http://127.0.0.1:8080/v1/chat/completions \
  -H 'content-type: application/json' \
  -d '{
    "model": "auto",
    "messages": [{"role": "user", "content": "Give me one useful idea for a Minecraft plugin."}]
  }'
```

Existing OpenAI client code can point at the gateway by changing only `base_url`:

```python
from openai import OpenAI

client = OpenAI(base_url="http://127.0.0.1:8080/v1", api_key="unused")
answer = client.chat.completions.create(
    model="auto",
    messages=[{"role": "user", "content": "hello"}],
)
print(answer.choices[0].message.content)
```

The default environment mode automatically enables any provider whose key is present:

| Environment variable | Provider | Default model |
| --- | --- | --- |
| `OPENAI_API_KEY` | OpenAI-compatible | `gpt-4o-mini` |
| `ANTHROPIC_API_KEY` | Anthropic | `claude-3-5-haiku-latest` |
| `GEMINI_API_KEY` | Gemini | `gemini-2.0-flash` |
| `OLLAMA_MODEL` | Local Ollama | `llama3.2` |

Set `OPENAI_MODEL`, `ANTHROPIC_MODEL`, `GEMINI_MODEL`, or `OLLAMA_MODEL` to change a default. If no cloud key is present, the gateway starts with Ollama as a local provider.

Useful gateway settings:

| Variable | Default | Purpose |
| --- | --- | --- |
| `AI_GATEWAY_API_KEY` | unset | Shared application bearer key |
| `AI_GATEWAY_ADMIN_API_KEY` | unset | Admin/dashboard bearer key |
| `AI_GATEWAY_RATE_LIMIT` | `0` | Requests per minute per key; `0` disables it |
| `AI_GATEWAY_DATABASE` | `ai-gateway.db` | SQLite database path |
| `AI_GATEWAY_HOST` | `127.0.0.1` | Local bind address |
| `AI_GATEWAY_PORT` | `8080` | Listen port |

## Production Quick Start

For a shared deployment, set separate gateway and admin credentials. The gateway key is used by applications; the admin key creates and revokes client keys and opens the dashboard.

```bash
export OPENAI_API_KEY=sk-...
export AI_GATEWAY_API_KEY=gateway-internal-secret
export AI_GATEWAY_ADMIN_API_KEY=admin-secret
export AI_GATEWAY_RATE_LIMIT=120
ai-gateway
```

Create a client key. The plaintext token is shown only in this response, so store it in your application secret manager:

```bash
curl -X POST http://127.0.0.1:8080/admin/api-keys \
  -H 'authorization: Bearer admin-secret' \
  -H 'content-type: application/json' \
  -d '{"name":"my-app"}'
```

Use the returned `ag_...` token from then on:

```bash
curl http://127.0.0.1:8080/v1/chat/completions \
  -H 'authorization: Bearer ag_...' \
  -H 'content-type: application/json' \
  -d '{"model":"auto","messages":[{"role":"user","content":"hello"}]}'
```

Open `http://127.0.0.1:8080/dashboard` to view provider state, request totals, latency, token totals, and managed keys. The dashboard never displays key plaintext.

## Docker

```bash
export OPENAI_API_KEY=sk-...
docker compose up --build
```

Or use a local `.env` file:

```bash
cp .env.example .env
# edit .env, then:
docker compose up --build
```

The container uses the same environment-first setup and publishes port `8080`. SQLite data is kept in the `ai-gateway-data` volume. Set `AI_GATEWAY_ADMIN_API_KEY` to enable key management and the dashboard:

```bash
export AI_GATEWAY_API_KEY=gateway-secret
export AI_GATEWAY_ADMIN_API_KEY=admin-secret
curl http://127.0.0.1:8080/v1/chat/completions \
  -H "authorization: Bearer $AI_GATEWAY_API_KEY" \
  -H 'content-type: application/json' \
  -d '{"messages":[{"role":"user","content":"hello"}]}'
```

The Compose service includes a healthcheck on `/`. For a direct `docker run`, set `AI_GATEWAY_HOST=0.0.0.0` so the published port is reachable from outside the container.

## What It Does

- **One stable API**: OpenAI-compatible `/v1/chat/completions` and `/v1/models`.
- **Provider adapters**: OpenAI-compatible APIs, Anthropic Messages, Gemini `generateContent`, and Ollama.
- **Routing**: choose by provider name, model, or task; priority determines the default order.
- **Failover**: bounded retries and a cooldown for providers that keep failing.
- **Operations**: `/health`, `/v1/health`, Prometheus-compatible `/metrics`, and a built-in dashboard.
- **Usage and access**: SQLite request/token totals, hashed managed keys, revocation, and per-identity rate limits.
- **Security basics**: separate gateway/admin bearer credentials and environment-based provider secrets.
- **Small footprint**: Python standard library at runtime, Docker-ready, no framework lock-in.

## Advanced Configuration

Environment mode is the recommended starting point. Use a JSON file when you need explicit task routes, priorities, custom headers, or several models from the same vendor:

```bash
cp config.example.json config.json
ai-gateway --config config.json
```

Example task routing:

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

Validate a file before deploying it:

```bash
ai-gateway check-config --config config.json
```

## API Endpoints

| Method | Endpoint | Purpose |
| --- | --- | --- |
| `POST` | `/v1/chat/completions` | OpenAI-compatible chat completion |
| `GET` | `/v1/models` | Models currently configured |
| `GET` | `/` | Service information and endpoint links |
| `GET` | `/health` | Gateway and provider health |
| `GET` | `/metrics` | Request counters and latency |
| `GET` | `/dashboard` | Browser dashboard |
| `GET` | `/admin/stats` | Usage totals (admin key required) |
| `GET` | `/admin/api-keys` | Managed key metadata (admin key required) |
| `POST` | `/admin/api-keys` | Create a managed key (admin key required) |
| `DELETE` | `/admin/api-keys/:id` | Revoke a managed key (admin key required) |

Streaming is intentionally rejected for now so every adapter has consistent, predictable behavior. Non-streaming completions work across all providers.

## Scope

AI Gateway is infrastructure, not a hosted-service panel. It deliberately keeps the runtime small and inspectable while covering the operational basics: one endpoint, provider adapters, routing, failover, health, usage, keys, rate limits, and a small administration UI. It does not attempt to replace a billing or multi-tenant SaaS platform.

## Development

```bash
PYTHONPATH=src python -m unittest discover -s tests -v
python -m py_compile src/ai_gateway/*.py
```

The code is split into configuration, provider adapters, routing, and the HTTP service so it can be embedded behind another server or extended with a new provider without changing client integrations.

## License

MIT
