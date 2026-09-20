# AI Gateway

<p align="center">
  <strong>One fast, OpenAI-compatible endpoint for multiple LLM providers.</strong><br>
  Route, fail over, meter, and protect your model traffic from a single Rust binary.
</p>

<p align="center">
  <a href="https://github.com/HP-network/ai-gateway/actions/workflows/ci.yml"><img src="https://github.com/HP-network/ai-gateway/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://github.com/HP-network/ai-gateway/releases"><img src="https://img.shields.io/github/v/release/HP-network/ai-gateway" alt="Release"></a>
  <a href="https://github.com/HP-network/ai-gateway/blob/main/LICENSE"><img src="https://img.shields.io/github/license/HP-network/ai-gateway" alt="License"></a>
  <img src="https://img.shields.io/badge/runtime-Rust%20%2B%20Tokio-orange" alt="Rust and Tokio">
</p>

AI Gateway keeps your application pointed at one stable API while providers, models, and credentials change behind it. It is deliberately a gateway, not a hosted billing panel: the process is small, inspectable, Docker-ready, and easy to run beside an existing app.

## Start Here

Choose one path. You do not need a JSON file for the first two.

### Docker: recommended

```bash
git clone https://github.com/HP-network/ai-gateway.git
cd ai-gateway
cp .env.example .env
# Put one provider key in .env, then:
docker compose up -d --build
```

Check that it is alive:

```bash
curl http://127.0.0.1:8080/
```

### Local Rust binary

```bash
git clone https://github.com/HP-network/ai-gateway.git
cd ai-gateway
cp .env.example .env
# Put OPENAI_API_KEY=sk-... (or another provider) in .env
cargo run --release
```

For a reusable command, install it once:

```bash
cargo install --path .
ai-gateway
```

### Local Ollama

```bash
ollama pull llama3.2
OLLAMA_MODEL=llama3.2 cargo run --release
```

The environment mode automatically enables any provider whose key is present. If no cloud key is present, it uses Ollama at `http://127.0.0.1:11434`.

The binary reads a local `.env` file automatically and never overwrites variables already set by the shell. You can still use normal shell exports or a process manager in production.

## Make A Request

The request shape is the OpenAI Chat Completions shape, so existing SDKs only need a new `base_url`.

```bash
curl http://127.0.0.1:8080/v1/chat/completions \
  -H 'content-type: application/json' \
  -d '{
    "model": "auto",
    "messages": [{"role": "user", "content": "Give me one useful idea for a Minecraft plugin."}]
  }'
```

```python
from openai import OpenAI

client = OpenAI(
    base_url="http://127.0.0.1:8080/v1",
    api_key="unused-unless-you-enable-gateway-auth",
)

response = client.chat.completions.create(
    model="auto",
    messages=[{"role": "user", "content": "hello"}],
)
print(response.choices[0].message.content)
```

## Providers

| Provider | Enable with | Default model | Adapter |
| --- | --- | --- | --- |
| OpenAI or compatible API | `OPENAI_API_KEY` | `gpt-4o-mini` | `/chat/completions` |
| Anthropic | `ANTHROPIC_API_KEY` | `claude-3-5-haiku-latest` | `/v1/messages` |
| Gemini | `GEMINI_API_KEY` | `gemini-2.0-flash` | `generateContent` |
| Ollama | `OLLAMA_MODEL` | `llama3.2` | `/api/chat` |

Change a model or endpoint without changing client code:

```bash
OPENAI_MODEL=gpt-4.1-mini \
OPENAI_BASE_URL=https://api.openai.com/v1 \
OPENAI_API_KEY=sk-... \
ai-gateway
```

Any OpenAI-compatible vendor can be added with `OPENAI_BASE_URL`, or declared explicitly in `config.json` when several vendors must coexist.

## What You Get

- **Provider abstraction**: OpenAI-compatible, Anthropic, Gemini, and Ollama request/response normalization.
- **Routing**: select a model, provider name, or task route; priorities define the normal order.
- **Failover**: bounded retries and a cooldown for providers that repeatedly fail.
- **Access control**: optional gateway/admin bearer keys plus hashed, revocable client keys.
- **Usage accounting**: durable SQLite totals for requests, failures, latency, and tokens.
- **Rate limits**: sliding-window limits per master or managed client key.
- **Operations**: provider health, Prometheus text metrics, and a browser dashboard at `/dashboard`.
- **Small runtime**: one async Rust process, rustls HTTPS, and no Python runtime in production.

```mermaid
flowchart LR
    App[Your app / OpenAI SDK] --> Gateway[AI Gateway<br/>Axum + Tokio]
    Gateway --> Route[Model + task routing]
    Route --> OpenAI[OpenAI-compatible]
    Route --> Anthropic[Anthropic]
    Route --> Gemini[Gemini]
    Route --> Ollama[Ollama]
    Gateway --> Store[(SQLite usage + keys)]
    Gateway --> Ops[/health  /metrics  /dashboard]
```

## Production Setup

Set separate application and admin credentials before exposing the port:

```bash
export OPENAI_API_KEY=sk-...
export AI_GATEWAY_API_KEY=app-secret
export AI_GATEWAY_ADMIN_API_KEY=admin-secret
export AI_GATEWAY_RATE_LIMIT=120
ai-gateway
```

Create a managed key. The plaintext token is returned once and is never stored or shown again:

```bash
curl -X POST http://127.0.0.1:8080/admin/api-keys \
  -H 'authorization: Bearer admin-secret' \
  -H 'content-type: application/json' \
  -d '{"name":"my-app"}'
```

Use the returned `ag_...` token in the client application:

```bash
curl http://127.0.0.1:8080/v1/chat/completions \
  -H 'authorization: Bearer ag_...' \
  -H 'content-type: application/json' \
  -d '{"model":"auto","messages":[{"role":"user","content":"hello"}]}'
```

When auth is not configured, local environment mode is intentionally open on loopback. Docker binds to `0.0.0.0`, so configure `AI_GATEWAY_API_KEY` and `AI_GATEWAY_ADMIN_API_KEY` before publishing it outside the host.

## Configuration

Environment variables cover the common case:

| Variable | Default | Purpose |
| --- | --- | --- |
| `AI_GATEWAY_HOST` | `127.0.0.1` | Listen address in environment mode |
| `AI_GATEWAY_PORT` | `8080` | Listen port |
| `AI_GATEWAY_API_KEY` | unset | Application bearer key |
| `AI_GATEWAY_ADMIN_API_KEY` | unset | Dashboard and admin bearer key |
| `AI_GATEWAY_DATABASE` | `ai-gateway.db` | SQLite path |
| `AI_GATEWAY_RATE_LIMIT` | `0` | Requests/minute per identity; `0` disables it |
| `AI_GATEWAY_TIMEOUT` | `45` | Provider timeout in seconds |
| `AI_GATEWAY_MAX_RETRIES` | `2` | Maximum failover retries |
| `RUST_LOG` | `ai_gateway=info,tower_http=info` | Log filter |

For explicit routes, headers, priorities, or multiple instances of a vendor:

```bash
cp config.example.json config.json
# edit config.json
ai-gateway --config config.json check-config
ai-gateway --config config.json
```

Secrets can be referenced with `api_key_env` instead of putting them in JSON. The service rejects ambiguous or invalid configuration before it binds a port.

## API Surface

| Method | Endpoint | Auth | Purpose |
| --- | --- | --- | --- |
| `POST` | `/v1/chat/completions` | app key | OpenAI-compatible completion |
| `GET` | `/v1/models` | app key | Configured models |
| `GET` | `/` | none | Version and endpoint summary |
| `GET` | `/health` | app key | Provider health and cooldown state |
| `GET` | `/metrics` | app key | Prometheus-compatible counters |
| `GET` | `/dashboard` | browser + admin key | Operations console |
| `GET` | `/admin/stats` | admin key | Usage totals |
| `GET/POST` | `/admin/api-keys` | admin key | List or create client keys |
| `DELETE` | `/admin/api-keys/:id` | admin key | Revoke a client key |

Streaming is intentionally rejected in `0.3.0` so every adapter has the same predictable response contract. The next compatibility milestone is provider-native streaming with a consistent SSE layer.

## Build And Test

Requirements: Rust 1.88+ and Cargo.

```bash
cargo fmt --check
cargo test --locked
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo build --release --locked
```

Validate configuration without starting the server:

```bash
cargo run --release -- check-config --config config.json
```

## Positioning

Use this project when you want a fast, self-hosted compatibility layer in front of a few providers, with routing, failover, keys, usage, and operational visibility in one binary. If you need a full multi-tenant platform with billing, account pools, quotas, and a large web control plane, use a project built for that scope and place this gateway behind it.

## License

MIT
