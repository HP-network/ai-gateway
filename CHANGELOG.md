# Changelog

## 0.3.0

- Rewrote the runtime in Rust with Axum and Tokio for a small, high-concurrency process.
- Kept the OpenAI-compatible API while adding native OpenAI-compatible, Anthropic, Gemini, and Ollama adapters.
- Added provider priority routing, task routes, bounded failover, cooldowns, health reporting, and Prometheus metrics.
- Added SQLite usage accounting, hashed managed API keys, revocation, per-key rate limits, and the operations dashboard.
- Replaced the Python Docker image and CI pipeline with a locked Rust build and non-root Debian runtime.

## 0.2.0

- Added environment-first startup with OpenAI, Anthropic, Gemini, and Ollama auto-discovery.
- Added SQLite-backed aggregate usage counters and hashed managed API keys.
- Added admin key creation, listing, revocation, and a built-in operations dashboard.
- Added per-identity sliding-window rate limiting.
- Added provider status states, safer configuration validation, and a Docker healthcheck.
- Added 32 unit and HTTP-level tests covering configuration, routing, storage, authentication, and rate limiting.

## 0.1.3

- Bound local environment mode to loopback by default.
