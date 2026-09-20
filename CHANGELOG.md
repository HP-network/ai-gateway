# Changelog

## 0.5.0

- Added atomic per-key request and token quota reservations with SQLite-compatible migration for existing databases.
- Added optional request and token limits to managed key creation and the operations dashboard.
- Added per-provider concurrency gates and live available-slot health reporting.
- Added model aliases with startup validation so clients can use stable names while providers change.

## 0.4.1

- Fixed a Clippy warning on the streaming SSE formatter so the release pipeline stays warning-free.

## 0.4.0

- Added OpenAI-compatible streaming responses over SSE.
- Added streaming adapters for Anthropic, Gemini, and Ollama with normalized chunks.
- Added provider request streaming and failover before the first chunk is sent.

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
