# Changelog

## 0.7.0

- Added durable per-request audit records with request IDs, provider, model, status, latency, token usage, error details, and estimated cost.
- Added `X-Request-ID` propagation and new `/admin/requests` and `/admin/breakdown` endpoints.
- Added provider input/output token prices and cost aggregation in the operations console.
- Streaming usage is now collected through completion, with upstream failures and client disconnects recorded separately.
- Managed-key token budgets now reserve request capacity atomically to prevent concurrent quota overshoot.
- Expanded the dashboard with provider breakdowns and a recent-request view.

## 0.6.0

- Added a one-provider environment setup for OpenAI-compatible services such as OpenAI, OpenRouter, DeepSeek, and SiliconFlow.
- Added `ai-gateway init` to create a starter `.env` without overwriting an existing file.
- Expanded `check-config` output with listen address, authentication state, and provider details.
- Reworked the README around a copy, configure, and run quick start, with advanced routing kept separate.
- Added Docker build coverage to CI and a `.dockerignore` for smaller build contexts.

## 0.5.0

- Added per-key request quotas and token usage limits with SQLite-compatible migration for existing databases.
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
