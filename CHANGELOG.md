# Changelog

## 0.2.0

- Added environment-first startup with OpenAI, Anthropic, Gemini, and Ollama auto-discovery.
- Added SQLite-backed aggregate usage counters and hashed managed API keys.
- Added admin key creation, listing, revocation, and a built-in operations dashboard.
- Added per-identity sliding-window rate limiting.
- Added provider status states, safer configuration validation, and a Docker healthcheck.
- Added 32 unit and HTTP-level tests covering configuration, routing, storage, authentication, and rate limiting.

## 0.1.3

- Bound local environment mode to loopback by default.
