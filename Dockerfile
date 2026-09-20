FROM rust:1.88-bookworm AS builder

WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY dashboard.html ./dashboard.html
COPY .env.example ./.env.example
RUN cargo build --release --locked

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install --no-install-recommends -y ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 10001 gateway

WORKDIR /app
COPY --from=builder /src/target/release/ai-gateway /usr/local/bin/ai-gateway
RUN mkdir -p /data && chown gateway:gateway /data

USER gateway
ENV AI_GATEWAY_HOST=0.0.0.0 \
    AI_GATEWAY_DATABASE=/data/ai-gateway.db \
    RUST_LOG=ai_gateway=info,tower_http=info

EXPOSE 8080
VOLUME ["/data"]
HEALTHCHECK --interval=30s --timeout=5s --retries=3 CMD curl --fail --silent http://127.0.0.1:8080/live || exit 1

ENTRYPOINT ["/usr/local/bin/ai-gateway"]
