FROM python:3.12-slim

WORKDIR /app
COPY pyproject.toml README.md LICENSE ./
COPY src ./src
COPY config.example.json ./config.example.json

RUN pip install --no-cache-dir .

ENV AI_GATEWAY_CONFIG=/etc/ai-gateway/config.json
EXPOSE 8080

CMD ["ai-gateway", "serve", "--config", "/etc/ai-gateway/config.json"]
