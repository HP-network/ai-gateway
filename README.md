# AI Gateway

<p align="center">
  <strong>把任何 OpenAI-compatible API 变成一个稳定入口。</strong><br>
  一个 Rust 二进制，负责路由、故障切换、鉴权、限流和用量统计。
</p>

<p align="center">
  <a href="https://github.com/HP-network/ai-gateway/actions/workflows/ci.yml"><img src="https://github.com/HP-network/ai-gateway/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://github.com/HP-network/ai-gateway/releases"><img src="https://img.shields.io/github/v/release/HP-network/ai-gateway" alt="Release"></a>
  <a href="https://github.com/HP-network/ai-gateway/blob/main/LICENSE"><img src="https://img.shields.io/github/license/HP-network/ai-gateway" alt="License"></a>
  <img src="https://img.shields.io/badge/Rust-1.88%2B-orange" alt="Rust 1.88 or newer">
</p>

## 60 秒启动

需要 Docker。先复制项目并创建配置：

```bash
git clone https://github.com/HP-network/ai-gateway.git
cd ai-gateway
cp .env.example .env
```

打开 `.env`，只填这三项就够了：

```dotenv
AI_GATEWAY_UPSTREAM_API_KEY=sk-...
AI_GATEWAY_UPSTREAM_BASE_URL=https://api.openai.com/v1
AI_GATEWAY_UPSTREAM_MODEL=gpt-4o-mini
```

启动并检查：

```bash
docker compose up -d --build
curl http://127.0.0.1:8080/health
```

发出第一条请求：

```bash
curl http://127.0.0.1:8080/v1/chat/completions \
  -H 'content-type: application/json' \
  -d '{"model":"auto","messages":[{"role":"user","content":"你好"}]}'
```

客户端只需要把 `base_url` 改成 `http://127.0.0.1:8080/v1`。原来的 OpenAI SDK、LangChain 或 LiteLLM 客户端不用改请求格式。

### 常用上游

| 服务 | `AI_GATEWAY_UPSTREAM_BASE_URL` | 示例模型 |
| --- | --- | --- |
| OpenAI | `https://api.openai.com/v1` | `gpt-4o-mini` |
| OpenRouter | `https://openrouter.ai/api/v1` | `openai/gpt-4o-mini` |
| DeepSeek | `https://api.deepseek.com/v1` | `deepseek-chat` |
| SiliconFlow | `https://api.siliconflow.cn/v1` | `deepseek-ai/DeepSeek-V3` |
| 本地 Ollama | `http://host.docker.internal:11434` | `llama3.2` |

把上表中的地址和模型放进 `.env`，不需要写 JSON。

## 不用 Docker

安装 Rust 1.88 或更新版本：

```bash
git clone https://github.com/HP-network/ai-gateway.git
cd ai-gateway
cargo run --release -- init
# 编辑 .env，填入上游 key
cargo run --release -- check-config
cargo run --release
```

`init` 不会覆盖已有的 `.env`。也可以直接安装二进制：

```bash
cargo install --path .
ai-gateway check-config
ai-gateway
```

## 接入 SDK

```python
from openai import OpenAI

client = OpenAI(
    base_url="http://127.0.0.1:8080/v1",
    api_key="unused",  # 设置 AI_GATEWAY_API_KEY 后改成对应值
)

answer = client.chat.completions.create(
    model="auto",
    messages=[{"role": "user", "content": "给我一个实用的插件点子"}],
)
print(answer.choices[0].message.content)
```

流式请求保持 OpenAI SSE 格式：

```bash
curl --no-buffer http://127.0.0.1:8080/v1/chat/completions \
  -H 'content-type: application/json' \
  -d '{"model":"auto","stream":true,"messages":[{"role":"user","content":"说三句短句"}]}'
```

## 生产配置

默认只监听 `127.0.0.1`。如果要让其他机器访问，至少设置应用和管理密钥：

```dotenv
AI_GATEWAY_HOST=0.0.0.0
AI_GATEWAY_API_KEY=app-secret
AI_GATEWAY_ADMIN_API_KEY=admin-secret
AI_GATEWAY_RATE_LIMIT=120
```

然后客户端带上：

```bash
curl http://127.0.0.1:8080/v1/models \
  -H 'authorization: Bearer app-secret'
```

管理面板在 `/dashboard`，管理 API 需要 `AI_GATEWAY_ADMIN_API_KEY`：

```bash
curl http://127.0.0.1:8080/admin/stats \
  -H 'authorization: Bearer admin-secret'
```

创建一个可撤销的客户端 key：

```bash
curl -X POST http://127.0.0.1:8080/admin/api-keys \
  -H 'authorization: Bearer admin-secret' \
  -H 'content-type: application/json' \
  -d '{"name":"my-app","request_limit":10000,"token_limit":5000000}'
```

返回的 `ag_...` token 只展示一次。SQLite 数据默认保存到 `ai-gateway.db`，Docker Compose 会放在持久化 volume 中。
请求审计默认保留 30 天，可通过 `AI_GATEWAY_AUDIT_RETENTION_DAYS` 调整；设为 `0` 时不自动清理。

## 能力

- 一个 OpenAI-compatible `/v1/chat/completions` 入口
- OpenAI-compatible、Anthropic、Gemini、Ollama 适配器
- 非流式和 SSE streaming，统一返回 OpenAI 格式
- provider 优先级、模型别名、task route 和失败切换
- provider 并发控制、超时和 cooldown
- 可选主密钥、管理密钥、哈希客户端 key 和撤销
- 每个身份的滑动窗口限流、请求/Token 配额
- 原子 token 预算预留，避免并发请求穿透配额
- 请求级审计、`X-Request-ID`、provider 成本和错误追踪
- SQLite 用量统计、Prometheus `/metrics` 和 `/dashboard`
- 多阶段 Docker 构建，运行时使用非 root 用户

## 多 Provider 与高级路由

只有需要多个供应商、不同优先级或 task route 时才使用 `config.json`：

```bash
cp config.example.json config.json
ai-gateway check-config --config config.json
ai-gateway --config config.json
```

JSON 中的密钥建议使用 `api_key_env`，不要把真实 token 提交到仓库。`routing.model_aliases` 可以把客户端稳定名称映射到真实模型，例如 `fast` 或 `local`。

每个 provider 还可以设置 `input_price_per_million` 和 `output_price_per_million`。环境模式对应：

```dotenv
AI_GATEWAY_UPSTREAM_INPUT_PRICE=0.15
AI_GATEWAY_UPSTREAM_OUTPUT_PRICE=0.60
```

价格单位是每百万 token 的美元成本，未设置时成本显示为零。

## API

| 方法 | 地址 | 作用 |
| --- | --- | --- |
| `POST` | `/v1/chat/completions` | OpenAI-compatible 对话和流式输出 |
| `GET` | `/v1/models` | 已配置模型 |
| `GET` | `/health` | provider 状态和并发槽位 |
| `GET` | `/metrics` | Prometheus 指标 |
| `GET` | `/dashboard` | 浏览器运营面板 |
| `GET` | `/admin/stats` | 聚合用量 |
| `GET` | `/admin/requests?limit=50` | 最近请求、状态、延迟和错误 |
| `GET` | `/admin/breakdown` | provider/model 用量与成本分解 |
| `GET/POST` | `/admin/api-keys` | 管理客户端 key |
| `DELETE` | `/admin/api-keys/:id` | 撤销客户端 key |

## 配置检查与故障排查

先运行：

```bash
ai-gateway check-config
```

它会输出监听地址、鉴权状态和已发现的 provider，不会启动端口。常见问题：

- `no provider`：检查 `AI_GATEWAY_UPSTREAM_API_KEY`，或确认 `OPENAI_API_KEY` / `OLLAMA_MODEL` 已设置。
- `401`：设置了 `AI_GATEWAY_API_KEY` 后，请求必须带 `Authorization: Bearer ...`。
- Docker 访问宿主机 Ollama：使用 `http://host.docker.internal:11434`，Compose 已配置映射。
- 上游超时：调整 `AI_GATEWAY_TIMEOUT`，并检查 provider 的 base URL 是否包含正确的 `/v1`。

## 开发

```bash
cargo fmt --check
cargo test --locked
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo build --release --locked
docker build --tag ai-gateway:test .
```

## 许可证

MIT
