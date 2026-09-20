# AI Gateway

一个可以直接替换 `base_url` 的 AI API 网关。

它把 OpenAI、OpenRouter、DeepSeek、SiliconFlow、Anthropic、Gemini 和 Ollama 放到一个入口，提供故障切换、限流、客户端密钥、用量审计和成本统计。运行时是 Rust 单二进制，默认使用 SQLite，不需要 Postgres、Redis 或前端构建工具。

<p>
  <a href="https://github.com/HP-network/ai-gateway/actions/workflows/ci.yml"><img src="https://github.com/HP-network/ai-gateway/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://github.com/HP-network/ai-gateway/releases"><img src="https://img.shields.io/github/v/release/HP-network/ai-gateway" alt="Release"></a>
  <a href="https://github.com/HP-network/ai-gateway/blob/main/LICENSE"><img src="https://img.shields.io/github/license/HP-network/ai-gateway" alt="MIT license"></a>
  <img src="https://img.shields.io/badge/Rust-1.88%2B-orange" alt="Rust 1.88 or newer">
</p>

## 先决定它适不适合你

适合：你有一个或多个模型 API，想让应用只认一个稳定地址；需要自动切换、密钥隔离、限流、审计，或者想在本地跑 Ollama。

不适合：你需要订阅额度池、充值支付、OAuth 账号池或完整的 SaaS 用户系统。那类需求应该选择专门的配额分发平台；本项目刻意保持单机、低依赖和容易迁移。

## 60 秒启动

### Docker

```bash
git clone https://github.com/HP-network/ai-gateway.git
cd ai-gateway
cp .env.example .env
```

编辑 `.env`，最少填 provider 和上游 key。下面以 OpenRouter 为例，换成 OpenAI、DeepSeek 或 SiliconFlow 时只改这两行：

```dotenv
AI_GATEWAY_UPSTREAM_PROVIDER=openrouter
AI_GATEWAY_UPSTREAM_API_KEY=sk-or-v1-...
```

程序会自动选择对应 Base URL 和默认模型。需要自定义模型时再加 `AI_GATEWAY_UPSTREAM_MODEL`；自定义 OpenAI-compatible 服务时可以省略 provider，直接填写 `AI_GATEWAY_UPSTREAM_BASE_URL`、`AI_GATEWAY_UPSTREAM_MODEL` 和 key。

复制 `.env.example` 的 Docker 演示默认不会把固定 key 写进仓库。容器第一次启动时会在持久化 volume 中生成随机的 app/admin key：

```bash
docker compose up -d --build
docker compose exec -T ai-gateway cat /data/generated-keys.env > .docker-keys.env
chmod 600 .docker-keys.env
set -a; source .docker-keys.env; set +a
curl http://127.0.0.1:8080/live
```

记下 admin key，启动后用它打开 dashboard；客户端使用 app key。你也可以在 `.env` 中显式设置两把 key，入口脚本会保留你的值。

调用：

```bash
curl http://127.0.0.1:8080/v1/chat/completions \
  -H "Authorization: Bearer $AI_GATEWAY_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"model":"auto","messages":[{"role":"user","content":"用一句话介绍你自己"}]}'
```

### 本地二进制

需要 Rust 1.88 或更新版本：

```bash
git clone https://github.com/HP-network/ai-gateway.git
cd ai-gateway
cargo run --release -- init --provider openrouter
```

`init` 会生成 `.env`、随机的应用 key 和管理 key。把上游 token 填进去，然后：

```bash
cargo run --release -- doctor
cargo run --release
```

可用的预设：`openai`、`openrouter`、`deepseek`、`siliconflow`、`anthropic`、`gemini`、`ollama`。已有 `.env` 时 `init` 不会覆盖它。

## 接入任何 OpenAI SDK

只改 `base_url`，请求格式不变：

```python
from openai import OpenAI

client = OpenAI(
    base_url="http://127.0.0.1:8080/v1",
    api_key="your-ai-gateway-key",
)

result = client.chat.completions.create(
    model="auto",
    messages=[{"role": "user", "content": "给我一个可执行的产品点子"}],
)
print(result.choices[0].message.content)
```

流式响应也是标准 OpenAI SSE：

```bash
curl --no-buffer http://127.0.0.1:8080/v1/chat/completions \
  -H "Authorization: Bearer ${AI_GATEWAY_API_KEY}" \
  -H "Content-Type: application/json" \
  -d '{"model":"auto","stream":true,"messages":[{"role":"user","content":"说三句短句"}]}'
```

## 上游预设

| 服务 | Base URL | 示例模型 |
| --- | --- | --- |
| OpenAI | `https://api.openai.com/v1` | `gpt-4o-mini` |
| OpenRouter | `https://openrouter.ai/api/v1` | `openai/gpt-4o-mini` |
| DeepSeek | `https://api.deepseek.com/v1` | `deepseek-chat` |
| SiliconFlow | `https://api.siliconflow.cn/v1` | `deepseek-ai/DeepSeek-V3` |
| Anthropic | `https://api.anthropic.com` | `claude-3-5-haiku-latest` |
| Gemini | `https://generativelanguage.googleapis.com` | `gemini-2.0-flash` |
| Ollama | `http://127.0.0.1:11434` | `llama3.2` |

OpenAI-compatible 服务只需要设置 provider 和 key；多个 provider、模型别名和 task route 再使用 `config.json`，不要为了单个上游一开始就写 JSON。

## 管理面板

启动后打开 <http://127.0.0.1:8080/dashboard>，输入 `AI_GATEWAY_ADMIN_API_KEY`。面板提供：

- 总请求、成功率、tokens、平均延迟和估算成本
- provider 状态、并发槽位和错误次数
- 最近请求审计、provider/model 分解
- 创建和撤销客户端 API key，并设置请求/token 配额

管理 API 示例：

```bash
export ADMIN_KEY=your-admin-key
curl http://127.0.0.1:8080/admin/stats \
  -H "Authorization: Bearer $ADMIN_KEY"

curl -X POST http://127.0.0.1:8080/admin/api-keys \
  -H "Authorization: Bearer $ADMIN_KEY" \
  -H "Content-Type: application/json" \
  -d '{"name":"my-app","request_limit":10000,"token_limit":5000000}'
```

创建 key 的 token 只在创建响应中显示一次。数据库默认是 `ai-gateway.db`；Compose 会把它放在持久化 volume 中。审计记录默认保留 30 天，可用 `AI_GATEWAY_AUDIT_RETENTION_DAYS=0` 关闭自动清理。

`token_limit` 会在请求开始时按消息 JSON 大小加 `max_tokens` 做预留，用来防止并发请求穿透配额；上游返回的真实 usage 会在请求结束后结算。它不是 tokenizer 精确计费，生产计费请以 provider 的 usage 为准。

## 多 provider 路由

复制 `config.example.json` 后再启动：

```bash
cp config.example.json config.json
ai-gateway check-config --config config.json
ai-gateway --config config.json
```

可以配置 provider 优先级、失败冷却、最大并发、模型别名和 task route：

```json
{
  "routing": {
    "default_model": "auto",
    "max_retries": 2,
    "model_aliases": {"fast": "gpt-4o-mini"},
    "task_routes": {"code": ["deepseek", "openai"]}
  }
}
```

密钥使用 `api_key_env` 从环境变量读取，不要把真实 token 写进仓库。价格字段是每百万 token 的美元价格，用于估算成本，不会向上游重新计费。

## API 速查

| 方法 | 地址 | 用途 |
| --- | --- | --- |
| `POST` | `/v1/chat/completions` | 对话和 SSE streaming |
| `GET` | `/v1/models` | 已配置模型 |
| `GET` | `/live` | 无鉴权存活探针 |
| `GET` | `/ready` | provider 就绪探针 |
| `GET` | `/health` | provider 健康状态 |
| `GET` | `/metrics` | Prometheus 指标 |
| `GET` | `/dashboard` | 浏览器管理面板 |
| `GET` | `/admin/stats` | 聚合用量 |
| `GET` | `/admin/requests?limit=50` | 最近请求 |
| `GET` | `/admin/breakdown` | provider/model 统计 |
| `GET/POST` | `/admin/api-keys` | 客户端 key |
| `DELETE` | `/admin/api-keys/:id` | 撤销 key |

## 配置排查

```bash
ai-gateway doctor
ai-gateway check-config
```

`doctor` 用人话显示监听地址、鉴权、provider、模型和凭据状态。常见问题：

- `no provider credentials`：给上游填写 `AI_GATEWAY_UPSTREAM_API_KEY`，或使用 Ollama。
- `401`：客户端 key 和管理 key 是两套值，分别使用 `AI_GATEWAY_API_KEY` 与 `AI_GATEWAY_ADMIN_API_KEY`。
- Docker 访问宿主机 Ollama：使用 `http://host.docker.internal:11434`。
- 上游超时：检查 Base URL 是否包含正确的 `/v1`，再调整 `AI_GATEWAY_TIMEOUT`。

生产环境暴露到局域网或公网时，务必设置应用 key 和管理 key，并在反向代理层启用 TLS。不要把 `.env`、数据库或真实 provider token 提交到 Git。

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
