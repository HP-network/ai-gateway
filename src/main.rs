use anyhow::{anyhow, Context, Result};
use async_stream::try_stream;
use axum::{
    body::Body,
    extract::{DefaultBodyLimit, Path, State},
    http::{header, HeaderMap, StatusCode},
    response::{Html, IntoResponse, Response},
    routing::{delete, get, post},
    Json, Router,
};
use bytes::Bytes;
use futures_util::StreamExt;
use reqwest::Client;
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, VecDeque},
    env, fs, io,
    net::SocketAddr,
    path::{Path as FsPath, PathBuf},
    pin::Pin,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::net::TcpListener;
use tower_http::{catch_panic::CatchPanicLayer, trace::TraceLayer};
use tracing::{error, info};

const VERSION: &str = "0.4.1";
const MAX_BODY_BYTES: usize = 2_000_000;

#[derive(Clone)]
struct AppState {
    config: Arc<Config>,
    client: Client,
    router: Arc<RouterState>,
    store: Arc<Mutex<Store>>,
    metrics: Arc<Mutex<Metrics>>,
    limiter: Arc<RateLimiter>,
}

#[derive(Clone, Debug)]
struct Config {
    server: ServerConfig,
    routing: RoutingConfig,
    providers: Vec<ProviderConfig>,
}

#[derive(Clone, Debug)]
struct ServerConfig {
    host: String,
    port: u16,
    api_key: Option<String>,
    admin_api_key: Option<String>,
    database_path: String,
    rate_limit_per_minute: usize,
    request_timeout_seconds: f64,
}

#[derive(Clone, Debug)]
struct RoutingConfig {
    default_model: String,
    max_retries: usize,
    failure_cooldown_seconds: f64,
    task_routes: HashMap<String, Vec<String>>,
}

#[derive(Clone, Debug)]
struct ProviderConfig {
    name: String,
    kind: String,
    base_url: String,
    model: String,
    api_key: Option<String>,
    timeout_seconds: f64,
    priority: i64,
    headers: HashMap<String, String>,
}

#[derive(Default)]
struct Metrics {
    total_requests: u64,
    successful_requests: u64,
    failed_requests: u64,
    total_latency_seconds: f64,
}

impl Metrics {
    fn record(&mut self, success: bool, elapsed: Duration) {
        self.total_requests += 1;
        if success {
            self.successful_requests += 1;
        } else {
            self.failed_requests += 1;
        }
        self.total_latency_seconds += elapsed.as_secs_f64();
    }

    fn prometheus(&self) -> String {
        let average = if self.total_requests == 0 {
            0.0
        } else {
            self.total_latency_seconds / self.total_requests as f64
        };
        format!(
            "# HELP ai_gateway_requests_total Total chat completion requests.\n\
             # TYPE ai_gateway_requests_total counter\n\
             ai_gateway_requests_total {}\n\
             # HELP ai_gateway_requests_success_total Successful chat completion requests.\n\
             # TYPE ai_gateway_requests_success_total counter\n\
             ai_gateway_requests_success_total {}\n\
             # HELP ai_gateway_requests_failed_total Failed chat completion requests.\n\
             # TYPE ai_gateway_requests_failed_total counter\n\
             ai_gateway_requests_failed_total {}\n\
             # HELP ai_gateway_request_latency_seconds Average request latency.\n\
             # TYPE ai_gateway_request_latency_seconds gauge\n\
             ai_gateway_request_latency_seconds {average:.6}\n",
            self.total_requests, self.successful_requests, self.failed_requests
        )
    }
}

struct RateLimiter {
    per_minute: usize,
    windows: Mutex<HashMap<String, VecDeque<Instant>>>,
}

impl RateLimiter {
    fn new(per_minute: usize) -> Self {
        Self {
            per_minute,
            windows: Mutex::new(HashMap::new()),
        }
    }

    fn allow(&self, identity: &str) -> bool {
        if self.per_minute == 0 {
            return true;
        }
        let now = Instant::now();
        let mut windows = self.windows.lock().expect("rate limiter poisoned");
        let window = windows.entry(identity.to_owned()).or_default();
        while window
            .front()
            .map(|value| now.duration_since(*value) >= Duration::from_secs(60))
            .unwrap_or(false)
        {
            window.pop_front();
        }
        if window.len() >= self.per_minute {
            return false;
        }
        window.push_back(now);
        if windows.len() > 10_000 {
            windows.retain(|_, values| {
                values
                    .back()
                    .map(|value| now.duration_since(*value) < Duration::from_secs(60))
                    .unwrap_or(false)
            });
        }
        true
    }
}

struct RouterState {
    routing: RoutingConfig,
    providers: Vec<Mutex<ProviderRuntime>>,
}

struct ProviderRuntime {
    config: ProviderConfig,
    failures: u32,
    unavailable_until: Option<Instant>,
    requests: u64,
    errors: u64,
    latency_total: Duration,
}

#[derive(Serialize)]
struct ProviderHealth {
    name: String,
    kind: String,
    model: String,
    healthy: bool,
    status: String,
    failures: u32,
    requests: u64,
    errors: u64,
    average_latency_ms: f64,
}

impl RouterState {
    fn new(config: &Config) -> Self {
        let providers = config
            .providers
            .iter()
            .cloned()
            .map(|config| {
                Mutex::new(ProviderRuntime {
                    config,
                    failures: 0,
                    unavailable_until: None,
                    requests: 0,
                    errors: 0,
                    latency_total: Duration::ZERO,
                })
            })
            .collect();
        Self {
            routing: config.routing.clone(),
            providers,
        }
    }

    async fn route(&self, client: &Client, request: &ChatRequest) -> Result<ChatResponse> {
        let candidates = self.candidates(request);
        if candidates.is_empty() {
            return Err(anyhow!("no healthy provider matches this request"));
        }
        let attempts = candidates
            .len()
            .min(self.routing.max_retries.saturating_add(1));
        let mut errors = Vec::new();
        for index in candidates.into_iter().take(attempts) {
            let (provider, started) = {
                let mut runtime = self.providers[index]
                    .lock()
                    .expect("provider state poisoned");
                runtime.requests += 1;
                (runtime.config.clone(), Instant::now())
            };
            match provider_chat(client, &provider, request).await {
                Ok(response) => {
                    let mut runtime = self.providers[index]
                        .lock()
                        .expect("provider state poisoned");
                    runtime.failures = 0;
                    runtime.unavailable_until = None;
                    runtime.latency_total += started.elapsed();
                    return Ok(response);
                }
                Err(error) => {
                    let elapsed = started.elapsed();
                    let mut runtime = self.providers[index]
                        .lock()
                        .expect("provider state poisoned");
                    runtime.errors += 1;
                    runtime.failures += 1;
                    runtime.latency_total += elapsed;
                    if runtime.failures >= 2 {
                        runtime.unavailable_until = Some(
                            Instant::now()
                                + Duration::from_secs_f64(self.routing.failure_cooldown_seconds),
                        );
                    }
                    errors.push(format!("{}: {error}", provider.name));
                }
            }
        }
        Err(anyhow!("all providers failed: {}", errors.join("; ")))
    }

    async fn route_stream(
        &self,
        client: &Client,
        request: &ChatRequest,
    ) -> Result<(ProviderConfig, reqwest::Response)> {
        let candidates = self.candidates(request);
        if candidates.is_empty() {
            return Err(anyhow!("no healthy provider matches this request"));
        }
        let attempts = candidates
            .len()
            .min(self.routing.max_retries.saturating_add(1));
        let mut errors = Vec::new();
        for index in candidates.into_iter().take(attempts) {
            let (provider, started) = {
                let mut runtime = self.providers[index]
                    .lock()
                    .expect("provider state poisoned");
                runtime.requests += 1;
                (runtime.config.clone(), Instant::now())
            };
            match provider_stream(client, &provider, request).await {
                Ok(response) => {
                    let mut runtime = self.providers[index]
                        .lock()
                        .expect("provider state poisoned");
                    runtime.failures = 0;
                    runtime.unavailable_until = None;
                    runtime.latency_total += started.elapsed();
                    return Ok((provider, response));
                }
                Err(error) => {
                    let elapsed = started.elapsed();
                    let mut runtime = self.providers[index]
                        .lock()
                        .expect("provider state poisoned");
                    runtime.errors += 1;
                    runtime.failures += 1;
                    runtime.latency_total += elapsed;
                    if runtime.failures >= 2 {
                        runtime.unavailable_until = Some(
                            Instant::now()
                                + Duration::from_secs_f64(self.routing.failure_cooldown_seconds),
                        );
                    }
                    errors.push(format!("{}: {error}", provider.name));
                }
            }
        }
        Err(anyhow!("all providers failed: {}", errors.join("; ")))
    }

    fn candidates(&self, request: &ChatRequest) -> Vec<usize> {
        let route_names = request
            .task
            .as_ref()
            .and_then(|task| self.routing.task_routes.get(task));
        let requested_model = request
            .model
            .as_deref()
            .filter(|value| !value.is_empty() && *value != "auto")
            .or_else(|| {
                if self.routing.default_model.is_empty() || self.routing.default_model == "auto" {
                    None
                } else {
                    Some(self.routing.default_model.as_str())
                }
            });
        let mut indices: Vec<usize> = (0..self.providers.len())
            .filter(|index| {
                let runtime = self.providers[*index]
                    .lock()
                    .expect("provider state poisoned");
                let available = runtime
                    .unavailable_until
                    .map(|until| Instant::now() >= until)
                    .unwrap_or(true);
                let route_match = route_names
                    .map(|names| names.iter().any(|name| name == &runtime.config.name))
                    .unwrap_or(true);
                let model_match = requested_model
                    .map(|model| runtime.config.name == model || runtime.config.model == model)
                    .unwrap_or(true);
                available && route_match && model_match
            })
            .collect();
        if let Some(names) = route_names {
            indices.sort_by_key(|index| {
                let runtime = self.providers[*index]
                    .lock()
                    .expect("provider state poisoned");
                names
                    .iter()
                    .position(|name| name == &runtime.config.name)
                    .unwrap_or(usize::MAX)
            });
        } else {
            indices.sort_by(|left, right| {
                let left_runtime = self.providers[*left]
                    .lock()
                    .expect("provider state poisoned");
                let right_runtime = self.providers[*right]
                    .lock()
                    .expect("provider state poisoned");
                right_runtime
                    .config
                    .priority
                    .cmp(&left_runtime.config.priority)
                    .then_with(|| left_runtime.config.name.cmp(&right_runtime.config.name))
            });
        }
        indices
    }

    fn health(&self) -> Vec<ProviderHealth> {
        self.providers
            .iter()
            .map(|provider| {
                let runtime = provider.lock().expect("provider state poisoned");
                let status = if runtime.requests == 0 {
                    "unknown"
                } else if runtime
                    .unavailable_until
                    .map(|until| Instant::now() < until)
                    .unwrap_or(false)
                {
                    "unavailable"
                } else if runtime.failures == 0 {
                    "healthy"
                } else {
                    "degraded"
                };
                ProviderHealth {
                    name: runtime.config.name.clone(),
                    kind: runtime.config.kind.clone(),
                    model: runtime.config.model.clone(),
                    healthy: status == "healthy",
                    status: status.to_owned(),
                    failures: runtime.failures,
                    requests: runtime.requests,
                    errors: runtime.errors,
                    average_latency_ms: if runtime.requests == 0 {
                        0.0
                    } else {
                        runtime.latency_total.as_secs_f64() * 1000.0 / runtime.requests as f64
                    },
                }
            })
            .collect()
    }
}

#[derive(Clone, Debug)]
struct ChatRequest {
    model: Option<String>,
    messages: Vec<Message>,
    temperature: Option<f64>,
    max_tokens: Option<i64>,
    stream: bool,
    task: Option<String>,
    extra: Map<String, Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Message {
    role: String,
    content: Value,
}

impl ChatRequest {
    fn parse(value: Value) -> Result<Self> {
        let object = value
            .as_object()
            .ok_or_else(|| anyhow!("request body must be a JSON object"))?;
        let raw_messages = object
            .get("messages")
            .and_then(Value::as_array)
            .filter(|items| !items.is_empty())
            .ok_or_else(|| anyhow!("messages must be a non-empty array"))?;
        let mut messages = Vec::with_capacity(raw_messages.len());
        for raw in raw_messages {
            let message: Message =
                serde_json::from_value(raw.clone()).context("each message must be an object")?;
            if !matches!(
                message.role.as_str(),
                "system" | "user" | "assistant" | "tool"
            ) {
                return Err(anyhow!(
                    "message role must be system, user, assistant, or tool"
                ));
            }
            if !message.content.is_string() && !message.content.is_array() {
                return Err(anyhow!(
                    "message content must be a string or content-part list"
                ));
            }
            messages.push(message);
        }
        let model = optional_nonempty_string(object.get("model"))?;
        let temperature = object
            .get("temperature")
            .map(|value| {
                value
                    .as_f64()
                    .ok_or_else(|| anyhow!("temperature must be between 0 and 2"))
            })
            .transpose()?;
        if temperature
            .map(|value| !value.is_finite() || !(0.0..=2.0).contains(&value))
            .unwrap_or(false)
        {
            return Err(anyhow!("temperature must be between 0 and 2"));
        }
        let max_tokens = object
            .get("max_tokens")
            .map(|value| {
                value
                    .as_i64()
                    .ok_or_else(|| anyhow!("max_tokens must be a positive integer"))
            })
            .transpose()?;
        if max_tokens.map(|value| value < 1).unwrap_or(false) {
            return Err(anyhow!("max_tokens must be a positive integer"));
        }
        let stream = object
            .get("stream")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let task = optional_nonempty_string(object.get("task"))?;
        let mut extra = object.clone();
        for key in [
            "model",
            "messages",
            "temperature",
            "max_tokens",
            "stream",
            "task",
        ] {
            extra.remove(key);
        }
        Ok(Self {
            model,
            messages,
            temperature,
            max_tokens,
            stream,
            task,
            extra,
        })
    }
}

fn optional_nonempty_string(value: Option<&Value>) -> Result<Option<String>> {
    value
        .map(|value| {
            value
                .as_str()
                .filter(|text| !text.trim().is_empty())
                .map(str::to_owned)
                .ok_or_else(|| anyhow!("value must be a non-empty string"))
        })
        .transpose()
}

#[derive(Clone, Debug)]
struct ChatResponse {
    model: String,
    content: String,
    finish_reason: String,
    usage: Usage,
}

#[derive(Clone, Debug, Default)]
struct Usage {
    prompt_tokens: u64,
    completion_tokens: u64,
    total_tokens: u64,
}

impl Usage {
    fn from_value(value: Option<&Value>) -> Self {
        let object = value.and_then(Value::as_object);
        let get = |keys: &[&str]| {
            keys.iter()
                .find_map(|key| {
                    object
                        .and_then(|value| value.get(*key))
                        .and_then(Value::as_u64)
                })
                .unwrap_or(0)
        };
        let prompt_tokens = get(&["prompt_tokens", "input_tokens", "promptTokenCount"]);
        let completion_tokens =
            get(&["completion_tokens", "output_tokens", "candidatesTokenCount"]);
        let total_tokens =
            get(&["total_tokens", "totalTokenCount"]).max(prompt_tokens + completion_tokens);
        Self {
            prompt_tokens,
            completion_tokens,
            total_tokens,
        }
    }

    fn json(&self) -> Value {
        json!({
            "prompt_tokens": self.prompt_tokens,
            "completion_tokens": self.completion_tokens,
            "total_tokens": self.total_tokens
        })
    }
}

async fn provider_chat(
    client: &Client,
    provider: &ProviderConfig,
    request: &ChatRequest,
) -> Result<ChatResponse> {
    let response = provider_request(client, provider, request, false)?
        .send()
        .await?;
    let status = response.status();
    let body: Value = response.json().await.unwrap_or_else(|_| json!({}));
    if !status.is_success() {
        return Err(anyhow!(
            "provider returned HTTP {status}: {}",
            body.to_string().chars().take(500).collect::<String>()
        ));
    }
    normalize_response(provider, body)
}

async fn provider_stream(
    client: &Client,
    provider: &ProviderConfig,
    request: &ChatRequest,
) -> Result<reqwest::Response> {
    let response = provider_request(client, provider, request, true)?
        .send()
        .await?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(anyhow!(
            "provider returned HTTP {status}: {}",
            body.chars().take(500).collect::<String>()
        ));
    }
    Ok(response)
}

fn provider_request(
    client: &Client,
    provider: &ProviderConfig,
    request: &ChatRequest,
    stream: bool,
) -> Result<reqwest::RequestBuilder> {
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(reqwest::header::CONTENT_TYPE, "application/json".parse()?);
    for (name, value) in &provider.headers {
        headers.insert(
            reqwest::header::HeaderName::try_from(name)?,
            reqwest::header::HeaderValue::try_from(value)?,
        );
    }
    let (url, payload) = match provider.kind.as_str() {
        "openai" | "openai-compatible" => {
            let mut body = request.extra.clone();
            body.insert("model".to_owned(), Value::String(provider.model.clone()));
            body.insert(
                "messages".to_owned(),
                serde_json::to_value(&request.messages)?,
            );
            body.insert("stream".to_owned(), Value::Bool(stream));
            if let Some(value) = request.temperature {
                body.insert("temperature".to_owned(), json!(value));
            }
            if let Some(value) = request.max_tokens {
                body.insert("max_tokens".to_owned(), json!(value));
            }
            (
                format!(
                    "{}/chat/completions",
                    provider.base_url.trim_end_matches('/')
                ),
                Value::Object(body),
            )
        }
        "anthropic" => {
            let mut messages = Vec::new();
            let mut system = Vec::new();
            for message in &request.messages {
                if message.role == "system" {
                    system.push(content_text(&message.content));
                } else {
                    messages.push(json!({"role": message.role, "content": message.content}));
                }
            }
            let mut body = request.extra.clone();
            body.insert("model".to_owned(), Value::String(provider.model.clone()));
            body.insert(
                "max_tokens".to_owned(),
                json!(request.max_tokens.unwrap_or(1024)),
            );
            body.insert("messages".to_owned(), Value::Array(messages));
            body.insert("stream".to_owned(), Value::Bool(stream));
            if !system.is_empty() {
                body.insert("system".to_owned(), Value::String(system.join("\n")));
            }
            if let Some(value) = request.temperature {
                body.insert("temperature".to_owned(), json!(value));
            }
            (
                format!("{}/v1/messages", provider.base_url.trim_end_matches('/')),
                Value::Object(body),
            )
        }
        "gemini" => {
            let contents = request
                .messages
                .iter()
                .map(|message| {
                    json!({
                        "role": if message.role == "assistant" { "model" } else { "user" },
                        "parts": [{"text": content_text(&message.content)}]
                    })
                })
                .collect::<Vec<_>>();
            let mut body = request.extra.clone();
            body.insert("contents".to_owned(), Value::Array(contents));
            if let Some(value) = request.temperature {
                body.insert("generationConfig".to_owned(), json!({"temperature": value}));
            }
            (
                format!(
                    "{}/v1beta/models/{}:{}",
                    provider.base_url.trim_end_matches('/'),
                    provider.model,
                    if stream {
                        "streamGenerateContent"
                    } else {
                        "generateContent"
                    }
                ),
                Value::Object(body),
            )
        }
        "ollama" => {
            let mut body = request.extra.clone();
            body.insert("model".to_owned(), Value::String(provider.model.clone()));
            body.insert(
                "messages".to_owned(),
                serde_json::to_value(&request.messages)?,
            );
            body.insert("stream".to_owned(), Value::Bool(stream));
            if let Some(value) = request.temperature {
                body.insert("options".to_owned(), json!({"temperature": value}));
            }
            (
                format!("{}/api/chat", provider.base_url.trim_end_matches('/')),
                Value::Object(body),
            )
        }
        kind => return Err(anyhow!("unsupported provider kind: {kind}")),
    };
    let mut builder = client
        .post(url)
        .headers(headers)
        .timeout(Duration::from_secs_f64(provider.timeout_seconds));
    match provider.kind.as_str() {
        "openai" | "openai-compatible" => {
            if let Some(key) = &provider.api_key {
                builder = builder.bearer_auth(key);
            }
        }
        "anthropic" => {
            builder = builder
                .header("x-api-key", provider.api_key.clone().unwrap_or_default())
                .header("anthropic-version", "2023-06-01");
        }
        "gemini" => {
            if let Some(key) = &provider.api_key {
                builder = builder.query(&[("key", key)]);
            }
            if stream {
                builder = builder.query(&[("alt", "sse")]);
            }
        }
        _ => {}
    }
    Ok(builder.json(&payload))
}

type ByteStream = Pin<Box<dyn futures_util::Stream<Item = Result<Bytes, io::Error>> + Send>>;

fn provider_stream_body(
    response: reqwest::Response,
    provider: &ProviderConfig,
    model: String,
) -> ByteStream {
    if matches!(provider.kind.as_str(), "openai" | "openai-compatible") {
        return Box::pin(
            response
                .bytes_stream()
                .map(|chunk| chunk.map_err(|error| io::Error::other(error.to_string()))),
        );
    }
    let kind = provider.kind.clone();
    let stream = try_stream! {
        let mut source = response.bytes_stream();
        let mut buffer = String::new();
        let mut normalizer = StreamNormalizer::new(kind, model);
        yield normalizer.initial();
        while let Some(chunk) = source.next().await {
            let chunk = chunk.map_err(|error| io::Error::other(error.to_string()))?;
            buffer.push_str(&String::from_utf8_lossy(&chunk));
            while let Some(position) = buffer.find('\n') {
                let line = buffer[..position].trim_end_matches('\r').to_owned();
                buffer.drain(..=position);
                for frame in normalizer.process_line(&line) {
                    yield frame;
                }
            }
        }
        if !buffer.trim().is_empty() {
            for frame in normalizer.process_line(buffer.trim()) {
                yield frame;
            }
        }
        for frame in normalizer.finish() {
            yield frame;
        }
    };
    Box::pin(stream)
}

struct StreamNormalizer {
    kind: String,
    id: String,
    created: i64,
    model: String,
    finished: bool,
}

impl StreamNormalizer {
    fn new(kind: String, model: String) -> Self {
        Self {
            kind,
            id: format!("chatcmpl-{}", uuid::Uuid::new_v4().simple()),
            created: unix_now(),
            model,
            finished: false,
        }
    }

    fn initial(&mut self) -> Bytes {
        stream_chunk(
            &self.id,
            self.created,
            &self.model,
            Some("assistant"),
            "",
            None,
        )
    }

    fn process_line(&mut self, line: &str) -> Vec<Bytes> {
        let line = line.trim();
        if line.is_empty() || line.starts_with("event:") {
            return Vec::new();
        }
        let data = line.strip_prefix("data:").map(str::trim).unwrap_or(line);
        if data.is_empty() || data == "[DONE]" {
            self.finished = true;
            return vec![stream_done()];
        }
        let Ok(value) = serde_json::from_str::<Value>(data) else {
            return Vec::new();
        };
        match self.kind.as_str() {
            "anthropic" => self.anthropic(value),
            "gemini" => self.gemini(value),
            "ollama" => self.ollama(value),
            _ => Vec::new(),
        }
    }

    fn anthropic(&mut self, value: Value) -> Vec<Bytes> {
        match value.get("type").and_then(Value::as_str) {
            Some("content_block_delta") => value
                .get("delta")
                .and_then(|delta| delta.get("text"))
                .and_then(Value::as_str)
                .map(|text| {
                    vec![stream_chunk(
                        &self.id,
                        self.created,
                        &self.model,
                        None,
                        text,
                        None,
                    )]
                })
                .unwrap_or_default(),
            Some("message_delta") => {
                let reason = value
                    .get("delta")
                    .and_then(|delta| delta.get("stop_reason"))
                    .and_then(Value::as_str)
                    .unwrap_or("stop");
                self.finish_with(reason)
            }
            Some("message_stop") => self.finish_with("stop"),
            _ => Vec::new(),
        }
    }

    fn gemini(&mut self, value: Value) -> Vec<Bytes> {
        let mut frames = Vec::new();
        if let Some(parts) = value
            .get("candidates")
            .and_then(Value::as_array)
            .and_then(|candidates| candidates.first())
            .and_then(|candidate| candidate.get("content"))
            .and_then(|content| content.get("parts"))
            .and_then(Value::as_array)
        {
            for text in parts
                .iter()
                .filter_map(|part| part.get("text").and_then(Value::as_str))
            {
                frames.push(stream_chunk(
                    &self.id,
                    self.created,
                    &self.model,
                    None,
                    text,
                    None,
                ));
            }
        }
        if value
            .get("candidates")
            .and_then(Value::as_array)
            .and_then(|candidates| candidates.first())
            .and_then(|candidate| candidate.get("finishReason"))
            .and_then(Value::as_str)
            .is_some()
        {
            frames.extend(self.finish_with("stop"));
        }
        frames
    }

    fn ollama(&mut self, value: Value) -> Vec<Bytes> {
        let mut frames = Vec::new();
        if let Some(text) = value
            .get("message")
            .and_then(|message| message.get("content"))
            .and_then(Value::as_str)
            .filter(|text| !text.is_empty())
        {
            frames.push(stream_chunk(
                &self.id,
                self.created,
                &self.model,
                None,
                text,
                None,
            ));
        }
        if value.get("done").and_then(Value::as_bool).unwrap_or(false) {
            frames.extend(self.finish_with("stop"));
        }
        frames
    }

    fn finish_with(&mut self, reason: &str) -> Vec<Bytes> {
        if self.finished {
            return Vec::new();
        }
        self.finished = true;
        vec![
            stream_chunk(&self.id, self.created, &self.model, None, "", Some(reason)),
            stream_done(),
        ]
    }

    fn finish(&mut self) -> Vec<Bytes> {
        self.finish_with("stop")
    }
}

fn stream_chunk(
    id: &str,
    created: i64,
    model: &str,
    role: Option<&str>,
    content: &str,
    finish_reason: Option<&str>,
) -> Bytes {
    let mut delta = Map::new();
    if let Some(role) = role {
        delta.insert("role".to_owned(), Value::String(role.to_owned()));
    }
    delta.insert("content".to_owned(), Value::String(content.to_owned()));
    let value = json!({
        "id": id,
        "object": "chat.completion.chunk",
        "created": created,
        "model": model,
        "choices": [{"index": 0, "delta": delta, "finish_reason": finish_reason}]
    });
    Bytes::from(format!("data: {value}\n\n"))
}

fn stream_done() -> Bytes {
    Bytes::from_static(b"data: [DONE]\n\n")
}

fn normalize_response(provider: &ProviderConfig, body: Value) -> Result<ChatResponse> {
    let usage = Usage::from_value(body.get("usage").or_else(|| body.get("usageMetadata")));
    match provider.kind.as_str() {
        "openai" | "openai-compatible" => {
            let choice = body
                .get("choices")
                .and_then(Value::as_array)
                .and_then(|choices| choices.first())
                .ok_or_else(|| anyhow!("invalid OpenAI response"))?;
            let message = choice
                .get("message")
                .ok_or_else(|| anyhow!("invalid OpenAI response"))?;
            Ok(ChatResponse {
                model: provider.model.clone(),
                content: content_text(message.get("content").unwrap_or(&Value::Null)),
                finish_reason: choice
                    .get("finish_reason")
                    .and_then(Value::as_str)
                    .unwrap_or("stop")
                    .to_owned(),
                usage,
            })
        }
        "anthropic" => Ok(ChatResponse {
            model: provider.model.clone(),
            content: body
                .get("content")
                .and_then(Value::as_array)
                .map(|parts| {
                    parts
                        .iter()
                        .map(|part| part.get("text").and_then(Value::as_str).unwrap_or_default())
                        .collect::<String>()
                })
                .ok_or_else(|| anyhow!("invalid Anthropic response"))?,
            finish_reason: body
                .get("stop_reason")
                .and_then(Value::as_str)
                .unwrap_or("stop")
                .to_owned(),
            usage,
        }),
        "gemini" => Ok(ChatResponse {
            model: provider.model.clone(),
            content: body
                .get("candidates")
                .and_then(Value::as_array)
                .and_then(|items| items.first())
                .and_then(|candidate| candidate.get("content"))
                .and_then(|content| content.get("parts"))
                .and_then(Value::as_array)
                .map(|parts| {
                    parts
                        .iter()
                        .map(|part| part.get("text").and_then(Value::as_str).unwrap_or_default())
                        .collect::<String>()
                })
                .ok_or_else(|| anyhow!("invalid Gemini response"))?,
            finish_reason: "stop".to_owned(),
            usage,
        }),
        "ollama" => {
            let prompt_tokens = body
                .get("prompt_eval_count")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            let completion_tokens = body.get("eval_count").and_then(Value::as_u64).unwrap_or(0);
            Ok(ChatResponse {
                model: provider.model.clone(),
                content: body
                    .get("message")
                    .and_then(|message| message.get("content"))
                    .and_then(Value::as_str)
                    .ok_or_else(|| anyhow!("invalid Ollama response"))?
                    .to_owned(),
                finish_reason: "stop".to_owned(),
                usage: Usage {
                    prompt_tokens,
                    completion_tokens,
                    total_tokens: prompt_tokens + completion_tokens,
                },
            })
        }
        kind => Err(anyhow!("unsupported provider kind: {kind}")),
    }
}

fn content_text(value: &Value) -> String {
    if let Some(text) = value.as_str() {
        return text.to_owned();
    }
    value
        .as_array()
        .map(|parts| {
            parts
                .iter()
                .filter_map(|part| part.get("text").and_then(Value::as_str))
                .collect()
        })
        .unwrap_or_default()
}

#[derive(Serialize)]
struct ApiKey {
    id: i64,
    name: String,
    prefix: String,
    created_at: i64,
    last_used_at: Option<i64>,
    revoked_at: Option<i64>,
    requests: i64,
    tokens: i64,
}

struct Store {
    connection: Connection,
}

impl Store {
    fn open(path: &str) -> Result<Self> {
        let path = if path == ":memory:" {
            PathBuf::from(path)
        } else {
            let path = expand_user(FsPath::new(path));
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }
            path
        };
        let connection = Connection::open(path)?;
        connection.execute_batch("PRAGMA journal_mode = WAL; PRAGMA busy_timeout = 5000;")?;
        connection.execute_batch(
            "CREATE TABLE IF NOT EXISTS api_keys (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                prefix TEXT NOT NULL,
                digest TEXT NOT NULL UNIQUE,
                created_at INTEGER NOT NULL,
                last_used_at INTEGER,
                revoked_at INTEGER,
                requests INTEGER NOT NULL DEFAULT 0,
                tokens INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS usage (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                requests INTEGER NOT NULL DEFAULT 0,
                successful_requests INTEGER NOT NULL DEFAULT 0,
                failed_requests INTEGER NOT NULL DEFAULT 0,
                prompt_tokens INTEGER NOT NULL DEFAULT 0,
                completion_tokens INTEGER NOT NULL DEFAULT 0,
                total_tokens INTEGER NOT NULL DEFAULT 0,
                latency_seconds REAL NOT NULL DEFAULT 0
            );
            INSERT OR IGNORE INTO usage (id) VALUES (1);",
        )?;
        Ok(Self { connection })
    }

    fn has_keys(&self) -> Result<bool> {
        Ok(self
            .connection
            .query_row("SELECT EXISTS(SELECT 1 FROM api_keys)", [], |row| {
                row.get(0)
            })?)
    }

    fn create_key(&mut self, name: &str) -> Result<(String, ApiKey)> {
        let name = if name.trim().is_empty() {
            "default"
        } else {
            name.trim()
        };
        if name.len() > 100 {
            return Err(anyhow!("key name must be 100 characters or fewer"));
        }
        let token = format!("ag_{}", uuid::Uuid::new_v4().simple());
        let now = unix_now();
        self.connection.execute(
            "INSERT INTO api_keys (name, prefix, digest, created_at) VALUES (?, ?, ?, ?)",
            params![name, &token[..11], digest(&token), now],
        )?;
        let id = self.connection.last_insert_rowid();
        Ok((
            token,
            self.key_by_id(id)?.context("created key disappeared")?,
        ))
    }

    fn lookup(&self, token: &str) -> Result<Option<ApiKey>> {
        let digest = digest(token);
        Ok(self
            .connection
            .query_row(
                "SELECT id,name,prefix,created_at,last_used_at,revoked_at,requests,tokens
                 FROM api_keys WHERE digest = ? AND revoked_at IS NULL",
                [digest],
                row_to_key,
            )
            .optional()?)
    }

    fn touch(&mut self, id: i64) -> Result<()> {
        self.connection.execute(
            "UPDATE api_keys SET last_used_at = ?, requests = requests + 1
             WHERE id = ? AND revoked_at IS NULL",
            params![unix_now(), id],
        )?;
        Ok(())
    }

    fn revoke(&mut self, id: i64) -> Result<bool> {
        Ok(self.connection.execute(
            "UPDATE api_keys SET revoked_at = ? WHERE id = ? AND revoked_at IS NULL",
            params![unix_now(), id],
        )? == 1)
    }

    fn list_keys(&self) -> Result<Vec<ApiKey>> {
        let mut statement = self.connection.prepare(
            "SELECT id,name,prefix,created_at,last_used_at,revoked_at,requests,tokens
             FROM api_keys ORDER BY created_at DESC",
        )?;
        let keys = statement
            .query_map([], row_to_key)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(keys)
    }

    fn record(
        &mut self,
        success: bool,
        usage: &Usage,
        elapsed: Duration,
        key_id: Option<i64>,
    ) -> Result<()> {
        self.connection.execute(
            "UPDATE usage SET requests=requests+1,
             successful_requests=successful_requests+?,
             failed_requests=failed_requests+?,
             prompt_tokens=prompt_tokens+?,
             completion_tokens=completion_tokens+?,
             total_tokens=total_tokens+?,
             latency_seconds=latency_seconds+? WHERE id=1",
            params![
                success as i64,
                (!success) as i64,
                usage.prompt_tokens as i64,
                usage.completion_tokens as i64,
                usage.total_tokens as i64,
                elapsed.as_secs_f64()
            ],
        )?;
        if let Some(key_id) = key_id {
            self.connection.execute(
                "UPDATE api_keys SET tokens=tokens+? WHERE id=?",
                params![usage.total_tokens as i64, key_id],
            )?;
        }
        Ok(())
    }

    fn stats(&self) -> Result<Value> {
        let row = self.connection.query_row(
            "SELECT requests,successful_requests,failed_requests,prompt_tokens,
             completion_tokens,total_tokens,latency_seconds FROM usage WHERE id=1",
            [],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, i64>(5)?,
                    row.get::<_, f64>(6)?,
                ))
            },
        )?;
        let active: i64 = self.connection.query_row(
            "SELECT COUNT(*) FROM api_keys WHERE revoked_at IS NULL",
            [],
            |row| row.get(0),
        )?;
        let average = if row.0 == 0 {
            0.0
        } else {
            row.6 * 1000.0 / row.0 as f64
        };
        Ok(json!({
            "requests": row.0,
            "successful_requests": row.1,
            "failed_requests": row.2,
            "prompt_tokens": row.3,
            "completion_tokens": row.4,
            "total_tokens": row.5,
            "active_keys": active,
            "average_latency_ms": (average * 100.0).round() / 100.0
        }))
    }

    fn key_by_id(&self, id: i64) -> Result<Option<ApiKey>> {
        Ok(self
            .connection
            .query_row(
                "SELECT id,name,prefix,created_at,last_used_at,revoked_at,requests,tokens
                 FROM api_keys WHERE id=?",
                [id],
                row_to_key,
            )
            .optional()?)
    }
}

fn row_to_key(row: &rusqlite::Row<'_>) -> rusqlite::Result<ApiKey> {
    Ok(ApiKey {
        id: row.get(0)?,
        name: row.get(1)?,
        prefix: row.get(2)?,
        created_at: row.get(3)?,
        last_used_at: row.get(4)?,
        revoked_at: row.get(5)?,
        requests: row.get(6)?,
        tokens: row.get(7)?,
    })
}

fn digest(value: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(value.as_bytes());
    format!("{:x}", hasher.finalize())
}

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

#[derive(Deserialize)]
struct CreateKeyRequest {
    name: String,
}

#[derive(Clone, Copy)]
enum Principal {
    Anonymous,
    Master,
    Admin,
    Managed(i64),
}

fn bearer(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| {
            value
                .strip_prefix("Bearer ")
                .or_else(|| value.strip_prefix("bearer "))
        })
}

fn constant_time_eq(left: &str, right: &str) -> bool {
    left.len() == right.len()
        && left
            .as_bytes()
            .iter()
            .zip(right.as_bytes())
            .fold(0_u8, |acc, (a, b)| acc | (a ^ b))
            == 0
}

fn principal(state: &AppState, headers: &HeaderMap) -> Result<Option<Principal>> {
    let supplied = bearer(headers);
    if let (Some(value), Some(expected)) = (supplied, state.config.server.api_key.as_deref()) {
        if constant_time_eq(value, expected) {
            return Ok(Some(Principal::Master));
        }
    }
    if let (Some(value), Some(expected)) = (supplied, state.config.server.admin_api_key.as_deref())
    {
        if constant_time_eq(value, expected) {
            return Ok(Some(Principal::Admin));
        }
    }
    if let Some(value) = supplied {
        if let Some(key) = state.store.lock().expect("store poisoned").lookup(value)? {
            return Ok(Some(Principal::Managed(key.id)));
        }
    }
    if state.config.server.api_key.is_none()
        && state.config.server.admin_api_key.is_none()
        && !state.store.lock().expect("store poisoned").has_keys()?
    {
        return Ok(Some(Principal::Anonymous));
    }
    Ok(None)
}

fn admin_allowed(state: &AppState, headers: &HeaderMap) -> Result<bool> {
    let value = bearer(headers);
    if let Some(expected) = state.config.server.admin_api_key.as_deref() {
        return Ok(value.is_some_and(|value| constant_time_eq(value, expected)));
    }
    Ok(state
        .config
        .server
        .api_key
        .as_deref()
        .is_some_and(|expected| value.is_some_and(|value| constant_time_eq(value, expected))))
}

fn api_allowed(
    state: &AppState,
    headers: &HeaderMap,
    allow_admin: bool,
) -> Result<Option<Principal>> {
    let value = principal(state, headers)?;
    if allow_admin && matches!(value, Some(Principal::Admin)) {
        return Ok(value);
    }
    if matches!(value, Some(Principal::Admin)) {
        return Ok(None);
    }
    Ok(value)
}

fn error_response(status: StatusCode, message: impl Into<String>, error_type: &str) -> Response {
    (
        status,
        Json(json!({"error": {"message": message.into(), "type": error_type}})),
    )
        .into_response()
}

async fn root(State(state): State<AppState>) -> Json<Value> {
    Json(json!({
        "name": "ai-gateway",
        "version": VERSION,
        "runtime": "rust",
        "status": "ok",
        "providers": state.router.providers.len(),
        "endpoints": {
            "chat": "/v1/chat/completions",
            "models": "/v1/models",
            "health": "/health",
            "metrics": "/metrics",
            "dashboard": "/dashboard"
        }
    }))
}

async fn dashboard() -> Html<&'static str> {
    Html(include_str!("../dashboard.html"))
}

async fn health(State(state): State<AppState>, headers: HeaderMap) -> Response {
    match api_allowed(&state, &headers, true) {
        Ok(Some(_)) => {
            let providers = state.router.health();
            let statuses: Vec<&str> = providers
                .iter()
                .map(|provider| provider.status.as_str())
                .collect();
            let status = if statuses.iter().all(|value| *value == "unknown") {
                "unknown"
            } else if statuses.iter().all(|value| *value == "healthy") {
                "ok"
            } else if statuses.iter().all(|value| *value == "unavailable") {
                "unavailable"
            } else {
                "degraded"
            };
            (
                StatusCode::OK,
                Json(json!({"status": status, "providers": providers})),
            )
                .into_response()
        }
        Ok(None) => error_response(
            StatusCode::UNAUTHORIZED,
            "invalid API key",
            "authentication_error",
        ),
        Err(error) => error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            error.to_string(),
            "internal_error",
        ),
    }
}

async fn models(State(state): State<AppState>, headers: HeaderMap) -> Response {
    match api_allowed(&state, &headers, true) {
        Ok(Some(_)) => (
            StatusCode::OK,
            Json(json!({
                "object": "list",
                "data": state.router.health().iter().map(|provider| json!({
                    "id": provider.model,
                    "object": "model",
                    "owned_by": provider.name
                })).collect::<Vec<_>>()
            })),
        )
            .into_response(),
        Ok(None) => error_response(
            StatusCode::UNAUTHORIZED,
            "invalid API key",
            "authentication_error",
        ),
        Err(error) => error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            error.to_string(),
            "internal_error",
        ),
    }
}

async fn metrics(State(state): State<AppState>, headers: HeaderMap) -> Response {
    match api_allowed(&state, &headers, true) {
        Ok(Some(_)) => {
            let body = state.metrics.lock().expect("metrics poisoned").prometheus();
            (
                StatusCode::OK,
                [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
                body,
            )
                .into_response()
        }
        Ok(None) => error_response(
            StatusCode::UNAUTHORIZED,
            "invalid API key",
            "authentication_error",
        ),
        Err(error) => error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            error.to_string(),
            "internal_error",
        ),
    }
}

async fn chat(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    let identity = match api_allowed(&state, &headers, false) {
        Ok(Some(principal)) => principal,
        Ok(None) => {
            return error_response(
                StatusCode::UNAUTHORIZED,
                "invalid API key",
                "authentication_error",
            )
        }
        Err(error) => {
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                error.to_string(),
                "internal_error",
            )
        }
    };
    let identity_name = match identity {
        Principal::Anonymous => format!("anonymous:{}", "client"),
        Principal::Master => "master".to_owned(),
        Principal::Admin => "admin".to_owned(),
        Principal::Managed(id) => format!("api-key:{id}"),
    };
    if !state.limiter.allow(&identity_name) {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            [(header::RETRY_AFTER, "60")],
            Json(json!({"error": {"message": "rate limit exceeded", "type": "rate_limit_error"}})),
        )
            .into_response();
    }
    let request = match ChatRequest::parse(body) {
        Ok(request) => request,
        Err(error) => {
            return error_response(
                StatusCode::BAD_REQUEST,
                error.to_string(),
                "invalid_request_error",
            )
        }
    };
    let key_id = match identity {
        Principal::Managed(id) => Some(id),
        _ => None,
    };
    if let Some(id) = key_id {
        if let Err(error) = state.store.lock().expect("store poisoned").touch(id) {
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                error.to_string(),
                "internal_error",
            );
        }
    }
    if request.stream {
        return stream_chat(state, request, key_id).await;
    }
    let started = Instant::now();
    match state.router.route(&state.client, &request).await {
        Ok(response) => {
            let elapsed = started.elapsed();
            state
                .metrics
                .lock()
                .expect("metrics poisoned")
                .record(true, elapsed);
            if let Err(error) = state.store.lock().expect("store poisoned").record(
                true,
                &response.usage,
                elapsed,
                key_id,
            ) {
                error!(%error, "failed to persist usage");
            }
            (
                StatusCode::OK,
                Json(response_json(&response, request.model.as_deref())),
            )
                .into_response()
        }
        Err(error) => {
            let elapsed = started.elapsed();
            state
                .metrics
                .lock()
                .expect("metrics poisoned")
                .record(false, elapsed);
            if let Err(store_error) = state.store.lock().expect("store poisoned").record(
                false,
                &Usage::default(),
                elapsed,
                key_id,
            ) {
                error!(%store_error, "failed to persist failed usage");
            }
            error_response(StatusCode::BAD_GATEWAY, error.to_string(), "provider_error")
        }
    }
}

async fn stream_chat(state: AppState, request: ChatRequest, key_id: Option<i64>) -> Response {
    let started = Instant::now();
    match state.router.route_stream(&state.client, &request).await {
        Ok((provider, response)) => {
            let elapsed = started.elapsed();
            state
                .metrics
                .lock()
                .expect("metrics poisoned")
                .record(true, elapsed);
            if let Err(error) = state.store.lock().expect("store poisoned").record(
                true,
                &Usage::default(),
                elapsed,
                key_id,
            ) {
                error!(%error, "failed to persist streaming usage");
            }
            let model = request.model.clone().unwrap_or(provider.model.clone());
            let stream = provider_stream_body(response, &provider, model);
            (
                StatusCode::OK,
                [
                    (header::CONTENT_TYPE, "text/event-stream"),
                    (header::CACHE_CONTROL, "no-cache"),
                    (header::CONNECTION, "keep-alive"),
                ],
                Body::from_stream(stream),
            )
                .into_response()
        }
        Err(error) => {
            let elapsed = started.elapsed();
            state
                .metrics
                .lock()
                .expect("metrics poisoned")
                .record(false, elapsed);
            if let Err(store_error) = state.store.lock().expect("store poisoned").record(
                false,
                &Usage::default(),
                elapsed,
                key_id,
            ) {
                error!(%store_error, "failed to persist failed streaming usage");
            }
            error_response(StatusCode::BAD_GATEWAY, error.to_string(), "provider_error")
        }
    }
}

fn response_json(response: &ChatResponse, requested_model: Option<&str>) -> Value {
    json!({
        "id": format!("chatcmpl-{}", uuid::Uuid::new_v4().simple()),
        "object": "chat.completion",
        "created": unix_now(),
        "model": requested_model.unwrap_or(&response.model),
        "choices": [{
            "index": 0,
            "message": {"role": "assistant", "content": response.content},
            "finish_reason": response.finish_reason
        }],
        "usage": response.usage.json()
    })
}

async fn admin_stats(State(state): State<AppState>, headers: HeaderMap) -> Response {
    if !matches!(admin_allowed(&state, &headers), Ok(true)) {
        return error_response(
            StatusCode::UNAUTHORIZED,
            "admin API key required",
            "authentication_error",
        );
    }
    match state.store.lock().expect("store poisoned").stats() {
        Ok(value) => (StatusCode::OK, Json(value)).into_response(),
        Err(error) => error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            error.to_string(),
            "internal_error",
        ),
    }
}

async fn list_keys(State(state): State<AppState>, headers: HeaderMap) -> Response {
    if !matches!(admin_allowed(&state, &headers), Ok(true)) {
        return error_response(
            StatusCode::UNAUTHORIZED,
            "admin API key required",
            "authentication_error",
        );
    }
    match state.store.lock().expect("store poisoned").list_keys() {
        Ok(keys) => (StatusCode::OK, Json(json!({"data": keys}))).into_response(),
        Err(error) => error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            error.to_string(),
            "internal_error",
        ),
    }
}

async fn create_key(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<CreateKeyRequest>,
) -> Response {
    if !matches!(admin_allowed(&state, &headers), Ok(true)) {
        return error_response(
            StatusCode::UNAUTHORIZED,
            "admin API key required",
            "authentication_error",
        );
    }
    match state
        .store
        .lock()
        .expect("store poisoned")
        .create_key(&body.name)
    {
        Ok((token, key)) => (
            StatusCode::CREATED,
            Json(json!({"key": token, "data": key})),
        )
            .into_response(),
        Err(error) => error_response(
            StatusCode::BAD_REQUEST,
            error.to_string(),
            "invalid_request_error",
        ),
    }
}

async fn revoke_key(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<i64>,
) -> Response {
    if !matches!(admin_allowed(&state, &headers), Ok(true)) {
        return error_response(
            StatusCode::UNAUTHORIZED,
            "admin API key required",
            "authentication_error",
        );
    }
    match state.store.lock().expect("store poisoned").revoke(id) {
        Ok(true) => (StatusCode::OK, Json(json!({"revoked": true}))).into_response(),
        Ok(false) => error_response(
            StatusCode::NOT_FOUND,
            "key not found",
            "invalid_request_error",
        ),
        Err(error) => error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            error.to_string(),
            "internal_error",
        ),
    }
}

fn build_app(state: AppState) -> Router {
    Router::new()
        .route("/", get(root))
        .route("/dashboard", get(dashboard))
        .route("/health", get(health))
        .route("/v1/health", get(health))
        .route("/v1/models", get(models))
        .route("/metrics", get(metrics))
        .route("/v1/chat/completions", post(chat))
        .route("/chat/completions", post(chat))
        .route("/admin/stats", get(admin_stats))
        .route("/admin/api-keys", get(list_keys).post(create_key))
        .route("/admin/api-keys/{id}", delete(revoke_key))
        .with_state(state)
        .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
        .layer(CatchPanicLayer::new())
        .layer(TraceLayer::new_for_http())
}

#[tokio::main]
async fn main() -> Result<()> {
    load_dotenv(FsPath::new(".env"))?;
    tracing_subscriber::fmt()
        .with_env_filter(
            env::var("RUST_LOG").unwrap_or_else(|_| "ai_gateway=info,tower_http=info".to_owned()),
        )
        .init();
    let args: Vec<String> = env::args().collect();
    if args.iter().any(|arg| arg == "--help" || arg == "-h") {
        println!(
            "ai-gateway {VERSION}\n\nUsage:\n  ai-gateway [--config <path>]\n  ai-gateway check-config [--config <path>]\n  ai-gateway --version\n  ai-gateway --help\n\nEnvironment mode discovers providers from OPENAI_API_KEY, ANTHROPIC_API_KEY, GEMINI_API_KEY, and OLLAMA_MODEL."
        );
        return Ok(());
    }
    if args.iter().any(|arg| arg == "--version") {
        println!("ai-gateway {VERSION}");
        return Ok(());
    }
    let config_path = args
        .windows(2)
        .find(|window| window[0] == "--config")
        .map(|window| window[1].as_str());
    let config = load_config(config_path)?;
    if args.iter().any(|arg| arg == "check-config") {
        println!(
            "{}",
            json!({
                "valid": true,
                "providers": config.providers.iter().map(|provider| provider.name.clone()).collect::<Vec<_>>()
            })
        );
        return Ok(());
    }
    let address: SocketAddr = format!("{}:{}", config.server.host, config.server.port)
        .parse()
        .context("invalid listen address")?;
    let timeout = Duration::from_secs_f64(config.server.request_timeout_seconds);
    let client = Client::builder()
        .connect_timeout(timeout)
        .timeout(timeout)
        .user_agent(format!("ai-gateway/{VERSION}"))
        .build()?;
    let store = Store::open(&config.server.database_path)?;
    let state = AppState {
        router: Arc::new(RouterState::new(&config)),
        limiter: Arc::new(RateLimiter::new(config.server.rate_limit_per_minute)),
        metrics: Arc::new(Mutex::new(Metrics::default())),
        store: Arc::new(Mutex::new(store)),
        client,
        config: Arc::new(config),
    };
    let listener = TcpListener::bind(address).await?;
    info!(%address, version = VERSION, "ai-gateway listening");
    axum::serve(listener, build_app(state)).await?;
    Ok(())
}

#[derive(Default, Deserialize)]
struct RawFileConfig {
    server: Option<RawServer>,
    routing: Option<RawRouting>,
    #[serde(default)]
    providers: Vec<RawProvider>,
}

#[derive(Default, Deserialize)]
struct RawServer {
    host: Option<String>,
    port: Option<u16>,
    api_key: Option<String>,
    api_key_env: Option<String>,
    admin_api_key: Option<String>,
    admin_api_key_env: Option<String>,
    database_path: Option<String>,
    rate_limit_per_minute: Option<usize>,
    request_timeout_seconds: Option<f64>,
}

#[derive(Default, Deserialize)]
struct RawRouting {
    default_model: Option<String>,
    max_retries: Option<usize>,
    failure_cooldown_seconds: Option<f64>,
    #[serde(default)]
    task_routes: HashMap<String, Vec<String>>,
}

#[derive(Default, Deserialize)]
struct RawProvider {
    name: Option<String>,
    kind: Option<String>,
    base_url: Option<String>,
    model: Option<String>,
    api_key: Option<String>,
    api_key_env: Option<String>,
    timeout_seconds: Option<f64>,
    priority: Option<i64>,
    headers: Option<HashMap<String, String>>,
}

fn load_config(explicit: Option<&str>) -> Result<Config> {
    let path = explicit
        .map(PathBuf::from)
        .or_else(|| env::var("AI_GATEWAY_CONFIG").ok().map(PathBuf::from))
        .or_else(|| {
            FsPath::new("config.json")
                .exists()
                .then(|| PathBuf::from("config.json"))
        });
    if let Some(path) = path {
        return parse_file_config(&path);
    }
    env_config()
}

fn parse_file_config(path: &FsPath) -> Result<Config> {
    let raw: RawFileConfig = serde_json::from_str(&fs::read_to_string(path)?)?;
    let server_raw = raw.server.unwrap_or_default();
    let routing_raw = raw.routing.unwrap_or_default();
    let default_timeout = server_raw.request_timeout_seconds.unwrap_or(45.0);
    let providers = raw
        .providers
        .into_iter()
        .map(|provider| provider_from_raw(provider, default_timeout))
        .collect::<Result<Vec<_>>>()?;
    if providers.is_empty() {
        return Err(anyhow!("providers must be a non-empty array"));
    }
    let port = server_raw.port.unwrap_or(8080);
    if port == 0 {
        return Err(anyhow!("server.port must be between 1 and 65535"));
    }
    let server = ServerConfig {
        host: nonempty(
            server_raw.host.unwrap_or_else(|| "0.0.0.0".to_owned()),
            "server.host",
        )?,
        port,
        api_key: secret(server_raw.api_key, server_raw.api_key_env)?,
        admin_api_key: secret(server_raw.admin_api_key, server_raw.admin_api_key_env)?,
        database_path: nonempty(
            server_raw
                .database_path
                .unwrap_or_else(|| "ai-gateway.db".to_owned()),
            "server.database_path",
        )?,
        rate_limit_per_minute: server_raw.rate_limit_per_minute.unwrap_or(0),
        request_timeout_seconds: positive(default_timeout, "server.request_timeout_seconds")?,
    };
    let routing = RoutingConfig {
        default_model: routing_raw
            .default_model
            .unwrap_or_else(|| "auto".to_owned()),
        max_retries: routing_raw.max_retries.unwrap_or(2),
        failure_cooldown_seconds: nonnegative(
            routing_raw.failure_cooldown_seconds.unwrap_or(30.0),
            "routing.failure_cooldown_seconds",
        )?,
        task_routes: routing_raw.task_routes,
    };
    validate_routes(&routing.task_routes, &providers)?;
    Ok(Config {
        server,
        routing,
        providers,
    })
}

fn env_config() -> Result<Config> {
    let mut providers = Vec::new();
    if let Some(key) = env::var("OPENAI_API_KEY")
        .ok()
        .filter(|value| !value.is_empty())
    {
        providers.push(ProviderConfig {
            name: "openai".to_owned(),
            kind: "openai-compatible".to_owned(),
            base_url: env::var("OPENAI_BASE_URL")
                .unwrap_or_else(|_| "https://api.openai.com/v1".to_owned()),
            model: env::var("OPENAI_MODEL").unwrap_or_else(|_| "gpt-4o-mini".to_owned()),
            api_key: Some(key),
            timeout_seconds: 45.0,
            priority: 30,
            headers: HashMap::new(),
        });
    }
    if let Some(key) = env::var("ANTHROPIC_API_KEY")
        .ok()
        .filter(|value| !value.is_empty())
    {
        providers.push(ProviderConfig {
            name: "anthropic".to_owned(),
            kind: "anthropic".to_owned(),
            base_url: env::var("ANTHROPIC_BASE_URL")
                .unwrap_or_else(|_| "https://api.anthropic.com".to_owned()),
            model: env::var("ANTHROPIC_MODEL")
                .unwrap_or_else(|_| "claude-3-5-haiku-latest".to_owned()),
            api_key: Some(key),
            timeout_seconds: 45.0,
            priority: 20,
            headers: HashMap::new(),
        });
    }
    if let Some(key) = env::var("GEMINI_API_KEY")
        .ok()
        .filter(|value| !value.is_empty())
    {
        providers.push(ProviderConfig {
            name: "gemini".to_owned(),
            kind: "gemini".to_owned(),
            base_url: env::var("GEMINI_BASE_URL")
                .unwrap_or_else(|_| "https://generativelanguage.googleapis.com".to_owned()),
            model: env::var("GEMINI_MODEL").unwrap_or_else(|_| "gemini-2.0-flash".to_owned()),
            api_key: Some(key),
            timeout_seconds: 45.0,
            priority: 20,
            headers: HashMap::new(),
        });
    }
    if env::var("OLLAMA_BASE_URL").is_ok()
        || env::var("OLLAMA_MODEL").is_ok()
        || providers.is_empty()
    {
        providers.push(ProviderConfig {
            name: "ollama".to_owned(),
            kind: "ollama".to_owned(),
            base_url: env::var("OLLAMA_BASE_URL")
                .unwrap_or_else(|_| "http://127.0.0.1:11434".to_owned()),
            model: env::var("OLLAMA_MODEL").unwrap_or_else(|_| "llama3.2".to_owned()),
            api_key: None,
            timeout_seconds: 45.0,
            priority: 10,
            headers: HashMap::new(),
        });
    }
    Ok(Config {
        server: ServerConfig {
            host: env::var("AI_GATEWAY_HOST").unwrap_or_else(|_| "127.0.0.1".to_owned()),
            port: env::var("AI_GATEWAY_PORT")
                .ok()
                .map(|value| value.parse())
                .transpose()?
                .unwrap_or(8080),
            api_key: env::var("AI_GATEWAY_API_KEY")
                .ok()
                .filter(|value| !value.is_empty()),
            admin_api_key: env::var("AI_GATEWAY_ADMIN_API_KEY")
                .ok()
                .filter(|value| !value.is_empty()),
            database_path: env::var("AI_GATEWAY_DATABASE")
                .unwrap_or_else(|_| "ai-gateway.db".to_owned()),
            rate_limit_per_minute: env::var("AI_GATEWAY_RATE_LIMIT")
                .ok()
                .map(|value| value.parse())
                .transpose()?
                .unwrap_or(0),
            request_timeout_seconds: positive(
                env::var("AI_GATEWAY_TIMEOUT")
                    .ok()
                    .map(|value| value.parse())
                    .transpose()?
                    .unwrap_or(45.0),
                "AI_GATEWAY_TIMEOUT",
            )?,
        },
        routing: RoutingConfig {
            default_model: "auto".to_owned(),
            max_retries: env::var("AI_GATEWAY_MAX_RETRIES")
                .ok()
                .map(|value| value.parse())
                .transpose()?
                .unwrap_or(2),
            failure_cooldown_seconds: env::var("AI_GATEWAY_FAILURE_COOLDOWN")
                .ok()
                .map(|value| value.parse())
                .transpose()?
                .unwrap_or(30.0),
            task_routes: HashMap::new(),
        },
        providers,
    })
}

fn provider_from_raw(raw: RawProvider, default_timeout: f64) -> Result<ProviderConfig> {
    let kind = nonempty(
        raw.kind
            .ok_or_else(|| anyhow!("provider kind is required"))?,
        "provider.kind",
    )?;
    if !matches!(
        kind.as_str(),
        "openai" | "openai-compatible" | "anthropic" | "gemini" | "ollama"
    ) {
        return Err(anyhow!("unsupported provider kind: {kind}"));
    }
    Ok(ProviderConfig {
        name: nonempty(
            raw.name
                .ok_or_else(|| anyhow!("provider name is required"))?,
            "provider.name",
        )?,
        kind,
        base_url: nonempty(
            raw.base_url
                .ok_or_else(|| anyhow!("provider base_url is required"))?,
            "provider.base_url",
        )?
        .trim_end_matches('/')
        .to_owned(),
        model: nonempty(
            raw.model
                .ok_or_else(|| anyhow!("provider model is required"))?,
            "provider.model",
        )?,
        api_key: secret(raw.api_key, raw.api_key_env)?,
        timeout_seconds: positive(
            raw.timeout_seconds.unwrap_or(default_timeout),
            "provider.timeout_seconds",
        )?,
        priority: raw.priority.unwrap_or(0),
        headers: raw.headers.unwrap_or_default(),
    })
}

fn validate_routes(
    routes: &HashMap<String, Vec<String>>,
    providers: &[ProviderConfig],
) -> Result<()> {
    let names: Vec<&str> = providers
        .iter()
        .map(|provider| provider.name.as_str())
        .collect();
    for route in routes.values() {
        for name in route {
            if !names.contains(&name.as_str()) {
                return Err(anyhow!("task route references unknown provider: {name}"));
            }
        }
    }
    Ok(())
}

fn secret(value: Option<String>, env_name: Option<String>) -> Result<Option<String>> {
    if value.is_some() && env_name.is_some() {
        return Err(anyhow!(
            "use either a literal secret or an environment variable, not both"
        ));
    }
    if let Some(value) = value {
        return Ok((!value.is_empty()).then_some(value));
    }
    Ok(env_name
        .and_then(|name| env::var(name).ok())
        .filter(|value| !value.is_empty()))
}

fn nonempty(value: String, name: &str) -> Result<String> {
    if value.trim().is_empty() {
        Err(anyhow!("{name} must be a non-empty string"))
    } else {
        Ok(value.trim().to_owned())
    }
}

fn positive(value: f64, name: &str) -> Result<f64> {
    if !value.is_finite() || value < 0.1 {
        Err(anyhow!("{name} must be >= 0.1"))
    } else {
        Ok(value)
    }
}

fn nonnegative(value: f64, name: &str) -> Result<f64> {
    if !value.is_finite() || value < 0.0 {
        Err(anyhow!("{name} must be >= 0"))
    } else {
        Ok(value)
    }
}

fn expand_user(path: &FsPath) -> PathBuf {
    let text = path.to_string_lossy();
    if let Some(home) = env::var_os("HOME") {
        if let Some(rest) = text.strip_prefix("~/") {
            return PathBuf::from(home).join(rest);
        }
    }
    path.to_path_buf()
}

fn load_dotenv(path: &FsPath) -> Result<usize> {
    if !path.exists() {
        return Ok(0);
    }
    let contents =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
    let mut loaded = 0;
    for (line_number, line) in contents.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let line = line.strip_prefix("export ").unwrap_or(line);
        let (name, raw_value) = line.split_once('=').ok_or_else(|| {
            anyhow!(
                "{}:{} must use KEY=VALUE syntax",
                path.display(),
                line_number + 1
            )
        })?;
        let name = name.trim();
        if name.is_empty()
            || !name
                .chars()
                .all(|character| character == '_' || character.is_ascii_alphanumeric())
        {
            return Err(anyhow!(
                "{}:{} has an invalid environment variable name",
                path.display(),
                line_number + 1
            ));
        }
        if env::var_os(name).is_some() {
            continue;
        }
        let value = raw_value.trim();
        let value = match (value.strip_prefix('"'), value.strip_suffix('"')) {
            (Some(value), Some(_)) => value.strip_suffix('"').unwrap_or(value),
            _ => match (value.strip_prefix('\''), value.strip_suffix('\'')) {
                (Some(value), Some(_)) => value.strip_suffix('\'').unwrap_or(value),
                _ => value,
            },
        };
        env::set_var(name, value);
        loaded += 1;
    }
    Ok(loaded)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn provider(name: &str, model: &str, priority: i64) -> ProviderConfig {
        ProviderConfig {
            name: name.to_owned(),
            kind: "openai-compatible".to_owned(),
            base_url: "http://localhost/v1".to_owned(),
            model: model.to_owned(),
            api_key: None,
            timeout_seconds: 1.0,
            priority,
            headers: HashMap::new(),
        }
    }

    #[test]
    fn parses_chat_request_and_keeps_extra_fields() {
        let request = ChatRequest::parse(json!({
            "model": "auto",
            "messages": [{"role": "user", "content": "hello"}],
            "temperature": 0.2,
            "response_format": {"type": "json_object"}
        }))
        .expect("valid request");
        assert_eq!(request.messages.len(), 1);
        assert_eq!(request.extra["response_format"]["type"], "json_object");
    }

    #[test]
    fn parses_streaming_and_rejects_non_finite_temperature() {
        let streaming = ChatRequest::parse(json!({
            "messages": [{"role": "user", "content": "x"}],
            "stream": true
        }))
        .unwrap();
        assert!(streaming.stream);
        assert!(ChatRequest::parse(json!({
            "messages": [{"role": "user", "content": "x"}],
            "temperature": "nan"
        }))
        .is_err());
    }

    #[test]
    fn normalizes_ollama_stream_chunks_to_openai_sse() {
        let mut normalizer = StreamNormalizer::new("ollama".to_owned(), "llama3.2".to_owned());
        let initial = String::from_utf8(normalizer.initial().to_vec()).unwrap();
        assert!(initial.contains("chat.completion.chunk"));
        let frames = normalizer
            .process_line(r#"{"message":{"role":"assistant","content":"hello"},"done":false}"#);
        assert_eq!(frames.len(), 1);
        let frame = String::from_utf8(frames[0].to_vec()).unwrap();
        assert!(frame.contains("hello"));
        let final_frames =
            normalizer.process_line(r#"{"message":{"content":""},"done":true,"eval_count":2}"#);
        assert_eq!(final_frames.len(), 2);
        assert!(String::from_utf8(final_frames[1].to_vec())
            .unwrap()
            .contains("[DONE]"));
    }

    #[test]
    fn normalizes_anthropic_stream_events_to_openai_sse() {
        let mut normalizer =
            StreamNormalizer::new("anthropic".to_owned(), "claude-haiku".to_owned());
        let _ = normalizer.initial();
        let frames = normalizer.process_line(
            r#"{"type":"content_block_delta","delta":{"type":"text_delta","text":"hi"}}"#,
        );
        assert_eq!(frames.len(), 1);
        assert!(String::from_utf8(frames[0].to_vec())
            .unwrap()
            .contains("hi"));
        let final_frames = normalizer
            .process_line(r#"{"type":"message_delta","delta":{"stop_reason":"end_turn"}}"#);
        assert_eq!(final_frames.len(), 2);
    }

    #[test]
    fn usage_normalizes_provider_field_names() {
        let usage = Usage::from_value(Some(&json!({
            "promptTokenCount": 4,
            "candidatesTokenCount": 6,
            "totalTokenCount": 12
        })));
        assert_eq!(usage.prompt_tokens, 4);
        assert_eq!(usage.completion_tokens, 6);
        assert_eq!(usage.total_tokens, 12);
    }

    #[test]
    fn router_applies_default_model_and_priority() {
        let config = Config {
            server: ServerConfig {
                host: "127.0.0.1".to_owned(),
                port: 8080,
                api_key: None,
                admin_api_key: None,
                database_path: ":memory:".to_owned(),
                rate_limit_per_minute: 0,
                request_timeout_seconds: 1.0,
            },
            routing: RoutingConfig {
                default_model: "second".to_owned(),
                max_retries: 1,
                failure_cooldown_seconds: 1.0,
                task_routes: HashMap::new(),
            },
            providers: vec![provider("first", "one", 20), provider("second", "two", 10)],
        };
        let router = RouterState::new(&config);
        let request = ChatRequest::parse(json!({
            "messages": [{"role": "user", "content": "hello"}]
        }))
        .unwrap();
        assert_eq!(router.candidates(&request), vec![1]);
    }

    #[test]
    fn rate_limiter_scopes_windows_by_identity() {
        let limiter = RateLimiter::new(1);
        assert!(limiter.allow("api-key:one"));
        assert!(!limiter.allow("api-key:one"));
        assert!(limiter.allow("api-key:two"));
    }

    #[test]
    fn constant_time_comparison_requires_equal_values() {
        assert!(constant_time_eq("secret", "secret"));
        assert!(!constant_time_eq("secret", "secreT"));
        assert!(!constant_time_eq("secret", "secret-longer"));
    }

    #[test]
    fn openai_response_is_normalized() {
        let config = provider("openai", "gpt", 1);
        let response = normalize_response(
            &config,
            json!({
                "choices": [{"message": {"content": "ok"}, "finish_reason": "stop"}],
                "usage": {"prompt_tokens": 2, "completion_tokens": 3, "total_tokens": 5}
            }),
        )
        .unwrap();
        assert_eq!(response.content, "ok");
        assert_eq!(response.usage.total_tokens, 5);
    }

    #[test]
    fn dotenv_parser_keeps_existing_environment_values() {
        let path = env::temp_dir().join(format!("ai-gateway-dotenv-{}", uuid::Uuid::new_v4()));
        fs::write(
            &path,
            "AI_GATEWAY_TEST_DOTENV=from-file\nexport AI_GATEWAY_TEST_QUOTED=\"quoted value\"\n",
        )
        .unwrap();
        env::remove_var("AI_GATEWAY_TEST_DOTENV");
        env::remove_var("AI_GATEWAY_TEST_QUOTED");
        assert_eq!(load_dotenv(&path).unwrap(), 2);
        assert_eq!(env::var("AI_GATEWAY_TEST_DOTENV").unwrap(), "from-file");
        assert_eq!(env::var("AI_GATEWAY_TEST_QUOTED").unwrap(), "quoted value");
        env::set_var("AI_GATEWAY_TEST_DOTENV", "from-shell");
        env::remove_var("AI_GATEWAY_TEST_QUOTED");
        assert_eq!(load_dotenv(&path).unwrap(), 1);
        assert_eq!(env::var("AI_GATEWAY_TEST_DOTENV").unwrap(), "from-shell");
        env::remove_var("AI_GATEWAY_TEST_DOTENV");
        env::remove_var("AI_GATEWAY_TEST_QUOTED");
        let _ = fs::remove_file(path);
    }
}
