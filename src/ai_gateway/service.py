from __future__ import annotations

import json
import secrets
import threading
import time
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import urlsplit

from . import __version__
from .limits import RateLimiter
from .models import ChatRequest, RequestValidationError
from .router import Router
from .store import ApiKey, Store


@dataclass
class Metrics:
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    total_latency_seconds: float = 0.0

    def __post_init__(self) -> None:
        self._lock = threading.Lock()

    def record(self, *, success: bool, latency_seconds: float) -> None:
        with self._lock:
            self.total_requests += 1
            self.successful_requests += int(success)
            self.failed_requests += int(not success)
            self.total_latency_seconds += latency_seconds

    def prometheus(self) -> str:
        with self._lock:
            total = self.total_requests
            successful = self.successful_requests
            failed = self.failed_requests
            average = self.total_latency_seconds / total if total else 0
        return "\n".join([
            "# HELP ai_gateway_requests_total Total chat completion requests.",
            "# TYPE ai_gateway_requests_total counter",
            f"ai_gateway_requests_total {total}",
            "# HELP ai_gateway_requests_success_total Successful chat completion requests.",
            "# TYPE ai_gateway_requests_success_total counter",
            f"ai_gateway_requests_success_total {successful}",
            "# HELP ai_gateway_requests_failed_total Failed chat completion requests.",
            "# TYPE ai_gateway_requests_failed_total counter",
            f"ai_gateway_requests_failed_total {failed}",
            "# HELP ai_gateway_request_latency_seconds Average request latency.",
            "# TYPE ai_gateway_request_latency_seconds gauge",
            f"ai_gateway_request_latency_seconds {average:.6f}",
        ]) + "\n"


class GatewayService:
    def __init__(
        self,
        router: Router,
        api_key: str | None = None,
        *,
        admin_api_key: str | None = None,
        store: Store | None = None,
        rate_limit_per_minute: int = 0,
        request_timeout_seconds: float = 45.0,
    ):
        self.router = router
        self.api_key = api_key
        self.admin_api_key = admin_api_key
        self.store = store
        self.rate_limiter = RateLimiter(rate_limit_per_minute)
        self.request_timeout_seconds = request_timeout_seconds
        self.metrics = Metrics()

    def authenticate(self, supplied: str | None) -> bool:
        return self.resolve_identity(supplied) is not None

    def resolve_identity(self, supplied: str | None) -> str | None:
        return self.resolve_principal(supplied)[0]

    def resolve_principal(self, supplied: str | None) -> tuple[str | None, ApiKey | None]:
        if self.api_key:
            if supplied and secrets.compare_digest(supplied, self.api_key):
                return "master", None
        if self.store and supplied:
            key = self.store.lookup(supplied)
            if key:
                return f"api-key:{key.id}", key
        if not self.api_key and not self.admin_api_key and (not self.store or not self.store.has_keys()):
            return "anonymous", None
        return None, None

    def authenticate_admin(self, supplied: str | None) -> bool:
        if self.admin_api_key and supplied and secrets.compare_digest(supplied, self.admin_api_key):
            return True
        if self.admin_api_key:
            return False
        return bool(self.api_key and supplied and secrets.compare_digest(supplied, self.api_key))

    def allow_request(self, identity: str, client: str) -> bool:
        return self.rate_limiter.allow(identity if identity != "anonymous" else client)

    def chat(self, body: Any, *, key_id: int | None = None) -> dict[str, Any]:
        request = ChatRequest.from_dict(body)
        started = time.monotonic()
        try:
            response = self.router.route(request)
        except Exception:
            elapsed = time.monotonic() - started
            self.metrics.record(success=False, latency_seconds=elapsed)
            if self.store:
                self.store.record_request(success=False, prompt_tokens=0, completion_tokens=0, total_tokens=0, latency_seconds=elapsed, key_id=key_id)
            raise
        elapsed = time.monotonic() - started
        self.metrics.record(success=True, latency_seconds=elapsed)
        if self.store:
            self.store.record_request(
                success=True,
                prompt_tokens=response.usage.prompt_tokens,
                completion_tokens=response.usage.completion_tokens,
                total_tokens=response.usage.total_tokens,
                latency_seconds=elapsed,
                key_id=key_id,
            )
        return response.as_openai(request.model)


def make_handler(service: GatewayService) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        server_version = "ai-gateway/0.2.0"

        def setup(self) -> None:
            super().setup()
            self.connection.settimeout(service.request_timeout_seconds)

        def do_GET(self) -> None:  # noqa: N802
            path = urlsplit(self.path).path
            if path == "/":
                self._json(HTTPStatus.OK, service_info(service))
                return
            if path == "/dashboard":
                self._html(DASHBOARD_HTML)
                return
            if path.startswith("/admin/"):
                if not self._require_admin():
                    return
                if path == "/admin/stats":
                    self._json(HTTPStatus.OK, service.store.stats() if service.store else {})
                    return
                if path == "/admin/api-keys":
                    self._json(HTTPStatus.OK, {"data": [key_payload(key) for key in (service.store.list_keys() if service.store else [])]})
                    return
                self._not_found()
                return
            if not self._require_api_or_admin():
                return
            if path in {"/health", "/v1/health"}:
                health = service.router.health()
                statuses = {item["status"] for item in health}
                status = "unknown" if health and statuses == {"unknown"} else "ok" if health and statuses == {"healthy"} else "degraded"
                if health and statuses == {"unavailable"}:
                    status = "unavailable"
                self._json(HTTPStatus.OK, {"status": status, "providers": health})
            elif path == "/metrics":
                data = service.metrics.prometheus().encode("utf-8")
                self.send_response(HTTPStatus.OK)
                self.send_header("content-type", "text/plain; version=0.0.4")
                self.send_header("content-length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
            elif path == "/v1/models":
                self._json(HTTPStatus.OK, {"object": "list", "data": [{"id": state["model"], "object": "model", "owned_by": state["name"]} for state in service.router.health()]})
            else:
                self._not_found()

        def do_POST(self) -> None:  # noqa: N802
            path = urlsplit(self.path).path
            if path == "/admin/api-keys":
                if not self._require_admin():
                    return
                try:
                    body = self._read_json()
                    name = body.get("name", "")
                    if not isinstance(name, str):
                        raise RequestValidationError("name must be a string")
                    if not service.store:
                        raise RequestValidationError("persistent storage is disabled")
                    token, key = service.store.create_key(name)
                    self._json(HTTPStatus.CREATED, {"key": token, "data": key_payload(key)})
                except (RequestValidationError, ValueError, json.JSONDecodeError, UnicodeDecodeError) as exc:
                    self._json(HTTPStatus.BAD_REQUEST, {"error": {"message": str(exc), "type": "invalid_request_error"}})
                return
            if not self._require_api():
                return
            if path not in {"/v1/chat/completions", "/chat/completions"}:
                self._not_found()
                return
            identity, key = service.resolve_principal(self._api_key())
            identity = identity or "anonymous"
            if not service.allow_request(identity, self.client_address[0]):
                self._json(HTTPStatus.TOO_MANY_REQUESTS, {"error": {"message": "rate limit exceeded", "type": "rate_limit_error"}}, {"retry-after": "60"})
                return
            try:
                body = self._read_json()
                if service.store and key:
                    service.store.touch(key.id)
                self._json(HTTPStatus.OK, service.chat(body, key_id=key.id if key else None))
            except (RequestValidationError, json.JSONDecodeError, UnicodeDecodeError) as exc:
                self._json(HTTPStatus.BAD_REQUEST, {"error": {"message": str(exc), "type": "invalid_request_error"}})
            except Exception as exc:
                self._json(HTTPStatus.BAD_GATEWAY, {"error": {"message": str(exc), "type": "provider_error"}})

        def do_DELETE(self) -> None:  # noqa: N802
            path = urlsplit(self.path).path
            if not self._require_admin():
                return
            if not path.startswith("/admin/api-keys/") or not service.store:
                self._not_found()
                return
            try:
                key_id = int(path.rsplit("/", 1)[1])
            except ValueError:
                self._not_found()
                return
            if service.store.revoke_key(key_id):
                self._json(HTTPStatus.OK, {"revoked": True})
            else:
                self._json(HTTPStatus.NOT_FOUND, {"error": {"message": "key not found", "type": "invalid_request_error"}})

        def _require_api(self) -> bool:
            if service.authenticate(self._api_key()):
                return True
            self._json(HTTPStatus.UNAUTHORIZED, {"error": {"message": "invalid API key", "type": "authentication_error"}})
            return False

        def _require_api_or_admin(self) -> bool:
            if service.authenticate(self._api_key()) or service.authenticate_admin(self._api_key()):
                return True
            self._json(HTTPStatus.UNAUTHORIZED, {"error": {"message": "invalid API key", "type": "authentication_error"}})
            return False

        def _require_admin(self) -> bool:
            if service.authenticate_admin(self._api_key()):
                return True
            self._json(HTTPStatus.UNAUTHORIZED, {"error": {"message": "admin API key required", "type": "authentication_error"}})
            return False

        def _api_key(self) -> str | None:
            value = self.headers.get("authorization", "")
            return value[7:] if value.lower().startswith("bearer ") else None

        def _read_json(self) -> dict[str, Any]:
            try:
                length = int(self.headers.get("content-length", "0"))
            except ValueError as exc:
                raise RequestValidationError("content-length must be an integer") from exc
            if length < 0:
                raise RequestValidationError("content-length must not be negative")
            if length > 2_000_000:
                raise RequestValidationError("request body is too large")
            body = json.loads(self.rfile.read(length).decode("utf-8"))
            if not isinstance(body, dict):
                raise RequestValidationError("request body must be a JSON object")
            return body

        def _json(self, status: HTTPStatus, payload: dict[str, Any], headers: dict[str, str] | None = None) -> None:
            data = json.dumps(payload).encode("utf-8")
            self.send_response(status)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(data)))
            for name, value in (headers or {}).items():
                self.send_header(name, value)
            self.end_headers()
            self.wfile.write(data)

        def _html(self, content: str) -> None:
            data = content.encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("content-type", "text/html; charset=utf-8")
            self.send_header("content-length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _not_found(self) -> None:
            self._json(HTTPStatus.NOT_FOUND, {"error": {"message": "not found", "type": "invalid_request_error"}})

        def log_message(self, format: str, *args: Any) -> None:
            return

    return Handler


def key_payload(key: ApiKey) -> dict[str, Any]:
    return {
        "id": key.id,
        "name": key.name,
        "prefix": key.prefix,
        "created_at": key.created_at,
        "last_used_at": key.last_used_at,
        "revoked_at": key.revoked_at,
        "requests": key.requests,
        "tokens": key.tokens,
    }


def service_info(service: GatewayService) -> dict[str, Any]:
    return {
        "name": "ai-gateway",
        "version": __version__,
        "status": "ok",
        "providers": len(service.router.states),
        "endpoints": {
            "chat": "/v1/chat/completions",
            "models": "/v1/models",
            "health": "/health",
            "metrics": "/metrics",
            "dashboard": "/dashboard",
        },
    }


DASHBOARD_HTML = """<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>AI Gateway Dashboard</title><style>
:root{color-scheme:dark;--bg:#0b1020;--panel:#121a2c;--line:#263451;--text:#e7edf8;--muted:#91a0b8;--good:#36d399;--warn:#fbbf24;--bad:#fb7185}*{box-sizing:border-box}body{margin:0;background:radial-gradient(circle at 10% 0,#182b4b 0,transparent 36%),var(--bg);color:var(--text);font:15px/1.5 system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}main{max-width:1120px;margin:auto;padding:42px 22px}header{display:flex;justify-content:space-between;align-items:flex-end;gap:20px;margin-bottom:28px}h1{font-size:34px;letter-spacing:-.04em;margin:0}h2{font-size:18px;margin:0 0 14px}.muted{color:var(--muted)}.grid{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-bottom:22px}.card,.panel{background:rgba(18,26,44,.88);border:1px solid var(--line);border-radius:14px;box-shadow:0 14px 40px #0002}.card{padding:18px}.value{font-size:28px;font-weight:700;margin-top:5px}.panel{padding:20px;margin-bottom:18px}.row{display:flex;justify-content:space-between;gap:12px;padding:12px 0;border-bottom:1px solid var(--line)}.row:last-child{border-bottom:0}.status{font-weight:650}.healthy{color:var(--good)}.degraded{color:var(--warn)}.unavailable{color:var(--bad)}.unknown{color:var(--muted)}table{width:100%;border-collapse:collapse}th,td{text-align:left;padding:11px 8px;border-bottom:1px solid var(--line)}th{font-size:12px;text-transform:uppercase;color:var(--muted);letter-spacing:.08em}button,input{background:#182640;border:1px solid #3a5787;border-radius:8px;color:var(--text);padding:8px 12px}button{cursor:pointer}button:hover{background:#315184}.toolbar{display:flex;gap:8px;margin-bottom:8px}.empty{color:var(--muted);padding:14px 0}@media(max-width:760px){.grid{grid-template-columns:repeat(2,1fr)}header{display:block}header .muted{margin-top:8px}.toolbar{flex-wrap:wrap}}
</style></head><body><main><header><div><div class="muted">OPERATIONS</div><h1>AI Gateway</h1><div class="muted">Provider health, traffic, keys, and usage</div></div><button onclick="load()">Refresh</button></header><section class="panel" id="login"><h2>Admin access</h2><div class="muted">Enter the admin API key to view operational data.</div><p><input id="key" type="password" autocomplete="off" placeholder="Admin API key"><button onclick="signIn()">Open dashboard</button></p></section><div id="app" hidden><section class="grid" id="stats"></section><section class="panel"><h2>Providers</h2><div id="providers" class="empty">Loading...</div></section><section class="panel"><h2>API keys</h2><div class="toolbar"><input id="key-name" placeholder="New key name"><button onclick="createKey()">Create key</button></div><div id="keys" class="empty">Loading...</div></section></div></main><script>
let entered=sessionStorage.getItem('ai_gateway_admin_key')||'';let auth={headers:entered?{Authorization:'Bearer '+entered}:{}};
function signIn(){entered=document.querySelector('#key').value.trim();if(!entered)return;sessionStorage.setItem('ai_gateway_admin_key',entered);auth={headers:{Authorization:'Bearer '+entered}};load()}
async function get(path){const r=await fetch(path,auth);if(!r.ok)throw Error(await r.text());return r.json()}
async function createKey(){try{const name=document.querySelector('#key-name').value.trim();if(!name)return;const r=await fetch('/admin/api-keys',{method:'POST',headers:{...auth.headers,'content-type':'application/json'},body:JSON.stringify({name})});const data=await r.json();if(!r.ok)throw Error(data.error?.message||'cannot create key');alert('Save this token now; it will not be shown again:\n\n'+data.key);document.querySelector('#key-name').value='';load()}catch(e){alert(e.message)}}
async function revokeKey(id){try{if(!confirm('Revoke this key?'))return;const r=await fetch('/admin/api-keys/'+id,{method:'DELETE',headers:auth.headers});if(!r.ok)throw Error(await r.text());load()}catch(e){alert(e.message)}}
function esc(s){return String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]))}
async function load(){if(!entered)return;try{const [s,h,k]=await Promise.all([get('/admin/stats'),get('/health'),get('/admin/api-keys')]);document.querySelector('#login').hidden=true;document.querySelector('#app').hidden=false;document.querySelector('#stats').innerHTML=[['Requests',s.requests],['Successful',s.successful_requests],['Tokens',s.total_tokens],['Avg latency',s.average_latency_ms+' ms']].map(x=>`<div class="card"><div class="muted">${x[0]}</div><div class="value">${x[1]}</div></div>`).join('');document.querySelector('#providers').innerHTML=h.providers.map(p=>`<div class="row"><div><strong>${esc(p.name)}</strong><div class="muted">${esc(p.kind)} · ${esc(p.model)}</div></div><div class="status ${p.status}">${p.status} · ${p.requests} req</div></div>`).join('')||'<div class="empty">No providers</div>';document.querySelector('#keys').innerHTML=k.data.length?`<table><thead><tr><th>Name</th><th>Prefix</th><th>Requests</th><th>State</th><th></th></tr></thead><tbody>${k.data.map(x=>`<tr><td>${esc(x.name)}</td><td>${esc(x.prefix)}...</td><td>${x.requests}</td><td class="${x.revoked_at?'unavailable':'healthy'}">${x.revoked_at?'revoked':'active'}</td><td>${x.revoked_at?'':'<button onclick="revokeKey('+x.id+')">Revoke</button>'}</td></tr>`).join('')}</tbody></table>`:'<div class="empty">No managed keys yet</div>'}catch(e){document.querySelector('#app').hidden=false;document.querySelector('#providers').innerHTML='<div class="empty">'+esc(e.message)+'</div>'}}if(entered)load();setInterval(()=>{if(entered)load()},15000);
</script></body></html>"""


def serve(service: GatewayService, host: str, port: int) -> None:
    server = ThreadingHTTPServer((host, port), make_handler(service))
    server.daemon_threads = True
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        if service.store:
            service.store.close()
