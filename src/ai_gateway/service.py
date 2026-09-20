from __future__ import annotations

import json
import secrets
import time
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable

from .models import ChatRequest, RequestValidationError
from .router import Router


@dataclass
class Metrics:
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    total_latency_seconds: float = 0.0

    def prometheus(self) -> str:
        average = self.total_latency_seconds / self.total_requests if self.total_requests else 0
        return "\n".join([
            "# HELP ai_gateway_requests_total Total chat completion requests.",
            "# TYPE ai_gateway_requests_total counter",
            f"ai_gateway_requests_total {self.total_requests}",
            "# HELP ai_gateway_requests_success_total Successful chat completion requests.",
            "# TYPE ai_gateway_requests_success_total counter",
            f"ai_gateway_requests_success_total {self.successful_requests}",
            "# HELP ai_gateway_requests_failed_total Failed chat completion requests.",
            "# TYPE ai_gateway_requests_failed_total counter",
            f"ai_gateway_requests_failed_total {self.failed_requests}",
            "# HELP ai_gateway_request_latency_seconds Average request latency.",
            "# TYPE ai_gateway_request_latency_seconds gauge",
            f"ai_gateway_request_latency_seconds {average:.6f}",
        ]) + "\n"


class GatewayService:
    def __init__(self, router: Router, api_key: str | None = None):
        self.router = router
        self.api_key = api_key
        self.metrics = Metrics()

    def authenticate(self, supplied: str | None) -> bool:
        if not self.api_key:
            return True
        return bool(supplied) and secrets.compare_digest(supplied, self.api_key)

    def chat(self, body: Any) -> dict[str, Any]:
        request = ChatRequest.from_dict(body)
        started = time.monotonic()
        self.metrics.total_requests += 1
        try:
            response = self.router.route(request)
        except Exception:
            self.metrics.failed_requests += 1
            self.metrics.total_latency_seconds += time.monotonic() - started
            raise
        self.metrics.successful_requests += 1
        self.metrics.total_latency_seconds += time.monotonic() - started
        return response.as_openai(request.model)


def make_handler(service: GatewayService) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        server_version = "ai-gateway/0.1"

        def do_GET(self) -> None:  # noqa: N802
            if not service.authenticate(self._api_key()):
                self._json(HTTPStatus.UNAUTHORIZED, {"error": {"message": "invalid API key", "type": "authentication_error"}})
                return
            if self.path == "/health" or self.path == "/v1/health":
                self._json(HTTPStatus.OK, {"status": "ok", "providers": service.router.health()})
            elif self.path == "/metrics":
                data = service.metrics.prometheus().encode("utf-8")
                self.send_response(HTTPStatus.OK)
                self.send_header("content-type", "text/plain; version=0.0.4")
                self.send_header("content-length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
            elif self.path == "/v1/models":
                self._json(HTTPStatus.OK, {"object": "list", "data": [{"id": state["model"], "object": "model", "owned_by": state["name"]} for state in service.router.health()]})
            else:
                self._json(HTTPStatus.NOT_FOUND, {"error": {"message": "not found", "type": "invalid_request_error"}})

        def do_POST(self) -> None:  # noqa: N802
            if not service.authenticate(self._api_key()):
                self._json(HTTPStatus.UNAUTHORIZED, {"error": {"message": "invalid API key", "type": "authentication_error"}})
                return
            if self.path not in {"/v1/chat/completions", "/chat/completions"}:
                self._json(HTTPStatus.NOT_FOUND, {"error": {"message": "not found", "type": "invalid_request_error"}})
                return
            try:
                length = int(self.headers.get("content-length", "0"))
                if length > 2_000_000:
                    raise RequestValidationError("request body is too large")
                body = json.loads(self.rfile.read(length).decode("utf-8"))
                self._json(HTTPStatus.OK, service.chat(body))
            except (RequestValidationError, json.JSONDecodeError, UnicodeDecodeError) as exc:
                self._json(HTTPStatus.BAD_REQUEST, {"error": {"message": str(exc), "type": "invalid_request_error"}})
            except Exception as exc:
                self._json(HTTPStatus.BAD_GATEWAY, {"error": {"message": str(exc), "type": "provider_error"}})

        def _api_key(self) -> str | None:
            value = self.headers.get("authorization", "")
            return value[7:] if value.lower().startswith("bearer ") else None

        def _json(self, status: HTTPStatus, payload: dict[str, Any]) -> None:
            data = json.dumps(payload).encode("utf-8")
            self.send_response(status)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, format: str, *args: Any) -> None:
            return

    return Handler


def serve(service: GatewayService, host: str, port: int) -> None:
    server = ThreadingHTTPServer((host, port), make_handler(service))
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
