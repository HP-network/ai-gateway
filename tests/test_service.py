import unittest
from unittest.mock import patch
from http.client import HTTPConnection
import json
import tempfile
from threading import Thread
from pathlib import Path

from ai_gateway.models import ChatResponse, GatewayConfig, ProviderConfig, RoutingConfig, ServerConfig
from ai_gateway.router import Router
from ai_gateway.service import GatewayService, make_handler
from http.server import ThreadingHTTPServer
from ai_gateway.store import Store


class ServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        config = GatewayConfig(
            ServerConfig(api_key="gateway-secret"),
            RoutingConfig(max_retries=0),
            (ProviderConfig("fake", "openai-compatible", "http://fake", "model"),),
        )
        self.router = Router(config)
        self.service = GatewayService(self.router, "gateway-secret")

    def test_authentication_uses_constant_time_comparison(self) -> None:
        self.assertFalse(self.service.authenticate("wrong"))
        self.assertTrue(self.service.authenticate("gateway-secret"))

    def test_chat_returns_openai_shape_and_updates_metrics(self) -> None:
        with patch.object(self.router.states["fake"].provider, "chat", return_value=ChatResponse("model", "hello", provider="fake")):
            payload = self.service.chat({"messages": [{"role": "user", "content": "hi"}]})
        self.assertEqual(payload["choices"][0]["message"]["content"], "hello")
        self.assertEqual(self.service.metrics.successful_requests, 1)
        self.assertEqual(self.service.metrics.failed_requests, 0)
        self.assertIn("ai_gateway_requests_total 1", self.service.metrics.prometheus())

    def test_root_describes_available_endpoints(self) -> None:
        server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(self.service))
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        connection = HTTPConnection("127.0.0.1", server.server_port, timeout=2)
        connection.request("GET", "/")
        response = connection.getresponse()
        self.assertEqual(response.status, 200)
        body = response.read().decode()
        self.assertIn("/v1/chat/completions", body)
        self.assertIn('"version": "0.2.0"', body)
        connection.close()

    def test_managed_key_can_call_chat_and_admin_can_revoke_it(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        store = Store(Path(directory.name) / "gateway.db")
        self.addCleanup(store.close)
        service = GatewayService(self.router, admin_api_key="admin-secret", store=store, rate_limit_per_minute=1)
        server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(service))
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        Thread(target=server.serve_forever, daemon=True).start()

        status, payload = self.request(server, "POST", "/admin/api-keys", {"Authorization": "Bearer admin-secret"}, {"name": "test-client"})
        self.assertEqual(status, 201)
        token = payload["key"]
        key_id = payload["data"]["id"]
        with patch.object(self.router.states["fake"].provider, "chat", return_value=ChatResponse("model", "hello", provider="fake")):
            status, payload = self.request(server, "POST", "/v1/chat/completions", {"Authorization": f"Bearer {token}"}, {"messages": [{"role": "user", "content": "hi"}]})
        self.assertEqual(status, 200)
        self.assertEqual(payload["choices"][0]["message"]["content"], "hello")
        status, _ = self.request(server, "POST", "/v1/chat/completions", {"Authorization": f"Bearer {token}"}, {"messages": [{"role": "user", "content": "again"}]})
        self.assertEqual(status, 429)
        status, stats = self.request(server, "GET", "/admin/stats", {"Authorization": "Bearer admin-secret"})
        self.assertEqual(status, 200)
        self.assertEqual(stats["requests"], 1)
        status, _ = self.request(server, "DELETE", f"/admin/api-keys/{key_id}", {"Authorization": "Bearer admin-secret"})
        self.assertEqual(status, 200)

    def test_admin_can_read_health_but_not_call_chat(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        store = Store(Path(directory.name) / "gateway.db")
        self.addCleanup(store.close)
        service = GatewayService(self.router, admin_api_key="admin-secret", store=store)
        server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(service))
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        Thread(target=server.serve_forever, daemon=True).start()
        status, payload = self.request(server, "GET", "/health", {"Authorization": "Bearer admin-secret"})
        self.assertEqual(status, 200)
        self.assertIn("providers", payload)
        status, _ = self.request(server, "POST", "/v1/chat/completions", {"Authorization": "Bearer admin-secret"}, {"messages": [{"role": "user", "content": "no"}]})
        self.assertEqual(status, 401)

    def test_gateway_key_cannot_manage_when_admin_key_is_separate(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        store = Store(Path(directory.name) / "gateway.db")
        self.addCleanup(store.close)
        service = GatewayService(self.router, api_key="gateway-secret", admin_api_key="admin-secret", store=store)
        server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(service))
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        Thread(target=server.serve_forever, daemon=True).start()
        status, _ = self.request(server, "POST", "/admin/api-keys", {"Authorization": "Bearer gateway-secret"}, {"name": "nope"})
        self.assertEqual(status, 401)

    @staticmethod
    def request(server: ThreadingHTTPServer, method: str, path: str, headers: dict[str, str], body: dict | None = None) -> tuple[int, dict]:
        connection = HTTPConnection("127.0.0.1", server.server_port, timeout=2)
        encoded = json.dumps(body).encode() if body is not None else None
        request_headers = {"content-type": "application/json", **headers} if encoded else headers
        connection.request(method, path, body=encoded, headers=request_headers)
        response = connection.getresponse()
        payload = json.loads(response.read().decode())
        connection.close()
        return response.status, payload
