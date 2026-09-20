import unittest
from unittest.mock import patch
from http.client import HTTPConnection
from threading import Thread

from ai_gateway.models import ChatResponse, GatewayConfig, ProviderConfig, RoutingConfig, ServerConfig
from ai_gateway.router import Router
from ai_gateway.service import GatewayService, make_handler
from http.server import ThreadingHTTPServer


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
        self.assertIn("/v1/chat/completions", response.read().decode())
        connection.close()
