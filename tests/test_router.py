import unittest
from unittest.mock import patch

from ai_gateway.models import ChatRequest, ChatResponse, GatewayConfig, ProviderConfig, RoutingConfig, ServerConfig
from ai_gateway.providers import ProviderError
from ai_gateway.router import Router


class RouterTests(unittest.TestCase):
    def config(self) -> GatewayConfig:
        return GatewayConfig(ServerConfig(), RoutingConfig(max_retries=1), (
            ProviderConfig("first", "openai-compatible", "http://first", "one", priority=20),
            ProviderConfig("second", "openai-compatible", "http://second", "two", priority=10),
        ))

    def test_fails_over_to_next_provider(self) -> None:
        router = Router(self.config())
        first, second = router.states["first"].provider, router.states["second"].provider
        with patch.object(first, "chat", side_effect=ProviderError("down")), patch.object(second, "chat", return_value=ChatResponse("two", "ok", provider="second")):
            response = router.route(ChatRequest.from_dict({"messages": [{"role": "user", "content": "hi"}]}))
        self.assertEqual(response.content, "ok")
        self.assertEqual(response.provider, "second")

    def test_health_tracks_provider_failures(self) -> None:
        router = Router(self.config())
        first = router.states["first"].provider
        with patch.object(first, "chat", side_effect=ProviderError("down")), patch.object(router.states["second"].provider, "chat", side_effect=ProviderError("down")):
            with self.assertRaises(ProviderError):
                router.route(ChatRequest.from_dict({"messages": [{"role": "user", "content": "hi"}]}))
        self.assertEqual(router.health()[0]["errors"], 1)

    def test_task_route_order_wins_over_global_priority(self) -> None:
        config = GatewayConfig(
            ServerConfig(),
            RoutingConfig(max_retries=0, task_routes={"chat": ("second", "first")}),
            self.config().providers,
        )
        router = Router(config)
        first, second = router.states["first"].provider, router.states["second"].provider
        with patch.object(first, "chat", return_value=ChatResponse("one", "first", provider="first")), patch.object(second, "chat", return_value=ChatResponse("two", "second", provider="second")):
            response = router.route(ChatRequest.from_dict({"task": "chat", "messages": [{"role": "user", "content": "hi"}]}))
        self.assertEqual(response.provider, "second")
