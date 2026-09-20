import unittest

from ai_gateway.models import ChatRequest, RequestValidationError


class ModelTests(unittest.TestCase):
    def test_parses_openai_request_and_preserves_extra_fields(self) -> None:
        request = ChatRequest.from_dict({
            "model": "auto",
            "messages": [{"role": "user", "content": "hello"}],
            "temperature": 0.2,
            "stream": False,
            "response_format": {"type": "json_object"},
        })
        self.assertEqual(request.messages[0].content, "hello")
        self.assertEqual(request.extra["response_format"]["type"], "json_object")

    def test_rejects_empty_messages_and_bad_temperature(self) -> None:
        with self.assertRaises(RequestValidationError):
            ChatRequest.from_dict({"messages": []})
        with self.assertRaises(RequestValidationError):
            ChatRequest.from_dict({"messages": [{"role": "user", "content": "x"}], "temperature": 3})
