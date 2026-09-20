import unittest

from ai_gateway.models import ChatRequest, ChatResponse, RequestValidationError, Usage


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

    def test_rejects_non_finite_temperature(self) -> None:
        with self.assertRaises(RequestValidationError):
            ChatRequest.from_dict({"messages": [{"role": "user", "content": "x"}], "temperature": float("nan")})

    def test_parses_gemini_usage_fields(self) -> None:
        usage = Usage.from_dict({"promptTokenCount": 4, "candidatesTokenCount": 6, "totalTokenCount": 12})
        self.assertEqual(usage.as_dict(), {"prompt_tokens": 4, "completion_tokens": 6, "total_tokens": 12})

    def test_completion_ids_are_unique(self) -> None:
        first = ChatResponse("model", "one").as_openai()
        second = ChatResponse("model", "two").as_openai()
        self.assertNotEqual(first["id"], second["id"])
