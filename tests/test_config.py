import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from ai_gateway.config import ConfigError, load_config


class ConfigTests(unittest.TestCase):
    def write(self, value: dict) -> Path:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        path = Path(directory.name) / "config.json"
        path.write_text(json.dumps(value), encoding="utf-8")
        return path

    def test_loads_provider_secret_from_environment(self) -> None:
        path = self.write({"providers": [{"name": "local", "kind": "ollama", "base_url": "http://localhost", "model": "llama", "api_key_env": "TEST_KEY"}]})
        with patch.dict(os.environ, {"TEST_KEY": "secret"}, clear=True):
            config = load_config(path)
        self.assertEqual(config.providers[0].api_key, "secret")

    def test_rejects_unknown_task_route_provider(self) -> None:
        path = self.write({"routing": {"task_routes": {"chat": ["missing"]}}, "providers": [{"name": "local", "kind": "ollama", "base_url": "http://localhost", "model": "llama"}]})
        with self.assertRaises(ConfigError):
            load_config(path)
