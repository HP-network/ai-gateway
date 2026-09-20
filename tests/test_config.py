import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from ai_gateway.config import ConfigError, config_from_env, load_config, load_config_or_env


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

    def test_rejects_invalid_integer_settings_with_config_error(self) -> None:
        path = self.write({
            "server": {"port": "8080"},
            "providers": [{"name": "local", "kind": "ollama", "base_url": "http://localhost", "model": "llama"}],
        })
        with self.assertRaises(ConfigError):
            load_config(path)

    def test_environment_mode_needs_no_json_file(self) -> None:
        with patch.dict(os.environ, {"OPENAI_API_KEY": "openai-secret"}, clear=True):
            config = config_from_env()
        self.assertEqual(config.providers[0].name, "openai")
        self.assertEqual(config.providers[0].api_key, "openai-secret")
        self.assertEqual(config.server.host, "127.0.0.1")
        self.assertEqual(config.server.port, 8080)

    def test_environment_mode_keeps_ollama_as_local_fallback(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            config = config_from_env()
        self.assertEqual([provider.name for provider in config.providers], ["ollama"])

    def test_explicit_config_wins_over_environment_mode(self) -> None:
        path = self.write({"providers": [{"name": "local", "kind": "ollama", "base_url": "http://localhost", "model": "llama"}]})
        with patch.dict(os.environ, {"OPENAI_API_KEY": "should-not-be-used"}, clear=True):
            config = load_config_or_env(path)
        self.assertEqual([provider.name for provider in config.providers], ["local"])

    def test_parses_admin_storage_and_rate_limit_settings(self) -> None:
        path = self.write({
            "server": {
                "admin_api_key": "admin",
                "database_path": "/tmp/gateway.db",
                "rate_limit_per_minute": 12,
            },
            "providers": [{"name": "local", "kind": "ollama", "base_url": "http://localhost", "model": "llama"}],
        })
        config = load_config(path)
        self.assertEqual(config.server.admin_api_key, "admin")
        self.assertEqual(config.server.database_path, "/tmp/gateway.db")
        self.assertEqual(config.server.rate_limit_per_minute, 12)

    def test_rejects_unknown_provider_kind(self) -> None:
        path = self.write({"providers": [{"name": "bad", "kind": "unknown", "base_url": "http://localhost", "model": "x"}]})
        with self.assertRaises(ConfigError):
            load_config(path)
