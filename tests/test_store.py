import tempfile
import unittest
from pathlib import Path

from ai_gateway.store import Store


class StoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.store = Store(Path(self.directory.name) / "gateway.db")
        self.addCleanup(self.store.close)

    def test_created_key_is_only_returned_in_plaintext_once(self) -> None:
        token, created = self.store.create_key("desktop")
        self.assertTrue(token.startswith("ag_"))
        self.assertEqual(self.store.lookup(token).name, "desktop")
        self.assertIsNone(self.store.lookup("ag_invalid"))
        self.assertNotIn(token, str(self.store.list_keys()))
        self.assertEqual(created.requests, 0)

    def test_touch_and_revoke(self) -> None:
        token, created = self.store.create_key("worker")
        self.store.touch(created.id)
        self.assertEqual(self.store.lookup(token).requests, 1)
        self.assertTrue(self.store.revoke_key(created.id))
        self.assertIsNone(self.store.lookup(token))
        self.assertFalse(self.store.revoke_key(created.id))

    def test_usage_is_persisted(self) -> None:
        token, created = self.store.create_key("usage")
        self.store.record_request(success=True, prompt_tokens=3, completion_tokens=5, total_tokens=13, latency_seconds=0.2, key_id=created.id)
        stats = self.store.stats()
        self.assertEqual(stats["requests"], 1)
        self.assertEqual(stats["total_tokens"], 13)
        self.assertEqual(self.store.lookup(token).tokens, 13)

    def test_key_names_are_bounded(self) -> None:
        with self.assertRaises(ValueError):
            self.store.create_key("x" * 101)

    def test_expands_user_home_for_file_databases(self) -> None:
        path = "~/ai-gateway-test.db"
        store = Store(path)
        self.addCleanup(store.close)
        self.assertFalse(store.path.startswith("~"))
        Path(store.path).unlink(missing_ok=True)
