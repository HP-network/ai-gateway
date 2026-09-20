from __future__ import annotations

import hashlib
import os
import secrets
import sqlite3
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class ApiKey:
    id: int
    name: str
    prefix: str
    created_at: int
    last_used_at: int | None
    revoked_at: int | None
    requests: int
    tokens: int


class Store:
    """Small SQLite store for gateway keys and operational usage counters."""

    def __init__(self, path: str | Path = "ai-gateway.db"):
        self.path = str(Path(path).expanduser()) if str(path) != ":memory:" else ":memory:"
        if self.path != ":memory:":
            Path(self.path).expanduser().parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._connection = sqlite3.connect(self.path, check_same_thread=False)
        if self.path != ":memory:":
            os.chmod(self.path, 0o600)
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA journal_mode=WAL")
        self._connection.execute("PRAGMA busy_timeout=5000")
        self._init_schema()

    def _init_schema(self) -> None:
        with self._lock, self._connection:
            self._connection.executescript("""
                CREATE TABLE IF NOT EXISTS api_keys (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    prefix TEXT NOT NULL,
                    digest TEXT NOT NULL UNIQUE,
                    created_at INTEGER NOT NULL,
                    last_used_at INTEGER,
                    revoked_at INTEGER,
                    requests INTEGER NOT NULL DEFAULT 0,
                    tokens INTEGER NOT NULL DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS usage (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    requests INTEGER NOT NULL DEFAULT 0,
                    successful_requests INTEGER NOT NULL DEFAULT 0,
                    failed_requests INTEGER NOT NULL DEFAULT 0,
                    prompt_tokens INTEGER NOT NULL DEFAULT 0,
                    completion_tokens INTEGER NOT NULL DEFAULT 0,
                    total_tokens INTEGER NOT NULL DEFAULT 0,
                    latency_seconds REAL NOT NULL DEFAULT 0
                );
                INSERT OR IGNORE INTO usage (id) VALUES (1);
            """)

    @staticmethod
    def digest(token: str) -> str:
        return hashlib.sha256(token.encode("utf-8")).hexdigest()

    def create_key(self, name: str) -> tuple[str, ApiKey]:
        clean_name = name.strip() or "default"
        if len(clean_name) > 100:
            raise ValueError("key name must be 100 characters or fewer")
        token = "ag_" + secrets.token_urlsafe(24)
        now = int(time.time())
        prefix = token[:11]
        with self._lock, self._connection:
            cursor = self._connection.execute(
                "INSERT INTO api_keys (name, prefix, digest, created_at) VALUES (?, ?, ?, ?)",
                (clean_name, prefix, self.digest(token), now),
            )
            row = self._connection.execute("SELECT * FROM api_keys WHERE id = ?", (cursor.lastrowid,)).fetchone()
        return token, self._row_to_key(row)

    def authenticate(self, token: str | None) -> ApiKey | None:
        return self.lookup(token, touch=True)

    def lookup(self, token: str | None, *, touch: bool = False) -> ApiKey | None:
        if not token:
            return None
        digest = self.digest(token)
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM api_keys WHERE digest = ? AND revoked_at IS NULL", (digest,)
            ).fetchone()
            if row is None:
                return None
            if not touch:
                return self._row_to_key(row)
            now = int(time.time())
            with self._connection:
                self._connection.execute(
                    "UPDATE api_keys SET last_used_at = ?, requests = requests + 1 WHERE id = ?",
                    (now, row["id"]),
                )
            return self._row_to_key(dict(row) | {"last_used_at": now, "requests": row["requests"] + 1})

    def revoke_key(self, key_id: int) -> bool:
        with self._lock, self._connection:
            result = self._connection.execute(
                "UPDATE api_keys SET revoked_at = ? WHERE id = ? AND revoked_at IS NULL",
                (int(time.time()), key_id),
            )
        return result.rowcount == 1

    def touch(self, key_id: int) -> None:
        now = int(time.time())
        with self._lock, self._connection:
            self._connection.execute(
                "UPDATE api_keys SET last_used_at = ?, requests = requests + 1 WHERE id = ? AND revoked_at IS NULL",
                (now, key_id),
            )

    def list_keys(self) -> list[ApiKey]:
        with self._lock:
            rows = self._connection.execute("SELECT * FROM api_keys ORDER BY created_at DESC").fetchall()
        return [self._row_to_key(row) for row in rows]

    def record_request(
        self,
        *,
        success: bool,
        prompt_tokens: int,
        completion_tokens: int,
        total_tokens: int | None = None,
        latency_seconds: float,
        key_id: int | None = None,
    ) -> None:
        total = max(0, total_tokens if total_tokens is not None else 0, prompt_tokens + completion_tokens)
        with self._lock, self._connection:
            self._connection.execute(
                """UPDATE usage SET requests = requests + 1,
                    successful_requests = successful_requests + ?,
                    failed_requests = failed_requests + ?,
                    prompt_tokens = prompt_tokens + ?,
                    completion_tokens = completion_tokens + ?,
                    total_tokens = total_tokens + ?,
                    latency_seconds = latency_seconds + ? WHERE id = 1""",
                (int(success), int(not success), prompt_tokens, completion_tokens, total, latency_seconds),
            )
            if key_id is not None:
                self._connection.execute(
                    "UPDATE api_keys SET tokens = tokens + ? WHERE id = ?",
                    (total, key_id),
                )

    def stats(self) -> dict[str, Any]:
        with self._lock:
            row = self._connection.execute("SELECT * FROM usage WHERE id = 1").fetchone()
            active = self._connection.execute("SELECT COUNT(*) FROM api_keys WHERE revoked_at IS NULL").fetchone()[0]
        result = dict(row)
        result["active_keys"] = active
        result["average_latency_ms"] = round(result["latency_seconds"] / result["requests"] * 1000, 2) if result["requests"] else 0
        return result

    def has_keys(self) -> bool:
        with self._lock:
            return self._connection.execute("SELECT 1 FROM api_keys LIMIT 1").fetchone() is not None

    @staticmethod
    def _row_to_key(row: Any) -> ApiKey:
        return ApiKey(
            id=row["id"], name=row["name"], prefix=row["prefix"], created_at=row["created_at"],
            last_used_at=row["last_used_at"], revoked_at=row["revoked_at"], requests=row["requests"], tokens=row["tokens"],
        )

    def close(self) -> None:
        with self._lock:
            self._connection.close()
