from __future__ import annotations

import threading
import time
from collections import deque


class RateLimiter:
    def __init__(self, per_minute: int = 0):
        self.per_minute = max(0, per_minute)
        self._lock = threading.Lock()
        self._windows: dict[str, deque[float]] = {}

    def allow(self, identity: str) -> bool:
        if not self.per_minute:
            return True
        now = time.monotonic()
        with self._lock:
            window = self._windows.setdefault(identity, deque())
            while window and now - window[0] >= 60:
                window.popleft()
            if len(window) >= self.per_minute:
                return False
            window.append(now)
            if len(self._windows) > 10000:
                self._windows = {
                    key: value for key, value in self._windows.items()
                    if value and now - value[-1] < 60
                }
            return True
