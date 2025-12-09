from __future__ import annotations

import threading
import time
from functools import lru_cache
from typing import Any, Dict, Tuple

from core.config import get_settings


class TTLCache:
    """Lightweight in-memory cache with per-item TTL support."""

    def __init__(self, default_ttl: int = 900) -> None:
        self.default_ttl = max(default_ttl, 0)
        self._data: Dict[str, Tuple[Any, float]] = {}
        self._lock = threading.Lock()

    def _expired(self, expires_at: float) -> bool:
        return expires_at <= time.time()

    def get(self, key: str, default: Any = None) -> Any:
        with self._lock:
            entry = self._data.get(key)
            if entry is None:
                return default
            value, expires_at = entry
            if self._expired(expires_at):
                self._data.pop(key, None)
                return default
            return value

    def set(self, key: str, value: Any, ttl: int | None = None) -> None:
        ttl_value = self.default_ttl if ttl is None else max(int(ttl), 0)
        if ttl_value <= 0:
            with self._lock:
                self._data.pop(key, None)
            return
        expires_at = time.time() + ttl_value
        with self._lock:
            self._data[key] = (value, expires_at)

    def delete(self, key: str) -> None:
        with self._lock:
            self._data.pop(key, None)

    def clear(self) -> None:
        with self._lock:
            self._data.clear()

    def cleanup(self) -> None:
        """Eagerly remove expired entries to keep the cache small."""

        now = time.time()
        with self._lock:
            expired_keys = [k for k, (_, expiry) in self._data.items() if expiry <= now]
            for key in expired_keys:
                self._data.pop(key, None)


@lru_cache(maxsize=1)
def get_cache() -> TTLCache:
    settings = get_settings()
    return TTLCache(default_ttl=settings.cache_ttl)
