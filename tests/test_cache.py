"""Tests for core.cache TTLCache."""

import time
from unittest.mock import patch

from core.cache import TTLCache


class TestTTLCache:
    def test_set_and_get(self):
        cache = TTLCache(default_ttl=60)
        cache.set("key1", "value1")
        assert cache.get("key1") == "value1"

    def test_get_missing_key_returns_default(self):
        cache = TTLCache(default_ttl=60)
        assert cache.get("missing") is None
        assert cache.get("missing", "fallback") == "fallback"

    def test_expired_entry_returns_default(self):
        cache = TTLCache(default_ttl=1)
        cache.set("key1", "value1", ttl=1)
        with patch("core.cache.time") as mock_time:
            mock_time.time.return_value = time.time() + 10
            assert cache.get("key1") is None

    def test_delete_key(self):
        cache = TTLCache(default_ttl=60)
        cache.set("key1", "value1")
        cache.delete("key1")
        assert cache.get("key1") is None

    def test_clear_removes_all(self):
        cache = TTLCache(default_ttl=60)
        cache.set("a", 1)
        cache.set("b", 2)
        cache.clear()
        assert cache.get("a") is None
        assert cache.get("b") is None

    def test_zero_ttl_removes_key(self):
        cache = TTLCache(default_ttl=60)
        cache.set("key1", "value1")
        cache.set("key1", "value1", ttl=0)
        assert cache.get("key1") is None

    def test_cleanup_removes_expired(self):
        cache = TTLCache(default_ttl=60)
        # Insert with a past expiry by manipulating internal state
        cache._data["old"] = ("val", time.time() - 10)
        cache._data["fresh"] = ("val2", time.time() + 300)
        cache.cleanup()
        assert "old" not in cache._data
        assert "fresh" in cache._data

    def test_max_size_evicts_oldest(self):
        cache = TTLCache(default_ttl=60, max_size=3)
        now = time.time()
        # Insert 3 items with staggered expiry
        cache._data["a"] = ("v1", now + 10)
        cache._data["b"] = ("v2", now + 20)
        cache._data["c"] = ("v3", now + 30)
        # Adding a 4th should evict the one with earliest expiry ("a")
        cache.set("d", "v4")
        assert cache.get("a") is None
        assert cache.get("d") == "v4"
        assert len(cache) <= 3

    def test_len(self):
        cache = TTLCache(default_ttl=60)
        assert len(cache) == 0
        cache.set("a", 1)
        cache.set("b", 2)
        assert len(cache) == 2

    def test_custom_ttl_override(self):
        cache = TTLCache(default_ttl=10)
        cache.set("short", "val", ttl=1)
        cache.set("long", "val", ttl=3600)
        # short should expire quickly, long should not
        with patch("core.cache.time") as mock_time:
            mock_time.time.return_value = time.time() + 5
            assert cache.get("short") is None
            assert cache.get("long") == "val"
