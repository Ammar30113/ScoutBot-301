from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
import logging
import time
from typing import Dict, List, Optional

import requests

from core.cache import get_cache
from core.config import get_settings
from core.logger import get_logger

logger = get_logger(__name__)
LOG_SAMPLE_LIMIT = 5
_warn_counts: dict[str, int] = defaultdict(int)
_NO_DATA = object()
RATE_LIMIT_COOLDOWN = 60


def _warn_sample(reason: str, message: str, *, level: int = logging.WARNING) -> None:
    count = _warn_counts[reason] + 1
    _warn_counts[reason] = count
    if count <= LOG_SAMPLE_LIMIT:
        logger.log(level, message)
    elif count == LOG_SAMPLE_LIMIT + 1:
        logger.info("%s (suppressing further repeats; %s occurrences)", message, count)


class MarketstackProvider:
    """EOD-only Marketstack wrapper for daily bars (free plan compatible)."""

    BASE_URL = "http://api.marketstack.com/v1"

    def __init__(self) -> None:
        settings = get_settings()
        self.api_key = settings.marketstack_api_key
        self.cache = get_cache()
        self.ttl = settings.marketstack_cache_ttl
        self.no_data_ttl = max(60, min(int(self.ttl / 2) if self.ttl else 0, 900))
        self._rate_limit_until = 0.0
        if not self.api_key:
            logger.warning("MarketstackProvider initialized without API key")

    def _rate_limited(self) -> bool:
        return time.time() < self._rate_limit_until

    def _set_rate_limit(self, seconds: int, reason: str) -> None:
        until = time.time() + max(int(seconds), 1)
        if until > self._rate_limit_until:
            self._rate_limit_until = until
            logger.warning("Marketstack rate limit: cooling down %ds (%s)", int(seconds), reason or "rate limit")

    def _rate_limit_seconds(self, message: str) -> int:
        msg = (message or "").lower()
        if any(marker in msg for marker in ("month", "monthly")):
            return self._seconds_until_next_month()
        if any(marker in msg for marker in ("per day", "daily", "day", "quota", "limit", "usage")):
            return self._seconds_until_next_day()
        return RATE_LIMIT_COOLDOWN

    def _seconds_until_next_day(self) -> int:
        now = datetime.now(timezone.utc)
        next_midnight = datetime(now.year, now.month, now.day, tzinfo=timezone.utc) + timedelta(days=1)
        return max(int((next_midnight - now).total_seconds()), RATE_LIMIT_COOLDOWN)

    def _seconds_until_next_month(self) -> int:
        now = datetime.now(timezone.utc)
        year = now.year + (1 if now.month == 12 else 0)
        month = 1 if now.month == 12 else now.month + 1
        next_month = datetime(year, month, 1, tzinfo=timezone.utc)
        return max(int((next_month - now).total_seconds()), RATE_LIMIT_COOLDOWN)

    def _cache_no_data(self, cache_key: str) -> None:
        cached = self.cache.get(cache_key)
        if cached is not None and cached is not _NO_DATA:
            return
        self.cache.set(cache_key, _NO_DATA, self.no_data_ttl)

    def _parse_error(self, payload: dict) -> tuple[Optional[str], str]:
        if not isinstance(payload, dict):
            return None, ""
        error = payload.get("error")
        if not error:
            return None, ""
        if isinstance(error, dict):
            code = str(error.get("code") or "")
            message = str(error.get("message") or "")
        else:
            code = ""
            message = str(error)
        msg_lower = f"{code} {message}".lower()
        if any(marker in msg_lower for marker in ("limit", "usage", "quota", "rate")):
            return "rate_limit", message or code
        return "error", message or code

    def _handle_payload_error(self, symbol: str, cache_key: str, context: str, payload: dict) -> bool:
        err_type, message = self._parse_error(payload)
        if not err_type:
            return False
        if err_type == "rate_limit":
            cooldown = self._rate_limit_seconds(message)
            self._set_rate_limit(cooldown, message)
            return True
        _warn_sample(
            f"{context}_error",
            f"Marketstack {context} error for {symbol}: {message}" if message else f"Marketstack {context} error for {symbol}",
        )
        return True

    def _parse_timestamp(self, value: str) -> Optional[float]:
        if not value:
            return None
        try:
            return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S%z").timestamp()
        except ValueError:
            cleaned = value.replace("Z", "+00:00")
            if cleaned.endswith("+0000") or cleaned.endswith("-0000"):
                cleaned = f"{cleaned[:-2]}:{cleaned[-2:]}"
            try:
                parsed = datetime.fromisoformat(cleaned)
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone.utc)
                return parsed.timestamp()
            except ValueError:
                return None

    def _normalize_row(self, row: dict) -> Optional[Dict[str, float]]:
        if not isinstance(row, dict):
            return None
        timestamp = self._parse_timestamp(str(row.get("date") or ""))
        if timestamp is None:
            return None
        try:
            open_price = float(row["open"])
            high = float(row["high"])
            low = float(row["low"])
            close = float(row["close"])
        except (TypeError, ValueError, KeyError):
            return None
        volume = row.get("volume")
        return {
            "open": open_price,
            "high": high,
            "low": low,
            "close": close,
            "volume": float(volume) if volume is not None else 0.0,
            "timestamp": timestamp,
        }

    def get_price(self, symbol: str) -> Optional[float]:
        return None

    def get_aggregates(self, symbol: str, timespan: str = "1day", limit: int = 60) -> List[Dict[str, float]]:
        if timespan.lower() not in ("1day", "day", "1d"):
            return []
        cache_key = f"ms:1day:{symbol.upper()}"
        cached = self.cache.get(cache_key) or []
        if cached is _NO_DATA:
            return []
        if not self.api_key:
            return cached
        if self._rate_limited():
            return cached
        params = {"access_key": self.api_key, "symbols": symbol.upper(), "limit": limit, "sort": "DESC"}
        try:
            response = requests.get(f"{self.BASE_URL}/eod", params=params, timeout=10)
            if response.status_code == 429:
                self._set_rate_limit(RATE_LIMIT_COOLDOWN, "http 429")
                return cached
            response.raise_for_status()
            payload = response.json() or {}
        except Exception as exc:  # pragma: no cover - network guard
            _warn_sample("eod_failed", f"Marketstack EOD failed for {symbol}: {exc}")
            return cached

        if self._handle_payload_error(symbol, cache_key, "aggregates", payload):
            return cached
        data = payload.get("data", []) or []
        if not data:
            _warn_sample("aggregates_empty", f"Marketstack aggregates empty for {symbol}", level=logging.INFO)
            self._cache_no_data(cache_key)
            return cached

        normalized: List[Dict[str, float]] = []
        for row in reversed(data):
            entry = self._normalize_row(row)
            if entry:
                normalized.append(entry)
        if normalized:
            self.cache.set(cache_key, normalized, self.ttl)
            return normalized
        self._cache_no_data(cache_key)
        return cached

    def get_daily_bars_multi(self, symbols: List[str], limit: int = 60) -> Dict[str, List[Dict[str, float]]]:
        """
        Fetch daily bars for symbols. Marketstack's EOD endpoint applies limits per request,
        so we fetch per symbol to ensure enough history.
        """

        results: Dict[str, List[Dict[str, float]]] = {}
        if not self.api_key or not symbols:
            return results
        if self._rate_limited():
            return results
        unique_symbols = list(dict.fromkeys(sym.upper() for sym in symbols if sym))
        for sym in unique_symbols:
            bars = self.get_aggregates(sym, timespan="1day", limit=limit)
            if bars:
                results[sym] = bars
        return results
