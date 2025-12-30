from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional
from collections import defaultdict
import logging
import time

import requests

from core.config import get_settings
from core.logger import get_logger
from core.cache import get_cache

logger = get_logger(__name__)
LOG_SAMPLE_LIMIT = 5
_warn_counts: dict[str, int] = defaultdict(int)
_NO_DATA = object()
RATE_LIMIT_COOLDOWN = 70
try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover - py<3.9
    ZoneInfo = None

_DEFAULT_TZ = ZoneInfo("America/New_York") if ZoneInfo else timezone.utc


def _parse_timestamp(value: str | None) -> float | None:
    if not value:
        return None
    raw = str(value)
    if raw.endswith("Z"):
        raw = raw.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=_DEFAULT_TZ)
    return parsed.timestamp()


def _warn_sample(reason: str, message: str, *, level: int = logging.WARNING) -> None:
    count = _warn_counts[reason] + 1
    _warn_counts[reason] = count
    if count <= LOG_SAMPLE_LIMIT:
        logger.log(level, message)
    elif count == LOG_SAMPLE_LIMIT + 1:
        logger.info("%s (suppressing further repeats; %s occurrences)", message, count)


class AlphaVantageProvider:
    BASE_URL = "https://www.alphavantage.co/query"
    _rate_limit_until = 0.0
    _disabled = False

    def __init__(self) -> None:
        settings = get_settings()
        self.api_key = settings.alphavantage_api_key
        self._strip_on_rate_limit = settings.strip_rate_limited_keys
        self.cache = get_cache()
        self.ttl = settings.cache_ttl
        self.no_data_ttl = max(60, min(int(self.ttl / 2) if self.ttl else 0, 900))
        if not self.api_key:
            logger.warning("AlphaVantageProvider initialized without API key")

    def is_rate_limited(self) -> bool:
        return AlphaVantageProvider._disabled or time.time() < AlphaVantageProvider._rate_limit_until

    def _rate_limited(self) -> bool:
        return self.is_rate_limited()

    def _set_rate_limit(self, seconds: int, reason: str) -> None:
        until = time.time() + max(int(seconds), 1)
        if until > AlphaVantageProvider._rate_limit_until:
            AlphaVantageProvider._rate_limit_until = until
            logger.warning("AlphaVantage rate limit: cooling down %ds (%s)", int(seconds), reason or "rate limit")
        if self._strip_on_rate_limit:
            self._disable_provider(reason)

    def _disable_provider(self, reason: str) -> None:
        if AlphaVantageProvider._disabled:
            return
        AlphaVantageProvider._disabled = True
        self.api_key = ""
        logger.warning("AlphaVantage disabled after rate limit (%s)", reason or "rate limit")

    def _rate_limit_seconds(self, message: str) -> int:
        msg = (message or "").lower()
        if any(marker in msg for marker in ("month", "monthly")):
            return self._seconds_until_next_month()
        if any(marker in msg for marker in ("per day", "daily", "day", "premium", "limit")):
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
        if "Note" in payload:
            return "rate_limit", str(payload.get("Note") or "")
        if "Information" in payload and "Thank you" in str(payload.get("Information") or ""):
            return "rate_limit", str(payload.get("Information") or "")
        if "Error Message" in payload:
            return "no_data", str(payload.get("Error Message") or "")
        if "Error" in payload:
            return "no_data", str(payload.get("Error") or "")
        return None, ""

    def _handle_payload_error(self, symbol: str, cache_key: str, context: str, payload: dict) -> bool:
        err_type, message = self._parse_error(payload)
        if not err_type:
            return False
        if err_type == "rate_limit":
            cooldown = self._rate_limit_seconds(message)
            self._set_rate_limit(cooldown, message)
            return True
        _warn_sample(
            f"{context}_no_data",
            f"AlphaVantage {context} empty for {symbol}: {message}" if message else f"AlphaVantage {context} empty for {symbol}",
            level=logging.INFO,
        )
        self._cache_no_data(cache_key)
        return True

    def get_price(self, symbol: str) -> Optional[float]:
        cache_key = f"av:price:{symbol.upper()}"
        cached = self.cache.get(cache_key)
        if cached is _NO_DATA:
            return None
        if not self.api_key:
            return cached
        if self._rate_limited():
            return cached if cached is not None else None
        params = {"function": "GLOBAL_QUOTE", "symbol": symbol.upper(), "apikey": self.api_key}
        try:
            response = requests.get(self.BASE_URL, params=params, timeout=10)
            if response.status_code == 429:
                self._set_rate_limit(RATE_LIMIT_COOLDOWN, "http 429")
                return cached if cached is not None else None
            response.raise_for_status()
            payload = response.json() or {}
            if self._handle_payload_error(symbol, cache_key, "price", payload):
                return cached if cached is not None else None
            data = payload.get("Global Quote", {}) or {}
            price = data.get("05. price")
            if price is None:
                self._cache_no_data(cache_key)
                return cached if cached is not None else None
            value = float(price)
            self.cache.set(cache_key, value, self.ttl)
            return value
        except Exception as exc:  # pragma: no cover - network guard
            logger.warning("AlphaVantage price fetch failed for %s: %s", symbol, exc)
            return cached

    def get_aggregates(self, symbol: str, timespan: str = "1day", limit: int = 60) -> List[Dict[str, float]]:
        cache_key = f"av:daily:{symbol.upper()}"
        cached = self.cache.get(cache_key) or []
        if cached is _NO_DATA:
            return []
        if not self.api_key:
            return cached
        if self._rate_limited():
            return cached
        params = {"function": "TIME_SERIES_DAILY_ADJUSTED", "symbol": symbol.upper(), "apikey": self.api_key}
        try:
            response = requests.get(self.BASE_URL, params=params, timeout=10)
            if response.status_code == 429:
                self._set_rate_limit(RATE_LIMIT_COOLDOWN, "http 429")
                return cached
            response.raise_for_status()
            payload = response.json() or {}
            if self._handle_payload_error(symbol, cache_key, "aggregates", payload):
                return cached
            data = payload.get("Time Series (Daily)", {}) or {}
            if not data:
                _warn_sample("aggregates_empty", f"AlphaVantage aggregates empty for {symbol}", level=logging.INFO)
                self._cache_no_data(cache_key)
                return cached
        except Exception as exc:  # pragma: no cover - network guard
            _warn_sample("aggregates_failed", f"AlphaVantage aggregates failed for {symbol}: {exc}")
            return cached
        normalized: List[Dict[str, float]] = []
        for date_str, values in list(data.items())[:limit]:
            ts = _parse_timestamp(date_str)
            if ts is None:
                continue
            normalized.append(
                {
                    "open": float(values["1. open"]),
                    "high": float(values["2. high"]),
                    "low": float(values["3. low"]),
                    "close": float(values["4. close"]),
                    "volume": float(values["6. volume"]),
                    "timestamp": ts,
                }
            )
        normalized.sort(key=lambda row: row["timestamp"])
        if normalized:
            self.cache.set(cache_key, normalized, self.ttl)
        return normalized

    def get_intraday_5m(self, symbol: str, limit: int = 60) -> List[Dict[str, float]]:
        """Fetch 5-minute intraday bars."""

        cache_key = f"av:intraday5m:{symbol.upper()}"
        cached = self.cache.get(cache_key) or []
        if cached is _NO_DATA:
            return []
        if not self.api_key:
            return cached
        if self._rate_limited():
            return cached
        params = {
            "function": "TIME_SERIES_INTRADAY",
            "symbol": symbol.upper(),
            "interval": "5min",
            "apikey": self.api_key,
            "outputsize": "compact",
        }
        try:
            response = requests.get(self.BASE_URL, params=params, timeout=10)
            if response.status_code == 429:
                self._set_rate_limit(RATE_LIMIT_COOLDOWN, "http 429")
                return cached
            response.raise_for_status()
            payload = response.json() or {}
            if self._handle_payload_error(symbol, cache_key, "intraday", payload):
                return cached
            data = payload.get("Time Series (5min)", {}) or {}
            if not data:
                _warn_sample("intraday_empty", f"AlphaVantage intraday aggregates empty for {symbol}", level=logging.INFO)
                self._cache_no_data(cache_key)
                return cached
        except Exception as exc:  # pragma: no cover - network guard
            _warn_sample("intraday_failed", f"AlphaVantage intraday aggregates failed for {symbol}: {exc}")
            return cached

        normalized: List[Dict[str, float]] = []
        for date_str, values in list(data.items())[:limit]:
            ts = _parse_timestamp(date_str)
            if ts is None:
                continue
            normalized.append(
                {
                    "open": float(values["1. open"]),
                    "high": float(values["2. high"]),
                    "low": float(values["3. low"]),
                    "close": float(values["4. close"]),
                    "volume": float(values.get("5. volume", 0.0)),
                    "timestamp": ts,
                }
            )
        normalized.sort(key=lambda row: row["timestamp"])
        if normalized:
            self.cache.set(cache_key, normalized, self.ttl)
        return normalized

    def get_market_cap(self, symbol: str) -> Optional[float]:
        """Fetch market cap via AlphaVantage OVERVIEW endpoint."""

        cache_key = f"av:market_cap:{symbol.upper()}"
        cached = self.cache.get(cache_key)
        if cached is _NO_DATA:
            return 0.0
        if not self.api_key:
            return cached if cached is not None else 0.0
        if self._rate_limited():
            return cached if cached is not None else 0.0
        params = {"function": "OVERVIEW", "symbol": symbol.upper(), "apikey": self.api_key}
        try:
            response = requests.get(self.BASE_URL, params=params, timeout=10)
            if response.status_code == 429:
                self._set_rate_limit(RATE_LIMIT_COOLDOWN, "http 429")
                return cached if cached is not None else 0.0
            response.raise_for_status()
            data = response.json() or {}
            if self._handle_payload_error(symbol, cache_key, "market cap", data):
                return cached if cached is not None else 0.0
            raw_cap = data.get("MarketCapitalization")
            if raw_cap is None:
                self._cache_no_data(cache_key)
                return cached if cached is not None else 0.0
            value = float(raw_cap)
            self.cache.set(cache_key, value, self.ttl)
            return value
        except Exception as exc:  # pragma: no cover - network guard
            logger.warning("AlphaVantage market cap fetch failed for %s: %s", symbol, exc)
            return cached if cached is not None else 0.0

    def get_batch_quotes(self, symbols: List[str]) -> Dict[str, float]:
        """
        Fetch latest prices via AlphaVantage batch endpoint.
        Returns mapping of symbol -> price; falls back to cache on errors.
        """

        if not self.api_key or not symbols:
            return {}
        if self._rate_limited():
            return {}
        joined = ",".join(sorted(set(sym.upper() for sym in symbols)))
        cache_key = f"av:batch_quotes:{joined}"
        cached = self.cache.get(cache_key) or {}
        params = {"function": "BATCH_STOCK_QUOTES", "symbols": joined, "apikey": self.api_key}
        try:
            response = requests.get(self.BASE_URL, params=params, timeout=10)
            if response.status_code == 429:
                self._set_rate_limit(RATE_LIMIT_COOLDOWN, "http 429")
                return cached
            response.raise_for_status()
            payload = response.json() or {}
            if self._handle_payload_error("batch", cache_key, "batch quotes", payload):
                return cached
            quotes_payload = payload.get("Stock Quotes", []) or []
            quotes: Dict[str, float] = {}
            for item in quotes_payload:
                sym = item.get("1. symbol")
                price = item.get("2. price")
                if sym and price is not None:
                    quotes[sym.upper()] = float(price)
            if quotes:
                self.cache.set(cache_key, quotes, self.ttl)
                for sym, price in quotes.items():
                    self.cache.set(f"av:price:{sym}", price, self.ttl)
            return quotes if quotes else cached
        except Exception as exc:  # pragma: no cover - network guard
            logger.warning("AlphaVantage batch quotes failed: %s", exc)
            return cached
