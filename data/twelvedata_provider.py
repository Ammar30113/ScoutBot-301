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
RATE_LIMIT_COOLDOWN = 60
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


MULTI_SYMBOL_CHUNK = 80


class TwelveDataProvider:
    """Lightweight TwelveData wrapper for price + aggregates."""

    BASE_URL = "https://api.twelvedata.com"
    _rate_limit_until = 0.0
    _disabled = False

    def __init__(self) -> None:
        settings = get_settings()
        self.api_key = settings.twelvedata_api_key
        self._strip_on_rate_limit = settings.strip_rate_limited_keys
        self.cache = get_cache()
        self.ttl = settings.cache_ttl
        self.no_data_ttl = max(60, min(int(self.ttl / 2) if self.ttl else 0, 900))
        if not self.api_key:
            logger.warning("TwelveDataProvider initialized without API key")

    def is_rate_limited(self) -> bool:
        return TwelveDataProvider._disabled or time.time() < TwelveDataProvider._rate_limit_until

    def _rate_limited(self) -> bool:
        return self.is_rate_limited()

    def _set_rate_limit(self, seconds: int, reason: str) -> None:
        until = time.time() + max(int(seconds), 1)
        if until > TwelveDataProvider._rate_limit_until:
            TwelveDataProvider._rate_limit_until = until
            logger.warning("TwelveData rate limit: cooling down %ds (%s)", int(seconds), reason or "rate limit")
        if self._strip_on_rate_limit:
            self._disable_provider(reason)

    def _disable_provider(self, reason: str) -> None:
        if TwelveDataProvider._disabled:
            return
        TwelveDataProvider._disabled = True
        self.api_key = ""
        logger.warning("TwelveData disabled after rate limit (%s)", reason or "rate limit")

    def _rate_limit_seconds(self, message: str) -> int:
        msg = (message or "").lower()
        if any(marker in msg for marker in ("month", "monthly")):
            return self._seconds_until_next_month()
        if any(marker in msg for marker in ("per day", "daily", "day", "credits", "plan")):
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
        status = str(payload.get("status") or "").lower()
        message = str(payload.get("message") or payload.get("error") or "").strip()
        code = payload.get("code")
        if status != "error" and code is None and not message:
            return None, ""
        msg_lower = message.lower()
        if code == 429 or any(marker in msg_lower for marker in ("rate limit", "per minute", "per day", "credits", "limit")):
            return "rate_limit", message or str(code or "")
        if "symbol" in msg_lower and any(marker in msg_lower for marker in ("not found", "invalid")):
            return "no_data", message
        if "no data" in msg_lower:
            return "no_data", message
        return "error", message

    def _handle_payload_error(self, symbol: str, cache_key: str, context: str, payload: dict) -> bool:
        err_type, message = self._parse_error(payload)
        if not err_type:
            return False
        if err_type == "rate_limit":
            cooldown = self._rate_limit_seconds(message)
            self._set_rate_limit(cooldown, message)
            return True
        if err_type == "no_data":
            _warn_sample(
                f"{context}_no_data",
                f"TwelveData {context} empty for {symbol}: {message}" if message else f"TwelveData {context} empty for {symbol}",
                level=logging.INFO,
            )
            self._cache_no_data(cache_key)
            return True
        _warn_sample(
            f"{context}_error",
            f"TwelveData {context} error for {symbol}: {message}" if message else f"TwelveData {context} error for {symbol}",
        )
        return True

    def get_price(self, symbol: str) -> Optional[float]:
        cache_key = f"td:price:{symbol.upper()}"
        cached = self.cache.get(cache_key)
        if cached is _NO_DATA:
            return None
        if not self.api_key:
            return cached
        if self._rate_limited():
            return cached if cached is not None else None
        params = {"symbol": symbol.upper(), "apikey": self.api_key, "interval": "1min", "outputsize": 1}
        try:
            response = requests.get(f"{self.BASE_URL}/time_series", params=params, timeout=10)
            if response.status_code == 429:
                self._set_rate_limit(RATE_LIMIT_COOLDOWN, "http 429")
                return cached if cached is not None else None
            response.raise_for_status()
            payload = response.json() or {}
            if self._handle_payload_error(symbol, cache_key, "price", payload):
                return cached if cached is not None else None
            values = payload.get("values", [])
            if not values:
                self._cache_no_data(cache_key)
                return cached if cached is not None else None
            price = float(values[0].get("close", 0.0))
            self.cache.set(cache_key, price, self.ttl)
            return price
        except Exception as exc:  # pragma: no cover - network guard
            logger.warning("TwelveData price fetch failed for %s: %s", symbol, exc)
            return cached

    def get_aggregates(self, symbol: str, timespan: str = "1day", limit: int = 60) -> List[Dict[str, float]]:
        cache_key = f"td:{timespan}:{symbol.upper()}"
        cached = self.cache.get(cache_key) or []
        if cached is _NO_DATA:
            return []
        if not self.api_key:
            return cached
        if self._rate_limited():
            return cached
        interval = self._normalize_timespan(timespan)
        params = {
            "symbol": symbol.upper(),
            "interval": interval,
            "apikey": self.api_key,
            "outputsize": limit,
        }
        try:
            response = requests.get(f"{self.BASE_URL}/time_series", params=params, timeout=10)
            if response.status_code == 429:
                self._set_rate_limit(RATE_LIMIT_COOLDOWN, "http 429")
                return cached
            response.raise_for_status()
            payload = response.json() or {}
            if self._handle_payload_error(symbol, cache_key, "aggregates", payload):
                return cached
            values = payload.get("values", []) or []
            if not values:
                _warn_sample("aggregates_empty", f"TwelveData aggregates empty for {symbol}", level=logging.INFO)
                self._cache_no_data(cache_key)
                return cached
        except Exception as exc:  # pragma: no cover - network guard
            _warn_sample("aggregates_failed", f"TwelveData aggregates failed for {symbol}: {exc}")
            return cached
        normalized: List[Dict[str, float]] = []
        for row in reversed(values):  # API returns newest first
            ts = _parse_timestamp(row.get("datetime"))
            if ts is None:
                continue
            normalized.append(
                {
                    "open": float(row["open"]),
                    "high": float(row["high"]),
                    "low": float(row["low"]),
                    "close": float(row["close"]),
                    "volume": float(row.get("volume", 0.0)),
                    "timestamp": ts,
                }
            )
        if normalized:
            self.cache.set(cache_key, normalized, self.ttl)
        return normalized

    def get_intraday_1m(self, symbol: str, limit: int = 60) -> List[Dict[str, float]]:
        """Fetch raw 1-minute bars."""

        cache_key = f"td:intraday1m:{symbol.upper()}"
        cached = self.cache.get(cache_key) or []
        if cached is _NO_DATA:
            return []
        if not self.api_key:
            return cached
        if self._rate_limited():
            return cached
        params = {
            "symbol": symbol.upper(),
            "interval": "1min",
            "apikey": self.api_key,
            "outputsize": limit,
        }
        try:
            response = requests.get(f"{self.BASE_URL}/time_series", params=params, timeout=10)
            if response.status_code == 429:
                self._set_rate_limit(RATE_LIMIT_COOLDOWN, "http 429")
                return cached
            response.raise_for_status()
            payload = response.json() or {}
            if self._handle_payload_error(symbol, cache_key, "intraday", payload):
                return cached
            values = payload.get("values", []) or []
            if not values:
                _warn_sample("intraday_empty", f"TwelveData intraday aggregates empty for {symbol}", level=logging.INFO)
                self._cache_no_data(cache_key)
                return cached
        except Exception as exc:  # pragma: no cover - network guard
            _warn_sample("intraday_failed", f"TwelveData intraday aggregates failed for {symbol}: {exc}")
            return cached
        normalized: List[Dict[str, float]] = []
        for row in reversed(values):  # API returns newest first
            ts = _parse_timestamp(row.get("datetime"))
            if ts is None:
                continue
            normalized.append(
                {
                    "open": float(row["open"]),
                    "high": float(row["high"]),
                    "low": float(row["low"]),
                    "close": float(row["close"]),
                    "volume": float(row.get("volume", 0.0)),
                    "timestamp": ts,
                }
            )
        if normalized:
            self.cache.set(cache_key, normalized, self.ttl)
        return normalized

    def _normalize_timespan(self, timespan: str) -> str:
        mapping = {"1day": "1day", "1hour": "1h", "1min": "1min"}
        return mapping.get(timespan.lower(), "1day")

    def get_market_cap(self, symbol: str) -> Optional[float]:
        """Fetch market cap via TwelveData profile endpoint."""

        cache_key = f"td:market_cap:{symbol.upper()}"
        cached = self.cache.get(cache_key)
        if cached is _NO_DATA:
            return 0.0
        if not self.api_key:
            return cached if cached is not None else 0.0
        if self._rate_limited():
            return cached if cached is not None else 0.0
        params = {"symbol": symbol.upper(), "apikey": self.api_key}
        try:
            response = requests.get(f"{self.BASE_URL}/profile", params=params, timeout=10)
            if response.status_code == 429:
                self._set_rate_limit(RATE_LIMIT_COOLDOWN, "http 429")
                return cached if cached is not None else 0.0
            response.raise_for_status()
            data = response.json() or {}
            if self._handle_payload_error(symbol, cache_key, "market cap", data):
                return cached if cached is not None else 0.0
            raw_cap = data.get("market_cap") or data.get("market_capitalization")
            if raw_cap is None:
                self._cache_no_data(cache_key)
                return cached if cached is not None else 0.0
            value = float(raw_cap)
            self.cache.set(cache_key, value, self.ttl)
            return value
        except Exception as exc:  # pragma: no cover - network guard
            logger.warning("TwelveData market cap fetch failed for %s: %s", symbol, exc)
            return cached if cached is not None else 0.0

    def get_daily_bars_multi(self, symbols: List[str], limit: int = 60) -> Dict[str, List[Dict[str, float]]]:
        """
        Fetch daily bars for multiple symbols using TwelveData multi-symbol API.
        Returns mapping of symbol -> list of bars.
        """

        results: Dict[str, List[Dict[str, float]]] = {}
        if not self.api_key or not symbols:
            return results
        if self._rate_limited():
            return results
        unique_symbols = list(dict.fromkeys(sym.upper() for sym in symbols))
        chunks = [unique_symbols[i : i + MULTI_SYMBOL_CHUNK] for i in range(0, len(unique_symbols), MULTI_SYMBOL_CHUNK)]
        for chunk in chunks:
            chunk_results = self._fetch_multi_chunk(chunk, limit)
            results.update(chunk_results)
        return results

    def _fetch_multi_chunk(self, symbols: List[str], limit: int) -> Dict[str, List[Dict[str, float]]]:
        results: Dict[str, List[Dict[str, float]]] = {}
        if not symbols:
            return results
        if self._rate_limited():
            return results
        joined = ",".join(symbols)
        params = {"symbol": joined, "interval": "1day", "apikey": self.api_key, "outputsize": limit}
        try:
            response = requests.get(f"{self.BASE_URL}/time_series", params=params, timeout=10)
            if response.status_code == 429:
                self._set_rate_limit(RATE_LIMIT_COOLDOWN, "http 429")
                return results
            if response.status_code == 414:
                _warn_sample("batch_uri_too_long", f"TwelveData batch request too long; chunk size={len(symbols)}")
                return results
            response.raise_for_status()
            data = response.json() or {}
        except Exception as exc:  # pragma: no cover - network guard
            _warn_sample("batch_failed", f"TwelveData batch daily bars failed: {exc}")
            return results

        if not isinstance(data, dict):
            return results
        if self._handle_payload_error(",".join(symbols), "td:1day:batch", "batch daily bars", data):
            return results
        for sym, payload in data.items():
            if not isinstance(payload, dict):
                continue
            sym_u = sym.upper()
            if self._handle_payload_error(sym_u, f"td:1day:{sym_u}", "batch daily bars", payload):
                continue
            values = payload.get("values")
            if not values:
                self._cache_no_data(f"td:1day:{sym_u}")
                continue
            bars: List[Dict[str, float]] = []
            for row in reversed(values):
                try:
                    ts = _parse_timestamp(row.get("datetime"))
                    if ts is None:
                        continue
                    bars.append(
                        {
                            "open": float(row["open"]),
                            "high": float(row["high"]),
                            "low": float(row["low"]),
                            "close": float(row["close"]),
                            "volume": float(row.get("volume", 0.0)),
                            "timestamp": ts,
                        }
                    )
                except Exception:
                    continue
            if bars:
                results[sym_u] = bars
                self.cache.set(f"td:1day:{sym_u}", bars, self.ttl)
        return results
