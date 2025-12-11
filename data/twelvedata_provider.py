from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Optional
from collections import defaultdict

import requests

from core.config import get_settings
from core.logger import get_logger
from core.cache import get_cache

logger = get_logger(__name__)
LOG_SAMPLE_LIMIT = 5
_warn_counts: dict[str, int] = defaultdict(int)


def _warn_sample(reason: str, message: str) -> None:
    count = _warn_counts[reason] + 1
    _warn_counts[reason] = count
    if count <= LOG_SAMPLE_LIMIT:
        logger.warning(message)
    elif count == LOG_SAMPLE_LIMIT + 1:
        logger.info("%s (suppressing further repeats; %s occurrences)", message, count)


MULTI_SYMBOL_CHUNK = 150


class TwelveDataProvider:
    """Lightweight TwelveData wrapper for price + aggregates."""

    BASE_URL = "https://api.twelvedata.com"

    def __init__(self) -> None:
        settings = get_settings()
        self.api_key = settings.twelvedata_api_key
        self.cache = get_cache()
        self.ttl = settings.cache_ttl
        if not self.api_key:
            logger.warning("TwelveDataProvider initialized without API key")

    def get_price(self, symbol: str) -> Optional[float]:
        cache_key = f"td:price:{symbol.upper()}"
        cached = self.cache.get(cache_key)
        if not self.api_key:
            return cached
        params = {"symbol": symbol.upper(), "apikey": self.api_key, "interval": "1min", "outputsize": 1}
        try:
            response = requests.get(f"{self.BASE_URL}/time_series", params=params, timeout=10)
            if response.status_code == 429:
                logger.warning("TwelveData price rate-limited for %s", symbol)
                return cached
            response.raise_for_status()
            values = response.json().get("values", [])
            if not values:
                if cached is not None:
                    return cached
                return None
            price = float(values[0].get("close", 0.0))
            self.cache.set(cache_key, price, self.ttl)
            return price
        except Exception as exc:  # pragma: no cover - network guard
            logger.warning("TwelveData price fetch failed for %s: %s", symbol, exc)
            return cached

    def get_aggregates(self, symbol: str, timespan: str = "1day", limit: int = 60) -> List[Dict[str, float]]:
        cache_key = f"td:{timespan}:{symbol.upper()}"
        cached = self.cache.get(cache_key) or []
        if not self.api_key:
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
                _warn_sample("aggregates_rate_limited", f"TwelveData aggregates rate-limited for {symbol}")
                return cached
            response.raise_for_status()
            values = response.json().get("values", []) or []
            if not values:
                _warn_sample("aggregates_empty", f"TwelveData aggregates empty for {symbol}")
                return cached
        except Exception as exc:  # pragma: no cover - network guard
            _warn_sample("aggregates_failed", f"TwelveData aggregates failed for {symbol}: {exc}")
            return cached
        normalized: List[Dict[str, float]] = []
        for row in reversed(values):  # API returns newest first
            normalized.append(
                {
                    "open": float(row["open"]),
                    "high": float(row["high"]),
                    "low": float(row["low"]),
                    "close": float(row["close"]),
                    "volume": float(row.get("volume", 0.0)),
                    "timestamp": datetime.fromisoformat(row["datetime"]).timestamp(),
                }
            )
        if normalized:
            self.cache.set(cache_key, normalized, self.ttl)
        return normalized

    def get_intraday_1m(self, symbol: str, limit: int = 60) -> List[Dict[str, float]]:
        """Fetch raw 1-minute bars."""

        cache_key = f"td:intraday1m:{symbol.upper()}"
        cached = self.cache.get(cache_key) or []
        if not self.api_key:
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
                _warn_sample("intraday_rate_limited", f"TwelveData intraday rate-limited for {symbol}")
                return cached
            response.raise_for_status()
            values = response.json().get("values", []) or []
            if not values:
                _warn_sample("intraday_empty", f"TwelveData intraday aggregates empty for {symbol}")
                return cached
        except Exception as exc:  # pragma: no cover - network guard
            _warn_sample("intraday_failed", f"TwelveData intraday aggregates failed for {symbol}: {exc}")
            return cached
        normalized: List[Dict[str, float]] = []
        for row in reversed(values):  # API returns newest first
            normalized.append(
                {
                    "open": float(row["open"]),
                    "high": float(row["high"]),
                    "low": float(row["low"]),
                    "close": float(row["close"]),
                    "volume": float(row.get("volume", 0.0)),
                    "timestamp": datetime.fromisoformat(row["datetime"]).timestamp(),
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
        if not self.api_key:
            return cached if cached is not None else 0.0
        params = {"symbol": symbol.upper(), "apikey": self.api_key}
        try:
            response = requests.get(f"{self.BASE_URL}/profile", params=params, timeout=10)
            if response.status_code == 429:
                logger.warning("TwelveData market cap rate-limited for %s", symbol)
                return cached if cached is not None else 0.0
            response.raise_for_status()
            data = response.json() or {}
            raw_cap = data.get("market_cap") or data.get("market_capitalization")
            if raw_cap is None:
                if cached is not None:
                    return cached
                return 0.0
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
        joined = ",".join(symbols)
        params = {"symbol": joined, "interval": "1day", "apikey": self.api_key, "outputsize": limit}
        try:
            response = requests.get(f"{self.BASE_URL}/time_series", params=params, timeout=10)
            if response.status_code == 429:
                _warn_sample("batch_rate_limited", f"TwelveData batch daily bars rate-limited for {len(symbols)} symbols")
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
        for sym, payload in data.items():
            values = payload.get("values") if isinstance(payload, dict) else None
            if not values:
                continue
            bars: List[Dict[str, float]] = []
            for row in reversed(values):
                try:
                    bars.append(
                        {
                            "open": float(row["open"]),
                            "high": float(row["high"]),
                            "low": float(row["low"]),
                            "close": float(row["close"]),
                            "volume": float(row.get("volume", 0.0)),
                            "timestamp": datetime.fromisoformat(row["datetime"]).timestamp(),
                        }
                    )
                except Exception:
                    continue
            if bars:
                sym_u = sym.upper()
                results[sym_u] = bars
                self.cache.set(f"td:1day:{sym_u}", bars, self.ttl)
        return results
