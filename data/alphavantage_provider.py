from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Optional

import requests

from core.config import get_settings
from core.logger import get_logger
from core.cache import get_cache

logger = get_logger(__name__)


class AlphaVantageProvider:
    BASE_URL = "https://www.alphavantage.co/query"

    def __init__(self) -> None:
        settings = get_settings()
        self.api_key = settings.alphavantage_api_key
        self.cache = get_cache()
        self.ttl = settings.cache_ttl
        if not self.api_key:
            logger.warning("AlphaVantageProvider initialized without API key")

    def get_price(self, symbol: str) -> Optional[float]:
        cache_key = f"av:price:{symbol.upper()}"
        cached = self.cache.get(cache_key)
        if not self.api_key:
            return cached
        params = {"function": "GLOBAL_QUOTE", "symbol": symbol.upper(), "apikey": self.api_key}
        try:
            response = requests.get(self.BASE_URL, params=params, timeout=10)
            if response.status_code == 429:
                logger.warning("AlphaVantage price rate-limited for %s", symbol)
                return cached
            response.raise_for_status()
            payload = response.json().get("Global Quote", {}) or {}
            price = payload.get("05. price")
            if price is None:
                if cached is not None:
                    return cached
                return None
            value = float(price)
            self.cache.set(cache_key, value, self.ttl)
            return value
        except Exception as exc:  # pragma: no cover - network guard
            logger.warning("AlphaVantage price fetch failed for %s: %s", symbol, exc)
            return cached

    def get_aggregates(self, symbol: str, timespan: str = "1day", limit: int = 60) -> List[Dict[str, float]]:
        cache_key = f"av:daily:{symbol.upper()}"
        cached = self.cache.get(cache_key) or []
        if not self.api_key:
            return cached
        params = {"function": "TIME_SERIES_DAILY_ADJUSTED", "symbol": symbol.upper(), "apikey": self.api_key}
        try:
            response = requests.get(self.BASE_URL, params=params, timeout=10)
            if response.status_code == 429:
                logger.warning("AlphaVantage aggregates rate-limited for %s", symbol)
                return cached
            response.raise_for_status()
            data = response.json().get("Time Series (Daily)", {}) or {}
            if not data:
                logger.warning("AlphaVantage aggregates empty for %s", symbol)
                return cached
        except Exception as exc:  # pragma: no cover - network guard
            logger.warning("AlphaVantage aggregates failed for %s: %s", symbol, exc)
            return cached
        normalized: List[Dict[str, float]] = []
        for date_str, values in list(data.items())[:limit]:
            normalized.append(
                {
                    "open": float(values["1. open"]),
                    "high": float(values["2. high"]),
                    "low": float(values["3. low"]),
                    "close": float(values["4. close"]),
                    "volume": float(values["6. volume"]),
                    "timestamp": datetime.fromisoformat(date_str).timestamp(),
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
        if not self.api_key:
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
                logger.warning("AlphaVantage intraday rate-limited for %s", symbol)
                return cached
            response.raise_for_status()
            data = response.json().get("Time Series (5min)", {}) or {}
            if not data:
                logger.warning("AlphaVantage intraday aggregates empty for %s", symbol)
                return cached
        except Exception as exc:  # pragma: no cover - network guard
            logger.warning("AlphaVantage intraday aggregates failed for %s: %s", symbol, exc)
            return cached

        normalized: List[Dict[str, float]] = []
        for date_str, values in list(data.items())[:limit]:
            normalized.append(
                {
                    "open": float(values["1. open"]),
                    "high": float(values["2. high"]),
                    "low": float(values["3. low"]),
                    "close": float(values["4. close"]),
                    "volume": float(values.get("5. volume", 0.0)),
                    "timestamp": datetime.fromisoformat(date_str).timestamp(),
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
        if not self.api_key:
            return cached if cached is not None else 0.0
        params = {"function": "OVERVIEW", "symbol": symbol.upper(), "apikey": self.api_key}
        try:
            response = requests.get(self.BASE_URL, params=params, timeout=10)
            if response.status_code == 429:
                logger.warning("AlphaVantage fundamentals rate-limited for %s", symbol)
                return cached if cached is not None else 0.0
            response.raise_for_status()
            data = response.json() or {}
            raw_cap = data.get("MarketCapitalization")
            if raw_cap is None:
                if cached is not None:
                    return cached
                return 0.0
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
        joined = ",".join(sorted(set(sym.upper() for sym in symbols)))
        cache_key = f"av:batch_quotes:{joined}"
        cached = self.cache.get(cache_key) or {}
        params = {"function": "BATCH_STOCK_QUOTES", "symbols": joined, "apikey": self.api_key}
        try:
            response = requests.get(self.BASE_URL, params=params, timeout=10)
            if response.status_code == 429:
                logger.warning("AlphaVantage batch quotes rate-limited for %s symbols", len(symbols))
                return cached
            response.raise_for_status()
            payload = response.json().get("Stock Quotes", []) or []
            quotes: Dict[str, float] = {}
            for item in payload:
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
