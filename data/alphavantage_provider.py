from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Optional

import requests

from core.config import get_settings
from core.logger import get_logger

logger = get_logger(__name__)


class AlphaVantageProvider:
    BASE_URL = "https://www.alphavantage.co/query"

    def __init__(self) -> None:
        settings = get_settings()
        self.api_key = settings.alphavantage_api_key
        if not self.api_key:
            logger.warning("AlphaVantageProvider initialized without API key")

    def get_price(self, symbol: str) -> Optional[float]:
        if not self.api_key:
            return None
        params = {"function": "GLOBAL_QUOTE", "symbol": symbol.upper(), "apikey": self.api_key}
        try:
            response = requests.get(self.BASE_URL, params=params, timeout=10)
            response.raise_for_status()
            payload = response.json().get("Global Quote", {})
            price = payload.get("05. price")
            if price is None:
                return None
            return float(price)
        except Exception as exc:  # pragma: no cover - network guard
            logger.warning("AlphaVantage price fetch failed for %s: %s", symbol, exc)
            return None

    def get_aggregates(self, symbol: str, timespan: str = "1day", limit: int = 60) -> List[Dict[str, float]]:
        if not self.api_key:
            return []
        params = {"function": "TIME_SERIES_DAILY_ADJUSTED", "symbol": symbol.upper(), "apikey": self.api_key}
        try:
            response = requests.get(self.BASE_URL, params=params, timeout=10)
            response.raise_for_status()
            data = response.json().get("Time Series (Daily)", {}) or {}
        except Exception as exc:  # pragma: no cover - network guard
            logger.warning("AlphaVantage aggregates failed for %s: %s", symbol, exc)
            return []
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
        return normalized

    def get_intraday_5m(self, symbol: str, limit: int = 60) -> List[Dict[str, float]]:
        """Fetch 5-minute intraday bars."""

        if not self.api_key:
            return []
        params = {
            "function": "TIME_SERIES_INTRADAY",
            "symbol": symbol.upper(),
            "interval": "5min",
            "apikey": self.api_key,
            "outputsize": "compact",
        }
        try:
            response = requests.get(self.BASE_URL, params=params, timeout=10)
            response.raise_for_status()
            data = response.json().get("Time Series (5min)", {}) or {}
        except Exception as exc:  # pragma: no cover - network guard
            logger.warning("AlphaVantage intraday aggregates failed for %s: %s", symbol, exc)
            return []

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
        return normalized

    def get_market_cap(self, symbol: str) -> Optional[float]:
        """Fetch market cap via AlphaVantage OVERVIEW endpoint."""

        if not self.api_key:
            return None
        params = {"function": "OVERVIEW", "symbol": symbol.upper(), "apikey": self.api_key}
        try:
            response = requests.get(self.BASE_URL, params=params, timeout=10)
            response.raise_for_status()
            data = response.json() or {}
            raw_cap = data.get("MarketCapitalization")
            if raw_cap is None:
                return None
            return float(raw_cap)
        except Exception as exc:  # pragma: no cover - network guard
            logger.warning("AlphaVantage market cap fetch failed for %s: %s", symbol, exc)
            return None
