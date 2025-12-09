from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Optional

import requests

from core.config import get_settings
from core.logger import get_logger

logger = get_logger(__name__)


class TwelveDataProvider:
    """Lightweight TwelveData wrapper for price + aggregates."""

    BASE_URL = "https://api.twelvedata.com"

    def __init__(self) -> None:
        settings = get_settings()
        self.api_key = settings.twelvedata_api_key
        if not self.api_key:
            logger.warning("TwelveDataProvider initialized without API key")

    def get_price(self, symbol: str) -> Optional[float]:
        if not self.api_key:
            return None
        params = {"symbol": symbol.upper(), "apikey": self.api_key, "interval": "1min", "outputsize": 1}
        try:
            response = requests.get(f"{self.BASE_URL}/time_series", params=params, timeout=10)
            response.raise_for_status()
            values = response.json().get("values", [])
            if not values:
                return None
            return float(values[0].get("close", 0.0))
        except Exception as exc:  # pragma: no cover - network guard
            logger.warning("TwelveData price fetch failed for %s: %s", symbol, exc)
            return None

    def get_aggregates(self, symbol: str, timespan: str = "1day", limit: int = 60) -> List[Dict[str, float]]:
        if not self.api_key:
            return []
        interval = self._normalize_timespan(timespan)
        params = {
            "symbol": symbol.upper(),
            "interval": interval,
            "apikey": self.api_key,
            "outputsize": limit,
        }
        try:
            response = requests.get(f"{self.BASE_URL}/time_series", params=params, timeout=10)
            response.raise_for_status()
            values = response.json().get("values", []) or []
        except Exception as exc:  # pragma: no cover - network guard
            logger.warning("TwelveData aggregates failed for %s: %s", symbol, exc)
            return []
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
        return normalized

    def get_intraday_1m(self, symbol: str, limit: int = 60) -> List[Dict[str, float]]:
        """Fetch raw 1-minute bars."""

        if not self.api_key:
            return []
        params = {
            "symbol": symbol.upper(),
            "interval": "1min",
            "apikey": self.api_key,
            "outputsize": limit,
        }
        try:
            response = requests.get(f"{self.BASE_URL}/time_series", params=params, timeout=10)
            response.raise_for_status()
            values = response.json().get("values", []) or []
        except Exception as exc:  # pragma: no cover - network guard
            logger.warning("TwelveData intraday aggregates failed for %s: %s", symbol, exc)
            return []
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
        return normalized

    def _normalize_timespan(self, timespan: str) -> str:
        mapping = {"1day": "1day", "1hour": "1h", "1min": "1min"}
        return mapping.get(timespan.lower(), "1day")

    def get_market_cap(self, symbol: str) -> Optional[float]:
        """Fetch market cap via TwelveData profile endpoint."""

        if not self.api_key:
            return None
        params = {"symbol": symbol.upper(), "apikey": self.api_key}
        try:
            response = requests.get(f"{self.BASE_URL}/profile", params=params, timeout=10)
            response.raise_for_status()
            data = response.json() or {}
            raw_cap = data.get("market_cap") or data.get("market_capitalization")
            if raw_cap is None:
                return None
            return float(raw_cap)
        except Exception as exc:  # pragma: no cover - network guard
            logger.warning("TwelveData market cap fetch failed for %s: %s", symbol, exc)
            return None
