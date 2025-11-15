from __future__ import annotations

from datetime import datetime
from typing import Dict, List

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

    def get_price(self, symbol: str) -> float:
        params = {"function": "GLOBAL_QUOTE", "symbol": symbol.upper(), "apikey": self.api_key}
        response = requests.get(self.BASE_URL, params=params, timeout=10)
        response.raise_for_status()
        payload = response.json().get("Global Quote", {})
        price = payload.get("05. price")
        if price is None:
            raise RuntimeError("AlphaVantage quote missing price")
        return float(price)

    def get_aggregates(self, symbol: str, timespan: str = "1day", limit: int = 60) -> List[Dict[str, float]]:
        params = {"function": "TIME_SERIES_DAILY_ADJUSTED", "symbol": symbol.upper(), "apikey": self.api_key}
        response = requests.get(self.BASE_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json().get("Time Series (Daily)", {})
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
