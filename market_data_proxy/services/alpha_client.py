from __future__ import annotations

import os
from typing import Dict, Optional

import requests

ALPHA_URL = "https://www.alphavantage.co/query"


class AlphaClient:
    def __init__(self) -> None:
        # TODO: Provide ALPHAVANTAGE_API_KEY via environment variables
        self.api_key = os.getenv("ALPHAVANTAGE_API_KEY") or os.getenv("ALPHA_VANTAGE_KEY")

    def get_latest_price(self, symbol: str) -> Optional[Dict[str, float]]:
        if not self.api_key:
            return None

        params = {
            "function": "TIME_SERIES_DAILY_ADJUSTED",
            "symbol": symbol,
            "apikey": self.api_key,
        }

        try:
            response = requests.get(ALPHA_URL, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
        except Exception:
            return None

        series = data.get("Time Series (Daily)")
        if not isinstance(series, dict):
            return None

        latest_date = sorted(series.keys())[-1]
        latest = series[latest_date]
        price = latest.get("4. close")
        if price is None:
            return None

        return {"symbol": symbol.upper(), "price": float(price), "source": "alpha"}
