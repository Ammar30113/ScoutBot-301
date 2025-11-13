from __future__ import annotations

import os
import time
from typing import Any, Dict, Optional

import requests

RETRY_ATTEMPTS = 3
RETRY_BACKOFF_SECONDS = 2
TIMEOUT_SECONDS = 5
BASE_URL = "https://api.massive.com/v1"


class MassiveClient:
    def __init__(self) -> None:
        # TODO: Provide MASSIVE_API_KEY via environment variables
        self.api_key = os.getenv("MASSIVE_API_KEY")

    def get_latest_price(self, symbol: str) -> Optional[Dict[str, Any]]:
        if not self.api_key:
            return None

        url = f"{BASE_URL}/reference/tickers/{symbol}/aggregates"
        params = {"timespan": "minute", "limit": 1}
        headers = {"Authorization": f"Bearer {self.api_key}"}

        for attempt in range(1, RETRY_ATTEMPTS + 1):
            try:
                response = requests.get(url, headers=headers, params=params, timeout=TIMEOUT_SECONDS)
                response.raise_for_status()
                data = response.json()
                results = data.get("results")
                if not results:
                    return None
                latest = results[-1]
                price = latest.get("close") or latest.get("price") or latest.get("c")
                if price is None:
                    return None
                return {"symbol": symbol.upper(), "price": float(price), "source": "massive"}
            except Exception:
                if attempt == RETRY_ATTEMPTS:
                    return None
                time.sleep(RETRY_BACKOFF_SECONDS * attempt)

        return None
