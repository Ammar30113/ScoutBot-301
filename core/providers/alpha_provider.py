from __future__ import annotations

from typing import Any, Dict, List, Optional

import requests

from utils.logger import get_logger

logger = get_logger("providers.alpha_vantage")

ALPHA_URL = "https://www.alphavantage.co/query"


def fetch_ohlc(symbol: str, days: int, api_key: Optional[str]) -> List[Dict[str, Any]]:
    if not api_key:
        logger.info("AlphaVantage API key missing; skipping provider for %s", symbol)
        return []

    params = {
        "function": "TIME_SERIES_DAILY_ADJUSTED",
        "symbol": symbol,
        "apikey": api_key,
        "outputsize": "full",
    }

    try:
        response = requests.get(ALPHA_URL, params=params, timeout=10)
        response.raise_for_status()
        payload = response.json()
    except Exception as exc:  # pragma: no cover - network guard
        logger.warning("AlphaVantage request failed for %s: %s", symbol, exc)
        return []

    series = payload.get("Time Series (Daily)")
    if not isinstance(series, dict):
        logger.info("AlphaVantage payload missing time series for %s", symbol)
        return []

    rows: List[Dict[str, Any]] = []
    for date_str, values in series.items():
        try:
            rows.append(
                {
                    "timestamp": date_str,
                    "open": float(values.get("1. open")),
                    "high": float(values.get("2. high")),
                    "low": float(values.get("3. low")),
                    "close": float(values.get("4. close")),
                    "volume": int(float(values.get("6. volume") or 0)),
                }
            )
        except (TypeError, ValueError) as exc:
            logger.debug("Skipping AlphaVantage row for %s due to %s", symbol, exc)

    rows.sort(key=lambda row: row["timestamp"])
    if days > 0:
        rows = rows[-days:]
    return rows
