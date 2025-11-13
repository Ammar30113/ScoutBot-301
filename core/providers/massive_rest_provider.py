from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests

from utils.logger import get_logger

logger = get_logger("providers.massive_rest")

BASE_URL = "https://api.massive.com/v1"


def fetch_ohlc(symbol: str, days: int, api_key: Optional[str]) -> List[Dict[str, Any]]:
    if not api_key:
        logger.info("MASSIVE_API_KEY missing; skipping Massive REST fallback for %s", symbol)
        return []

    path = f"/reference/tickers/{symbol}/aggregates"
    url = f"{BASE_URL}{path}"
    params = {"timespan": "day", "limit": max(days, 1)}
    headers = {"Authorization": f"Bearer {api_key}"}

    try:
        response = requests.get(url, headers=headers, params=params, timeout=10)
        response.raise_for_status()
        payload = response.json()
    except Exception as exc:  # pragma: no cover - network guard
        logger.warning("Massive REST request failed for %s: %s", symbol, exc)
        return []

    records = payload.get("results")
    if not isinstance(records, list):
        logger.info("Massive REST returned no aggregates for %s", symbol)
        return []

    rows: List[Dict[str, Any]] = []
    for row in records:
        try:
            rows.append(
                {
                    "timestamp": _normalize_timestamp(row),
                    "open": float(row.get("open") or row.get("o")),
                    "high": float(row.get("high") or row.get("h")),
                    "low": float(row.get("low") or row.get("l")),
                    "close": float(row.get("close") or row.get("c")),
                    "volume": int(row.get("volume") or row.get("v") or 0),
                }
            )
        except (TypeError, ValueError) as exc:
            logger.debug("Skipping Massive REST row for %s due to %s", symbol, exc)

    rows.sort(key=lambda entry: entry["timestamp"])
    if days > 0:
        rows = rows[-days:]
    return rows


def _normalize_timestamp(row: Dict[str, Any]) -> str:
    ts = row.get("timestamp") or row.get("t")
    if ts is None:
        return ""
    if isinstance(ts, (int, float)):
        seconds = ts / 1000 if ts > 1e12 else ts
        return datetime.fromtimestamp(seconds, tz=timezone.utc).date().isoformat()
    return str(ts)[:10]
