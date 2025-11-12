"""Massive.com REST integration using plain HTTP requests.

This module avoids the official SDK to keep dependencies compatible with
alpaca-trade-api (which pins websockets<11). The behaviour mirrors the
previous Massive wrapper while staying lightweight and deployment-safe.
"""

from __future__ import annotations

import logging
import os
import time
from typing import Any, Dict, Optional

import requests

logger = logging.getLogger("massive_client")
logger.setLevel(logging.INFO)

BASE_URL = "https://api.massive.com/v1"
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 5


def _get_api_key() -> str:
    key = os.getenv("MASSIVE_API_KEY") or os.getenv("POLYGON_API_KEY")
    logger.info("MASSIVE_API_KEY detected: %s", bool(key))
    if not key:
        logger.error("[❌] MASSIVE_API_KEY missing from environment variables")
        raise SystemExit(1)
    return key


API_KEY = _get_api_key()


def _request(path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    url = f"{BASE_URL}{path}"
    headers = {"Authorization": f"Bearer {API_KEY}"}

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, headers=headers, params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as exc:
            status = exc.response.status_code if exc.response else "unknown"
            if status == 401:
                logger.error("[❌] Massive API rejected credentials: %s", exc)
                raise SystemExit(1)
            logger.warning("[Retry %s/%s] HTTP %s for %s", attempt, MAX_RETRIES, status, url)
        except requests.exceptions.RequestException as exc:
            logger.warning("[Retry %s/%s] Connection error: %s", attempt, MAX_RETRIES, exc)

        sleep_for = RETRY_DELAY_SECONDS * attempt
        logger.info("Backing off for %ss before retry", sleep_for)
        time.sleep(sleep_for)

    logger.error("[🚫] Exhausted retries for %s", url)
    raise RuntimeError("Massive API unreachable")


def _verify_connectivity() -> None:
    try:
        _request("/reference/markets", params={"limit": 1})
    except Exception as exc:
        logger.error("[❌] Massive API connectivity test failed: %s", exc)
        raise SystemExit(1)
    logger.info("[✅] Massive API verified and operational")


_verify_connectivity()


def get_latest_quote(symbol: str) -> Optional[Dict[str, Any]]:
    symbol = symbol.upper()
    path = f"/reference/tickers/{symbol}/aggregates"
    params = {"timespan": "day", "limit": 1}

    try:
        payload = _request(path, params=params)
    except SystemExit:
        raise
    except Exception as exc:
        logger.error("[❌] Unexpected Massive error for %s: %s", symbol, exc)
        return None

    results = payload.get("results") or []
    if not results:
        logger.warning("[⚠️] Massive returned no aggregates for %s", symbol)
        return None

    latest = results[-1]
    price = latest.get("close") or latest.get("price") or latest.get("p")
    timestamp = latest.get("timestamp") or latest.get("t")
    data = {"symbol": symbol, "price": price, "timestamp": timestamp}
    logger.info("[📈] %s Massive snapshot: %s", symbol, data)
    return data
