"""Massive.com REST integration using plain HTTP requests.

This module avoids the official SDK to keep dependencies compatible with
alpaca-trade-api (which pins websockets<11). The behaviour mirrors the
previous Massive wrapper while staying lightweight and deployment-safe.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Any, Dict, Optional

import threading

import requests

logger = logging.getLogger("massive_client")
logger.setLevel(logging.INFO)

BASE_URL = "https://api.massive.com/v1"
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 5
FAILURE_BACKOFF_SECONDS = int(os.getenv("MASSIVE_FAILURE_BACKOFF_SECONDS", "120"))

_CIRCUIT_BREAK_UNTIL = 0.0
_CIRCUIT_LOCK = threading.Lock()
_last_error: Optional[str] = None


def _get_api_key() -> str:
    key = os.getenv("MASSIVE_API_KEY") or os.getenv("POLYGON_API_KEY")
    logger.info("MASSIVE_API_KEY detected: %s", bool(key))
    if not key:
        logger.error("[❌] MASSIVE_API_KEY missing from environment variables")
        raise SystemExit(1)
    return key


API_KEY = _get_api_key()


def _is_circuit_open() -> bool:
    if FAILURE_BACKOFF_SECONDS <= 0:
        return False
    return time.time() < _CIRCUIT_BREAK_UNTIL


def _trip_circuit(reason: str) -> None:
    global _CIRCUIT_BREAK_UNTIL, _last_error
    if FAILURE_BACKOFF_SECONDS <= 0:
        return
    with _CIRCUIT_LOCK:
        _CIRCUIT_BREAK_UNTIL = time.time() + FAILURE_BACKOFF_SECONDS
        _last_error = reason
    logger.warning(
        "Massive circuit breaker open for %ss (reason: %s)",
        FAILURE_BACKOFF_SECONDS,
        reason,
    )


def _clear_circuit_on_success() -> None:
    global _CIRCUIT_BREAK_UNTIL, _last_error
    if _CIRCUIT_BREAK_UNTIL == 0:
        return
    with _CIRCUIT_LOCK:
        _CIRCUIT_BREAK_UNTIL = 0.0
        _last_error = None
    logger.info("Massive circuit breaker reset after successful call")


def get_circuit_status() -> Dict[str, Any]:
    """Return current circuit breaker state for debugging/metrics."""

    return {
        "open": _is_circuit_open(),
        "cooldown_seconds": max(int(_CIRCUIT_BREAK_UNTIL - time.time()), 0) if _is_circuit_open() else 0,
        "last_error": _last_error,
        "backoff_window": FAILURE_BACKOFF_SECONDS,
    }


def _request(path: str, params: Optional[Dict[str, Any]] = None, *, force: bool = False) -> Dict[str, Any]:
    url = f"{BASE_URL}{path}"
    headers = {"Authorization": f"Bearer {API_KEY}"}

    if not force and _is_circuit_open():
        raise RuntimeError("Massive API temporarily unavailable (circuit breaker open)")

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, headers=headers, params=params, timeout=10)
            response.raise_for_status()
            _clear_circuit_on_success()
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

    reason = f"Exhausted retries for {url}"
    logger.error("[🚫] %s", reason)
    _trip_circuit(reason)
    raise RuntimeError("Massive API unreachable")


def verify_connectivity(raise_on_failure: bool = False) -> bool:
    """
    Manually verify Massive API connectivity.

    The check is opt-in so local development and CI environments without
    internet access do not fail at import time.
    """

    try:
        _request("/reference/tickers/AAPL/aggregates", params={"timespan": "day", "limit": 1}, force=True)
    except Exception as exc:
        logger.error("[❌] Massive API connectivity test failed: %s", exc)
        if raise_on_failure:
            raise RuntimeError("Massive API unreachable") from exc
        return False

    logger.info("[✅] Massive API verified and operational")
    return True


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


async def get_quote(symbol: str) -> Optional[float]:
    """
    Async-friendly helper that mirrors the previous Massive SDK signature.
    """

    snapshot = await asyncio.to_thread(get_latest_quote, symbol)
    if not snapshot:
        return None
    return snapshot.get("price")
