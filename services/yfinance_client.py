import asyncio
import threading
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import yfinance as yf

from utils.logger import get_logger

logger = get_logger(__name__)

CACHE_TTL = timedelta(minutes=5)
THROTTLE_WINDOW_SECONDS = 30
THROTTLE_LIMIT = 10
THROTTLE_SLEEP_SECONDS = 10
MAX_FAILURES = 3

CacheEntry = Tuple[datetime, Dict[str, Any]]

CACHE: Dict[str, CacheEntry] = {}
LAST_REQUESTS: List[datetime] = []
FAIL_COUNTS: Dict[str, int] = {}
STATE_LOCK = threading.Lock()


async def fetch_yfinance_snapshot(symbol: str) -> Dict[str, Any]:
    """
    Fetch a lightweight snapshot using yfinance in a thread executor with
    caching, throttling, and retry tracking to minimize Yahoo rate limits.
    """
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, _sync_fetch, symbol.upper())


def _sync_fetch(symbol: str) -> Dict[str, Any]:
    now = datetime.utcnow()
    payload: Dict[str, Any] = {"provider": "yfinance", "symbol": symbol}

    cached = _get_cached(symbol, now)
    if cached is not None:
        payload["data"] = cached
        return payload

    if _has_exceeded_failures(symbol):
        payload["warning"] = "Skipping yfinance lookup after repeated failures"
        return payload

    _apply_throttling(now)
    _register_request(now)

    try:
        ticker = yf.Ticker(symbol)
        fast_info = dict(ticker.fast_info or {})
        history_frame = ticker.history(period="1d")
        history_payload: Dict[str, Any] = {}
        if not history_frame.empty:
            latest_row = history_frame.tail(1).reset_index().to_dict("records")
            history_payload = latest_row[0] if latest_row else {}
            date_value = history_payload.get("Date")
            if hasattr(date_value, "isoformat"):
                history_payload["Date"] = date_value.isoformat()

        result_data = {"fast_info": fast_info, "history": history_payload}

        _cache_result(symbol, now, result_data)
        _reset_failures(symbol)
        payload["data"] = result_data
    except Exception as exc:  # pragma: no cover - defensive
        failure_count = _increment_failure(symbol)
        logger.warning(
            "yfinance fetch failed for %s (attempt %s/%s): %s",
            symbol,
            failure_count,
            MAX_FAILURES,
            exc,
        )
        payload["error"] = str(exc)
        if failure_count >= MAX_FAILURES:
            payload["warning"] = "Skipping yfinance lookup after repeated failures"

    return payload


def _get_cached(symbol: str, now: datetime) -> Dict[str, Any] | None:
    with STATE_LOCK:
        entry = CACHE.get(symbol)
        if not entry:
            return None
        cached_time, data = entry
        if now - cached_time < CACHE_TTL:
            cached_data = data
        else:
            CACHE.pop(symbol, None)
            cached_data = None

    if cached_data is not None:
        logger.info("Using cached yfinance data for %s", symbol)
    return cached_data


def _cache_result(symbol: str, now: datetime, data: Dict[str, Any]) -> None:
    with STATE_LOCK:
        CACHE[symbol] = (now, data)


def _apply_throttling(now: datetime) -> None:
    with STATE_LOCK:
        LAST_REQUESTS[:] = [t for t in LAST_REQUESTS if (now - t).total_seconds() < THROTTLE_WINDOW_SECONDS]
        request_count = len(LAST_REQUESTS)
    if request_count >= THROTTLE_LIMIT:
        logger.warning("yfinance rate limit approaching — sleeping %s seconds", THROTTLE_SLEEP_SECONDS)
        time.sleep(THROTTLE_SLEEP_SECONDS)
        with STATE_LOCK:
            LAST_REQUESTS.clear()


def _register_request(timestamp: datetime) -> None:
    with STATE_LOCK:
        LAST_REQUESTS.append(timestamp)


def _increment_failure(symbol: str) -> int:
    with STATE_LOCK:
        FAIL_COUNTS[symbol] = FAIL_COUNTS.get(symbol, 0) + 1
        return FAIL_COUNTS[symbol]


def _reset_failures(symbol: str) -> None:
    with STATE_LOCK:
        FAIL_COUNTS.pop(symbol, None)


def _has_exceeded_failures(symbol: str) -> bool:
    with STATE_LOCK:
        failures = FAIL_COUNTS.get(symbol, 0)
    if failures >= MAX_FAILURES:
        logger.warning("Skipping yfinance lookups for %s after %s failures", symbol, failures)
        return True
    return False


def get_latest_price(symbol: str) -> Optional[float]:
    """
    Lightweight helper for synchronous price lookups used by the trading engine.
    """
    snapshot = _sync_fetch(symbol.upper())
    data = snapshot.get("data") or {}
    fast_info = data.get("fast_info") or {}

    price_candidates = [
        fast_info.get("lastPrice"),
        fast_info.get("regularMarketPrice"),
        fast_info.get("previousClose"),
    ]
    for price in price_candidates:
        if price is not None:
            return float(price)

    history = data.get("history") or {}
    close_value = history.get("Close") or history.get("close")
    if close_value is not None:
        return float(close_value)
    return None
