import asyncio
import logging
import os
from typing import Any, Dict, Optional

import requests
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("massive_client")
logger.setLevel(logging.INFO)

MASSIVE_QUOTES_URL = "https://api.massive.com/v1/quotes/{symbol}"
MASSIVE_HEALTHCHECK_URL = "https://api.massive.com/v1/reference/markets"
STOCKDATA_URL = "https://api.stockdata.org/v1/data/quote"

_MASSIVE_API_KEY: Optional[str] = None
_MASSIVE_READY: bool = False
_STOCKDATA_WARNING_EMITTED = False


def load_massive_key() -> Optional[str]:
    """Load Massive API key from env (supports MASSIVE_API_KEY and POLYGON_API_KEY)."""

    key = os.getenv("MASSIVE_API_KEY") or os.getenv("POLYGON_API_KEY")
    logger.info("MASSIVE_API_KEY detected: %s", bool(key))

    if not key:
        logger.error("❌ MASSIVE_API_KEY missing! Verify Railway environment variables.")
        return None

    logger.info("✅ MASSIVE_API_KEY loaded successfully.")
    return key


def test_massive_connection(key: str) -> None:
    """Test live connectivity to Massive and raise on auth failure."""

    logger.info("🌐 Testing Massive API connectivity: %s", MASSIVE_HEALTHCHECK_URL)
    headers = {"Authorization": f"Bearer {key}"}

    try:
        response = requests.get(MASSIVE_HEALTHCHECK_URL, headers=headers, timeout=10)
    except requests.exceptions.RequestException as exc:
        logger.error("🔥 Connection error while testing Massive API: %s", exc)
        raise RuntimeError("Massive API connection test failed") from exc

    if response.status_code == 200:
        logger.info("✅ Massive API reachable and key is valid.")
        return
    if response.status_code == 401:
        logger.error("🚫 Unauthorized — Invalid or expired MASSIVE_API_KEY.")
        raise RuntimeError("Massive API authentication failed (401 Unauthorized).")

    logger.warning("⚠️ Unexpected Massive API response: %s %s", response.status_code, response.reason)
    logger.warning("Response body: %s", response.text[:200])


def _ensure_massive_ready() -> Optional[str]:
    """Ensure the Massive API key is loaded and validated once."""

    global _MASSIVE_API_KEY, _MASSIVE_READY
    if _MASSIVE_API_KEY is None:
        _MASSIVE_API_KEY = load_massive_key()
    if not _MASSIVE_API_KEY:
        return None
    if not _MASSIVE_READY:
        test_massive_connection(_MASSIVE_API_KEY)
        _MASSIVE_READY = True
    return _MASSIVE_API_KEY


# Run validation at import time when a key exists, but allow startup without one.
if load_massive_key():
_INITIAL_KEY = load_massive_key()
if _INITIAL_KEY:
    _MASSIVE_API_KEY = _INITIAL_KEY
    _ensure_massive_ready()


def get_massive_data(symbol: str) -> Optional[Dict[str, Any]]:
    """Fetch quote data for ``symbol`` from Massive.com v1 quotes endpoint."""

    api_key = _ensure_massive_ready()
    if not api_key:
        logger.warning("[WARN] massive_client - Missing MASSIVE_API_KEY; cannot fetch %s", symbol.upper())
        return None

    url = MASSIVE_QUOTES_URL.format(symbol=symbol.upper())
    headers = {"Authorization": f"Bearer {api_key}"}

    try:
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        logger.info(f"[INFO] massive_client - Retrieved data for {symbol.upper()}")
        return data
    except requests.exceptions.HTTPError as exc:
        status = exc.response.status_code if exc.response else "unknown"
        logger.error(
            "[ERROR] massive_client - Massive API %s for %s: %s",
            status,
            symbol.upper(),
            exc.response.text if exc.response else exc,
        )
        return None
    except requests.exceptions.RequestException as exc:
        logger.error("[ERROR] massive_client - Massive request failed for %s: %s", symbol.upper(), exc)
        return None


async def get_quote(symbol: str) -> Optional[float]:
    """Return the latest price from Massive with StockData fallback when possible."""

    if not symbol:
        return None

    data = await asyncio.to_thread(get_massive_data, symbol)
    price = _extract_price(data)
    if price is not None:
        logger.info(f"[INFO] massive_client - Price for {symbol.upper()}: {price}")
        return price

    fallback_price = await asyncio.to_thread(_fetch_stockdata_price, symbol)
    if fallback_price is not None:
        logger.info(f"[INFO] massive_client - StockData fallback price for {symbol.upper()}: {fallback_price}")
    return fallback_price


def _extract_price(payload: Optional[Dict[str, Any]]) -> Optional[float]:
    if not isinstance(payload, dict):
        return None

    candidates = []
    if isinstance(payload.get("result"), dict):
        candidates.append(payload["result"])
    if isinstance(payload.get("results"), list) and payload["results"]:
        candidates.append(payload["results"][0])
    if isinstance(payload.get("data"), dict):
        candidates.append(payload["data"])
    candidates.append(payload)

    for record in candidates:
        if not isinstance(record, dict):
            continue
        for key in ("price", "lastTradePrice", "lastPrice", "close", "c", "p"):
            value = record.get(key)
            if value is None:
                continue
            try:
                return float(value)
            except (TypeError, ValueError):
                continue
        last_trade = record.get("lastTrade")
        if isinstance(last_trade, dict):
            for key in ("price", "p", "lastPrice"):
                value = last_trade.get(key)
                if value is None:
                    continue
                try:
                    return float(value)
                except (TypeError, ValueError):
                    continue
    return None


def _fetch_stockdata_price(symbol: str) -> Optional[float]:
    global _STOCKDATA_WARNING_EMITTED
    api_key = os.getenv("STOCKDATA_API_KEY")
    if not api_key:
        if not _STOCKDATA_WARNING_EMITTED:
            logger.warning("[WARN] StockData fallback disabled (STOCKDATA_API_KEY missing)")
            _STOCKDATA_WARNING_EMITTED = True
        return None

    params = {"symbols": symbol.upper(), "api_token": api_key}

    try:
        resp = requests.get(STOCKDATA_URL, params=params, timeout=10)
        resp.raise_for_status()
        payload = resp.json()
    except requests.exceptions.HTTPError as exc:
        status = exc.response.status_code if exc.response else "unknown"
        if status == 402:
            logger.warning("[WARN] massive_client - StockData quota/plan limit hit for %s", symbol.upper())
        else:
            logger.error(
                "[ERROR] massive_client - StockData HTTP %s for %s: %s",
                status,
                symbol.upper(),
                exc.response.text if exc.response else exc,
            )
        return None
    except requests.exceptions.RequestException as exc:
        logger.error("[ERROR] massive_client - StockData fallback failed for %s: %s", symbol.upper(), exc)
        return None

    records = payload.get("data") or []
    if not records:
        return None

    record = records[0]
    for field in ("price", "last", "close", "previous_close_price", "prev_close"):
        value = record.get(field)
        if value is None:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return None
