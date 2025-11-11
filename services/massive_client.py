import asyncio
import logging
import os
from typing import Any, Dict, Optional

import requests
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

MASSIVE_API_KEY = os.getenv("MASSIVE_API_KEY")
if not MASSIVE_API_KEY:
    MASSIVE_API_KEY = os.environ.get("MASSIVE_API_KEY")
logger.info("MASSIVE_API_KEY detected: %s", bool(MASSIVE_API_KEY))

_ENV_STATUS: Dict[str, str] = {}
MASSIVE_QUOTES_URL = "https://api.massive.com/v1/quotes/{symbol}"
STOCKDATA_URL = "https://api.stockdata.org/v1/data/quote"


def _log_env_status(name: str, value: Optional[str]) -> None:
    previous = _ENV_STATUS.get(name)
    if value and previous != "loaded":
        msg = "[INFO] massive_client - MASSIVE_API_KEY loaded successfully" if name == "MASSIVE_API_KEY" else f"[INFO] massive_client - {name} detected"
        logger.info(msg)
        _ENV_STATUS[name] = "loaded"
    elif not value and previous != "missing":
        if name == "MASSIVE_API_KEY":
            logger.warning("[WARN] MASSIVE_API_KEY missing; skipping Massive request")
        elif name == "STOCKDATA_API_KEY":
            logger.warning("[WARN] StockData fallback disabled (STOCKDATA_API_KEY missing)")
        else:
            logger.warning("[WARN] %s missing", name)
        _ENV_STATUS[name] = "missing"


def _get_env_key(name: str) -> Optional[str]:
    value = os.getenv(name)
    _log_env_status(name, value)
    return value


_get_env_key("MASSIVE_API_KEY")


def get_massive_data(symbol: str) -> Optional[Dict[str, Any]]:
    """Fetch quote data for ``symbol`` from Massive.com v1 quotes endpoint."""

    api_key = _get_env_key("MASSIVE_API_KEY")
    if not api_key:
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
    api_key = _get_env_key("STOCKDATA_API_KEY")
    if not api_key:
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
