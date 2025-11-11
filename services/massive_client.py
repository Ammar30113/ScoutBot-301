import asyncio
import logging
import os
from typing import Any, Dict, Optional

import requests
from dotenv import load_dotenv

load_dotenv()

_logged_loaded_key = False
_logged_missing_key = False


def _log_api_key_status(key: Optional[str]) -> None:
    global _logged_loaded_key, _logged_missing_key
    if key and not _logged_loaded_key:
        logging.info("[INFO] massive_client - MASSIVE_API_KEY loaded successfully")
        _logged_loaded_key = True
    elif not key and not _logged_missing_key:
        logging.warning("[WARN] MASSIVE_API_KEY missing; skipping Massive request")
        _logged_missing_key = True


def _get_env_key(name: str) -> Optional[str]:
    value = os.getenv(name)
    if name == "MASSIVE_API_KEY":
        _log_api_key_status(value)
    return value


_log_api_key_status(os.getenv("MASSIVE_API_KEY"))


def get_massive_data(symbol: str) -> Optional[Dict[str, Any]]:
    """Fetch dividend or quote data for ``symbol`` from Massive.com."""

    api_key = _get_env_key("MASSIVE_API_KEY")
    if not api_key:
        return None

    headers = {"Authorization": f"Bearer {api_key}"}
    url = f"https://api.massive.com/v3/reference/dividends?ticker={symbol.upper()}"

    try:
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        logging.info(f"[INFO] massive_client - Retrieved data for {symbol.upper()}")
        return data
    except requests.exceptions.RequestException as exc:
        logging.error(f"[ERROR] massive_client - {exc}")
        return None


async def get_quote(symbol: str) -> Optional[float]:
    """Compatibility helper that returns a numeric price when available."""

    if not symbol:
        return None

    data = await asyncio.to_thread(get_massive_data, symbol)
    price = _extract_price(data)
    if price is not None:
        return price

    fallback_price = await asyncio.to_thread(_fetch_stockdata_price, symbol)
    if fallback_price is not None:
        logging.info(f"[INFO] massive_client - StockData fallback price for {symbol.upper()}: {fallback_price}")
    return fallback_price


def _extract_price(data: Optional[Dict[str, Any]]) -> Optional[float]:
    if not isinstance(data, dict):
        return None
    results = data.get("results") or []
    if not isinstance(results, list) or not results:
        return None

    record = results[0]
    for field in ("price", "lastTradePrice", "lastPrice", "close", "amount"):
        value = record.get(field)
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

    url = "https://api.stockdata.org/v1/data/quote"
    params = {"symbols": symbol.upper(), "api_token": api_key}

    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        payload = resp.json()
    except requests.exceptions.RequestException as exc:
        logging.error(f"[ERROR] massive_client - StockData fallback failed for {symbol.upper()}: {exc}")
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
