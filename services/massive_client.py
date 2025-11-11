import asyncio
import logging
import os
from typing import Any, Dict, Optional

import requests
from dotenv import load_dotenv

load_dotenv()

MASSIVE_API_KEY = os.getenv("MASSIVE_API_KEY")
_warned_missing_key = False

if MASSIVE_API_KEY:
    logging.info("[INFO] massive_client - MASSIVE_API_KEY loaded successfully")


def get_massive_data(symbol: str) -> Optional[Dict[str, Any]]:
    """Fetch dividend or quote data for ``symbol`` from Massive.com."""

    global _warned_missing_key
    if not MASSIVE_API_KEY:
        if not _warned_missing_key:
            logging.warning("[WARN] MASSIVE_API_KEY missing; skipping Massive request")
            _warned_missing_key = True
        return None

    headers = {"Authorization": f"Bearer {MASSIVE_API_KEY}"}
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
    if not data:
        return None

    results = data.get("results") if isinstance(data, dict) else None
    if not results:
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
