"""Async Massive API client helpers."""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import requests

from .stockdata import fetch_stockdata_quote

API_KEY = os.getenv("MASSIVE_API_KEY")
BASE_URL = "https://api.massive.com/v3"


def get_massive_data(endpoint: str, params: Optional[Dict[str, Any]] = None) -> Optional[List[Dict[str, Any]]]:
    if not API_KEY:
        logging.warning("[WARN] massive_client - MASSIVE_API_KEY missing; skipping Massive request")
        return None
    headers = {"Authorization": f"Bearer {API_KEY}"}
    try:
        response = requests.get(f"{BASE_URL}{endpoint}", headers=headers, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        logging.info("[INFO] massive_client - Connected successfully using API key (Massive.com)")
        return data.get("results", [])
    except requests.exceptions.RequestException as exc:
        logging.error(f"[ERROR] massive_client - Massive API request failed: {exc}")
        return None


async def get_quote(symbol: str) -> Optional[float]:
    """Return the latest quote for ``symbol`` with StockData.org fallback."""

    endpoint = f"/reference/tickers/{symbol.upper()}"
    data = await asyncio.to_thread(get_massive_data, endpoint, None)
    price = _extract_price(data)
    logging.info(f"[INFO] massive_client - Finished fetch for {symbol}, {len(data) if data else 0} results.")

    if price is not None:
        return price

    return await _fallback_stockdata_price(symbol)


async def get_aggregate(symbol: str, timespan: str = "minute", limit: int = 1) -> Optional[Dict[str, Any]]:
    """Fetch aggregate data for ``symbol``; falls back to None when unavailable."""

    now = datetime.utcnow()
    start = (now - timedelta(days=5)).strftime("%Y-%m-%d")
    end = now.strftime("%Y-%m-%d")
    endpoint = f"/aggs/ticker/{symbol.upper()}/range/1/{timespan}/{start}/{end}"
    params = {"limit": limit}

    data = await asyncio.to_thread(get_massive_data, endpoint, params)
    if not data:
        return None
    return data[-1]


def _extract_price(results: Optional[List[Dict[str, Any]]]) -> Optional[float]:
    if not results:
        return None

    candidate = results[0]
    value = None
    for key in ("lastTradePrice", "price", "close", "c", "p", "lastPrice"):
        candidate_value = candidate.get(key)
        if candidate_value is not None:
            value = candidate_value
            break

    if value is None:
        last_trade = candidate.get("lastTrade") or {}
        value = last_trade.get("p") or last_trade.get("price")

    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


async def _fallback_stockdata_price(symbol: str) -> Optional[float]:
    try:
        payload = await fetch_stockdata_quote(symbol)
    except Exception as exc:  # pragma: no cover - network guard
        logging.error("[ERROR] massive_client - StockData fallback failed for %s: %s", symbol, exc)
        return None

    data = payload.get("data") or []
    if not data:
        return None

    record = data[0]
    for field_name in ("price", "last", "close", "previous_close_price", "prev_close"):
        value = record.get(field_name)
        if value is None:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return None
