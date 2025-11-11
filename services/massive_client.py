"""Async Massive API client helpers."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, Optional

import httpx

from utils.http_client import get_http_client
from utils.logger import get_logger
from utils.settings import get_settings

logger = get_logger(__name__)

BASE_URL = "https://api.massive.com/v2"


async def get_quote(symbol: str) -> Optional[float]:
    """Return the latest Massive last-trade price for ``symbol``."""

    settings = get_settings()
    api_key = settings.massive_api_key

    if not api_key:
        raise RuntimeError("MASSIVE_API_KEY not configured")

    headers = {"Authorization": f"Bearer {api_key}"}
    url = f"{BASE_URL}/snapshot/locale/us/markets/stocks/tickers/{symbol.upper()}"

    async with get_http_client() as client:
        try:
            response = await client.get(url, headers=headers)
            response.raise_for_status()
        except httpx.HTTPError as exc:  # pragma: no cover - network guard
            logger.warning("Massive quote lookup failed for %s: %s", symbol, exc)
            raise

    payload = response.json()
    ticker = payload.get("ticker") or {}
    trade = ticker.get("lastTrade") or {}
    return trade.get("p")


async def get_aggregate(symbol: str, timespan: str = "minute", limit: int = 1) -> Optional[Dict[str, Any]]:
    """Fetch the latest Massive aggregate candle for ``symbol``."""

    settings = get_settings()
    api_key = settings.massive_api_key

    if not api_key:
        raise RuntimeError("MASSIVE_API_KEY not configured")

    now = datetime.utcnow()
    start = (now - timedelta(days=5)).strftime("%Y-%m-%d")
    end = now.strftime("%Y-%m-%d")
    headers = {"Authorization": f"Bearer {api_key}"}
    url = f"{BASE_URL}/aggs/ticker/{symbol.upper()}/range/1/{timespan}/{start}/{end}"
    params = {"limit": limit}

    async with get_http_client() as client:
        try:
            response = await client.get(url, headers=headers, params=params)
            response.raise_for_status()
        except httpx.HTTPError as exc:  # pragma: no cover - network guard
            logger.warning("Massive aggregate lookup failed for %s: %s", symbol, exc)
            raise

    payload: Dict[str, Any] = response.json()
    results = payload.get("results") or []
    if not results:
        return None
    return results[-1]
