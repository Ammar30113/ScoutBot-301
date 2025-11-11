"""Finnhub async client helpers."""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any, Dict, List, Optional

import httpx

from utils.http_client import get_http_client
from utils.logger import get_logger
from utils.settings import get_settings

logger = get_logger(__name__)

BASE_URL = "https://finnhub.io/api/v1"


def _require_api_key() -> str:
    api_key = get_settings().finnhub_api_key
    if not api_key:
        raise RuntimeError("FINNHUB_API_KEY not configured")
    return api_key


async def get_quote(symbol: str) -> Optional[float]:
    """Fetch the latest Finnhub quote."""

    api_key = _require_api_key()
    url = f"{BASE_URL}/quote"
    params = {"symbol": symbol.upper()}
    headers = {"X-Finnhub-Token": api_key}

    async with get_http_client() as client:
        try:
            response = await client.get(url, headers=headers, params=params)
            response.raise_for_status()
        except httpx.HTTPError as exc:  # pragma: no cover - network guard
            logger.warning("Finnhub quote lookup failed for %s: %s", symbol, exc)
            raise

    payload = response.json()
    return payload.get("c")


async def get_company_news(symbol: str, days_back: int = 3) -> List[Dict[str, Any]]:
    """Return the most recent company news items for ``symbol`` (max 5)."""

    api_key = _require_api_key()
    today = date.today()
    start = today - timedelta(days=days_back)
    params = {
        "symbol": symbol.upper(),
        "from": start.isoformat(),
        "to": today.isoformat(),
    }
    headers = {"X-Finnhub-Token": api_key}
    url = f"{BASE_URL}/company-news"

    async with get_http_client() as client:
        try:
            response = await client.get(url, headers=headers, params=params)
            response.raise_for_status()
        except httpx.HTTPError as exc:  # pragma: no cover - network guard
            logger.warning("Finnhub news lookup failed for %s: %s", symbol, exc)
            raise

    payload = response.json()
    return list(payload[:5]) if isinstance(payload, list) else []


async def get_sentiment(symbol: str) -> Dict[str, Any]:
    """Return Finnhub's aggregated news sentiment payload."""

    api_key = _require_api_key()
    headers = {"X-Finnhub-Token": api_key}
    params = {"symbol": symbol.upper()}
    url = f"{BASE_URL}/news-sentiment"

    async with get_http_client() as client:
        try:
            response = await client.get(url, headers=headers, params=params)
            response.raise_for_status()
        except httpx.HTTPError as exc:  # pragma: no cover - network guard
            logger.warning("Finnhub sentiment lookup failed for %s: %s", symbol, exc)
            raise

    payload: Dict[str, Any] = response.json()
    return payload.get("sentiment", {}) or {}
