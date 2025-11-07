from csv import DictReader
from io import StringIO
from typing import Any, Dict, List

import httpx

from utils.http_client import get_http_client
from utils.logger import get_logger
from utils.settings import get_settings

FINVIZ_BASE_URL = "https://finviz.com/api/quote.ashx"
FINVIZ_EXPORT_URL = "https://finviz.com/export.ashx"
MICROCAP_FALLBACK = ["IWM", "IWC", "URTY", "SMLF", "VTWO"]

logger = get_logger(__name__)


async def fetch_finviz_snapshot(symbol: str) -> Dict[str, Any]:
    """
    Fetch quote/snapshot data from Finviz for a given symbol.
    """
    settings = get_settings()
    payload: Dict[str, Any] = {"provider": "finviz", "symbol": symbol.upper()}

    if not settings.finviz_api_key:
        payload["warning"] = "Finviz token not configured"
        return payload

    params = {"ticker": symbol.upper(), "token": settings.finviz_api_key}
    headers = {"User-Agent": "microcap-scout-bot/0.1.0"}

    async with get_http_client() as client:
        try:
            response = await client.get(FINVIZ_BASE_URL, params=params, headers=headers)
            response.raise_for_status()
        except httpx.HTTPError as exc:
            logger.warning("Finviz request failed: %s", exc)
            payload["error"] = str(exc)
            return payload

    try:
        payload["data"] = response.json()
    except ValueError:
        payload["data"] = {"raw": response.text}

    return payload


async def fetch_microcap_screen(limit: int = 25) -> List[str]:
    """
    Pull a lightweight Finviz microcap screener export. Falls back to a static list on error.
    """
    settings = get_settings()
    if not settings.finviz_api_key:
        logger.info("FINVIZ token missing; falling back to static microcap list")
        return MICROCAP_FALLBACK[:limit]

    params = {
        "v": "152",  # technical view for exports
        "f": "cap_mico,sh_relvol_o1,sh_price_o1",
        "o": "-volume",
        "c": "0",
        "t": settings.finviz_api_key,
    }

    async with get_http_client() as client:
        try:
            response = await client.get(FINVIZ_EXPORT_URL, params=params)
            response.raise_for_status()
            content = response.text
        except httpx.HTTPError as exc:
            logger.warning("Finviz screener request failed: %s", exc)
            return MICROCAP_FALLBACK[:limit]

    tickers: List[str] = []
    reader = DictReader(StringIO(content))
    for row in reader:
        ticker = row.get("Ticker") or row.get("Symbol")
        if ticker:
            tickers.append(ticker.strip().upper())
        if len(tickers) >= limit:
            break

    if not tickers:
        logger.warning("Finviz screener returned no tickers, using fallback")
        return MICROCAP_FALLBACK[:limit]

    return tickers
