from __future__ import annotations

from typing import Sequence, Set

import requests

from core.config import get_settings
from core.logger import get_logger

logger = get_logger(__name__)
settings = get_settings()

ALPACA_ETF_ENDPOINT = "reference/etfs/{symbol}/holdings"


def fetch_etf_holdings(etfs: Sequence[str]) -> Set[str]:
    """Try to fetch ETF holdings from Alpaca data API; return empty set on 404/unauthorized."""

    holdings: Set[str] = set()
    headers = {
        "APCA-API-KEY-ID": settings.alpaca_api_key,
        "APCA-API-SECRET-KEY": settings.alpaca_api_secret,
    }
    for etf in etfs:
        url = f"{settings.alpaca_data_url.rstrip('/')}/{ALPACA_ETF_ENDPOINT.format(symbol=etf.upper())}"
        try:
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code == 404:
                logger.info("Alpaca ETF holdings not available for %s", etf)
                continue
            response.raise_for_status()
        except requests.RequestException as exc:  # pragma: no cover - network guard
            logger.warning("Failed to fetch holdings for %s: %s", etf, exc)
            continue

        data = response.json().get("holdings") or response.json().get("results") or []
        for item in data:
            symbol = item.get("symbol") or item.get("ticker")
            if symbol:
                holdings.add(str(symbol).upper())
    return holdings
