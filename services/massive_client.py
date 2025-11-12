"""Massive.com REST client wrapper."""

from __future__ import annotations

import logging
import os
import time
from typing import Any, Dict, Optional

from massive import RESTClient
from requests.exceptions import ConnectionError, HTTPError

logger = logging.getLogger("massive_client")
logger.setLevel(logging.INFO)

MAX_RETRIES = 3
RETRY_DELAY = 5


def init_massive_client() -> RESTClient:
    key = os.getenv("MASSIVE_API_KEY") or os.getenv("POLYGON_API_KEY")
    logger.info("MASSIVE_API_KEY detected: %s", bool(key))
    if not key:
        logger.error("[❌] MASSIVE_API_KEY missing or invalid.")
        raise SystemExit(1)

    try:
        client = RESTClient(key)
        list(client.list_aggs("AAPL", 1, "day", limit=1))
        logger.info("[✅] Massive API verified and operational.")
        return client
    except Exception as exc:
        logger.error("[❌] Failed to initialize Massive API: %s", exc)
        raise SystemExit(1)


client = init_massive_client()


def get_latest_quote(symbol: str) -> Optional[Dict[str, Any]]:
    symbol = symbol.upper()
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            aggs = list(client.list_aggs(symbol, 1, "day", limit=1))
            if not aggs:
                logger.warning("[⚠️] No Massive data returned for %s.", symbol)
                return None

            latest = aggs[-1]
            result = {
                "symbol": symbol,
                "price": getattr(latest, "close", None),
                "timestamp": getattr(latest, "timestamp", None),
            }
            logger.info("[📈] %s: $%s (as of %s)", symbol, result["price"], result["timestamp"])
            return result
        except (HTTPError, ConnectionError) as exc:
            logger.warning("[Retry %s/%s] API error for %s: %s", attempt, MAX_RETRIES, symbol, exc)
            time.sleep(RETRY_DELAY * attempt)
        except Exception as exc:
            logger.error("[❌] Unexpected error fetching %s: %s", symbol, exc)
            return None

    logger.error("[🚫] Exhausted Massive retries for %s.", symbol)
    return None
