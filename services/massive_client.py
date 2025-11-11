import asyncio
import logging
import os
from typing import Optional

import requests
from dotenv import load_dotenv

load_dotenv()

MASSIVE_API_KEY = os.getenv("MASSIVE_API_KEY")
QUOTE_ENDPOINT = "https://api.massive.com/v1/quotes/{symbol}"


async def get_quote(symbol: str) -> Optional[float]:
    """Return the latest Massive.com quote for ``symbol`` or ``None`` on failure."""

    normalized = (symbol or "").strip().upper()
    if not normalized:
        logging.error("[ERROR] massive_client - Symbol required for Massive quote lookup")
        return None

    return await asyncio.to_thread(_fetch_quote, normalized)


def _fetch_quote(symbol: str) -> Optional[float]:
    if not MASSIVE_API_KEY:
        logging.warning("[WARN] MASSIVE_API_KEY missing; skipping Massive request")
        return None

    url = QUOTE_ENDPOINT.format(symbol=symbol)
    headers = {"Authorization": f"Bearer {MASSIVE_API_KEY}"}

    try:
        response = requests.get(url, headers=headers, timeout=10)
    except requests.exceptions.RequestException as exc:
        logging.error(f"[ERROR] massive_client - Request error for {symbol}: {exc}")
        return None

    if response.status_code >= 400:
        logging.error(
            "[ERROR] massive_client - Massive API returned %s for %s: %s",
            response.status_code,
            symbol,
            response.text,
        )
        return None

    try:
        payload = response.json()
    except ValueError:
        logging.error("[ERROR] massive_client - Unexpected JSON payload for %s", symbol)
        return None

    price = _extract_price(payload)
    if price is None:
        logging.error("[ERROR] massive_client - Missing price field in response for %s: %s", symbol, payload)
        return None

    logging.info("[INFO] massive_client - Connected successfully using MASSIVE_API_KEY")
    logging.info(f"[INFO] massive_client - Connected successfully, price for {symbol}: {price}")
    return price


def _extract_price(payload: object) -> Optional[float]:
    if not isinstance(payload, dict):
        return None

    candidates = [
        payload.get("price"),
        payload.get("lastTradePrice"),
        payload.get("lastPrice"),
        payload.get("close"),
        payload.get("c"),
        payload.get("p"),
    ]

    quote = payload.get("quote") if isinstance(payload.get("quote"), dict) else None
    if quote:
        candidates.append(quote.get("price"))
        candidates.append(quote.get("lastTradePrice"))

    for candidate in candidates:
        try:
            if candidate is not None:
                return float(candidate)
        except (TypeError, ValueError):
            continue

    return None
