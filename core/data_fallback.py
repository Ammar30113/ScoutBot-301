from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import requests

from utils.logger import get_logger
from utils.settings import get_settings

from .providers.alpha_provider import fetch_ohlc as fetch_alpha_ohlc
from .providers.massive_rest_provider import fetch_ohlc as fetch_massive_rest_ohlc
from .providers.yahoo_provider import fetch_ohlc as fetch_yahoo_ohlc

logger = get_logger("core.data_fallback")
settings = get_settings()


@dataclass
class Candle:
    symbol: str
    timestamp: str
    open: float
    high: float
    low: float
    close: float
    volume: int


def get_ohlc(symbol: str, days: int = 60) -> List[Candle]:
    """
    Retrieve OHLC data from the configured data sources in fallback order.
    """

    target = symbol.upper()
    lookback = max(days, 1)

    history = _fetch_microservice_history(target, lookback)
    if history:
        logger.info("Using Microservice for %s", target)
        return history
    logger.info("Microservice failed → Trying Yahoo for %s", target)

    history = _convert_rows(target, fetch_yahoo_ohlc(target, lookback), lookback)
    if history:
        logger.info("Using Yahoo Finance for %s", target)
        return history
    logger.info("Yahoo failed → Trying AlphaVantage for %s", target)

    history = _convert_rows(target, fetch_alpha_ohlc(target, lookback, settings.alpha_vantage_key), lookback)
    if history:
        logger.info("Using AlphaVantage for %s", target)
        return history
    logger.info("AlphaVantage failed → Trying Massive REST for %s", target)

    history = _convert_rows(target, fetch_massive_rest_ohlc(target, lookback, settings.massive_api_key), lookback)
    if history:
        logger.info("Using Massive REST for %s", target)
        return history

    logger.error("All data sources failed for %s", target)
    return []


def get_last_close(symbol: str) -> Optional[float]:
    candles = get_ohlc(symbol, days=1)
    if not candles:
        return None
    return candles[-1].close


def _fetch_microservice_history(symbol: str, days: int) -> List[Candle]:
    base_url = (settings.internal_massive_microservice_url or "").strip()
    if not base_url:
        return []
    url = f"{base_url.rstrip('/')}/history/{symbol}"
    try:
        response = requests.get(url, timeout=5)
        response.raise_for_status()
    except Exception as exc:  # pragma: no cover - network guard
        logger.warning("Microservice request failed for %s: %s", symbol, exc)
        return []

    payload = response.json()
    history = payload.get("history")
    if not isinstance(history, list):
        return []
    return _convert_rows(symbol, history, days)


def _convert_rows(symbol: str, rows: List[Dict[str, Any]], limit: int) -> List[Candle]:
    candles: List[Candle] = []
    for row in rows:
        try:
            ts = str(row.get("timestamp") or row.get("date") or row.get("built_at") or "")[:10]
            if not ts:
                continue
            candles.append(
                Candle(
                    symbol=symbol,
                    timestamp=ts,
                    open=float(row.get("open")),
                    high=float(row.get("high")),
                    low=float(row.get("low")),
                    close=float(row.get("close")),
                    volume=int(row.get("volume") or 0),
                )
            )
        except (TypeError, ValueError):
            continue

    candles.sort(key=lambda candle: candle.timestamp)
    if limit > 0:
        candles = candles[-limit:]
    return candles
