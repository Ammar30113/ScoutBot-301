from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd
import yfinance as yf

from utils.logger import get_logger

logger = get_logger("providers.yahoo")


def fetch_ohlc(symbol: str, days: int) -> List[Dict[str, Any]]:
    """
    Retrieve daily OHLC data from Yahoo Finance via yfinance.
    """
    try:
        ticker = yf.Ticker(symbol)
        history = ticker.history(period="max", auto_adjust=False)
    except Exception as exc:  # pragma: no cover - network guard
        logger.warning("Yahoo Finance request failed for %s: %s", symbol, exc)
        return []

    if history.empty:
        logger.info("Yahoo Finance returned no rows for %s", symbol)
        return []

    filtered = history.dropna(subset=["Open", "High", "Low", "Close"], how="any")
    if filtered.empty:
        logger.info("Yahoo Finance rows dropped after NaN filtering for %s", symbol)
        return []

    if days > 0:
        filtered = filtered.tail(days)

    rows: List[Dict[str, Any]] = []
    for timestamp, row in filtered.iterrows():
        try:
            ts = _normalize_timestamp(timestamp)
            rows.append(
                {
                    "timestamp": ts,
                    "open": float(row["Open"]),
                    "high": float(row["High"]),
                    "low": float(row["Low"]),
                    "close": float(row["Close"]),
                    "volume": int(row.get("Volume") or 0),
                }
            )
        except (TypeError, ValueError) as exc:
            logger.debug("Skipping Yahoo row for %s due to %s", symbol, exc)

    return rows


def _normalize_timestamp(timestamp: Any) -> str:
    if isinstance(timestamp, (str, bytes)):
        return str(timestamp)[:10]
    if isinstance(timestamp, pd.Timestamp):
        return timestamp.to_pydatetime().date().isoformat()
    return str(timestamp)
