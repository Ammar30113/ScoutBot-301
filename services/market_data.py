import asyncio
from datetime import datetime
from typing import Any, Dict, List

from utils.logger import get_logger
from utils.settings import get_settings

from .alpaca import fetch_alpaca_latest_quote
from .finviz import fetch_finviz_snapshot, fetch_microcap_screen
from .stockdata import fetch_stockdata_quote
from .yfinance_client import fetch_yfinance_snapshot

logger = get_logger(__name__)

CORE_TICKERS = ["AAPL", "NVDA", "TSLA", "AMD", "META", "MSFT", "GOOG", "AMZN"]
MIN_CHANGE_PERCENT = 1.0
MIN_VOLUME = 1_000_000
MICROCAP_LIMIT = 25

LAST_UNIVERSE: Dict[str, Any] = {"symbols": [], "built_at": None}


async def gather_symbol_insights(symbol: str | None = None) -> Dict[str, Any]:
    """
    Fetch a blended market snapshot from Finviz, StockData, Alpaca, and Yahoo Finance.
    """
    settings = get_settings()
    target_symbol = (symbol or settings.default_symbol).upper()
    logger.info("Gathering insights for %s", target_symbol)

    finviz_task = fetch_finviz_snapshot(target_symbol)
    stockdata_task = fetch_stockdata_quote(target_symbol)
    alpaca_task = fetch_alpaca_latest_quote(target_symbol)
    yfinance_task = fetch_yfinance_snapshot(target_symbol)

    finviz, stockdata, alpaca, yfinance_data = await asyncio.gather(
        finviz_task,
        stockdata_task,
        alpaca_task,
        yfinance_task,
    )

    return {
        "symbol": target_symbol,
        "sources": [finviz, stockdata, alpaca, yfinance_data],
    }


async def get_daily_universe() -> List[str]:
    """
    Merge Finviz microcaps with filtered large caps for the hybrid strategy.
    """
    microcaps_task = fetch_microcap_screen(MICROCAP_LIMIT)
    large_caps_task = _filter_large_caps(CORE_TICKERS)
    microcaps, large_caps = await asyncio.gather(microcaps_task, large_caps_task)

    merged: List[str] = []
    for ticker in microcaps + large_caps:
        if ticker not in merged:
            merged.append(ticker)

    LAST_UNIVERSE["symbols"] = merged
    LAST_UNIVERSE["built_at"] = datetime.utcnow().isoformat()

    logger.info("Hybrid universe ready with %s symbols", len(merged))
    return merged


def get_cached_universe() -> Dict[str, Any]:
    """
    Return the latest cached universe snapshot for APIs.
    """
    return LAST_UNIVERSE


async def _filter_large_caps(tickers: List[str]) -> List[str]:
    """
    Use yfinance fast info to enforce a volume and daily momentum threshold for core tickers.
    """
    snapshots = await asyncio.gather(*(fetch_yfinance_snapshot(t) for t in tickers))

    qualified: List[str] = []
    for snapshot in snapshots:
        data = snapshot.get("data") or {}
        fast_info = data.get("fast_info") or {}
        change_pct = fast_info.get("regularMarketChangePercent")
        volume = fast_info.get("regularMarketVolume") or fast_info.get("volume")

        if change_pct is None or volume is None:
            continue
        if change_pct < MIN_CHANGE_PERCENT:
            continue
        if volume < MIN_VOLUME:
            continue
        qualified.append(snapshot.get("symbol", "").upper())

    if not qualified:
        logger.warning("Large-cap filter returned empty set, falling back to baseline core tickers")
        return tickers

    return qualified
