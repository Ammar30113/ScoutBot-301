import asyncio
import statistics
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from utils.logger import get_logger
from utils.settings import get_settings

from .alpaca import fetch_alpaca_latest_quote
from .finviz import fetch_finviz_snapshot, fetch_microcap_screen
from .finnhub_client import get_company_news as get_finnhub_company_news
from .finnhub_client import get_sentiment as get_finnhub_sentiment
from .massive_client import get_quote as get_massive_quote

logger = get_logger(__name__)

CORE_TICKERS = ["AAPL", "NVDA", "TSLA", "AMD", "META", "MSFT", "GOOG", "AMZN"]
MICROCAP_LIMIT = 25
INSIGHTS_CACHE_TTL = timedelta(seconds=90)
SUMMARY_SAMPLE_SIZE = 10

LAST_UNIVERSE: Dict[str, Any] = {"symbols": [], "built_at": None}
INSIGHTS_CACHE: Dict[str, Tuple[datetime, Dict[str, Any]]] = {}

SENTIMENT_WEIGHT = 0.6
NEWS_WEIGHT = 0.4


async def get_market_snapshot(symbol: str) -> Dict[str, Any]:
    """Return a blended Massive/Finnhub snapshot for ``symbol``."""

    target = symbol.upper()
    price: Optional[float] = None
    try:
        price = await get_massive_quote(target)
    except Exception as exc:  # pragma: no cover - network guard
        logger.error("Massive API failed for %s: %s", target, exc)

    sentiment: Dict[str, Any] = {}
    news: List[Dict[str, Any]] = []
    try:
        sentiment = await get_finnhub_sentiment(target)
    except Exception as exc:  # pragma: no cover - network guard
        logger.warning("Finnhub sentiment failed for %s: %s", target, exc)
    try:
        news = await get_finnhub_company_news(target)
    except Exception as exc:  # pragma: no cover - network guard
        logger.warning("Finnhub news lookup failed for %s: %s", target, exc)

    return {
        "symbol": target,
        "price": price,
        "sentiment": sentiment or {},
        "news": news,
        "built_at": datetime.utcnow().isoformat(),
    }


def score_stock(snapshot: Dict[str, Any]) -> float:
    """Score a stock based on sentiment + news breadth."""

    if not snapshot:
        return 0.0

    sentiment_payload = snapshot.get("sentiment") or {}
    sentiment_score = _extract_sentiment_score(sentiment_payload)
    news_count = len(snapshot.get("news") or [])
    news_factor = min(news_count, 5) / 5
    score = SENTIMENT_WEIGHT * sentiment_score + NEWS_WEIGHT * news_factor
    return round(score, 2)


async def gather_symbol_insights(symbol: str | None = None) -> Dict[str, Any]:
    """
    Fetch a blended market snapshot from Finviz, Alpaca, and Massive/Finnhub enrichments.
    """
    settings = get_settings()
    target_symbol = (symbol or settings.default_symbol).upper()
    now = datetime.utcnow()

    cached = _get_cached_insights(target_symbol, now)
    if cached is not None:
        return cached
    logger.info("Gathering insights for %s", target_symbol)

    finviz_task = fetch_finviz_snapshot(target_symbol)
    alpaca_task = fetch_alpaca_latest_quote(target_symbol)
    snapshot_task = get_market_snapshot(target_symbol)

    finviz, alpaca, market_snapshot = await asyncio.gather(finviz_task, alpaca_task, snapshot_task)

    result = {
        "symbol": target_symbol,
        "sources": [finviz, alpaca],
        "market_snapshot": market_snapshot,
        "score": score_stock(market_snapshot),
    }
    _store_cached_insights(target_symbol, now, result)
    return result


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

    await _log_universe_summary(merged)
    logger.info("Hybrid universe ready with %s symbols", len(merged))
    return merged


def get_cached_universe() -> Dict[str, Any]:
    """
    Return the latest cached universe snapshot for APIs.
    """
    return LAST_UNIVERSE


async def _filter_large_caps(tickers: List[str]) -> List[str]:
    """Return the baseline list when advanced filtering data is unavailable."""

    logger.info("Returning baseline large-cap list (yfinance dependency removed)")
    return tickers


def _get_cached_insights(symbol: str, now: datetime) -> Dict[str, Any] | None:
    cached = INSIGHTS_CACHE.get(symbol)
    if not cached:
        return None
    cached_time, payload = cached
    if now - cached_time < INSIGHTS_CACHE_TTL:
        return payload
    INSIGHTS_CACHE.pop(symbol, None)
    return None


def _store_cached_insights(symbol: str, timestamp: datetime, payload: Dict[str, Any]) -> None:
    INSIGHTS_CACHE[symbol] = (timestamp, payload)


def _extract_sentiment_score(payload: Dict[str, Any]) -> float:
    raw = payload.get("score")
    if raw is None:
        raw = payload.get("companyNewsScore")
    if raw is None:
        raw = payload.get("bullishPercent")
    try:
        return float(raw)
    except (TypeError, ValueError):
        return 0.0


def summarize_universe(universe_data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Compute basic aggregate stats for a set of market snapshots."""

    prices = [entry["price"] for entry in universe_data if entry.get("price") is not None]
    if not prices:
        return {"symbols": len(universe_data), "avg_price": 0}

    return {
        "symbols": len(universe_data),
        "avg_price": round(statistics.mean(prices), 2),
        "min_price": round(min(prices), 2),
        "max_price": round(max(prices), 2),
    }


async def _log_universe_summary(symbols: List[str]) -> None:
    if not symbols:
        return
    sample = symbols[:SUMMARY_SAMPLE_SIZE]
    try:
        snapshots = await asyncio.gather(*(get_market_snapshot(symbol) for symbol in sample))
    except Exception as exc:  # pragma: no cover - network guard
        logger.warning("Unable to build universe summary: %s", exc)
        return

    summary = summarize_universe(snapshots)
    logger.info("Universe sample summary: %s", summary)
