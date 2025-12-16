import time
import logging
from typing import Dict, Tuple

from core.config import USE_SENTIMENT, USE_TWITTER_NEWS, SENTIMENT_CACHE_TTL
from data.twitter_news import get_symbol_news
from sentiment.gpt_provider import get_gpt_sentiment

log = logging.getLogger(__name__)

# cache: symbol -> (timestamp, value)
_cache: Dict[str, Tuple[float, float]] = {}


def _is_fresh(ts: float, ttl: int) -> bool:
    return (time.time() - ts) <= ttl


def get_sentiment(symbol: str) -> float:
    """
    Public sentiment entry point used by strategy.
    Returns a float in [-1, 1]. If USE_SENTIMENT is False, always returns 0.0.
    Uses an in-memory cache with TTL (SENTIMENT_CACHE_TTL seconds).
    """
    if not USE_SENTIMENT:
        log.info("sentiment.engine | Sentiment disabled via USE_SENTIMENT; returning 0.0")
        return 0.0

    ttl = SENTIMENT_CACHE_TTL

    # Cache lookup
    if symbol in _cache:
        ts, val = _cache[symbol]
        if _is_fresh(ts, ttl):
            log.info(f"sentiment.engine | Cache hit for {symbol}: {val:.4f}")
            return val
        else:
            log.info(f"sentiment.engine | Cache expired for {symbol}")

    news_context = []
    if USE_TWITTER_NEWS:
        try:
            news_context = get_symbol_news(symbol)
            if news_context:
                log.info("sentiment.engine | Twitter news attached for %s (%d items)", symbol, len(news_context))
        except Exception as exc:  # pragma: no cover - defensive
            log.warning("sentiment.engine | Twitter news unavailable for %s: %s", symbol, exc)
            news_context = []

    # Fetch fresh sentiment from GPT
    val = get_gpt_sentiment(symbol, news=news_context)
    _cache[symbol] = (time.time(), val)
    return val
