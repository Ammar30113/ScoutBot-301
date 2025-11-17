from __future__ import annotations

import logging

from data.finnhub_sentiment import fetch_sentiment as fetch_finnhub_sentiment
from data.newsapi_sentiment import fetch_sentiment as fetch_news_sentiment

logger = logging.getLogger(__name__)


def sentiment_score(symbol: str) -> float:
    fn_score = fetch_finnhub_sentiment(symbol)
    news_score = fetch_news_sentiment(symbol)
    weighted = 0.6 * news_score + 0.4 * fn_score
    return max(0.0, min(1.0, float(weighted)))


def passes_entry(symbol: str) -> bool:
    score = sentiment_score(symbol)
    if score <= 0.6:
        logger.info("Sentiment entry blocked for %s (score=%.2f)", symbol, score)
        return False
    return True


def passes_exit(symbol: str) -> bool:
    score = sentiment_score(symbol)
    if score < 0.3:
        logger.info("Sentiment exit triggered for %s (score=%.2f)", symbol, score)
        return True
    return False
