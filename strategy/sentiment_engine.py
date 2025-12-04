from __future__ import annotations

import logging

from sentiment.engine import SentimentEngine

logger = logging.getLogger(__name__)
_engine = SentimentEngine()


def sentiment_score(symbol: str) -> float:
    payload = _engine.get_sentiment(symbol)
    raw = float(payload.get("sentiment_score", 0.0) or 0.0)
    return (raw + 1.0) / 2.0


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
