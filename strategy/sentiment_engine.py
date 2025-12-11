from __future__ import annotations

import logging

from sentiment.engine import get_sentiment as _get_sentiment


logger = logging.getLogger(__name__)


def get_symbol_sentiment(symbol: str) -> float:
    """
    Adapter used by strategy modules.
    Returns sentiment in [-1, 1].
    """
    return _get_sentiment(symbol)


def sentiment_score(symbol: str) -> float:
    raw = get_symbol_sentiment(symbol)
    return (raw + 1.0) / 2.0
