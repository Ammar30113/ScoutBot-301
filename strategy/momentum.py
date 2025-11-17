from __future__ import annotations

from typing import List, Sequence, Tuple

import pandas as pd

from data.price_router import PriceRouter
from core.logger import get_logger

logger = get_logger(__name__)
router = PriceRouter()

MOMENTUM_TOP_K = 10


def compute_momentum_scores(symbols: Sequence[str], top_k: int = MOMENTUM_TOP_K) -> List[Tuple[str, float]]:
    scores: List[Tuple[str, float]] = []
    for symbol in symbols:
        try:
            bars = router.get_aggregates(symbol, "1day", 60)
        except Exception as exc:  # pragma: no cover - network guard
            logger.warning("Aggregates unavailable for %s: %s", symbol, exc)
            continue
        df = PriceRouter.aggregates_to_dataframe(bars)
        if df.empty or len(df) < 20:
            continue
        close = df["close"].astype(float)
        returns = close.pct_change()
        ret_5 = close.iloc[-1] / close.iloc[-6] - 1 if len(close) > 5 and close.iloc[-6] else 0.0
        ret_20 = close.iloc[-1] / close.iloc[-21] - 1 if len(close) > 20 and close.iloc[-21] else 0.0
        sma50 = close.rolling(window=50, min_periods=1).mean()
        trend_bias = (close.iloc[-1] - sma50.iloc[-1]) / sma50.iloc[-1] if sma50.iloc[-1] else 0.0
        volume = df["volume"].astype(float)
        vol_ratio = volume.iloc[-1] / volume.rolling(window=20, min_periods=1).mean().iloc[-1] if len(volume) else 0.0

        score = ret_5 * 0.4 + ret_20 * 0.4 + trend_bias * 0.2
        scores.append((symbol, score))
        logger.info(
            "Momentum %s → score=%.3f ret5=%.3f ret20=%.3f trend=%.3f vol_ratio=%.2f",
            symbol,
            score,
            ret_5,
            ret_20,
            trend_bias,
            vol_ratio,
        )

    scores = sorted(scores, key=lambda x: x[1], reverse=True)
    return scores[: top_k or MOMENTUM_TOP_K]
