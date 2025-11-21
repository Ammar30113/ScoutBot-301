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
            bars = router.get_aggregates(symbol, window=60)
        except Exception as exc:  # pragma: no cover - network guard
            logger.warning("Aggregates unavailable for %s: %s", symbol, exc)
            continue
        df = PriceRouter.aggregates_to_dataframe(bars)
        if df.empty or len(df) < 12:
            continue
        close = df["close"].astype(float)
        volume = df["volume"].astype(float)

        # 5-min bars: short-term velocity, slope, and volume expansion
        ret_short = (close.iloc[-1] / close.iloc[-3]) - 1 if len(close) >= 3 else 0.0
        ret_mid = (close.iloc[-1] / close.iloc[-12]) - 1 if len(close) >= 12 else 0.0
        slope = close.diff().rolling(6).mean().iloc[-1] if len(close) >= 6 else 0.0
        recent_vol = volume.tail(6).mean()
        base_vol = volume.tail(18).mean()
        vol_ratio = (recent_vol / base_vol) if pd.notna(base_vol) and base_vol else 0.0

        score = ret_short * 0.5 + ret_mid * 0.3 + slope * 0.2
        scores.append((symbol, score))
        logger.info(
            "Momentum %s → score=%.3f short=%.3f mid=%.3f slope=%.4f vol_ratio=%.2f",
            symbol,
            score,
            ret_short,
            ret_mid,
            slope,
            vol_ratio,
        )

    scores = sorted(scores, key=lambda x: x[1], reverse=True)
    return scores[: top_k or MOMENTUM_TOP_K]
