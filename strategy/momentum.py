from __future__ import annotations

from typing import List, Optional, Sequence, Tuple
from collections import defaultdict

import pandas as pd

from data.price_router import PriceRouter
from core.logger import get_logger

logger = get_logger(__name__)
router = PriceRouter()

MOMENTUM_TOP_K = 10
MOMENTUM_WINDOW_MINUTES = 120
LOG_SAMPLE_LIMIT = 5
_warn_counts: dict[str, int] = defaultdict(int)


def _warn_sample(symbol: str, exc: Exception) -> None:
    count = _warn_counts[symbol] + 1
    _warn_counts[symbol] = count
    if count <= LOG_SAMPLE_LIMIT:
        logger.warning("Aggregates unavailable for %s: %s", symbol, exc)
    elif count == LOG_SAMPLE_LIMIT + 1:
        logger.info("Aggregates unavailable for %s (suppressing repeats; %s occurrences)", symbol, count)


def compute_momentum_scores(
    symbols: Sequence[str], top_k: Optional[int] = MOMENTUM_TOP_K, *, crash_mode: bool = False
) -> List[Tuple[str, float]]:
    scores: List[Tuple[str, float]] = []
    for symbol in symbols:
        try:
            bars = router.get_aggregates(symbol, window=MOMENTUM_WINDOW_MINUTES)
        except Exception as exc:  # pragma: no cover - network guard
            _warn_sample(symbol, exc)
            continue
        df = PriceRouter.aggregates_to_dataframe(bars)
        if df.empty or len(df) < 12:
            continue
        close = df["close"].astype(float)
        volume = df["volume"].astype(float)

        # 5-min bars: short-term velocity, slope, and volume expansion
        ret_short = (close.iloc[-1] / close.iloc[-3]) - 1 if len(close) >= 3 else 0.0
        ret_mid = (close.iloc[-1] / close.iloc[-12]) - 1 if len(close) >= 12 else 0.0
        slope = close.pct_change().rolling(6).mean().iloc[-1] if len(close) >= 6 else 0.0
        recent_vol = volume.tail(6).mean()
        base_vol = volume.tail(18).mean()
        vol_ratio = (recent_vol / base_vol) if pd.notna(base_vol) and base_vol else 0.0

        if crash_mode:
            # allow negative short-term drifts; emphasize slope during crash
            score = ret_short * 0.3 + ret_mid * 0.3 + slope * 0.4
        else:
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
    if crash_mode:
        return scores  # do not trim universe during crash mode
    if top_k and top_k > 0:
        return scores[:top_k]
    return scores
