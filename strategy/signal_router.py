from __future__ import annotations

import logging
from typing import Dict, List

from data.price_router import PriceRouter
from strategy.momentum import compute_momentum_scores
from strategy.technicals import passes_entry_filter, compute_atr
from strategy.sentiment_engine import sentiment_score
from strategy.ml_classifier import generate_predictions
from strategy.reversal import compute_reversal_signal

logger = logging.getLogger(__name__)
price_router = PriceRouter()


def route_signals(universe: List[str]) -> List[Dict[str, float | str]]:
    momentum = compute_momentum_scores(universe, top_k=0)
    momentum_map = {sym: score for sym, score in momentum}

    ml_preds = generate_predictions(universe)
    signals: List[Dict[str, float | str]] = []
    max_rank = max(len(momentum_map), 1)

    for symbol, prob, features in ml_preds:
        rank_component = 1.0 - (list(momentum_map.keys()).index(symbol) / max_rank) if symbol in momentum_map else 0.0
        if prob < 0.35:
            continue
        sentiment = sentiment_score(symbol)

        try:
            bars = price_router.get_aggregates(symbol, window=120)
            df = PriceRouter.aggregates_to_dataframe(bars)
        except Exception as exc:  # pragma: no cover - network guard
            logger.warning("Technical data unavailable for %s: %s", symbol, exc)
            continue
        if df is None or df.empty:
            continue

        momentum_score = momentum_map.get(symbol, 0.0)
        vol_ratio = float(features.get("vol_ratio", 1.0) or 1.0)

        # volatility ratio via ATR relative to its recent average
        atr_series = compute_atr(df, window=14)
        atr_current = float(atr_series.iloc[-1]) if len(atr_series) else 0.0
        atr_avg = float(atr_series.rolling(window=30, min_periods=5).mean().iloc[-1]) if len(atr_series) else 0.0
        volatility_ratio = (atr_current / atr_avg) if atr_avg else 1.0

        reversal_score = compute_reversal_signal(df)
        reversal_allowed = (
            -0.10 <= momentum_score <= 0.10 and volatility_ratio > 1.05 and prob >= 0.40 and reversal_score != 0.0
        )

        momentum_base = prob >= 0.35 and passes_entry_filter(df)
        final_score = 0.4 * rank_component + 0.2 * 1.0 + 0.2 * sentiment + 0.2 * prob
        momentum_signal = momentum_base and final_score > 0.55

        if momentum_signal:
            if reversal_allowed:
                logger.info("Reversal candidate for %s but overridden by momentum", symbol)
                logger.info("Momentum dominates reversal")
            logger.info("Entering momentum trade: %s, prob=%.3f, score=%.3f", symbol, prob, momentum_score)
            signals.append(
                {
                    "symbol": symbol,
                    "score": final_score,
                    "prob": prob,
                    "sentiment": sentiment,
                    "type": "momentum",
                    "vol_ratio": vol_ratio,
                    "momentum_score": momentum_score,
                }
            )
        elif reversal_allowed:
            logger.info("Momentum weak, reversal allowed for %s", symbol)
            logger.info("Momentum skipped, reversal allowed")
            logger.info("Entering reversal trade: %s, prob=%.3f, rev_score=%.3f", symbol, prob, reversal_score)
            signals.append(
                {
                    "symbol": symbol,
                    "prob": prob,
                    "reversal_score": reversal_score,
                    "type": "reversal",
                    "vol_ratio": vol_ratio,
                    "momentum_score": momentum_score,
                }
            )
    return signals
