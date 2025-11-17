from __future__ import annotations

import logging
from typing import Dict, List

from data.price_router import PriceRouter
from strategy.momentum import compute_momentum_scores
from strategy.technicals import passes_entry_filter
from strategy.sentiment_engine import sentiment_score
from strategy.ml_classifier import generate_predictions

logger = logging.getLogger(__name__)
price_router = PriceRouter()


def route_signals(universe: List[str]) -> List[Dict[str, float | str]]:
    momentum = compute_momentum_scores(universe)
    momentum_map = {sym: score for sym, score in momentum}

    ml_preds = generate_predictions([sym for sym, _ in momentum])
    signals: List[Dict[str, float | str]] = []
    max_rank = max(len(momentum_map), 1)

    for symbol, prob, _ in ml_preds:
        rank_component = 1.0 - (list(momentum_map.keys()).index(symbol) / max_rank) if symbol in momentum_map else 0.0
        if prob < 0.4:
            continue
        sentiment = sentiment_score(symbol)

        try:
            bars = price_router.get_aggregates(symbol, "1day", 60)
            df = PriceRouter.aggregates_to_dataframe(bars)
        except Exception as exc:  # pragma: no cover - network guard
            logger.warning("Technical data unavailable for %s: %s", symbol, exc)
            continue
        if df is None or df.empty:
            continue
        if not passes_entry_filter(df):
            continue

        final_score = 0.4 * rank_component + 0.2 * 1.0 + 0.2 * sentiment + 0.2 * prob
        if final_score > 0.55 and prob > 0.6:
            signals.append({"symbol": symbol, "score": final_score, "prob": prob, "sentiment": sentiment})
    return signals
