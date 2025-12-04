from __future__ import annotations

import logging
import time
from typing import Dict, List

from data.price_router import PriceRouter
from sentiment.engine import SentimentEngine
from strategy.momentum import compute_momentum_scores
from strategy.technicals import passes_entry_filter, compute_atr
from strategy.ml_classifier import generate_predictions
from strategy.reversal import compute_reversal_signal

logger = logging.getLogger(__name__)
price_router = PriceRouter()
sentiment_engine = SentimentEngine()


def route_signals(universe: List[str], crash_mode: bool = False) -> List[Dict[str, float | str]]:
    if len(universe) > 20:
        sentiment_engine.preload(universe)

    momentum = compute_momentum_scores(universe, top_k=0, crash_mode=crash_mode)
    momentum_map = {sym: score for sym, score in momentum}

    ml_preds = generate_predictions(universe, crash_mode=crash_mode)
    signals: List[Dict[str, float | str]] = []
    max_rank = max(len(momentum_map), 1)
    rate_limited: set[str] = set()

    for symbol, prob, features in ml_preds:
        if symbol in rate_limited:
            continue
        time.sleep(0.01)  # stagger provider requests slightly for large universes
        rank_component = 1.0 - (list(momentum_map.keys()).index(symbol) / max_rank) if symbol in momentum_map else 0.0
        ml_threshold_trend = 0.22
        ml_threshold_reversal = 0.28
        if prob < ml_threshold_trend:
            continue
        sentiment_payload = sentiment_engine.get_sentiment(symbol)
        sentiment_raw = float(sentiment_payload.get("sentiment_score", 0.0) or 0.0)
        sentiment = (sentiment_raw + 1.0) / 2.0  # map [-1,1] to [0,1]

        try:
            bars = price_router.get_aggregates(symbol, window=120)
            df = PriceRouter.aggregates_to_dataframe(bars)
        except Exception as exc:  # pragma: no cover - network guard
            msg = str(exc).lower()
            if "429" in msg:
                rate_limited.add(symbol)
                logger.warning("Technical data rate-limited for %s (429); skipping", symbol)
            else:
                logger.warning("Technical data unavailable for %s: %s", symbol, exc)
            continue
        if df is None or df.empty:
            continue

        momentum_score = momentum_map.get(symbol, 0.0)
        vol_ratio = float(features.get("vol_ratio", 1.0) or 1.0)
        vol_ok = vol_ratio > 0.20

        # volatility ratio via ATR relative to its recent average
        atr_series = compute_atr(df, window=14)
        atr_current = float(atr_series.iloc[-1]) if len(atr_series) else 0.0
        atr_avg = float(atr_series.rolling(window=30, min_periods=5).mean().iloc[-1]) if len(atr_series) else 0.0
        volatility_ratio = (atr_current / atr_avg) if atr_avg else 1.0

        reversal_score = compute_reversal_signal(df)
        reverse_prob_cutoff = max(ml_threshold_reversal, 0.30 if crash_mode else ml_threshold_reversal)
        reversal_allowed = (
            -0.10 <= momentum_score <= 0.10
            and volatility_ratio > 1.05
            and prob >= reverse_prob_cutoff
            and reversal_score != 0.0
        )

        # slope confirmations
        close = df["close"].astype(float)
        short_slope = float(close.pct_change().tail(3).mean() or 0.0)
        mid_slope = float(close.pct_change().tail(12).mean() or 0.0)

        momentum_base = (
            prob >= ml_threshold_trend
            and passes_entry_filter(df, crash_mode=crash_mode)
            and vol_ok
            and short_slope > 0
            and mid_slope > -0.005
        )
        score_threshold = 0.32
        # Sentiment contributes ~15% of the final score (within 10-25% envelope)
        final_score = 0.45 * rank_component + 0.25 * prob + 0.15 * sentiment + 0.15 * momentum_score
        momentum_signal = momentum_base and final_score > score_threshold

        dip_buy_ok = short_slope < -0.20 and vol_ratio > 1.1 and prob > ml_threshold_reversal

        if momentum_signal:
            if reversal_allowed:
                logger.info("Reversal candidate for %s but overridden by momentum", symbol)
                logger.info("Momentum dominates reversal")
            logger.info(
                "Entering momentum trade: %s, prob=%.3f, score=%.3f, crash_mode=%s reason=%s threshold=%.2f",
                symbol,
                prob,
                momentum_score,
                crash_mode,
                "crash expansion" if crash_mode else "trend",
                score_threshold,
            )
            signals.append(
                {
                    "symbol": symbol,
                    "score": final_score,
                    "prob": prob,
                    "sentiment": sentiment,
                    "type": "momentum",
                    "vol_ratio": vol_ratio,
                    "momentum_score": momentum_score,
                    "reason": "crash expansion" if crash_mode else "trend",
                }
            )
        elif dip_buy_ok:
            logger.info(
                "Entering reversal trade: %s, prob=%.3f, rev_score=%.3f, crash_mode=%s reason=%s threshold=%.2f",
                symbol,
                prob,
                reversal_score,
                crash_mode,
                "dip buy",
                ml_threshold_reversal,
            )
            signals.append(
                {
                    "symbol": symbol,
                    "prob": prob,
                    "reversal_score": reversal_score,
                    "type": "reversal",
                    "vol_ratio": vol_ratio,
                    "momentum_score": momentum_score,
                    "reason": "dip buy",
                }
            )
        elif reversal_allowed:
            logger.info("Momentum weak, reversal allowed for %s", symbol)
            logger.info("Momentum skipped, reversal allowed")
            logger.info(
                "Entering reversal trade: %s, prob=%.3f, rev_score=%.3f, crash_mode=%s reason=%s threshold=%.2f",
                symbol,
                prob,
                reversal_score,
                crash_mode,
                "reversal",
                reverse_prob_cutoff,
            )
            signals.append(
                {
                    "symbol": symbol,
                    "prob": prob,
                    "reversal_score": reversal_score,
                    "type": "reversal",
                    "vol_ratio": vol_ratio,
                    "momentum_score": momentum_score,
                    "reason": "reversal",
                }
            )
        if crash_mode and len(signals) >= 3:
            logger.info("Crash mode signal cap reached (3); skipping remaining symbols")
            break
    return signals
