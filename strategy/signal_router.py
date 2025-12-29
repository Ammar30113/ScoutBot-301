from __future__ import annotations

import logging
import time
from typing import Dict, List

from core.config import get_settings
from data.price_router import PriceRouter
from strategy.momentum import compute_momentum_scores
from strategy.regime import compute_daily_regime
from strategy.technicals import passes_entry_filter, compute_atr
from strategy.ml_classifier import generate_predictions
from strategy.reversal import compute_reversal_signal
from strategy.sentiment_engine import get_symbol_sentiment
from strategy.orb import find_orb_setups
from trader.risk_model import STOP_LOSS_PCT, TAKE_PROFIT_PCT
from trader.trade_logger import log_trade

logger = logging.getLogger(__name__)
price_router = PriceRouter()
settings = get_settings()


def _log_signal(signal: Dict[str, float | str]) -> None:
    symbol = signal.get("symbol") if isinstance(signal, dict) else None
    if not symbol:
        return
    payload = {
        "event": "signal",
        "symbol": symbol,
        "signal_type": signal.get("type"),
        "score": signal.get("score"),
        "prob": signal.get("prob"),
        "sentiment": signal.get("sentiment"),
        "momentum_score": signal.get("momentum_score"),
        "reversal_score": signal.get("reversal_score"),
        "regime_score": signal.get("regime_score"),
        "regime": signal.get("regime"),
        "atr_pct": signal.get("atr_pct"),
        "daily_atr_pct": signal.get("daily_atr_pct"),
        "vol_ratio": signal.get("vol_ratio"),
        "score_threshold": signal.get("score_threshold"),
        "ml_threshold_trend": signal.get("ml_threshold_trend"),
        "ml_threshold_reversal": signal.get("ml_threshold_reversal"),
        "provider_intraday": signal.get("provider_intraday"),
        "provider_daily": signal.get("provider_daily"),
        "stop_loss_pct": signal.get("stop_loss_pct"),
        "take_profit_pct": signal.get("take_profit_pct"),
        "reason": signal.get("reason"),
    }
    log_trade({k: v for k, v in payload.items() if v is not None})


def route_signals(universe: List[str], crash_mode: bool = False, context=None) -> List[Dict[str, float | str]]:
    orb_signals = find_orb_setups(universe, crash_mode=crash_mode)
    skip_symbols = {sig["symbol"] for sig in orb_signals}
    orb_symbols = [sig.get("symbol") for sig in orb_signals if isinstance(sig, dict) and sig.get("symbol")]

    momentum = compute_momentum_scores(universe, top_k=0, crash_mode=crash_mode)
    momentum_map = {sym: score for sym, score in momentum}

    ml_preds = generate_predictions(universe, crash_mode=crash_mode)
    daily_bars_map = {}
    symbols_for_daily: List[str] = []
    if ml_preds:
        symbols_for_daily.extend(sym for sym, _, _ in ml_preds if sym)
    if orb_symbols:
        symbols_for_daily.extend(orb_symbols)
    if symbols_for_daily:
        symbols = list(dict.fromkeys(symbols_for_daily))
        if hasattr(price_router, "get_daily_bars_batch"):
            daily_bars_map = price_router.get_daily_bars_batch(symbols, limit=60)
        else:
            for sym in symbols:
                if not hasattr(price_router, "get_daily_aggregates"):
                    break
                try:
                    daily_bars_map[sym] = price_router.get_daily_aggregates(sym, limit=60)
                except Exception:
                    continue
    daily_regime_map = {}
    for sym, bars in (daily_bars_map or {}).items():
        df_daily = PriceRouter.aggregates_to_dataframe(bars)
        if df_daily is None or df_daily.empty:
            continue
        daily_regime_map[sym] = compute_daily_regime(df_daily)

    regime_gate_min = float(settings.regime_gate_min_score or 0.0)
    filtered_orb_signals: List[Dict[str, float | str]] = []
    if orb_signals and not crash_mode:
        for sig in orb_signals:
            symbol = sig.get("symbol") if isinstance(sig, dict) else None
            if not symbol:
                continue
            regime = daily_regime_map.get(symbol)
            regime_score = float(regime.score) if regime else 0.0
            if regime_score < regime_gate_min:
                continue
            sig["regime_score"] = regime_score
            sig["regime"] = regime.label if regime else "unknown"
            sig["daily_atr_pct"] = float(regime.atr_pct) if regime else 0.0
            filtered_orb_signals.append(sig)
    else:
        filtered_orb_signals = list(orb_signals)

    signals: List[Dict[str, float | str]] = list(filtered_orb_signals)
    for sig in filtered_orb_signals:
        _log_signal(sig)
    max_rank = max(len(momentum_map), 1)
    rate_limited: set[str] = set()

    for symbol, prob, features in ml_preds:
        if symbol in skip_symbols:
            continue
        if symbol in rate_limited:
            continue
        time.sleep(0.05)  # stagger provider requests slightly for large universes (reduce API bursts)
        rank_component = 1.0 - (list(momentum_map.keys()).index(symbol) / max_rank) if symbol in momentum_map else 0.0
        ml_threshold_trend = 0.22
        ml_threshold_reversal = 0.28
        if prob < ml_threshold_trend:
            continue
        regime = daily_regime_map.get(symbol)
        regime_score = float(regime.score) if regime else 0.0
        if not crash_mode and regime_score < regime_gate_min:
            continue
        regime_label = regime.label if regime else "unknown"
        daily_atr_pct = float(regime.atr_pct) if regime else 0.0
        sentiment = 0.0
        if settings.use_sentiment:
            sentiment_raw = float(get_symbol_sentiment(symbol) or 0.0)
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

        close = df["close"].astype(float)
        provider_lookup = getattr(price_router, "last_provider", None)
        if callable(provider_lookup):
            intraday_provider = provider_lookup(symbol, "intraday")
            daily_provider = provider_lookup(symbol, "daily")
        else:
            intraday_provider = None
            daily_provider = None

        momentum_score = momentum_map.get(symbol, 0.0)
        vol_ratio = float(features.get("vol_ratio", 1.0) or 1.0)
        vol_ok = vol_ratio > 0.20

        # volatility ratio via ATR relative to its recent average
        atr_series = compute_atr(df, window=14)
        atr_current = float(atr_series.iloc[-1]) if len(atr_series) else 0.0
        atr_avg = float(atr_series.rolling(window=30, min_periods=5).mean().iloc[-1]) if len(atr_series) else 0.0
        volatility_ratio = (atr_current / atr_avg) if atr_avg else 1.0

        entry_price = float(close.iloc[-1]) if len(close) else 0.0
        atr_pct_intraday = (atr_current / entry_price) if entry_price > 0 and atr_current > 0 else 0.0
        base_sl_pct = 0.005 if crash_mode else STOP_LOSS_PCT
        base_tp_pct = 0.015 if crash_mode else TAKE_PROFIT_PCT
        max_sl_pct = 0.05 if crash_mode else 0.08
        max_tp_pct = 0.12 if crash_mode else 0.20
        if atr_pct_intraday > 0:
            stop_loss_pct = max(base_sl_pct, min(atr_pct_intraday * settings.atr_multiplier, max_sl_pct))
        else:
            stop_loss_pct = base_sl_pct
        take_profit_pct = max(base_tp_pct, min(stop_loss_pct * 1.8, max_tp_pct))

        reversal_score = compute_reversal_signal(df)
        reverse_prob_cutoff = max(ml_threshold_reversal, 0.30 if crash_mode else ml_threshold_reversal)
        reversal_allowed = (
            -0.10 <= momentum_score <= 0.10
            and volatility_ratio > 1.05
            and prob >= reverse_prob_cutoff
            and reversal_score != 0.0
        )
        reversal_allowed = reversal_allowed and (regime_score >= -0.35 or crash_mode)

        # slope confirmations
        short_slope = float(close.pct_change().tail(3).mean() or 0.0)
        mid_slope = float(close.pct_change().tail(12).mean() or 0.0)

        momentum_base = (
            prob >= ml_threshold_trend
            and passes_entry_filter(df, crash_mode=crash_mode)
            and vol_ok
            and short_slope > 0
            and mid_slope > -0.005
        )
        momentum_base = momentum_base and (regime_score >= -0.20 or crash_mode)
        score_threshold = 0.32 - (0.05 * regime_score)
        # Sentiment contributes ~15% of the final score (within 10-25% envelope)
        raw_score = 0.45 * rank_component + 0.25 * prob + 0.15 * sentiment + 0.15 * momentum_score
        raw_score += 0.05 * regime_score

        # P&L penalty/boost injected from main
        pnl_penalty = context.pnl_penalty if hasattr(context, "pnl_penalty") else 0.0
        final_score = raw_score - pnl_penalty
        momentum_signal = momentum_base and final_score > score_threshold

        dip_buy_ok = short_slope < -0.20 and vol_ratio > 1.1 and prob > ml_threshold_reversal
        dip_buy_ok = dip_buy_ok and (regime_score >= -0.35 or crash_mode)

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
                    "regime_score": regime_score,
                    "regime": regime_label,
                    "daily_atr_pct": daily_atr_pct,
                    "atr_pct": atr_pct_intraday,
                    "stop_loss_pct": stop_loss_pct,
                    "take_profit_pct": take_profit_pct,
                    "score_threshold": score_threshold,
                    "ml_threshold_trend": ml_threshold_trend,
                    "ml_threshold_reversal": ml_threshold_reversal,
                    "provider_intraday": intraday_provider,
                    "provider_daily": daily_provider,
                    "reason": "crash expansion" if crash_mode else "trend",
                }
            )
            _log_signal(signals[-1])
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
                    "regime_score": regime_score,
                    "regime": regime_label,
                    "daily_atr_pct": daily_atr_pct,
                    "atr_pct": atr_pct_intraday,
                    "stop_loss_pct": stop_loss_pct,
                    "take_profit_pct": take_profit_pct,
                    "score_threshold": score_threshold,
                    "ml_threshold_trend": ml_threshold_trend,
                    "ml_threshold_reversal": ml_threshold_reversal,
                    "provider_intraday": intraday_provider,
                    "provider_daily": daily_provider,
                    "reason": "dip buy",
                }
            )
            _log_signal(signals[-1])
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
                    "regime_score": regime_score,
                    "regime": regime_label,
                    "daily_atr_pct": daily_atr_pct,
                    "atr_pct": atr_pct_intraday,
                    "stop_loss_pct": stop_loss_pct,
                    "take_profit_pct": take_profit_pct,
                    "score_threshold": score_threshold,
                    "ml_threshold_trend": ml_threshold_trend,
                    "ml_threshold_reversal": ml_threshold_reversal,
                    "provider_intraday": intraday_provider,
                    "provider_daily": daily_provider,
                    "reason": "reversal",
                }
            )
            _log_signal(signals[-1])
        if crash_mode and len(signals) >= 3:
            logger.info("Crash mode signal cap reached (3); skipping remaining symbols")
            break
    signals.sort(key=lambda item: float(item.get("score", 0.0)), reverse=True)
    return signals
