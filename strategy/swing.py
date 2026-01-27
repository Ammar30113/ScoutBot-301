from __future__ import annotations

import logging
from typing import Callable, Dict, Iterable, List

import pandas as pd
from ta.momentum import RSIIndicator
from ta.trend import SMAIndicator

from data.price_router import PriceRouter
from strategy.technicals import compute_atr

logger = logging.getLogger(__name__)

_MAX_SIGNALS = 5
_SIZE_MULTIPLIER = 0.55
_MIN_BARS = 35
_MIN_TREND_SCORE = 0.35
_MIN_DIP_SENTIMENT = 0.6
_BASE_STOP_LOSS_PCT = 0.02
_BASE_TAKE_PROFIT_PCT = 0.05
_MAX_HOLD_MINUTES = 24 * 60


def _safe_float(value: float | int | None) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _daily_trend_score(close: pd.Series) -> float:
    slope_5 = _safe_float(close.pct_change().tail(5).mean())
    slope_10 = _safe_float(close.pct_change().tail(10).mean())
    return max(0.0, min((slope_5 * 8.0) + (slope_10 * 4.0), 1.0))


def generate_swing_signals(
    symbols: Iterable[str],
    daily_bars_map: Dict[str, list],
    sentiment_lookup: Callable[[str], float] | None = None,
    max_signals: int = _MAX_SIGNALS,
) -> List[Dict[str, float | str]]:
    signals: List[Dict[str, float | str]] = []
    for symbol in symbols:
        bars = daily_bars_map.get(symbol)
        if not bars:
            continue
        frame = PriceRouter.aggregates_to_dataframe(bars)
        if frame is None or frame.empty or len(frame) < _MIN_BARS:
            continue

        close = frame["close"].astype(float)
        last_close = _safe_float(close.iloc[-1])
        if last_close <= 0:
            continue

        sma20 = SMAIndicator(close, window=20).sma_indicator()
        sma50 = SMAIndicator(close, window=50).sma_indicator()
        if sma20.isna().iloc[-1] or sma50.isna().iloc[-1]:
            continue
        sma20_val = _safe_float(sma20.iloc[-1])
        sma50_val = _safe_float(sma50.iloc[-1])
        if sma20_val <= 0 or sma50_val <= 0:
            continue

        rsi = RSIIndicator(close, window=14).rsi()
        rsi_val = _safe_float(rsi.iloc[-1])
        trend_score = _daily_trend_score(close)
        sentiment = 0.0
        if sentiment_lookup:
            try:
                sentiment_raw = _safe_float(sentiment_lookup(symbol))
            except Exception:
                sentiment_raw = 0.0
            sentiment = (sentiment_raw + 1.0) / 2.0

        trend_ok = last_close > sma20_val and sma20_val >= sma50_val and trend_score >= _MIN_TREND_SCORE
        dip_ok = last_close < sma20_val * 0.99 and rsi_val < 40 and sentiment >= _MIN_DIP_SENTIMENT

        if not trend_ok and not dip_ok:
            continue

        atr_series = compute_atr(frame, window=14)
        atr_val = _safe_float(atr_series.iloc[-1]) if len(atr_series) else 0.0
        atr_pct = (atr_val / last_close) if atr_val > 0 else 0.0
        stop_loss_pct = max(_BASE_STOP_LOSS_PCT, min(atr_pct * 2.0, 0.08)) if atr_pct else _BASE_STOP_LOSS_PCT
        take_profit_pct = max(_BASE_TAKE_PROFIT_PCT, min(stop_loss_pct * 2.5, 0.2))

        if trend_ok:
            score = 0.45 + (0.35 * trend_score) + (0.15 * sentiment)
            reason = "swing_trend"
        else:
            score = 0.4 + (0.35 * sentiment) + min(trend_score * 0.2, 0.15)
            reason = "swing_dip"

        signals.append(
            {
                "symbol": symbol,
                "type": "swing",
                "score": min(score, 0.95),
                "sentiment": sentiment,
                "daily_atr_pct": atr_pct,
                "stop_loss_pct": stop_loss_pct,
                "take_profit_pct": take_profit_pct,
                "max_hold_minutes": _MAX_HOLD_MINUTES,
                "size_multiplier": _SIZE_MULTIPLIER,
                "data_source": "daily",
                "reason": reason,
            }
        )
        if len(signals) >= max_signals:
            break

    if signals:
        logger.info("Swing fallback generated %s signals", len(signals))
    return signals
