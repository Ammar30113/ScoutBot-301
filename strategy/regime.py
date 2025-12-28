from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from strategy.technicals import compute_atr


@dataclass
class RegimeInfo:
    score: float
    trend: float
    momentum: float
    atr_pct: float
    label: str


def compute_daily_regime(frame: pd.DataFrame) -> RegimeInfo:
    if frame is None or frame.empty or "close" not in frame.columns:
        return RegimeInfo(score=0.0, trend=0.0, momentum=0.0, atr_pct=0.0, label="unknown")

    close = frame["close"].astype(float)
    last_close = float(close.iloc[-1]) if len(close) else 0.0

    fast_window = 10
    slow_window = 30 if len(close) >= 30 else 20
    fast_avg = float(close.tail(fast_window).mean()) if len(close) else 0.0
    slow_avg = float(close.tail(slow_window).mean()) if len(close) else 0.0
    if fast_avg > slow_avg:
        trend = 1.0
    elif fast_avg < slow_avg:
        trend = -1.0
    else:
        trend = 0.0

    momentum = 0.0
    if len(close) >= fast_window and last_close > 0:
        base = float(close.iloc[-fast_window])
        if base > 0:
            ret = (last_close / base) - 1.0
            momentum = max(min(ret / 0.10, 1.0), -1.0)

    atr_pct = 0.0
    if len(frame) >= 15 and last_close > 0:
        atr_series = compute_atr(frame, window=14)
        atr_value = float(atr_series.iloc[-1]) if len(atr_series) else 0.0
        if atr_value > 0:
            atr_pct = atr_value / last_close

    score = 0.6 * trend + 0.4 * momentum
    if score >= 0.2:
        label = "bull"
    elif score <= -0.2:
        label = "bear"
    else:
        label = "neutral"

    return RegimeInfo(score=score, trend=trend, momentum=momentum, atr_pct=atr_pct, label=label)
