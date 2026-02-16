from __future__ import annotations

import numpy as np
import pandas as pd
from ta.momentum import RSIIndicator

from strategy.technicals import compute_macd_hist, atr_bands


def compute_reversal_signal(df: pd.DataFrame) -> float:
    """
    Return a reversal score between -1 (bearish) and +1 (bullish).
    Conditions:
    - RSI < 38 or RSI > 72
    - MACD histogram crosses zero (directional)
    - Price touches ±1.5 ATR band
    """

    if df is None or df.empty or len(df) < 25:
        return 0.0

    close = df["close"].astype(float)
    rsi = RSIIndicator(close, window=14).rsi()
    if rsi.empty:
        return 0.0
    rsi_last = float(rsi.iloc[-1])
    if not (rsi_last < 38 or rsi_last > 72):
        return 0.0

    macd_hist = compute_macd_hist(close)
    if macd_hist is None or len(macd_hist) < 2:
        return 0.0
    prev_hist = float(macd_hist.iloc[-2])
    curr_hist = float(macd_hist.iloc[-1])
    bull_cross = prev_hist < 0 < curr_hist
    bear_cross = prev_hist > 0 > curr_hist
    if not (bull_cross or bear_cross):
        return 0.0

    mid_band, upper_band, lower_band, atr = atr_bands(df, multiplier=1.5, window=14)
    if atr is None or atr.empty or mid_band is None:
        return 0.0
    atr_val = float(atr.iloc[-1]) if len(atr) else 0.0
    if atr_val <= 0:
        return 0.0

    price_last = float(close.iloc[-1])
    upper_last = float(upper_band.iloc[-1])
    lower_last = float(lower_band.iloc[-1])

    band_touch = price_last >= upper_last or price_last <= lower_last
    if not band_touch:
        return 0.0

    direction = 1.0 if bull_cross else -1.0
    distance = price_last - float(mid_band.iloc[-1])
    band_position = distance / atr_val
    score = direction * min(1.0, abs(band_position) / 1.5)
    if not np.isfinite(score):
        return 0.0
    return float(max(min(score, 1.0), -1.0))
