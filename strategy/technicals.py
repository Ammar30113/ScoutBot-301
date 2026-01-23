from __future__ import annotations

import pandas as pd
from ta.momentum import RSIIndicator
from ta.trend import MACD, SMAIndicator

ENTRY_RSI_MIN = 38
ENTRY_RSI_MAX = 75
ENTRY_MACD_MIN = -0.02
EXIT_RSI_MIN = 75


def compute_vwap(df: pd.DataFrame) -> pd.Series:
    """Running VWAP for intraday bars."""

    price = df["close"].astype(float)
    volume = df["volume"].astype(float)
    cumulative_volume = volume.cumsum().replace(0, pd.NA)
    dollar_volume = (price * volume).cumsum()
    return dollar_volume / cumulative_volume


def passes_entry_filter(df: pd.DataFrame, crash_mode: bool = False) -> bool:
    if crash_mode:
        return True
    if df is None or df.empty or len(df) < 20:
        return False

    close = df["close"].astype(float)
    rsi = RSIIndicator(close, window=14).rsi().iloc[-1]
    macd = MACD(close).macd().iloc[-1]
    macd_signal = MACD(close).macd_signal().iloc[-1]
    vwap = compute_vwap(df).iloc[-1]

    # Momentum: less aggressive thresholds
    if not (ENTRY_RSI_MIN < rsi < ENTRY_RSI_MAX):
        return False
    if not (macd > ENTRY_MACD_MIN):
        return False
    vwap_diff = close.iloc[-1] - vwap
    if vwap_diff <= 0:
        return False

    return True


def passes_exit_filter(ohlcv_df: pd.DataFrame) -> bool:
    if ohlcv_df is None or ohlcv_df.empty or len(ohlcv_df) < 20:
        return True  # exit defensively on missing data
    close = ohlcv_df["close"].astype(float)
    rsi = RSIIndicator(close, window=14).rsi().iloc[-1]
    sma20 = SMAIndicator(close, window=20).sma_indicator().iloc[-1]
    macd_hist = _macd_hist(close).iloc[-1]
    price = close.iloc[-1]
    vwap = compute_vwap(ohlcv_df).iloc[-1]
    return bool(rsi > EXIT_RSI_MIN or macd_hist < 0 or price < sma20 or price < vwap)


def _macd_hist(close: pd.Series) -> pd.Series:
    macd = MACD(close, window_slow=26, window_fast=12, window_sign=9)
    return macd.macd_diff()


def compute_macd_hist(close: pd.Series) -> pd.Series:
    """Wrapper for MACD histogram (diff) to keep naming consistent."""

    return _macd_hist(close)


def compute_atr(df: pd.DataFrame, window: int = 14) -> pd.Series:
    """Average True Range."""

    high = df["high"].astype(float)
    low = df["low"].astype(float)
    close = df["close"].astype(float)
    prev_close = close.shift(1)
    tr = pd.concat(
        [
            high - low,
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return tr.rolling(window=window, min_periods=window).mean()


def atr_bands(df: pd.DataFrame, multiplier: float = 1.5, window: int = 14):
    """Return mid, upper, lower ATR bands and ATR series."""

    if df is None or df.empty:
        return None, None, None, None
    close = df["close"].astype(float)
    atr = compute_atr(df, window=window)
    mid = close.rolling(window=window, min_periods=window).mean()
    if mid is None or atr is None:
        return None, None, None, None
    upper = mid + multiplier * atr
    lower = mid - multiplier * atr
    return mid, upper, lower, atr


def relaxed_entry_filter(df: pd.DataFrame) -> bool:
    """Always allow entries (used for crash mode override)."""

    return True
