from __future__ import annotations

import pandas as pd
from ta.momentum import RSIIndicator
from ta.trend import MACD, SMAIndicator

ENTRY_RSI_MAX = 60
EXIT_RSI_MIN = 75


def passes_entry_filter(ohlcv_df: pd.DataFrame) -> bool:
    if ohlcv_df is None or ohlcv_df.empty or len(ohlcv_df) < 30:
        return False
    close = ohlcv_df["close"].astype(float)
    volume = ohlcv_df["volume"].astype(float).replace(0, pd.NA)
    price = close.iloc[-1]

    rsi = RSIIndicator(close, window=14).rsi().iloc[-1]
    sma50 = SMAIndicator(close, window=50).sma_indicator().iloc[-1]
    macd_hist = _macd_hist(close).iloc[-1]
    vwap = _vwap(close, volume).iloc[-1]

    return bool(rsi < ENTRY_RSI_MAX and price > sma50 and macd_hist > 0 and price > vwap)


def passes_exit_filter(ohlcv_df: pd.DataFrame) -> bool:
    if ohlcv_df is None or ohlcv_df.empty or len(ohlcv_df) < 30:
        return True  # exit defensively on missing data
    close = ohlcv_df["close"].astype(float)
    volume = ohlcv_df["volume"].astype(float).replace(0, pd.NA)
    rsi = RSIIndicator(close, window=14).rsi().iloc[-1]
    sma20 = SMAIndicator(close, window=20).sma_indicator().iloc[-1]
    macd_hist = _macd_hist(close).iloc[-1]
    price = close.iloc[-1]
    vwap = _vwap(close, volume).iloc[-1]
    return bool(rsi > EXIT_RSI_MIN or macd_hist < 0 or price < sma20 or price < vwap)


def _macd_hist(close: pd.Series) -> pd.Series:
    macd = MACD(close, window_slow=26, window_fast=12, window_sign=9)
    return macd.macd_diff()


def _vwap(close: pd.Series, volume: pd.Series) -> pd.Series:
    typical_price = close
    cumulative = (typical_price * volume).cumsum() / volume.cumsum()
    return cumulative
