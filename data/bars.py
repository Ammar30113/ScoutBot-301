"""Validated raw-price bars with explicit event and availability timestamps."""
from dataclasses import dataclass
from datetime import datetime
import math

import pandas as pd

from core.market_calendar import session_bounds


@dataclass(frozen=True)
class Bar:
    timestamp: float
    available_at: float
    open: float
    high: float
    low: float
    close: float
    volume: float

    @classmethod
    def from_record(cls, row: dict, *, daily: bool = True) -> "Bar":
        raw = row["timestamp"]
        ts = raw.timestamp() if isinstance(raw, (pd.Timestamp, datetime)) else float(raw)
        if daily:
            # Provider daily timestamps label an exchange date (often midnight UTC).
            day = str(row.get("session") or pd.Timestamp(ts, unit="s", tz="UTC").date())
            bounds = session_bounds(day)
            if bounds is None:
                raise ValueError(f"Not an exchange session: {day}")
            ts, available = bounds
        else:
            available = ts + 300
        bar = cls(ts, available, *(float(row[key]) for key in ("open", "high", "low", "close", "volume")))
        numbers = (bar.timestamp, bar.available_at, bar.open, bar.high, bar.low, bar.close, bar.volume)
        if not all(math.isfinite(v) for v in numbers):
            raise ValueError("Non-finite bar")
        if min(bar.open, bar.high, bar.low, bar.close) <= 0 or bar.volume < 0:
            raise ValueError("Invalid bar price/volume")
        if bar.high < max(bar.open, bar.close, bar.low) or bar.low > min(bar.open, bar.close):
            raise ValueError("Inconsistent OHLC bar")
        return bar


def completed_bars(records: list[dict], now: float, *, daily: bool = True) -> list[Bar]:
    bars = [Bar.from_record(row, daily=daily) for row in records]
    timestamps = [bar.timestamp for bar in bars]
    if len(set(timestamps)) != len(timestamps):
        raise ValueError("Duplicate bar timestamps")
    return sorted((bar for bar in bars if bar.available_at <= now), key=lambda bar: bar.timestamp)
