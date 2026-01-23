from __future__ import annotations

from typing import Dict, List

import pandas as pd

from backtest.data_feed import BarDataFeed
from data.price_router import resample_to_5m


class BacktestPriceRouter:
    """Price router backed by a historical BarDataFeed."""

    def __init__(self, feed: BarDataFeed) -> None:
        self.feed = feed

    def get_price(self, symbol: str) -> float:
        price = self.feed.get_price(symbol)
        if price is None:
            raise RuntimeError(f"Backtest price unavailable for {symbol}")
        return price

    def get_aggregates(self, symbol: str, window: int = 60, *, allow_stale: bool = False) -> List[Dict[str, float]]:
        end_ts = self.feed.cursor
        start_ts = end_ts - float(window) * 60.0
        raw = self.feed.get_raw_bars(symbol, start_ts, end_ts)
        frame = resample_to_5m(raw)
        if frame is None or frame.empty:
            return []
        return frame.to_dict("records")

    def bars_age_seconds(self, bars) -> float | None:
        if not bars:
            return None
        latest = None
        for item in bars:
            if not isinstance(item, dict):
                continue
            ts = item.get("timestamp")
            if ts is None:
                continue
            try:
                ts_value = float(ts)
            except (TypeError, ValueError):
                continue
            if latest is None or ts_value > latest:
                latest = ts_value
        if latest is None:
            return None
        return max(float(self.feed.cursor) - latest, 0.0)

    def get_daily_aggregates(self, symbol: str, limit: int = 60) -> List[Dict[str, float]]:
        end_ts = self.feed.cursor
        start_ts = end_ts - float(limit) * 86400.0
        raw = self.feed.get_raw_bars(symbol, start_ts, end_ts)
        frame = pd.DataFrame(raw)
        if frame.empty:
            return []
        frame = frame.sort_values("timestamp").reset_index(drop=True)
        frame["timestamp"] = pd.to_datetime(frame["timestamp"], unit="s", utc=True, errors="coerce")
        frame = frame.dropna(subset=["timestamp"]).set_index("timestamp")
        daily = frame.resample("1D").agg(
            {
                "open": "first",
                "high": "max",
                "low": "min",
                "close": "last",
                "volume": "sum",
            }
        )
        daily = daily.dropna().reset_index()
        daily["timestamp"] = daily["timestamp"].astype("int64") // 10**9
        records = daily.to_dict("records")
        if limit and len(records) > limit:
            records = records[-limit:]
        return records
