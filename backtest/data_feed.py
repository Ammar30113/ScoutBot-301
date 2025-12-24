from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import pandas as pd

REQUIRED_COLUMNS = ("timestamp", "open", "high", "low", "close", "volume")
_TIME_COLUMNS = ("datetime", "date", "time")


def _normalize_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if "timestamp" not in frame.columns:
        for col in _TIME_COLUMNS:
            if col in frame.columns:
                dt = pd.to_datetime(frame[col], utc=True, errors="coerce")
                frame = frame.copy()
                frame["timestamp"] = (dt.view("int64") // 10**9).astype(float)
                break
    if "timestamp" not in frame.columns:
        raise ValueError("CSV must include timestamp or datetime/date/time column")

    frame = frame.copy()
    frame["timestamp"] = pd.to_numeric(frame["timestamp"], errors="coerce")
    if frame["timestamp"].max(skipna=True) > 1_000_000_000_000:
        frame["timestamp"] = frame["timestamp"] / 1000.0

    for col in REQUIRED_COLUMNS[1:]:
        if col not in frame.columns:
            raise ValueError(f"CSV missing required column '{col}'")
        frame[col] = pd.to_numeric(frame[col], errors="coerce")

    frame = frame.dropna(subset=REQUIRED_COLUMNS)
    frame = frame[frame["timestamp"] > 0]
    frame = frame.sort_values("timestamp").reset_index(drop=True)
    return frame[list(REQUIRED_COLUMNS)]


def load_bars_csv(path: Path, symbol: Optional[str] = None) -> pd.DataFrame:
    frame = pd.read_csv(path)
    if symbol and "symbol" in frame.columns:
        frame = frame[frame["symbol"].astype(str).str.upper() == symbol.upper()]
    return _normalize_frame(frame)


def load_bars_directory(path: Path) -> Dict[str, pd.DataFrame]:
    frames: Dict[str, pd.DataFrame] = {}
    for file_path in sorted(path.glob("*.csv")):
        symbol = file_path.stem.upper()
        frame = load_bars_csv(file_path, symbol=symbol)
        if not frame.empty:
            frames[symbol] = frame
    return frames


@dataclass
class BarDataFeed:
    data: Dict[str, pd.DataFrame]
    cursor: float = 0.0

    def set_cursor(self, timestamp: float) -> None:
        self.cursor = float(timestamp)

    def available_range(self) -> tuple[Optional[float], Optional[float]]:
        if not self.data:
            return None, None
        starts = [float(frame["timestamp"].iloc[0]) for frame in self.data.values() if not frame.empty]
        ends = [float(frame["timestamp"].iloc[-1]) for frame in self.data.values() if not frame.empty]
        if not starts or not ends:
            return None, None
        return min(starts), max(ends)

    def get_price(self, symbol: str) -> Optional[float]:
        frame = self.data.get(symbol.upper())
        if frame is None or frame.empty:
            return None
        eligible = frame[frame["timestamp"] <= self.cursor]
        if eligible.empty:
            return None
        return float(eligible.iloc[-1]["close"])

    def get_raw_bars(self, symbol: str, start_ts: float, end_ts: float) -> List[Dict[str, float]]:
        frame = self.data.get(symbol.upper())
        if frame is None or frame.empty:
            return []
        mask = (frame["timestamp"] >= start_ts) & (frame["timestamp"] <= end_ts)
        if not mask.any():
            return []
        return frame.loc[mask, REQUIRED_COLUMNS].to_dict("records")

    def symbols(self) -> Iterable[str]:
        return list(self.data.keys())
