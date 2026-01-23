import json
from dataclasses import dataclass, asdict, field, fields
from pathlib import Path
from typing import Dict, Iterable, Optional

from core.config import get_settings

STATE_PATH = get_settings().portfolio_state_path


@dataclass
class PortfolioState:
    equity: float = 0.0
    prior_equity: float = 0.0
    realized_pnl: float = 0.0
    unrealized_pnl: float = 0.0
    realized_pct: float = 0.0
    unrealized_pct: float = 0.0
    equity_return_pct: float = 0.0
    day_start_equity: float = 0.0
    day_start_date: str = ""
    entry_timestamps: Dict[str, float] = field(default_factory=dict)

    def to_dict(self):
        return asdict(self)


def load_state() -> PortfolioState:
    if not STATE_PATH.exists():
        return PortfolioState()
    try:
        with open(STATE_PATH, "r") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return PortfolioState()
        allowed = {item.name for item in fields(PortfolioState)}
        filtered = {key: value for key, value in data.items() if key in allowed}
        state = PortfolioState(**filtered)
        if not isinstance(state.entry_timestamps, dict):
            state.entry_timestamps = {}
        return state
    except Exception:
        return PortfolioState()


def save_state(state: PortfolioState):
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(STATE_PATH, "w") as f:
        json.dump(state.to_dict(), f, indent=2)


def get_entry_timestamp(symbol: str) -> Optional[float]:
    state = load_state()
    return state.entry_timestamps.get(symbol.upper())


def set_entry_timestamp(symbol: str, timestamp: float) -> None:
    state = load_state()
    key = symbol.upper()
    if state.entry_timestamps.get(key) == timestamp:
        return
    state.entry_timestamps[key] = float(timestamp)
    save_state(state)


def clear_entry_timestamp(symbol: str) -> None:
    state = load_state()
    key = symbol.upper()
    if key in state.entry_timestamps:
        state.entry_timestamps.pop(key, None)
        save_state(state)


def sync_entry_timestamps(open_symbols: Iterable[str], default_timestamp: float | None = None) -> Dict[str, float]:
    state = load_state()
    open_set = {sym.upper() for sym in open_symbols if sym}
    current: Dict[str, float] = {}
    for sym, ts in (state.entry_timestamps or {}).items():
        sym_u = sym.upper()
        if sym_u in open_set:
            try:
                current[sym_u] = float(ts)
            except (TypeError, ValueError):
                continue
    changed = current != (state.entry_timestamps or {})
    for sym in open_set:
        if sym not in current:
            if default_timestamp is None:
                continue
            current[sym] = float(default_timestamp)
            changed = True
    if changed:
        state.entry_timestamps = current
        save_state(state)
    return current
