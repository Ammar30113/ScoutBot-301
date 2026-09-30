"""Explicit, deterministic research baseline; no learned or sentiment inputs."""
from dataclasses import dataclass
from statistics import mean
from typing import Literal

from data.bars import Bar


@dataclass(frozen=True)
class Decision:
    action: Literal["BUY", "SELL", "HOLD"]
    reason: str
    stop_loss_pct: float = 0.05
    take_profit_pct: float = 0.10


def decide(bars: list[Bar], *, held: bool, trend_days: int = 200) -> Decision:
    if len(bars) < trend_days + 1:
        return Decision("HOLD", "insufficient_history")
    closes = [bar.close for bar in bars]
    average = mean(closes[-trend_days:])
    # Fixed 20-session momentum confirmation, avoiding threshold search at runtime.
    rising = closes[-1] > closes[-21]
    if held and closes[-1] < average:
        return Decision("SELL", "below_trend")
    if not held and closes[-1] > average and rising:
        return Decision("BUY", "above_trend_positive_momentum")
    return Decision("HOLD", "no_entry_or_exit")
