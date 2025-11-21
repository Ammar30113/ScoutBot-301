from __future__ import annotations

import datetime as dt
import os
from typing import Optional

from strategy.technicals import passes_exit_filter
from data.price_router import PriceRouter

STOP_LOSS_PCT = 0.006
TAKE_PROFIT_PCT = 0.018
MAX_POSITIONS = 5
DAILY_BUDGET = float(os.getenv("DAILY_BUDGET_USD", 10000))
MAX_POSITION_SIZE = DAILY_BUDGET / 3
price_router = PriceRouter()


def stop_loss_price(entry_price: float) -> float:
    return round(entry_price * (1 - STOP_LOSS_PCT), 2)


def take_profit_price(entry_price: float) -> float:
    return round(entry_price * (1 + TAKE_PROFIT_PCT), 2)


def can_open_position(current_positions: int, allocation_amount: float) -> bool:
    return current_positions < MAX_POSITIONS and allocation_amount <= MAX_POSITION_SIZE


def should_exit(position: dict) -> bool:
    """
    position: {"entry_price": float, "current_price": float, "open_date": iso str, "symbol": str}
    Intraday exit profile: TP +1.8%, SL -0.6%, hard time cap at 90 minutes.
    """
    price = float(position.get("current_price", 0.0))
    entry = float(position.get("entry_price", 0.0))
    open_date = position.get("open_date") or position.get("entered_at")
    symbol = position.get("symbol")

    if not price or not entry:
        return True

    gain = (price / entry) - 1
    if gain >= TAKE_PROFIT_PCT or gain <= -STOP_LOSS_PCT:
        return True

    if open_date:
        try:
            cleaned_date = open_date.replace("Z", "+00:00") if isinstance(open_date, str) else open_date
            opened_at = dt.datetime.fromisoformat(cleaned_date)
            minutes_held = (dt.datetime.utcnow() - opened_at).total_seconds() / 60.0
            if minutes_held >= 90:
                return True
        except Exception:
            return True

    if symbol:
        try:
            bars = price_router.get_aggregates(symbol, window=120)
            df = PriceRouter.aggregates_to_dataframe(bars)
            if passes_exit_filter(df):
                return True
        except Exception:
            return False
    return False
