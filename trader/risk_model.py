from __future__ import annotations

import datetime as dt
import os
from typing import Optional

from strategy.technicals import passes_exit_filter
from data.price_router import PriceRouter

STOP_LOSS_PCT = 0.03
TAKE_PROFIT_PCT = 0.08
MAX_POSITIONS = 5
DAILY_BUDGET = float(os.getenv("DAILY_BUDGET_USD", 10000))
MAX_POSITION_SIZE = DAILY_BUDGET / 3
price_router = PriceRouter()


def stop_loss_price(entry_price: float) -> float:
    return round(entry_price * (1 - STOP_LOSS_PCT), 2)


def take_profit_price(entry_price: float) -> float:
    return round(entry_price * (1 + 0.05), 2)  # default TP at 5%; exit logic uses 8% hard stop


def can_open_position(current_positions: int, allocation_amount: float) -> bool:
    return current_positions < MAX_POSITIONS and allocation_amount <= MAX_POSITION_SIZE


def should_exit(position: dict) -> bool:
    """
    position: {"entry_price": float, "current_price": float, "open_date": iso str, "symbol": str}
    """
    price = float(position.get("current_price", 0.0))
    entry = float(position.get("entry_price", 0.0))
    open_date = position.get("open_date")
    symbol = position.get("symbol")

    if not price or not entry:
        return True

    # Time-based exit after 5 trading days
    if open_date:
        try:
            opened = dt.datetime.fromisoformat(open_date).date()
            if (dt.date.today() - opened).days >= 5:
                return True
        except Exception:
            return True

    change = (price - entry) / entry
    if change <= -STOP_LOSS_PCT or change >= TAKE_PROFIT_PCT:
        return True

    if symbol:
        try:
            bars = price_router.get_aggregates(symbol, "1day", 60)
            df = PriceRouter.aggregates_to_dataframe(bars)
            if passes_exit_filter(df):
                return True
        except Exception:
            return False
    return False
