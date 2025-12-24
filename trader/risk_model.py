from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

from core.config import get_settings
from data.price_router import PriceRouter
from strategy.technicals import passes_exit_filter

STOP_LOSS_PCT = 0.006
TAKE_PROFIT_PCT = 0.018
DAILY_BUDGET = float(os.getenv("DAILY_BUDGET_USD", 10000))
MAX_POSITIONS = int(os.getenv("MAX_POSITIONS", "5"))
# Allow explicit override; otherwise default to one-third of daily budget
MAX_POSITION_SIZE = float(os.getenv("MAX_POSITION_SIZE", DAILY_BUDGET / 3))
price_router = PriceRouter()
logger = logging.getLogger(__name__)
settings = get_settings()


def stop_loss_price(entry_price: float, crash_mode: bool = False) -> float:
    pct = 0.005 if crash_mode else STOP_LOSS_PCT
    return round(entry_price * (1 - pct), 2)


def take_profit_price(entry_price: float, crash_mode: bool = False) -> float:
    pct = 0.015 if crash_mode else TAKE_PROFIT_PCT
    return round(entry_price * (1 + pct), 2)


def daily_loss_exceeded(equity_return_pct: float | None) -> bool:
    if equity_return_pct is None:
        return False
    limit = float(settings.max_daily_loss_pct or 0.0)
    if limit <= 0:
        return False
    return equity_return_pct <= -limit


def _max_position_notional(equity: float | None, crash_mode: bool) -> float:
    max_positions = 3 if crash_mode else MAX_POSITIONS
    max_pos_size = (DAILY_BUDGET * 0.80 / max_positions) if crash_mode else MAX_POSITION_SIZE
    if equity is None or equity <= 0:
        return max_pos_size
    pct_cap = float(settings.max_position_pct or 0.0)
    if pct_cap <= 0:
        return max_pos_size
    equity_cap = equity * pct_cap
    return min(max_pos_size, equity_cap) if max_pos_size else equity_cap


def can_open_position(
    current_positions: int,
    allocation_amount: float,
    crash_mode: bool = False,
    *,
    equity: float | None = None,
    equity_return_pct: float | None = None,
) -> bool:
    if daily_loss_exceeded(equity_return_pct):
        return False
    max_positions = 3 if crash_mode else MAX_POSITIONS
    max_pos_size = _max_position_notional(equity, crash_mode)
    return current_positions < max_positions and allocation_amount <= max_pos_size


def should_exit(position: dict, crash_mode: bool = False) -> bool:
    """Determine if an open position should be closed."""
    price_raw = position.get("current_price", 0.0) if isinstance(position, dict) else getattr(position, "current_price", 0.0)
    entry_raw = position.get("entry_price", 0.0) if isinstance(position, dict) else getattr(position, "entry_price", 0.0)
    symbol = position.get("symbol") if isinstance(position, dict) else getattr(position, "symbol", None)

    price = float(price_raw)
    entry = float(entry_raw)

    if not price or not entry:
        return True

    tp_pct = 0.015 if crash_mode else TAKE_PROFIT_PCT
    sl_pct = 0.005 if crash_mode else STOP_LOSS_PCT
    max_minutes = 60 if crash_mode else 90

    gain = (price / entry) - 1
    if gain >= tp_pct or gain <= -sl_pct:
        return True

    # NEW time-based exit logic
    entry_timestamp = position.entry_timestamp if hasattr(position, "entry_timestamp") else position.get("entry_timestamp")
    if entry_timestamp is None:
        return False  # don't exit if we don't know when trade opened

    try:
        entry_ts = float(entry_timestamp)
    except (TypeError, ValueError):
        logger.warning("Invalid entry_timestamp for %s; skipping time-stop", symbol)
        return False

    elapsed_minutes = (datetime.now(timezone.utc).timestamp() - entry_ts) / 60

    if elapsed_minutes >= max_minutes:
        logger.info("Time-stop exit triggered for %s after %.1f minutes", symbol, elapsed_minutes)
        return True

    if symbol:
        try:
            bars = price_router.get_aggregates(symbol, window=120)
            df = PriceRouter.aggregates_to_dataframe(bars)
            if passes_exit_filter(df):
                return True
        except Exception as e:
            logger.warning("Risk exit forced due to price error: %s", e)
            return True  # FORCE EXIT when price unavailable
    return False
