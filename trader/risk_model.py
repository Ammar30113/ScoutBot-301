from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timezone

from core.config import get_settings
from data.price_router import PriceRouter
from strategy.technicals import passes_exit_filter, compute_atr

STOP_LOSS_PCT = 0.006
TAKE_PROFIT_PCT = 0.018
DAILY_BUDGET = float(os.getenv("DAILY_BUDGET_USD", 10000))
MAX_POSITIONS = int(os.getenv("MAX_POSITIONS", "5"))
# Allow explicit override; otherwise default to one-third of daily budget
MAX_POSITION_SIZE = float(os.getenv("MAX_POSITION_SIZE", DAILY_BUDGET / 3))
price_router = PriceRouter()
logger = logging.getLogger(__name__)
settings = get_settings()
_exit_error_counts: dict[str, tuple[int, float]] = {}
_EXIT_ERROR_LIMIT = 2
_EXIT_ERROR_RESET_SECONDS = 600


def _should_force_exit_on_error(symbol: str | None) -> bool:
    if not symbol:
        return True
    now = time.time()
    count, last_ts = _exit_error_counts.get(symbol, (0, 0.0))
    if now - last_ts > _EXIT_ERROR_RESET_SECONDS:
        count = 0
    count += 1
    _exit_error_counts[symbol] = (count, now)
    if count >= _EXIT_ERROR_LIMIT:
        _exit_error_counts.pop(symbol, None)
        return True
    return False


def _coerce_pct(value: object, fallback: float) -> float:
    try:
        pct = float(value)
    except (TypeError, ValueError):
        return fallback
    if pct <= 0 or pct >= 1:
        return fallback
    return pct


def _coerce_minutes(value: object, fallback: int) -> int:
    try:
        minutes = int(float(value))
    except (TypeError, ValueError):
        return fallback
    if minutes <= 0:
        return fallback
    return minutes


def stop_loss_price(entry_price: float, crash_mode: bool = False) -> float:
    pct = settings.crash_stop_loss_pct if crash_mode else STOP_LOSS_PCT
    return round(entry_price * (1 - pct), 2)


def take_profit_price(entry_price: float, crash_mode: bool = False) -> float:
    pct = settings.crash_take_profit_pct if crash_mode else TAKE_PROFIT_PCT
    return round(entry_price * (1 + pct), 2)


def daily_loss_exceeded(equity_return_pct: float | None) -> bool:
    if equity_return_pct is None:
        return False
    limit = float(settings.max_daily_loss_pct or 0.0)
    if limit <= 0:
        return False
    return equity_return_pct <= -limit


def _max_position_notional(equity: float | None, crash_mode: bool) -> float:
    max_positions = settings.crash_max_positions if crash_mode else MAX_POSITIONS
    max_pos_size = (DAILY_BUDGET * 0.80 / max_positions) if crash_mode else MAX_POSITION_SIZE
    if equity is None or equity <= 0:
        return max_pos_size
    pct_cap = float(settings.max_position_pct or 0.0)
    if pct_cap <= 0:
        return max_pos_size
    equity_cap = equity * pct_cap
    return min(max_pos_size, equity_cap) if max_pos_size else equity_cap


def max_position_notional(equity: float | None, crash_mode: bool = False) -> float:
    return _max_position_notional(equity, crash_mode)


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
    max_positions = settings.crash_max_positions if crash_mode else MAX_POSITIONS
    max_pos_size = _max_position_notional(equity, crash_mode)
    return current_positions < max_positions and allocation_amount <= max_pos_size


def should_exit(position: dict, crash_mode: bool = False) -> bool:
    """Determine if an open position should be closed."""
    price_raw = position.get("current_price", 0.0) if isinstance(position, dict) else getattr(position, "current_price", 0.0)
    entry_raw = position.get("entry_price", 0.0) if isinstance(position, dict) else getattr(position, "entry_price", 0.0)
    symbol = position.get("symbol") if isinstance(position, dict) else getattr(position, "symbol", None)
    data_source = position.get("data_source") if isinstance(position, dict) else getattr(position, "data_source", None)

    price = float(price_raw)
    entry = float(entry_raw)

    if not price or not entry:
        return True

    default_tp = settings.crash_take_profit_pct if crash_mode else TAKE_PROFIT_PCT
    default_sl = settings.crash_stop_loss_pct if crash_mode else STOP_LOSS_PCT
    tp_pct = _coerce_pct(position.get("take_profit_pct") if isinstance(position, dict) else None, default_tp)
    sl_pct = _coerce_pct(position.get("stop_loss_pct") if isinstance(position, dict) else None, default_sl)
    max_minutes = _coerce_minutes(
        position.get("max_hold_minutes") if isinstance(position, dict) else None,
        settings.crash_max_hold_minutes if crash_mode else settings.default_max_hold_minutes,
    )

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

    if data_source == "daily":
        return False

    if symbol:
        try:
            bars = price_router.get_aggregates(symbol, window=120)
            df = PriceRouter.aggregates_to_dataframe(bars)
            if df is not None and not df.empty:
                trailing_stop = _trailing_stop_from_bars(df, entry, entry_ts, crash_mode)
                if trailing_stop is not None and price <= trailing_stop:
                    logger.info("Trailing stop exit for %s at %.2f (trail %.2f)", symbol, price, trailing_stop)
                    return True
                if passes_exit_filter(df):
                    return True
        except Exception as e:
            logger.warning("Risk exit data error for %s: %s", symbol, e)
            return _should_force_exit_on_error(symbol)
    return False


def _trailing_stop_from_bars(
    frame,
    entry_price: float,
    entry_timestamp: float | None,
    crash_mode: bool,
) -> float | None:
    if frame is None or frame.empty:
        return None
    if entry_timestamp is not None:
        frame = frame[frame["timestamp"] >= entry_timestamp]
    if frame.empty:
        return None
    high_water = float(frame["high"].astype(float).max())
    if high_water <= entry_price:
        return None
    atr_series = compute_atr(frame, window=14)
    atr_value = float(atr_series.iloc[-1]) if len(atr_series) else 0.0
    if atr_value <= 0:
        return None
    base_trail_pct = 0.005 if crash_mode else 0.007
    max_trail_pct = 0.05 if crash_mode else 0.08
    trail_distance = atr_value * settings.atr_multiplier * 0.6
    min_distance = entry_price * base_trail_pct
    max_distance = entry_price * max_trail_pct
    distance = min(max(trail_distance, min_distance), max_distance)
    return high_water - distance
