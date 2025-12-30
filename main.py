import logging
import time
from datetime import datetime, time as dt_time, timezone

import pytz

from core.config import get_settings
from universe.universe_builder import get_universe
from strategy.signal_router import route_signals
from trader.allocation import allocate_positions
from trader.execution_adapter import execute_signals, close_position, list_positions, trading_client
from trader import risk_model
from data.price_router import PriceRouter
from strategy.crash_detector import get_crash_state
from trader.pnl_tracker import update_daily_pnl
from data.portfolio_state import sync_entry_timestamps
from types import SimpleNamespace

logging.basicConfig(level=logging.INFO, format="%Y-%m-%d %H:%M:%S | %(levelname)s | %(name)s | %(message)s")
logger = logging.getLogger(__name__)
price_router = PriceRouter()
context = SimpleNamespace()
settings = get_settings()


def market_open_now() -> bool:
    est = pytz.timezone("America/New_York")
    now = datetime.now(est)
    if now.weekday() >= 5:
        return False
    if trading_client is not None:
        try:
            clock = trading_client.get_clock()
            is_open = getattr(clock, "is_open", None)
            if is_open is not None:
                return bool(is_open)
        except Exception as exc:  # pragma: no cover - network guard
            logger.warning("Market clock unavailable; falling back to local time: %s", exc)
    now_time = now.time()
    market_open = dt_time(9, 30)
    market_close = dt_time(16, 0)
    return market_open <= now_time <= market_close


def microcap_cycle():
    while True:
        start = time.time()
        try:
            if not market_open_now():
                logger.info("Market closed — skipping cycle")
                continue
            # Compute P&L once per cycle
            pnl_state = update_daily_pnl(trading_client)
            pnl_penalty = 0.0
            equity_return_pct = None
            equity_value = None
            trade_allowed = True

            if pnl_state:
                equity_return_pct = pnl_state.equity_return_pct
                equity_value = pnl_state.equity
                if pnl_state.equity_return_pct < -0.01:
                    pnl_penalty = 0.05
                elif pnl_state.equity_return_pct > 0.02:
                    pnl_penalty = -0.03
                if risk_model.daily_loss_exceeded(pnl_state.equity_return_pct):
                    trade_allowed = False
                    logger.warning(
                        "Daily loss limit reached (return %.3f <= -%.3f); blocking new entries",
                        pnl_state.equity_return_pct,
                        settings.max_daily_loss_pct,
                    )

            # Pass penalty into signal router
            context.pnl_penalty = pnl_penalty
            logger.info(f"P&L penalty for this cycle: {pnl_penalty}")
            crash, drop, data_age = get_crash_state()
            if data_age is None:
                logger.warning("Intraday data check failed; crash gate disabled for this cycle")
                if settings.require_crash_data:
                    continue
                crash = False
                drop = 0.0
            elif data_age > settings.intraday_stale_seconds:
                logger.warning(
                    "Intraday data stale (age %.1f min > %.1f min); crash gate disabled for this cycle",
                    data_age / 60.0,
                    settings.intraday_stale_seconds / 60.0,
                )
                if settings.require_crash_data:
                    continue
                crash = False
                drop = 0.0
            logger.info("Crash mode = %s (SPY 5min drop = %.3f)", crash, drop)
            logger.info("=== Crash Mode %s ===", "ACTIVE" if crash else "OFF")

            if trade_allowed:
                universe = get_universe()
                if not universe:
                    logger.info("Universe empty; skipping cycle")
                    continue

                signals = route_signals(universe, crash_mode=crash, context=context)
                if not signals:
                    logger.info("No signals generated; skipping allocations")
                    continue
                allocations = allocate_positions(signals, crash_mode=crash)

                # Enforce max position caps before submitting
                filtered_allocations = {}
                open_positions = list_positions()
                open_count = len(open_positions)
                for symbol, shares in allocations.items():
                    try:
                        price = price_router.get_price(symbol)
                    except Exception as exc:  # pragma: no cover - network guard
                        logger.warning("Skipping %s for risk check; price unavailable: %s", symbol, exc)
                        continue
                    notional = shares * price
                    if risk_model.can_open_position(
                        open_count + len(filtered_allocations),
                        notional,
                        crash_mode=crash,
                        equity=equity_value,
                        equity_return_pct=equity_return_pct,
                    ):
                        filtered_allocations[symbol] = shares
                    else:
                        logger.info("Risk cap blocked %s (notional %.2f)", symbol, notional)

                signal_map = {
                    sig["symbol"]: sig for sig in signals if isinstance(sig, dict) and sig.get("symbol")
                }
                trade_signals = []
                for symbol, shares in filtered_allocations.items():
                    metadata = signal_map.get(symbol, {})
                    trade_signals.append(
                        {
                            "symbol": symbol,
                            "action": "BUY",
                            "requested_qty": shares,
                            "reason": metadata.get("reason") or metadata.get("type"),
                            "score": metadata.get("score"),
                        }
                    )
                execute_signals(trade_signals, crash_mode=crash)

            # Exit checks for existing positions
            open_positions = list_positions()
            entry_ts_map = sync_entry_timestamps(
                [pos.symbol for pos in open_positions],
                datetime.now(timezone.utc).timestamp(),
            )
            for pos in open_positions:
                symbol_key = pos.symbol.upper()
                try:
                    current_price = float(pos.current_price)
                    entry_price = float(pos.avg_entry_price)
                except Exception:
                    continue
                position_payload = {
                    "symbol": pos.symbol,
                    "current_price": current_price,
                    "entry_price": entry_price,
                    "entry_timestamp": entry_ts_map.get(symbol_key),
                }
                if risk_model.should_exit(position_payload, crash_mode=crash):
                    entry_ts = entry_ts_map.get(symbol_key)
                    exit_reason = "technical_exit"
                    if not current_price or not entry_price:
                        exit_reason = "invalid_price"
                    else:
                        gain = (current_price / entry_price) - 1
                        tp_pct = 0.015 if crash else risk_model.TAKE_PROFIT_PCT
                        sl_pct = 0.005 if crash else risk_model.STOP_LOSS_PCT
                        max_minutes = 60 if crash else 90
                        if gain >= tp_pct:
                            exit_reason = "take_profit"
                        elif gain <= -sl_pct:
                            exit_reason = "stop_loss"
                        elif entry_ts is not None:
                            try:
                                elapsed = (datetime.now(timezone.utc).timestamp() - float(entry_ts)) / 60
                                if elapsed >= max_minutes:
                                    exit_reason = "time_stop"
                            except (TypeError, ValueError):
                                exit_reason = "technical_exit"
                    close_position(pos.symbol, reason=exit_reason)

            logger.info("=== Cycle Complete ===")
            # After finishing a cycle:
            update_daily_pnl(trading_client)
            logger.info("Daily P/L updated.")
        except Exception as exc:  # pragma: no cover - defensive loop
            logger.exception("Cycle failed: %s", exc)
        finally:
            elapsed = time.time() - start
            interval = max(settings.scheduler_interval_seconds, 1)
            sleep_for = max(interval - elapsed, 0)
            time.sleep(sleep_for)


if __name__ == "__main__":
    microcap_cycle()
