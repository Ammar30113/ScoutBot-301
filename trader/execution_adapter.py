from __future__ import annotations

import logging
import math
import time
from datetime import datetime, timezone
from typing import Literal, TypedDict

from alpaca.trading.enums import OrderClass, OrderSide, TimeInForce
from alpaca.trading.requests import MarketOrderRequest, StopLossRequest, TakeProfitRequest

from core.config import get_settings
from data.portfolio_state import (
    clear_entry_metadata,
    clear_entry_timestamp,
    set_entry_metadata,
    set_entry_timestamp,
)
from data.price_router import PriceRouter
from trader.order_executor import trading_client
from trader.position_sizer import size_position
from trader.risk_model import MAX_POSITIONS, max_position_notional, stop_loss_price, take_profit_price
from trader.trade_logger import log_trade

logger = logging.getLogger(__name__)
settings = get_settings()
price_router = PriceRouter()

_halt_new_entries = False
_halt_reason = ""
_halt_until = 0.0
_pending_entries: dict[str, dict[str, object]] = {}
_pending_entry_ttl_seconds = 3600
_fill_slippage_warn_pct = 0.003


class TradeSignal(TypedDict, total=False):
    symbol: str
    action: Literal["BUY", "SELL", "CLOSE"]
    reason: str
    score: float
    requested_qty: int
    entry_price: float
    stop_loss_pct: float
    take_profit_pct: float
    stop_loss_price: float
    take_profit_price: float
    max_risk_pct: float


class ExecutionResult(TypedDict):
    symbol: str
    action: str
    submitted: bool
    skipped: bool
    order_id: str | None
    reason: str | None


def _set_halt(reason: str) -> None:
    global _halt_new_entries, _halt_reason, _halt_until
    _halt_new_entries = True
    _halt_reason = reason
    cooldown = max(int(settings.execution_halt_cooldown_seconds or 0), 0)
    _halt_until = time.time() + cooldown if cooldown else 0.0
    if cooldown:
        logger.error("Execution halted: %s (cooldown=%ss)", reason, cooldown)
    else:
        logger.error("Execution halted: %s", reason)


def _reset_halt_if_ready() -> None:
    global _halt_new_entries, _halt_reason, _halt_until
    if not _halt_new_entries:
        return
    if _halt_until and time.time() >= _halt_until:
        _halt_new_entries = False
        _halt_reason = ""
        _halt_until = 0.0
        logger.warning("Execution halt cleared; resuming new entries")


def _coerce_timestamp(value) -> float | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.timestamp()
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        if raw.endswith("Z"):
            raw = raw.replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(raw).timestamp()
        except ValueError:
            return None
    return None


def _coerce_float(value) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed):
        return None
    return parsed


def _track_pending_entry(
    order_id: str | None,
    symbol: str,
    expected_price: float | None,
    entry_metadata: dict[str, object] | None,
) -> None:
    if not order_id:
        return
    if not _can_fetch_orders():
        logger.warning("Order lookup unavailable; using submit timestamp for %s", symbol)
        set_entry_timestamp(symbol, datetime.now(timezone.utc).timestamp())
        if entry_metadata:
            set_entry_metadata(symbol, entry_metadata)
        return
    _pending_entries[order_id] = {
        "symbol": symbol,
        "submitted_at": time.time(),
        "expected_price": expected_price,
        "entry_metadata": entry_metadata or {},
    }


def _can_fetch_orders() -> bool:
    if trading_client is None:
        return False
    return callable(getattr(trading_client, "get_order_by_id", None)) or callable(getattr(trading_client, "get_order", None))


def _fetch_order(order_id: str):
    if trading_client is None:
        return None
    getter = getattr(trading_client, "get_order_by_id", None)
    if callable(getter):
        try:
            return getter(order_id)
        except Exception as exc:  # pragma: no cover - network guard
            logger.warning("Unable to fetch order %s: %s", order_id, exc)
            return None
    getter = getattr(trading_client, "get_order", None)
    if callable(getter):
        try:
            return getter(order_id)
        except Exception as exc:  # pragma: no cover - network guard
            logger.warning("Unable to fetch order %s: %s", order_id, exc)
            return None
    return None


def reconcile_pending_entries() -> None:
    if trading_client is None or not _pending_entries:
        return
    now = time.time()
    for order_id, payload in list(_pending_entries.items()):
        submitted_at = _coerce_float(payload.get("submitted_at")) or 0.0
        if submitted_at and (now - submitted_at) > _pending_entry_ttl_seconds:
            logger.warning("Pending entry %s expired after %.1f minutes", order_id, (now - submitted_at) / 60.0)
            _pending_entries.pop(order_id, None)
            continue
        order = _fetch_order(order_id)
        if order is None:
            continue
        status = str(getattr(order, "status", "") or "").lower()
        filled_at = _coerce_timestamp(getattr(order, "filled_at", None))
        if filled_at is None and status in ("filled", "partially_filled"):
            filled_at = now
        if filled_at is not None:
            symbol = str(payload.get("symbol") or "").upper()
            if symbol:
                set_entry_timestamp(symbol, filled_at)
                entry_metadata = payload.get("entry_metadata")
                if isinstance(entry_metadata, dict) and entry_metadata:
                    set_entry_metadata(symbol, entry_metadata)
                fill_price = _coerce_float(getattr(order, "filled_avg_price", None))
                if fill_price is not None:
                    expected_price = _coerce_float(payload.get("expected_price"))
                    if expected_price and expected_price > 0:
                        slippage_pct = abs(fill_price - expected_price) / expected_price
                        if slippage_pct >= _fill_slippage_warn_pct:
                            logger.warning(
                                "Fill slippage for %s: expected %.4f got %.4f (%.2f%%)",
                                symbol,
                                expected_price,
                                fill_price,
                                slippage_pct * 100.0,
                            )
                    log_trade(
                        {
                            "symbol": symbol,
                            "event": "fill",
                            "status": "filled",
                            "price": fill_price,
                            "order_id": order_id,
                        }
                    )
            _pending_entries.pop(order_id, None)
            continue
        if status in ("canceled", "rejected", "expired"):
            _pending_entries.pop(order_id, None)


def _log_skip(symbol: str, action: str, reason: str | None) -> ExecutionResult:
    payload_reason = reason or "skipped"
    log_trade(
        {
            "symbol": symbol,
            "action": action,
            "status": "skipped",
            "reason": payload_reason,
        }
    )
    if reason:
        logger.info("Skipping %s %s: %s", action, symbol, reason)
    else:
        logger.info("Skipping %s %s", action, symbol)
    return {
        "symbol": symbol,
        "action": action,
        "submitted": False,
        "skipped": True,
        "order_id": None,
        "reason": reason,
    }


def _safe_list_positions() -> dict[str, object] | None:
    if trading_client is None:
        return {}
    try:
        return {pos.symbol: pos for pos in trading_client.get_all_positions()}
    except Exception as exc:  # pragma: no cover - network guard
        _set_halt(f"alpaca_list_positions_failed: {exc}")
        return None


def _safe_get_account():
    if trading_client is None:
        return None
    try:
        return trading_client.get_account()
    except Exception as exc:  # pragma: no cover - network guard
        _set_halt(f"alpaca_get_account_failed: {exc}")
        return None


def execute_signals(signals: list[TradeSignal], *, crash_mode: bool = False) -> list[ExecutionResult]:
    if not signals:
        logger.info("No signals to execute")
        return []
    _reset_halt_if_ready()
    results: list[ExecutionResult] = []
    for signal in signals:
        results.append(execute_signal(signal, crash_mode=crash_mode))
    return results


def execute_signal(signal: TradeSignal, *, crash_mode: bool = False) -> ExecutionResult:
    symbol = (signal.get("symbol") or "").upper()
    action_raw = signal.get("action") or "BUY"
    action = str(action_raw).upper()
    reason = signal.get("reason")

    if not symbol:
        return _log_skip("", action, "missing_symbol")

    _reset_halt_if_ready()
    if action in ("SELL", "CLOSE"):
        close_reason = reason or ("sell_signal_close_only" if action == "SELL" else None)
        return close_position(symbol, reason=close_reason)

    if action != "BUY":
        return _log_skip(symbol, action, "unsupported_action")

    if trading_client is None:
        return _log_skip(symbol, action, "trading_client_unavailable")

    if _halt_new_entries:
        return _log_skip(symbol, action, _halt_reason or "alpaca_api_error")

    open_positions = _safe_list_positions()
    if open_positions is None:
        return _log_skip(symbol, action, _halt_reason or "alpaca_api_error")
    max_positions = 3 if crash_mode else MAX_POSITIONS
    if len(open_positions) >= max_positions:
        return _log_skip(symbol, action, "max_positions_reached")
    if symbol in open_positions:
        return _log_skip(symbol, action, "position_exists")

    entry_price = signal.get("entry_price")
    if entry_price is None:
        try:
            entry_price = price_router.get_price(symbol)
        except Exception as exc:  # pragma: no cover - network guard
            return _log_skip(symbol, action, f"price_unavailable:{exc}")
    try:
        entry_price = float(entry_price)
    except (TypeError, ValueError):
        return _log_skip(symbol, action, "invalid_entry_price")
    if entry_price <= 0:
        return _log_skip(symbol, action, "invalid_entry_price")

    stop_loss = signal.get("stop_loss_price")
    take_profit = signal.get("take_profit_price")
    stop_loss_pct = signal.get("stop_loss_pct")
    take_profit_pct = signal.get("take_profit_pct")
    if stop_loss is None and stop_loss_pct is not None:
        try:
            pct = float(stop_loss_pct)
            if 0 < pct < 1:
                stop_loss = round(entry_price * (1 - pct), 2)
        except (TypeError, ValueError):
            stop_loss = None
    if take_profit is None and take_profit_pct is not None:
        try:
            pct = float(take_profit_pct)
            if 0 < pct < 1:
                take_profit = round(entry_price * (1 + pct), 2)
        except (TypeError, ValueError):
            take_profit = None
    if stop_loss is None:
        stop_loss = stop_loss_price(entry_price, crash_mode=crash_mode)
    if take_profit is None:
        take_profit = take_profit_price(entry_price, crash_mode=crash_mode)
    try:
        stop_loss = float(stop_loss)
        take_profit = float(take_profit)
    except (TypeError, ValueError):
        return _log_skip(symbol, action, "invalid_bracket")

    if stop_loss <= 0 or take_profit <= 0 or stop_loss >= entry_price or take_profit <= entry_price:
        return _log_skip(symbol, action, "invalid_bracket")

    entry_metadata: dict[str, object] = {}
    calc_stop_pct = (entry_price - stop_loss) / entry_price if entry_price > 0 else None
    calc_tp_pct = (take_profit - entry_price) / entry_price if entry_price > 0 else None
    if calc_stop_pct is not None and calc_stop_pct > 0:
        entry_metadata["stop_loss_pct"] = calc_stop_pct
    if calc_tp_pct is not None and calc_tp_pct > 0:
        entry_metadata["take_profit_pct"] = calc_tp_pct
    if signal.get("max_hold_minutes") is not None:
        entry_metadata["max_hold_minutes"] = signal.get("max_hold_minutes")
    if signal.get("data_source"):
        entry_metadata["data_source"] = signal.get("data_source")

    account = _safe_get_account()
    if account is None:
        return _log_skip(symbol, action, _halt_reason or "alpaca_api_error")

    try:
        equity = float(account.equity)
    except Exception:
        equity = None

    try:
        buying_power = float(account.buying_power)
    except Exception:
        buying_power = None

    max_notional = max_position_notional(equity, crash_mode=crash_mode)
    max_risk_pct = float(signal.get("max_risk_pct") or settings.max_risk_pct or 0.0)
    if crash_mode:
        max_risk_pct *= 0.5

    sized_qty = size_position(
        entry_price,
        stop_loss,
        equity=equity,
        max_risk_pct=max_risk_pct,
        max_notional=max_notional,
        min_qty=1,
    )
    requested_qty = int(signal.get("requested_qty") or 0)
    qty = min(requested_qty, sized_qty) if requested_qty > 0 else sized_qty
    if qty <= 0:
        return _log_skip(symbol, action, "risk_sizing_blocked")

    notional = entry_price * qty
    if buying_power is None or buying_power <= 0:
        return _log_skip(symbol, action, "buying_power_unavailable")
    if notional > buying_power:
        return _log_skip(symbol, action, "buying_power_insufficient")

    if settings.dry_run:
        log_trade(
            {
                "symbol": symbol,
                "action": action,
                "status": "dry_run",
                "qty": qty,
                "price": entry_price,
                "stop_loss": stop_loss,
                "take_profit": take_profit,
                "reason": reason,
            }
        )
        logger.info("Dry-run: would submit BUY for %s qty=%s", symbol, qty)
        return {
            "symbol": symbol,
            "action": action,
            "submitted": False,
            "skipped": True,
            "order_id": None,
            "reason": "dry_run",
        }

    order = MarketOrderRequest(
        symbol=symbol,
        qty=qty,
        side=OrderSide.BUY,
        time_in_force=TimeInForce.DAY,
        order_class=OrderClass.BRACKET,
        take_profit=TakeProfitRequest(limit_price=take_profit),
        stop_loss=StopLossRequest(stop_price=stop_loss),
    )
    try:
        submitted = trading_client.submit_order(order)
        order_id = getattr(submitted, "id", None)
        status = str(getattr(submitted, "status", "") or "").lower()
        filled_at = _coerce_timestamp(getattr(submitted, "filled_at", None))
        if filled_at is not None or status == "filled":
            set_entry_timestamp(symbol, filled_at or datetime.now(timezone.utc).timestamp())
            if entry_metadata:
                set_entry_metadata(symbol, entry_metadata)
        else:
            _track_pending_entry(order_id, symbol, entry_price, entry_metadata)
        log_trade(
            {
                "symbol": symbol,
                "action": action,
                "status": "submitted",
                "qty": qty,
                "price": entry_price,
                "stop_loss": stop_loss,
                "take_profit": take_profit,
                "order_id": order_id,
                "reason": reason,
            }
        )
        logger.info("Submitted bracket order for %s qty=%s tp=%.2f sl=%.2f", symbol, qty, take_profit, stop_loss)
        return {
            "symbol": symbol,
            "action": action,
            "submitted": True,
            "skipped": False,
            "order_id": order_id,
            "reason": None,
        }
    except Exception as exc:  # pragma: no cover - network guard
        _set_halt(f"alpaca_submit_failed: {exc}")
        return _log_skip(symbol, action, _halt_reason or "alpaca_submit_failed")


def close_position(symbol: str, *, reason: str | None = None) -> ExecutionResult:
    symbol = symbol.upper()
    if not symbol:
        return _log_skip("", "CLOSE", "missing_symbol")
    if trading_client is None:
        return _log_skip(symbol, "CLOSE", "trading_client_unavailable")
    if settings.dry_run:
        log_trade(
            {
                "symbol": symbol,
                "action": "CLOSE",
                "status": "dry_run",
                "reason": reason,
            }
        )
        logger.info("Dry-run: would close %s", symbol)
        return {
            "symbol": symbol,
            "action": "CLOSE",
            "submitted": False,
            "skipped": True,
            "order_id": None,
            "reason": "dry_run",
        }

    positions = _safe_list_positions()
    if positions is None:
        return _log_skip(symbol, "CLOSE", _halt_reason or "alpaca_api_error")

    pos = positions.get(symbol)
    if not pos:
        return _log_skip(symbol, "CLOSE", "no_position")
    try:
        qty = float(pos.qty)
        held = float(getattr(pos, "held_for_orders", 0) or 0)
    except Exception:
        return _log_skip(symbol, "CLOSE", "position_parse_error")

    if qty <= 0 or held >= qty:
        return _log_skip(symbol, "CLOSE", "position_held")

    entry_price = None
    current_price = None
    try:
        entry_price = float(pos.avg_entry_price)
    except Exception:
        entry_price = None
    try:
        current_price = float(pos.current_price)
    except Exception:
        current_price = None

    if current_price is None:
        try:
            current_price = float(price_router.get_price(symbol))
        except Exception:
            current_price = None

    pnl = None
    pnl_pct = None
    if entry_price is not None and current_price is not None:
        pnl = (current_price - entry_price) * qty
        if entry_price > 0:
            pnl_pct = (current_price / entry_price) - 1

    try:
        trading_client.close_position(symbol)
        clear_entry_timestamp(symbol)
        clear_entry_metadata(symbol)
        log_trade(
            {
                "symbol": symbol,
                "action": "CLOSE",
                "status": "closed",
                "qty": qty,
                "price": current_price,
                "entry_price": entry_price,
                "pnl": pnl,
                "pnl_pct": pnl_pct,
                "reason": reason,
            }
        )
        logger.info("Closed position for %s", symbol)
        return {
            "symbol": symbol,
            "action": "CLOSE",
            "submitted": True,
            "skipped": False,
            "order_id": None,
            "reason": None,
        }
    except Exception as exc:  # pragma: no cover - network guard
        msg = str(exc).lower()
        benign_markers = ("insufficient qty", "insufficient quantity", "no position", "position does not exist")
        if any(marker in msg for marker in benign_markers):
            return _log_skip(symbol, "CLOSE", "position_unavailable")
        _set_halt(f"alpaca_close_failed: {exc}")
        return _log_skip(symbol, "CLOSE", _halt_reason or "alpaca_close_failed")


def list_positions():
    if trading_client is None:
        logger.warning("Trading client unavailable; cannot list positions.")
        return []
    try:
        return trading_client.get_all_positions()
    except Exception as exc:  # pragma: no cover - network guard
        logger.warning("Unable to list positions: %s", exc)
        return []
