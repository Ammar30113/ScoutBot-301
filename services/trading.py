import os
import threading
from datetime import datetime
from typing import Any, Dict, List, Optional

import alpaca_trade_api as tradeapi

from utils.logger import get_logger
from utils.settings import get_settings

from .yfinance_client import get_latest_price

logger = get_logger(__name__)

settings = get_settings()

TRADING_BUDGET = float(os.getenv("TRADING_BUDGET", settings.trading_budget))
ALLOCATION_RATIO = 0.10
TAKE_PROFIT_RATIO = 0.08
STOP_LOSS_RATIO = 0.04
MAX_UTILIZATION = 0.8

_alpaca_client: Optional[tradeapi.REST] = None
STATE_LOCK = threading.Lock()
ALLOCATED_CAPITAL = 0.0
TRADE_LOG: List[Dict[str, Any]] = []
MAX_LOG_LENGTH = 100


def get_account_cash(alpaca: tradeapi.REST) -> float:
    return float(alpaca.get_account().cash)


def get_trade_log(limit: int = 25) -> List[Dict[str, Any]]:
    with STATE_LOCK:
        return list(TRADE_LOG[-limit:])


def get_allocated_capital() -> float:
    with STATE_LOCK:
        return ALLOCATED_CAPITAL


def maybe_trade(symbol: str) -> None:
    """
    Attempt to open a bracket order for the provided symbol, obeying budget limits.
    """
    symbol = symbol.upper()
    allocation = TRADING_BUDGET * ALLOCATION_RATIO
    max_capital = MAX_UTILIZATION * TRADING_BUDGET

    if allocation <= 0:
        _record_action(symbol, "skipped", "Allocation is zero", {"budget": TRADING_BUDGET})
        return

    alpaca = _get_alpaca_client()
    if alpaca is None:
        _record_action(symbol, "skipped", "Alpaca credentials missing")
        return

    cash_balance: Optional[float] = None
    try:
        cash = get_account_cash(alpaca)
        cash_balance = cash
    except Exception as exc:  # pragma: no cover - network
        logger.warning("Unable to fetch Alpaca cash: %s", exc)
        _record_action(symbol, "skipped", f"Alpaca cash lookup failed: {exc}")
        return

    if cash < allocation:
        msg = f"Insufficient cash ({cash:.2f}) for allocation {allocation:.2f}"
        logger.info("%s skipped - %s", symbol, msg)
        _record_action(symbol, "skipped", msg, {"cash": cash})
        return

    entry_price = get_latest_price(symbol)
    if entry_price is None:
        msg = "No price data"
        logger.info("%s skipped - %s (cash %.2f)", symbol, msg, cash_balance or 0.0)
        _record_action(symbol, "skipped", msg, {"cash": cash_balance})
        return

    qty = max(int(allocation // entry_price), 1)
    position_cost = qty * entry_price

    with STATE_LOCK:
        if ALLOCATED_CAPITAL + position_cost > max_capital:
            msg = "Max utilization reached"
            logger.info(
                "%s skipped - %s (allocated %.2f, cash %.2f)",
                symbol,
                msg,
                ALLOCATED_CAPITAL,
                cash_balance or 0.0,
            )
            _record_action(
                symbol,
                "skipped",
                msg,
                {"allocated": ALLOCATED_CAPITAL, "cash": cash_balance},
            )
            return

    take_profit = round(entry_price * (1 + TAKE_PROFIT_RATIO), 2)
    stop_loss = round(entry_price * (1 - STOP_LOSS_RATIO), 2)

    try:
        alpaca.submit_order(
            symbol=symbol,
            qty=qty,
            side="buy",
            type="market",
            time_in_force="gtc",
            order_class="bracket",
            take_profit={"limit_price": take_profit},
            stop_loss={"stop_price": stop_loss},
        )
        _increment_allocation(position_cost)
        logger.info(
            "%s: entry %.2f, TP %.2f, SL %.2f, qty %s",
            symbol,
            entry_price,
            take_profit,
            stop_loss,
            qty,
        )
        _record_action(
            symbol,
            "submitted",
            "bracket order placed",
            {
                "entry": round(entry_price, 2),
                "qty": qty,
                "take_profit": take_profit,
                "stop_loss": stop_loss,
            },
        )
    except Exception as exc:  # pragma: no cover - network
        logger.warning("Failed to place order for %s: %s", symbol, exc)
        _record_action(symbol, "skipped", f"order failed: {exc}")


def _get_alpaca_client() -> Optional[tradeapi.REST]:
    global _alpaca_client
    with STATE_LOCK:
        client = _alpaca_client
    if client is not None:
        return client

    api_key = settings.alpaca_api_key
    secret_key = settings.alpaca_secret_key
    base_url = str(settings.alpaca_trading_url)

    if not api_key or not secret_key:
        logger.warning("Alpaca credentials not configured")
        return None

    client = tradeapi.REST(api_key, secret_key, base_url, api_version="v2")
    with STATE_LOCK:
        _alpaca_client = client

    return client


def _increment_allocation(amount: float) -> None:
    with STATE_LOCK:
        global ALLOCATED_CAPITAL
        ALLOCATED_CAPITAL += amount


def _record_action(symbol: str, status: str, reason: str, details: Optional[Dict[str, Any]] = None) -> None:
    entry = {
        "symbol": symbol,
        "status": status,
        "reason": reason,
        "details": details or {},
        "timestamp": datetime.utcnow().isoformat(),
    }
    with STATE_LOCK:
        TRADE_LOG.append(entry)
        if len(TRADE_LOG) > MAX_LOG_LENGTH:
            del TRADE_LOG[: len(TRADE_LOG) - MAX_LOG_LENGTH]
