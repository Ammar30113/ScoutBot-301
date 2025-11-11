import json
import os
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import alpaca_trade_api as tradeapi

from utils.logger import get_logger
from utils.settings import get_settings

from .yfinance_client import get_latest_price

logger = get_logger(__name__)

settings = get_settings()

TRADING_BUDGET = float(os.getenv("TRADING_BUDGET", settings.trading_budget))
DAILY_BUDGET = float(os.getenv("DAILY_BUDGET_USD", settings.daily_budget_usd))
ALLOCATION_RATIO = 0.10
TAKE_PROFIT_RATIO = 0.08
STOP_LOSS_RATIO = 0.04
MAX_UTILIZATION = 0.8
MIN_CONFIDENCE = 0.02
MAX_CONFIDENCE = 0.15
DEFAULT_CONFIDENCE = 0.08
PORTFOLIO_FILE = Path(os.getenv("PORTFOLIO_FILE", "data/portfolio_state.json"))
MODE = os.getenv("MODE", settings.mode)

_alpaca_client: Optional[tradeapi.REST] = None
STATE_LOCK = threading.Lock()
ALLOCATED_CAPITAL = 0.0
TRADE_LOG: List[Dict[str, Any]] = []
MAX_LOG_LENGTH = 100


def _today_iso() -> str:
    return datetime.utcnow().date().isoformat()


def _empty_portfolio_state(date_str: Optional[str] = None) -> Dict[str, Any]:
    return {"date": date_str or _today_iso(), "budget_used": 0.0, "positions": []}


def _load_portfolio_state_unlocked() -> Dict[str, Any]:
    if not PORTFOLIO_FILE.exists():
        return _empty_portfolio_state()
    try:
        with PORTFOLIO_FILE.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("Unable to load portfolio state, resetting: %s", exc)
        return _empty_portfolio_state()


def _save_portfolio_state_unlocked(state: Dict[str, Any]) -> None:
    try:
        PORTFOLIO_FILE.parent.mkdir(parents=True, exist_ok=True)
        with PORTFOLIO_FILE.open("w", encoding="utf-8") as handle:
            json.dump(state, handle, indent=2)
    except OSError as exc:
        logger.warning("Unable to persist portfolio state: %s", exc)


def _ensure_today_portfolio_state_unlocked() -> Dict[str, Any]:
    state = _load_portfolio_state_unlocked()
    today = _today_iso()
    if state.get("date") != today:
        state = _empty_portfolio_state(today)
        _save_portfolio_state_unlocked(state)
    return state


def _get_remaining_daily_budget() -> float:
    with STATE_LOCK:
        state = _ensure_today_portfolio_state_unlocked()
        remaining = DAILY_BUDGET - float(state.get("budget_used", 0.0))
        return max(remaining, 0.0)


def _record_portfolio_trade(symbol: str, qty: int, price: float) -> None:
    cost = qty * price
    with STATE_LOCK:
        state = _ensure_today_portfolio_state_unlocked()
        state["budget_used"] = float(state.get("budget_used", 0.0)) + cost
        state.setdefault("positions", []).append(
            {
                "symbol": symbol,
                "qty": qty,
                "price": round(price, 2),
                "timestamp": datetime.utcnow().isoformat(),
            }
        )
        _save_portfolio_state_unlocked(state)


def _calculate_allocation_from_confidence(confidence: Optional[float]) -> Tuple[float, float]:
    try:
        value = float(confidence) if confidence is not None else DEFAULT_CONFIDENCE
    except (TypeError, ValueError):
        value = DEFAULT_CONFIDENCE
    capped = max(MIN_CONFIDENCE, min(MAX_CONFIDENCE, value))
    allocation = capped * DAILY_BUDGET
    return allocation, capped


def get_account_cash(alpaca: tradeapi.REST) -> float:
    return float(alpaca.get_account().cash)


def get_trade_log(limit: int = 25) -> List[Dict[str, Any]]:
    with STATE_LOCK:
        return list(TRADE_LOG[-limit:])


def get_allocated_capital() -> float:
    with STATE_LOCK:
        return ALLOCATED_CAPITAL


def maybe_trade(symbol: str, confidence: Optional[float] = None) -> bool:
    """
    Attempt to open a bracket order for the provided symbol, observing daily limits.
    Returns False when the caller should stop evaluating additional symbols.
    """
    symbol = symbol.upper()
    planned_allocation, confidence_used = _calculate_allocation_from_confidence(confidence)
    max_capital = MAX_UTILIZATION * TRADING_BUDGET
    remaining_daily = _get_remaining_daily_budget()

    if remaining_daily <= 0:
        msg = "Daily budget exhausted"
        logger.info("%s skipped - %s", symbol, msg)
        _record_action(
            symbol,
            "skipped",
            msg,
            {"daily_budget": DAILY_BUDGET, "confidence": confidence_used},
        )
        return False

    allocation = min(planned_allocation, TRADING_BUDGET * ALLOCATION_RATIO, remaining_daily)

    if allocation <= 0:
        _record_action(
            symbol,
            "skipped",
            "Allocation is zero",
            {
                "budget": TRADING_BUDGET,
                "daily_budget": DAILY_BUDGET,
                "confidence": confidence_used,
                "planned_allocation": planned_allocation,
            },
        )
        return False

    alpaca = _get_alpaca_client()
    if alpaca is None:
        _record_action(symbol, "skipped", "Alpaca credentials missing")
        return False

    cash_balance: Optional[float] = None
    try:
        cash = get_account_cash(alpaca)
        cash_balance = cash
    except Exception as exc:  # pragma: no cover - network
        logger.warning("Unable to fetch Alpaca cash: %s", exc)
        _record_action(symbol, "skipped", f"Alpaca cash lookup failed: {exc}")
        return False

    if cash < allocation:
        msg = f"Insufficient cash ({cash:.2f}) for allocation {allocation:.2f}"
        logger.info("%s skipped - %s", symbol, msg)
        _record_action(symbol, "skipped", msg, {"cash": cash})
        return True

    entry_price = get_latest_price(symbol)
    if entry_price is None:
        msg = "No price data"
        logger.info("%s skipped - %s (cash %.2f)", symbol, msg, cash_balance or 0.0)
        _record_action(symbol, "skipped", msg, {"cash": cash_balance})
        return True

    qty = int(allocation // entry_price)
    if qty <= 0:
        msg = f"Price {entry_price:.2f} exceeds allocation {allocation:.2f}"
        logger.info("%s skipped - %s (cash %.2f)", symbol, msg, cash_balance or 0.0)
        _record_action(
            symbol,
            "skipped",
            msg,
            {
                "cash": cash_balance,
                "allocation": allocation,
                "price": entry_price,
                "confidence": confidence_used,
            },
        )
        return True

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
            return False

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
        _record_portfolio_trade(symbol, qty, entry_price)
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
                "confidence": confidence_used,
            },
        )
    except Exception as exc:  # pragma: no cover - network
        logger.warning("Failed to place order for %s: %s", symbol, exc)
        _record_action(symbol, "skipped", f"order failed: {exc}")
        return False

    return True


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

    sanitized_base = _sanitize_alpaca_base(base_url)
    os.environ["APCA_API_BASE_URL"] = sanitized_base

    try:
        client = tradeapi.REST(api_key, secret_key, sanitized_base, api_version="v2")
    except Exception as exc:  # pragma: no cover - network
        logger.warning("Failed to initialize Alpaca client: %s", exc)
        return None

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
        "mode": MODE,
    }
    with STATE_LOCK:
        TRADE_LOG.append(entry)
        if len(TRADE_LOG) > MAX_LOG_LENGTH:
            del TRADE_LOG[: len(TRADE_LOG) - MAX_LOG_LENGTH]


def _sanitize_alpaca_base(url: str) -> str:
    stripped = url.rstrip("/")
    if stripped.endswith("/v2"):
        stripped = stripped[: -len("/v2")]
    return stripped.rstrip("/")
