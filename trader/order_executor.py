import logging
from datetime import datetime, timezone

from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderClass, OrderSide, TimeInForce
from alpaca.trading.requests import MarketOrderRequest, StopLossRequest, TakeProfitRequest

from core.config import get_settings
from data.price_router import PriceRouter
from data.portfolio_state import set_entry_timestamp
from trader.risk_model import stop_loss_price, take_profit_price

logger = logging.getLogger(__name__)
settings = get_settings()
price_router = PriceRouter()

_base_url = (settings.alpaca_base_url or "").lower()
_mode = settings.trading_mode or "paper"
_is_live = _mode == "live" or ("paper" not in _base_url)

if _is_live and not settings.allow_live_trading:
    trading_client = None
    logger.error(
        "Live trading blocked: set ALLOW_LIVE_TRADING=true to enable live execution (MODE=%s, ALPACA_API_BASE_URL=%s)",
        settings.trading_mode,
        settings.alpaca_base_url,
    )
elif settings.alpaca_api_key and settings.alpaca_api_secret:
    trading_client = TradingClient(
        settings.alpaca_api_key,
        settings.alpaca_api_secret,
        paper=not _is_live,
    )
else:
    trading_client = None
    logger.warning("Alpaca credentials missing; trading operations will be skipped.")


def execute_trades(allocations, crash_mode: bool = False):
    if not allocations:
        logger.info("No allocations to trade")
        return
    if trading_client is None:
        logger.warning("Trading client unavailable; cannot execute trades. Check Alpaca API keys.")
        return

    try:
        open_positions = {pos.symbol: pos for pos in trading_client.get_all_positions()}
    except Exception as exc:  # pragma: no cover - network guard
        logger.warning("Unable to fetch open positions: %s", exc)
        open_positions = {}

    try:
        buying_power = float(trading_client.get_account().buying_power)
    except Exception as exc:  # pragma: no cover - network guard
        logger.warning("Unable to fetch buying power: %s", exc)
        buying_power = None

    for symbol, shares in allocations.items():
        if shares <= 0:
            continue
        if symbol in open_positions:
            logger.info("Skipping %s; already in open positions", symbol)
            continue
        try:
            price = price_router.get_price(symbol)
        except Exception as exc:  # pragma: no cover - network guard
            logger.warning("Price fetch failed for %s: %s", symbol, exc)
            continue

        notional = shares * price
        # NEW robust protection
        if buying_power is None or buying_power <= 0:
            logger.warning("Buying power unavailable — skipping trade.")
            return None

        if notional > buying_power:
            logger.warning("Trade rejected: required %.2f, available %.2f", notional, buying_power)
            return None

        tp = take_profit_price(price, crash_mode=crash_mode)
        sl = stop_loss_price(price, crash_mode=crash_mode)

        order = MarketOrderRequest(
            symbol=symbol,
            qty=shares,
            side=OrderSide.BUY,
            time_in_force=TimeInForce.DAY,
            order_class=OrderClass.BRACKET,
            take_profit=TakeProfitRequest(limit_price=tp),
            stop_loss=StopLossRequest(stop_price=sl),
        )
        try:
            trading_client.submit_order(order)
            set_entry_timestamp(symbol, datetime.now(timezone.utc).timestamp())
        except Exception as exc:  # pragma: no cover - network guard
            logger.warning("Order failed for %s: %s", symbol, exc)
            continue
        logger.info("Submitted bracket order for %s shares=%s tp=%.2f sl=%.2f", symbol, shares, tp, sl)
        if buying_power:
            buying_power = max(0.0, buying_power - notional)


def close_position(symbol: str) -> None:
    if trading_client is None:
        logger.warning("Trading client unavailable; cannot close position for %s", symbol)
        return
    try:
        positions = {pos.symbol: pos for pos in trading_client.get_all_positions()}
    except Exception as exc:  # pragma: no cover - network guard
        logger.warning("Unable to fetch positions before exit: %s", exc)
        return

    pos = positions.get(symbol)
    if not pos:
        logger.info("No open position for %s; skipping close", symbol)
        return
    try:
        qty = float(pos.qty)
        held = float(getattr(pos, "held_for_orders", 0) or 0)
    except Exception:
        logger.info("Unable to parse quantities for %s; skipping close", symbol)
        return

    if qty <= 0 or held >= qty:
        logger.info("No exit for %s: qty=%s held_for_orders=%s", symbol, qty, held)
        return

    try:
        trading_client.close_position(symbol)
        logger.info("Closed position for %s", symbol)
    except Exception as exc:  # pragma: no cover - network guard
        msg = str(exc).lower()
        benign_markers = ("insufficient qty", "insufficient quantity", "no position", "position does not exist")
        if any(marker in msg for marker in benign_markers):
            logger.info("No exit executed for %s: position unavailable", symbol)
            return
        logger.warning("Failed to close position for %s: %s", symbol, exc)


def list_positions():
    if trading_client is None:
        logger.warning("Trading client unavailable; cannot list positions.")
        return []
    try:
        return trading_client.get_all_positions()
    except Exception as exc:  # pragma: no cover - network guard
        logger.warning("Unable to list positions: %s", exc)
        return []
