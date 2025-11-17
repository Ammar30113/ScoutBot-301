import logging

from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderClass, OrderSide, TimeInForce
from alpaca.trading.requests import MarketOrderRequest, StopLossRequest, TakeProfitRequest

from core.config import get_settings
from data.price_router import PriceRouter
from trader.risk_model import stop_loss_price, take_profit_price

logger = logging.getLogger(__name__)
settings = get_settings()
price_router = PriceRouter()

trading_client = TradingClient(
    settings.alpaca_api_key,
    settings.alpaca_api_secret,
    paper="paper" in settings.alpaca_base_url,
)


def execute_trades(allocations):
    if not allocations:
        logger.info("No allocations to trade")
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
        buying_power = 0.0

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
        if buying_power and notional > buying_power:
            logger.warning(
                "Insufficient buying power for %s: needed %.2f, available %.2f", symbol, notional, buying_power
            )
            continue

        tp = take_profit_price(price)
        sl = stop_loss_price(price)

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
        except Exception as exc:  # pragma: no cover - network guard
            logger.warning("Order failed for %s: %s", symbol, exc)
            continue
        logger.info("Submitted bracket order for %s shares=%s tp=%.2f sl=%.2f", symbol, shares, tp, sl)
        if buying_power:
            buying_power = max(0.0, buying_power - notional)


def close_position(symbol: str) -> None:
    try:
        trading_client.close_position(symbol)
        logger.info("Closed position for %s", symbol)
    except Exception as exc:  # pragma: no cover - network guard
        logger.warning("Failed to close position for %s: %s", symbol, exc)


def list_positions():
    try:
        return trading_client.get_all_positions()
    except Exception as exc:  # pragma: no cover - network guard
        logger.warning("Unable to list positions: %s", exc)
        return []
