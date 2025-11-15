import logging

from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderClass, OrderSide, TimeInForce
from alpaca.trading.requests import MarketOrderRequest, StopLossRequest, TakeProfitRequest

from core.config import get_settings
from data.price_router import PriceRouter

logger = logging.getLogger(__name__)
settings = get_settings()
price_router = PriceRouter()

_trading_client = TradingClient(
    settings.alpaca_api_key,
    settings.alpaca_api_secret,
    paper="paper" in settings.alpaca_base_url,
)


def execute_trades(allocation):
    if not allocation:
        logger.info("No allocation provided; skipping trade execution")
        return

    for symbol, capital in allocation.items():
        try:
            price = price_router.get_price(symbol)
        except Exception as exc:  # pragma: no cover - network guard
            logger.warning("Unable to fetch price for %s: %s", symbol, exc)
            continue

        if price <= 0:
            logger.warning("Price unavailable for %s", symbol)
            continue

        qty = int(capital // price)
        if qty <= 0:
            logger.info("Capital %.2f insufficient for %s; skipping", capital, symbol)
            continue

        tp_price = round(price * 1.03, 2)
        sl_price = round(price * 0.97, 2)

        order = MarketOrderRequest(
            symbol=symbol,
            qty=qty,
            side=OrderSide.BUY,
            time_in_force=TimeInForce.DAY,
            order_class=OrderClass.BRACKET,
            take_profit=TakeProfitRequest(limit_price=tp_price),
            stop_loss=StopLossRequest(stop_price=sl_price),
        )

        try:
            _trading_client.submit_order(order_data=order)
        except Exception as exc:  # pragma: no cover - network guard
            logger.warning("Alpaca order failed for %s: %s", symbol, exc)
            continue

        logger.info(
            "Submitted bracket order for %s qty=%s tp=%s sl=%s",
            symbol,
            qty,
            tp_price,
            sl_price,
        )
