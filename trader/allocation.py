import logging
import math
import os

from data.price_router import PriceRouter

logger = logging.getLogger(__name__)
price_router = PriceRouter()
DAILY_BUDGET = float(os.getenv("DAILY_BUDGET_USD", 10000))


def allocate_positions(final_signals):
    if not final_signals:
        logger.warning("No signals to allocate capital")
        return {}

    budget = DAILY_BUDGET
    per_symbol = budget / len(final_signals)
    cap = DAILY_BUDGET / 3
    allocations = {}
    for signal in final_signals:
        symbol = signal["symbol"] if isinstance(signal, dict) else signal
        try:
            price = price_router.get_price(symbol)
        except Exception as exc:  # pragma: no cover - network guard
            logger.warning("Price unavailable for %s: %s", symbol, exc)
            continue
        allocation = min(per_symbol, cap)
        shares = math.floor(allocation / price) if price > 0 else 0
        if shares <= 0:
            logger.info("Capital %.2f insufficient for %s (price %.2f)", allocation, symbol, price)
            continue
        allocations[symbol] = shares
        logger.info("Allocating %s shares of %s (price %.2f, per_symbol %.2f)", shares, symbol, price, allocation)
    return allocations
