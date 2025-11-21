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
    max_per_position = DAILY_BUDGET / 3  # strict cap for safety
    allocations = {}
    for signal in final_signals:
        symbol = signal["symbol"] if isinstance(signal, dict) else signal
        try:
            price = price_router.get_price(symbol)
        except Exception as exc:  # pragma: no cover - network guard
            logger.warning("Price unavailable for %s: %s", symbol, exc)
            continue
        budget_for_position = min(per_symbol, max_per_position)
        shares = math.floor(budget_for_position / price) if price > 0 else 0
        if shares <= 0:
            logger.info("Capital %.2f insufficient for %s (price %.2f)", budget_for_position, symbol, price)
            continue
        allocations[symbol] = shares
        logger.info(
            "Allocating %s shares of %s (price %.2f, budget %.2f)", shares, symbol, price, budget_for_position
        )
    return allocations
