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

    budget_remaining = DAILY_BUDGET
    base_allocation = DAILY_BUDGET / 3
    allocations = {}
    for signal in final_signals:
        symbol = signal["symbol"] if isinstance(signal, dict) else signal
        signal_type = signal.get("type") if isinstance(signal, dict) else "momentum"
        vol_ratio = float(signal.get("vol_ratio", 1.0) if isinstance(signal, dict) else 1.0)

        try:
            price = price_router.get_price(symbol)
        except Exception as exc:  # pragma: no cover - network guard
            logger.warning("Price unavailable for %s: %s", symbol, exc)
            continue

        size = base_allocation
        if vol_ratio > 1.5:
            size *= 0.5
        elif vol_ratio < 0.7:
            size = min(base_allocation, base_allocation * 1.3)

        if signal_type == "reversal":
            size *= 0.6

        size = min(size, budget_remaining)
        shares = math.floor(size / price) if price > 0 else 0
        if shares <= 0:
            logger.info("Capital %.2f insufficient for %s (price %.2f)", size, symbol, price)
            continue
        notional = shares * price
        if notional > budget_remaining:
            logger.info("Skipping %s: notional %.2f exceeds remaining budget %.2f", symbol, notional, budget_remaining)
            continue

        allocations[symbol] = shares
        budget_remaining -= notional
        logger.info(
            "Allocating %s shares of %s (type=%s, price %.2f, budget %.2f, vol_ratio %.2f)",
            shares,
            symbol,
            signal_type,
            price,
            notional,
            vol_ratio,
        )

        if budget_remaining <= 0:
            logger.info("Budget exhausted; stopping allocations")
            break
    return allocations
