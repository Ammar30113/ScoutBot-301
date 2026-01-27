import logging
import math
import os

from data.price_router import PriceRouter

logger = logging.getLogger(__name__)
price_router = PriceRouter()
DAILY_BUDGET = float(os.getenv("DAILY_BUDGET_USD", 10000))


def _signal_strength(signal) -> float:
    if not isinstance(signal, dict):
        return 0.0
    for key in ("score", "prob", "reversal_score"):
        value = signal.get(key)
        if value is None:
            continue
        try:
            strength = float(value)
        except (TypeError, ValueError):
            continue
        if key == "reversal_score":
            strength = abs(strength)
        return min(max(strength, 0.0), 1.0)
    return 0.0


def allocate_positions(final_signals, crash_mode: bool = False):
    if not final_signals:
        logger.warning("No signals to allocate capital")
        return {}

    if crash_mode:
        max_positions = 3
        budget_remaining = DAILY_BUDGET * 0.80
        base_allocation = budget_remaining / max_positions
    else:
        budget_remaining = DAILY_BUDGET
        base_allocation = DAILY_BUDGET / 3

    allocations = {}
    for signal in final_signals:
        if crash_mode and len(allocations) >= 3:
            logger.info("Crash mode: max positions reached")
            break
        symbol = signal["symbol"] if isinstance(signal, dict) else signal
        signal_type = signal.get("type") if isinstance(signal, dict) else "momentum"
        vol_ratio = float(signal.get("vol_ratio", 1.0) if isinstance(signal, dict) else 1.0)
        size_multiplier = 1.0
        if isinstance(signal, dict):
            try:
                size_multiplier = float(signal.get("size_multiplier", 1.0))
            except (TypeError, ValueError):
                size_multiplier = 1.0
        if not math.isfinite(size_multiplier) or size_multiplier <= 0:
            size_multiplier = 1.0

        try:
            price = price_router.get_price(symbol)
        except Exception as exc:  # pragma: no cover - network guard
            logger.warning("Price unavailable for %s: %s", symbol, exc)
            continue

        size = base_allocation
        strength = _signal_strength(signal)
        if crash_mode:
            size *= 0.7 + (0.4 * strength)
        else:
            size *= 0.8 + (0.6 * strength)
        if crash_mode:
            if vol_ratio > 1.8:
                size *= 0.6
            elif vol_ratio < 0.8:
                size = min(base_allocation, base_allocation * 1.2)
            if signal_type == "reversal":
                size *= 0.75
        else:
            if vol_ratio > 1.5:
                size *= 0.5
            elif vol_ratio < 0.7:
                size = min(base_allocation, base_allocation * 1.3)
            if signal_type == "reversal":
                size *= 0.6
        size *= min(max(size_multiplier, 0.2), 1.5)

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
