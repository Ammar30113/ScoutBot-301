from __future__ import annotations

import math


def risk_per_share(entry_price: float, stop_price: float) -> float:
    try:
        entry = float(entry_price)
        stop = float(stop_price)
    except (TypeError, ValueError):
        return 0.0
    return abs(entry - stop)


def size_position(
    entry_price: float,
    stop_price: float,
    *,
    equity: float | None,
    max_risk_pct: float,
    max_notional: float,
    min_qty: int = 1,
) -> int:
    """Return share quantity sized by risk-per-trade and notional caps."""

    risk = risk_per_share(entry_price, stop_price)
    if risk <= 0 or not math.isfinite(risk):
        return 0
    if equity is None or equity <= 0 or not math.isfinite(equity):
        return 0
    if max_risk_pct <= 0 or not math.isfinite(max_risk_pct):
        return 0
    risk_cap = equity * max_risk_pct
    if risk_cap <= 0:
        return 0

    max_by_risk = math.floor(risk_cap / risk)
    if entry_price <= 0 or max_notional <= 0:
        max_by_notional = max_by_risk
    else:
        max_by_notional = math.floor(max_notional / float(entry_price))
    qty = min(max_by_risk, max_by_notional)
    if qty < min_qty:
        return 0
    return int(qty)
