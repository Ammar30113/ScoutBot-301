"""Shared paper-order sizing. No real brokerage order APIs are imported here."""
from __future__ import annotations

import math

from backtest.sim_broker import SimBroker
from core.config import Settings
from trader.position_sizer import size_position


def entry_quantity(broker: SimBroker, settings: Settings, price: float, stop_pct: float) -> tuple[int, float]:
    if not math.isfinite(price) or price <= 0 or not 0 < stop_pct < 1:
        return 0, 0.0
    equity = min(broker.equity(), settings.paper_initial_cash)
    # Reserve at a bounded price including modeled slippage; no borrowing or auto top-ups.
    limit = price * (1 + settings.paper_slippage_bps / 10000 + 0.005)
    notional_cap = min(settings.max_position_size, settings.daily_budget_usd - broker.exposure(),
                       equity * settings.max_gross_exposure_pct - broker.exposure())
    if settings.max_position_pct > 0:
        notional_cap = min(notional_cap, equity * settings.max_position_pct)
    available = broker.cash - broker.reserved_cash()
    notional_cap = min(notional_cap, (available - settings.paper_min_fee) / (1 + settings.paper_fee_bps / 10000))
    slots = len(broker.positions) + sum(o.action == "BUY" for o in broker.pending())
    portfolio_risk = max(0.0, equity * settings.max_portfolio_risk_pct - broker.stop_risk())
    # Budget fees on both legs as well as the modeled stop distance.
    risk_per_share = limit * (stop_pct + settings.paper_slippage_bps / 10000) + 2 * limit * settings.paper_fee_bps / 10000
    risk_cash = min(equity * settings.max_risk_pct, portfolio_risk) - 2 * settings.paper_min_fee
    if slots >= settings.max_positions or min(notional_cap, risk_cash) <= 0:
        return 0, limit
    qty = size_position(limit, limit * (1 - stop_pct), equity=equity,
                        max_risk_pct=settings.max_risk_pct, max_notional=notional_cap)
    return max(0, min(qty, math.floor(risk_cash / risk_per_share))), limit
