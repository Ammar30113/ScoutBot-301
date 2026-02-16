"""Tests for trader.position_sizer."""

import math

from trader.position_sizer import risk_per_share, size_position


class TestRiskPerShare:
    def test_basic_risk(self):
        assert risk_per_share(10.0, 9.5) == 0.5

    def test_invalid_types_return_zero(self):
        assert risk_per_share(None, 9.5) == 0.0
        assert risk_per_share("bad", 9.5) == 0.0

    def test_same_price_returns_zero(self):
        assert risk_per_share(10.0, 10.0) == 0.0


class TestSizePosition:
    def test_basic_sizing(self):
        qty = size_position(
            entry_price=50.0,
            stop_price=49.0,
            equity=100_000.0,
            max_risk_pct=0.01,
            max_notional=10_000.0,
            min_qty=1,
        )
        # risk per share = 1.0, risk cap = 100000 * 0.01 = 1000
        # max by risk = 1000, max by notional = 10000/50 = 200
        assert qty == 200

    def test_zero_risk_returns_zero(self):
        qty = size_position(
            entry_price=50.0,
            stop_price=50.0,  # zero risk
            equity=100_000.0,
            max_risk_pct=0.01,
            max_notional=10_000.0,
        )
        assert qty == 0

    def test_no_equity_returns_zero(self):
        qty = size_position(
            entry_price=50.0,
            stop_price=49.0,
            equity=None,
            max_risk_pct=0.01,
            max_notional=10_000.0,
        )
        assert qty == 0

    def test_negative_equity_returns_zero(self):
        qty = size_position(
            entry_price=50.0,
            stop_price=49.0,
            equity=-1000.0,
            max_risk_pct=0.01,
            max_notional=10_000.0,
        )
        assert qty == 0

    def test_zero_max_risk_returns_zero(self):
        qty = size_position(
            entry_price=50.0,
            stop_price=49.0,
            equity=100_000.0,
            max_risk_pct=0.0,
            max_notional=10_000.0,
        )
        assert qty == 0

    def test_inf_equity_returns_zero(self):
        qty = size_position(
            entry_price=50.0,
            stop_price=49.0,
            equity=math.inf,
            max_risk_pct=0.01,
            max_notional=10_000.0,
        )
        assert qty == 0

    def test_min_qty_enforced(self):
        qty = size_position(
            entry_price=50.0,
            stop_price=49.0,
            equity=10.0,  # very small equity
            max_risk_pct=0.001,
            max_notional=10_000.0,
            min_qty=5,
        )
        # risk cap = 10 * 0.001 = 0.01, max by risk = 0 < min_qty
        assert qty == 0

    def test_notional_cap_limits_qty(self):
        qty = size_position(
            entry_price=100.0,
            stop_price=99.0,
            equity=1_000_000.0,
            max_risk_pct=0.01,
            max_notional=500.0,  # can only afford 5 shares
        )
        assert qty == 5
