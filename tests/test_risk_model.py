"""Tests for trader.risk_model core functions."""

from unittest.mock import patch
from datetime import datetime, timezone

from trader.risk_model import (
    stop_loss_price,
    take_profit_price,
    daily_loss_exceeded,
    can_open_position,
    _coerce_pct,
    _coerce_minutes,
)


class TestStopLossPrice:
    def test_normal_mode(self):
        sl = stop_loss_price(100.0, crash_mode=False)
        assert sl == 99.40  # 100 * (1 - 0.006)

    def test_crash_mode(self):
        sl = stop_loss_price(100.0, crash_mode=True)
        assert sl == 99.50  # 100 * (1 - 0.005)

    def test_rounds_to_two_decimals(self):
        sl = stop_loss_price(33.33, crash_mode=False)
        assert sl == round(33.33 * (1 - 0.006), 2)


class TestTakeProfitPrice:
    def test_normal_mode(self):
        tp = take_profit_price(100.0, crash_mode=False)
        assert tp == 101.80  # 100 * (1 + 0.018)

    def test_crash_mode(self):
        tp = take_profit_price(100.0, crash_mode=True)
        assert tp == 101.50  # 100 * (1 + 0.015)


class TestDailyLossExceeded:
    def test_none_return_pct_not_exceeded(self):
        assert daily_loss_exceeded(None) is False

    def test_small_loss_not_exceeded(self):
        assert daily_loss_exceeded(-0.01) is False

    def test_large_loss_exceeded(self):
        assert daily_loss_exceeded(-0.05) is True

    def test_positive_return_not_exceeded(self):
        assert daily_loss_exceeded(0.05) is False


class TestCanOpenPosition:
    def test_allows_within_limits(self):
        assert can_open_position(
            current_positions=2,
            allocation_amount=1000.0,
            crash_mode=False,
            equity=100_000.0,
        ) is True

    def test_blocks_at_max_positions(self):
        assert can_open_position(
            current_positions=5,
            allocation_amount=1000.0,
            crash_mode=False,
        ) is False

    def test_crash_mode_lower_limit(self):
        assert can_open_position(
            current_positions=3,
            allocation_amount=1000.0,
            crash_mode=True,
        ) is False

    def test_blocks_when_daily_loss_exceeded(self):
        assert can_open_position(
            current_positions=0,
            allocation_amount=1000.0,
            crash_mode=False,
            equity_return_pct=-0.05,
        ) is False


class TestCoercePct:
    def test_valid_pct(self):
        assert _coerce_pct(0.5, 0.1) == 0.5

    def test_none_returns_fallback(self):
        assert _coerce_pct(None, 0.1) == 0.1

    def test_negative_returns_fallback(self):
        assert _coerce_pct(-0.5, 0.1) == 0.1

    def test_gte_one_returns_fallback(self):
        assert _coerce_pct(1.0, 0.1) == 0.1

    def test_string_returns_fallback(self):
        assert _coerce_pct("bad", 0.1) == 0.1


class TestCoerceMinutes:
    def test_valid_minutes(self):
        assert _coerce_minutes(60, 90) == 60

    def test_none_returns_fallback(self):
        assert _coerce_minutes(None, 90) == 90

    def test_zero_returns_fallback(self):
        assert _coerce_minutes(0, 90) == 90

    def test_float_truncated(self):
        assert _coerce_minutes(45.9, 90) == 45
