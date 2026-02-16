"""Tests for trader.pnl_tracker."""

from unittest.mock import MagicMock, patch
from types import SimpleNamespace

from data.portfolio_state import PortfolioState


class TestUpdateDailyPnl:
    @patch("trader.pnl_tracker.save_state")
    @patch("trader.pnl_tracker.load_state")
    def test_returns_none_when_client_is_none(self, mock_load, mock_save):
        from trader.pnl_tracker import update_daily_pnl

        mock_load.return_value = PortfolioState()
        result = update_daily_pnl(None)
        assert result is None
        mock_save.assert_not_called()

    @patch("trader.pnl_tracker.save_state")
    @patch("trader.pnl_tracker.load_state")
    def test_returns_none_on_api_failure(self, mock_load, mock_save):
        from trader.pnl_tracker import update_daily_pnl

        mock_load.return_value = PortfolioState()
        client = MagicMock()
        client.get_account.side_effect = Exception("API error")
        result = update_daily_pnl(client)
        assert result is None

    @patch("trader.pnl_tracker.save_state")
    @patch("trader.pnl_tracker.load_state")
    def test_computes_equity_return(self, mock_load, mock_save):
        from trader.pnl_tracker import update_daily_pnl

        state = PortfolioState(day_start_equity=100_000.0, day_start_date="2026-02-16")
        mock_load.return_value = state

        account = SimpleNamespace(equity="101000", realized_pl="500")
        positions = [SimpleNamespace(unrealized_pl="500")]
        client = MagicMock()
        client.get_account.return_value = account
        client.get_all_positions.return_value = positions

        with patch("trader.pnl_tracker.datetime") as mock_dt:
            from datetime import datetime, timezone

            mock_dt.now.return_value = datetime(2026, 2, 16, 12, 0, tzinfo=timezone.utc)
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            result = update_daily_pnl(client)

        assert result is not None
        assert result.equity == 101_000.0
        assert abs(result.equity_return_pct - 0.01) < 0.001

    @patch("trader.pnl_tracker.save_state")
    @patch("trader.pnl_tracker.load_state")
    def test_handles_zero_baseline(self, mock_load, mock_save):
        from trader.pnl_tracker import update_daily_pnl

        state = PortfolioState(day_start_equity=0.0, day_start_date="2026-02-16")
        mock_load.return_value = state

        account = SimpleNamespace(equity="50000", realized_pl="0")
        client = MagicMock()
        client.get_account.return_value = account
        client.get_all_positions.return_value = []

        with patch("trader.pnl_tracker.datetime") as mock_dt:
            from datetime import datetime, timezone

            mock_dt.now.return_value = datetime(2026, 2, 16, 12, 0, tzinfo=timezone.utc)
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            result = update_daily_pnl(client)

        # Should not crash; baseline should be set to equity
        assert result is not None
        assert result.equity == 50_000.0
