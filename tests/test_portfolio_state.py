"""Tests for data.portfolio_state."""

import json
import tempfile
from pathlib import Path
from unittest.mock import patch

from data.portfolio_state import PortfolioState, load_state, save_state


class TestPortfolioState:
    def test_default_state(self):
        state = PortfolioState()
        assert state.equity == 0.0
        assert state.entry_timestamps == {}
        assert state.entry_metadata == {}

    def test_to_dict(self):
        state = PortfolioState(equity=100.0)
        d = state.to_dict()
        assert d["equity"] == 100.0
        assert isinstance(d, dict)


class TestLoadSaveState:
    def test_load_from_nonexistent_file(self):
        with patch("data.portfolio_state.STATE_PATH", Path("/tmp/nonexistent_state.json")):
            state = load_state()
            assert isinstance(state, PortfolioState)
            assert state.equity == 0.0

    def test_save_and_load_roundtrip(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            path = Path(f.name)

        try:
            with patch("data.portfolio_state.STATE_PATH", path):
                state = PortfolioState(
                    equity=50_000.0,
                    entry_timestamps={"AAPL": 1700000000.0},
                    entry_metadata={"AAPL": {"stop_loss_pct": 0.006}},
                )
                save_state(state)
                loaded = load_state()
                assert loaded.equity == 50_000.0
                assert loaded.entry_timestamps["AAPL"] == 1700000000.0
                assert loaded.entry_metadata["AAPL"]["stop_loss_pct"] == 0.006
        finally:
            path.unlink(missing_ok=True)

    def test_load_corrupted_json_returns_default(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write("{invalid json")
            path = Path(f.name)

        try:
            with patch("data.portfolio_state.STATE_PATH", path):
                state = load_state()
                assert isinstance(state, PortfolioState)
                assert state.equity == 0.0
        finally:
            path.unlink(missing_ok=True)

    def test_load_non_dict_json_returns_default(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump([1, 2, 3], f)
            path = Path(f.name)

        try:
            with patch("data.portfolio_state.STATE_PATH", path):
                state = load_state()
                assert isinstance(state, PortfolioState)
                assert state.equity == 0.0
        finally:
            path.unlink(missing_ok=True)
