"""Offline engineering smoke test using invented prices, never investment evidence."""
from dataclasses import replace
from pathlib import Path
import tempfile

import pandas as pd

from backtest.data_feed import BarDataFeed
from backtest.runner import HistoricalDailyData
from core.config import Settings
from core.market_calendar import calendar
from trader.paper_engine import PaperEngine
from trader.paper_store import PaperStore


def main() -> None:
    rows = [dict(timestamp=day.timestamp(), open=10 + i * .01, high=10.02 + i * .01,
                 low=9.98 + i * .01, close=10 + i * .01, volume=100_000)
            for i, day in enumerate(calendar().sessions_in_range("2024-01-02", "2024-12-31"))]
    data = HistoricalDailyData(BarDataFeed({"TEST": pd.DataFrame(rows)}))
    settings = replace(Settings(), dry_run=False, paper_initial_cash=1000, paper_symbols=["TEST"],
                       paper_data_provider="historical", trend_days=200, paper_min_fee=.35,
                       max_risk_pct=.005, max_portfolio_risk_pct=.01, max_gross_exposure_pct=.5,
                       daily_budget_usd=500, max_position_size=250, max_position_pct=.25, max_positions=2)
    with tempfile.TemporaryDirectory(prefix="scout-paper-smoke-") as temp:
        path = Path(temp) / "paper.sqlite3"
        store = PaperStore(path, account=settings.paper_account_id, strategy=settings.strategy)
        try:
            for close in data.available["TEST"]:
                data.now = close
                with store.locked():
                    # Reload every session to exercise persistence at every transition.
                    engine = PaperEngine(settings, store.load())
                    report = engine.step(data, close + settings.data_delay_seconds)
                    store.save(engine.snapshot(), engine.broker.events)
                assert report["cash"] >= 0
            assert engine.broker.orders, "Smoke fixture must exercise order submission"
            assert any(o.status == "filled" for o in engine.broker.orders.values()), "No fills exercised"
            before = engine.snapshot()
            again = PaperEngine(settings, store.load())
            again.step(data, close + settings.data_delay_seconds)
            assert again.snapshot() == before
            assert not again.broker.events
            print(f"PASS: {len(rows)} synthetic sessions; persisted orders/fills, nonnegative cash, restart idempotency. No network or real orders.")
        finally:
            store.close()


if __name__ == "__main__":
    main()
