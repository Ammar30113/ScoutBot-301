"""Daily historical replay through the exact paper engine; no global monkeypatches."""
from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Iterable

from backtest.data_feed import BarDataFeed
from backtest.sim_broker import Trade
from core.config import Settings
from data.bars import Bar
from trader.paper_engine import PaperEngine


@dataclass
class BacktestResult:
    equity_curve: list[dict[str, float]]
    trades: list[Trade]
    final_equity: float
    total_return: float
    final_state: dict
    reports: list[dict]

    def summary(self) -> dict:
        from backtest.metrics import summarize_backtest
        return summarize_backtest(self)


class HistoricalDailyData:
    daily_provider_name = "historical"

    def __init__(self, feed: BarDataFeed) -> None:
        self.rows = {symbol: frame.to_dict("records") for symbol, frame in feed.data.items()}
        self.available = {symbol: [Bar.from_record(row).available_at for row in rows]
                          for symbol, rows in self.rows.items()}
        self.now = 0.0

    def get_daily_aggregates(self, symbol: str, limit: int = 60) -> list[dict]:
        return [row for row, close in zip(self.rows.get(symbol, []), self.available.get(symbol, []))
                if close <= self.now][-limit:]


class BacktestRunner:
    def __init__(self, feed: BarDataFeed, *, symbols: Iterable[str] | None = None,
                 settings: Settings | None = None, initial_cash: float = 1000.0,
                 slippage_bps: float = 10.0, fee_bps: float = 5.0, min_fee: float = 0.35) -> None:
        self.feed = feed
        base = settings or Settings(paper_initial_cash=initial_cash, paper_slippage_bps=slippage_bps,
                                    paper_fee_bps=fee_bps, paper_min_fee=min_fee)
        self.settings = replace(base, paper_symbols=list(symbols or feed.symbols()), dry_run=False,
                                paper_data_provider="historical", broker="simulated", trading_mode="paper")

    def run(self, start_ts: float | None = None, end_ts: float | None = None) -> BacktestResult:
        data = HistoricalDailyData(self.feed)
        closes = sorted({close for values in data.available.values() for close in values})
        if not closes:
            raise ValueError("Backtest feed has no daily bars")
        engine = PaperEngine(self.settings)
        curve, reports = [], []
        for close in closes:
            now = close + self.settings.data_delay_seconds
            if (start_ts is not None and now < start_ts) or (end_ts is not None and now > end_ts):
                continue
            data.now = close
            report = engine.step(data, now)
            curve.append({"timestamp": now, "equity": report["equity"]})
            reports.append(report)
        if not curve:
            raise ValueError("No daily sessions within requested interval")
        return BacktestResult(curve, engine.broker.trades, engine.broker.equity(),
                              engine.broker.equity() / self.settings.paper_initial_cash - 1,
                              engine.snapshot(), reports)
