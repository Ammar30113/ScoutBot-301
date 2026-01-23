from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time as dt_time, timezone
from types import SimpleNamespace
from typing import Dict, Iterable, List, Optional

import pytz

from backtest.data_feed import BarDataFeed
from backtest.router import BacktestPriceRouter
from backtest.sim_broker import SimBroker, Trade
from strategy.crash_detector import is_crash_mode
from strategy.signal_router import route_signals
from trader.allocation import allocate_positions
from trader import risk_model

EASTERN = pytz.timezone("America/New_York")


def _market_hours(timestamp: float) -> bool:
    now = datetime.fromtimestamp(timestamp, tz=EASTERN)
    if now.weekday() >= 5:
        return False
    return dt_time(9, 30) <= now.time() <= dt_time(16, 0)


def _patch_router(router: BacktestPriceRouter) -> None:
    import strategy.signal_router as signal_router
    import strategy.momentum as momentum
    import strategy.ml_classifier as ml_classifier
    import strategy.orb as orb
    import strategy.crash_detector as crash_detector
    import trader.allocation as allocation
    import trader.risk_model as risk_model_module

    signal_router.price_router = router
    signal_router.settings.use_sentiment = False
    momentum.router = router
    ml_classifier.price_router = router
    ml_classifier._ml_classifier = None
    orb.price_router = router
    crash_detector.price_router = router
    allocation.price_router = router
    risk_model_module.price_router = router


@dataclass
class BacktestResult:
    equity_curve: List[Dict[str, float]]
    trades: List[Trade]
    final_equity: float
    total_return: float

    def summary(self) -> Dict[str, float]:
        from backtest.metrics import summarize_backtest

        return summarize_backtest(self)


class BacktestRunner:
    def __init__(
        self,
        feed: BarDataFeed,
        *,
        symbols: Optional[Iterable[str]] = None,
        initial_cash: float = 100_000.0,
        step_minutes: int = 5,
        respect_market_hours: bool = True,
        slippage_bps: float = 0.0,
        fee_bps: float = 0.0,
        partial_fill_ratio: float = 1.0,
    ) -> None:
        self.feed = feed
        self.symbols = list(symbols) if symbols else list(feed.symbols())
        self.initial_cash = float(initial_cash)
        self.step_minutes = max(int(step_minutes), 1)
        self.respect_market_hours = bool(respect_market_hours)
        self.slippage_bps = float(slippage_bps)
        self.fee_bps = float(fee_bps)
        self.partial_fill_ratio = float(partial_fill_ratio)

    def run(self, start_ts: Optional[float] = None, end_ts: Optional[float] = None) -> BacktestResult:
        start, end = self.feed.available_range()
        if start is None or end is None:
            raise ValueError("Backtest feed has no data")
        cursor_start = float(start_ts) if start_ts is not None else float(start)
        cursor_end = float(end_ts) if end_ts is not None else float(end)

        router = BacktestPriceRouter(self.feed)
        _patch_router(router)
        broker = SimBroker(
            cash=self.initial_cash,
            slippage_bps=self.slippage_bps,
            fee_bps=self.fee_bps,
            partial_fill_ratio=self.partial_fill_ratio,
        )
        equity_curve: List[Dict[str, float]] = []
        context = SimpleNamespace(pnl_penalty=0.0)
        pnl_tracker = _PnLTracker(initial_equity=self.initial_cash)

        step_seconds = float(self.step_minutes) * 60.0
        cursor = cursor_start
        while cursor <= cursor_end:
            if self.respect_market_hours and not _market_hours(cursor):
                cursor += step_seconds
                continue

            self.feed.set_cursor(cursor)
            crash, _ = is_crash_mode()
            context.pnl_penalty = pnl_tracker.update(cursor, broker.equity())
            trade_allowed = not risk_model.daily_loss_exceeded(pnl_tracker.equity_return_pct)

            if trade_allowed:
                signals = route_signals(self.symbols, crash_mode=crash, context=context)
                allocations = allocate_positions(signals, crash_mode=crash)

                filtered: Dict[str, int] = {}
                open_count = len(broker.positions)
                equity_value = broker.equity()
                for symbol, shares in allocations.items():
                    try:
                        price = router.get_price(symbol)
                    except Exception:
                        continue
                    notional = float(shares) * float(price)
                    if risk_model.can_open_position(
                        open_count + len(filtered),
                        notional,
                        crash_mode=crash,
                        equity=equity_value,
                        equity_return_pct=pnl_tracker.equity_return_pct,
                    ):
                        filtered[symbol] = shares

                for symbol, shares in filtered.items():
                    try:
                        price = router.get_price(symbol)
                    except Exception:
                        continue
                    broker.open_position(symbol, shares, price, cursor)

            price_map = {}
            for symbol in list(broker.positions.keys()):
                try:
                    price_map[symbol] = router.get_price(symbol)
                except Exception:
                    continue
            broker.mark_to_market(price_map)

            for symbol, pos in list(broker.positions.items()):
                payload = {
                    "symbol": symbol,
                    "current_price": pos.current_price,
                    "entry_price": pos.entry_price,
                    "entry_timestamp": pos.entry_timestamp,
                }
                if risk_model.should_exit(payload, crash_mode=crash):
                    broker.close_position(symbol, pos.current_price, cursor)

            equity_curve.append({"timestamp": float(cursor), "equity": broker.equity()})
            cursor += step_seconds

        final_equity = broker.equity()
        total_return = (final_equity - self.initial_cash) / self.initial_cash if self.initial_cash else 0.0
        return BacktestResult(
            equity_curve=equity_curve,
            trades=broker.trades,
            final_equity=final_equity,
            total_return=total_return,
        )


class _PnLTracker:
    def __init__(self, initial_equity: float) -> None:
        self.day_start_equity = float(initial_equity)
        self.day_start_date = ""
        self.equity_return_pct = 0.0

    def update(self, timestamp: float, equity: float) -> float:
        today = datetime.fromtimestamp(timestamp, tz=timezone.utc).date().isoformat()
        if self.day_start_date != today or self.day_start_equity <= 0:
            self.day_start_date = today
            self.day_start_equity = equity if equity > 0 else self.day_start_equity

        baseline = self.day_start_equity if self.day_start_equity > 0 else (equity if equity > 0 else 1.0)
        self.equity_return_pct = (equity - baseline) / baseline

        if self.equity_return_pct < -0.01:
            return 0.05
        if self.equity_return_pct > 0.02:
            return -0.03
        return 0.0
