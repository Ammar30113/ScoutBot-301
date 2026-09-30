"""Deterministic daily strategy lifecycle used by paper trading and backtesting."""
from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone
import hashlib
import json
import math
from typing import Protocol

from backtest.sim_broker import PaperOrder, SimBroker
from core.config import Settings
from core.market_calendar import calendar, latest_completed_session
from data.bars import Bar, completed_bars
from strategy.daily_trend import decide
from trader.execution_adapter import entry_quantity


class DataIntegrityError(ValueError):
    """Safe diagnostic text originating from our own validation only."""


class DailyData(Protocol):
    def get_daily_aggregates(self, symbol: str, limit: int = 60) -> list[dict]: ...


class PaperEngine:
    def __init__(self, settings: Settings, state: dict | None = None) -> None:
        self.settings = settings
        # Changes to capital/costs or strategy parameters require a new paper account.
        names = ("paper_initial_cash", "paper_slippage_bps", "paper_fee_bps", "paper_min_fee", "paper_symbols",
                 "trend_days", "strategy", "paper_data_provider", "data_delay_seconds", "max_positions", "max_position_size",
                 "max_risk_pct", "max_portfolio_risk_pct", "max_gross_exposure_pct", "max_drawdown_pct",
                 "max_daily_loss_pct", "max_position_pct", "daily_budget_usd")
        identity = {name: getattr(settings, name) for name in names}
        self.fingerprint = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
        if state is not None and state.get("fingerprint") != self.fingerprint:
            raise ValueError("Paper configuration changed; use a new PAPER_STATE_PATH/account to preserve the old run")
        self.broker = SimBroker.from_dict(state["broker"]) if state else SimBroker(
            settings.paper_initial_cash, settings.paper_slippage_bps, settings.paper_fee_bps,
            min_fee=settings.paper_min_fee)
        expected_cash = settings.paper_initial_cash + sum(t.pnl for t in self.broker.trades) - sum(
            p.qty * p.entry_price + p.entry_fee for p in self.broker.positions.values())
        if not math.isclose(expected_cash, self.broker.cash, abs_tol=1e-7):
            raise ValueError("Paper cash does not reconcile to fills")
        self.risk = dict(state["risk"]) if state else {
            "day": "", "day_start_equity": settings.paper_initial_cash, "peak": settings.paper_initial_cash,
            "daily_halt": False, "drawdown_halt": False,
        }
        self.decisions = dict(state.get("decisions", {})) if state else {}
        self.last_step = float(state.get("last_step", 0)) if state else 0.0
        self.checkpoints = dict(state["checkpoints"]) if state else {}
        self.provider = state.get("provider") if state else None
        self.observed = dict(state["observed"]) if state else {}
        if not all(math.isfinite(float(v)) and float(v) >= 0 for v in
                   (self.last_step, self.risk["peak"], self.risk["day_start_equity"])):
            raise ValueError("Invalid persisted risk state")

    def snapshot(self) -> dict:
        return {"fingerprint": self.fingerprint, "broker": self.broker.to_dict(), "risk": self.risk,
                "decisions": self.decisions, "last_step": self.last_step,
                "checkpoints": self.checkpoints, "provider": self.provider, "observed": self.observed}

    def _risk_check(self, timestamp: float) -> None:
        day = datetime.fromtimestamp(timestamp, timezone.utc).date().isoformat()
        equity = self.broker.equity()
        if self.risk["day"] != day:
            self.risk.update(day=day, day_start_equity=equity, daily_halt=False)
        self.risk["peak"] = max(self.risk["peak"], equity)
        if equity <= self.risk["day_start_equity"] * (1 - self.settings.max_daily_loss_pct):
            self.risk["daily_halt"] = True
        if equity <= self.risk["peak"] * (1 - self.settings.max_drawdown_pct):
            self.risk["drawdown_halt"] = True
        if self.halted:
            self.broker.cancel_entries(timestamp, "risk_halt")

    @property
    def halted(self) -> bool:
        return bool(self.risk["daily_halt"] or self.risk["drawdown_halt"])

    def step(self, data: DailyData, now: float) -> dict:
        if not math.isfinite(now) or now < self.last_step:
            raise ValueError("Paper clock cannot move backwards")
        provider = getattr(data, "daily_provider_name", "historical")
        if self.provider and self.provider != provider:
            raise ValueError("Daily data source changed; use a new paper account")
        self.provider = provider
        expected_day = latest_completed_session(now - self.settings.data_delay_seconds)
        expected = calendar().session_close(expected_day).timestamp()
        symbols = list(dict.fromkeys([*self.broker.positions, *(o.symbol for o in self.broker.pending()),
                                     *self.settings.paper_symbols]))
        histories: dict[str, list[Bar]] = {}
        errors: dict[str, str] = {}
        for symbol in symbols:
            try:
                records = data.get_daily_aggregates(symbol, limit=max(self.settings.trend_days + 40, 260))
                bars = completed_bars(records, now - self.settings.data_delay_seconds)
                observed = {str(b.timestamp): [b.open, b.high, b.low, b.close] for b in bars}
                for timestamp, prices in self.observed.get(symbol, {}).items():
                    if timestamp in observed and any(not math.isclose(a, b, rel_tol=1e-8)
                                                     for a, b in zip(prices, observed[timestamp])):
                        raise DataIntegrityError("history_revised_review_required")
                previous = self.checkpoints.get(symbol)
                if previous:
                    overlap = next((b for b in bars if b.timestamp == previous["timestamp"]), None)
                    if overlap is None or any(not math.isclose(getattr(overlap, key), previous[key], rel_tol=1e-8)
                                              for key in ("open", "high", "low", "close")):
                        raise DataIntegrityError("history_revised_or_recovery_window_exceeded")
                    recent = [b for b in bars if b.timestamp > previous["timestamp"]]
                    prev_close = previous["close"]
                    for bar in recent:
                        if abs(bar.open / prev_close - 1) > 0.25:
                            raise DataIntegrityError("corporate_action_or_extreme_gap_review_required")
                        prev_close = bar.close
                for record in records:
                    if float(record.get("split_factor", 1)) != 1:
                        event = Bar.from_record(record)
                        if previous and event.timestamp > previous["timestamp"] and event.available_at <= now:
                            raise DataIntegrityError("corporate_action_review_required")
                if not bars:
                    raise ValueError("No completed daily bars")
                histories[symbol] = bars
                if len(bars) < self.settings.trend_days + 1:
                    errors[symbol] = "insufficient_history"
                # Missing sessions are not implicitly treated as flat/no-risk days.
                dates = [datetime.fromtimestamp(b.timestamp, timezone.utc).date().isoformat() for b in bars]
                if len(calendar().sessions_in_range(dates[0], dates[-1])) != len(dates):
                    histories.pop(symbol)
                    raise DataIntegrityError("missing_daily_sessions")
                if bars[-1].available_at < expected:
                    errors[symbol] = "stale_daily_data"
                self.observed[symbol] = observed
            except Exception as exc:
                # Do not include provider exceptions: some contain credential-bearing URLs.
                errors[symbol] = str(exc) if isinstance(exc, DataIntegrityError) else f"data_unavailable:{type(exc).__name__}"

        # Management is independent of entry signals and runs for every available owned position.
        timeline: dict[float, list[tuple[str, Bar]]] = {}
        for symbol, bars in histories.items():
            for bar in bars:
                if bar.timestamp > self.broker.processed.get(symbol, 0):
                    timeline.setdefault(bar.available_at, []).append((symbol, bar))
        for close, entries in sorted(timeline.items()):
            # Capture the baseline BEFORE applying opening gaps / fills for this session.
            self._risk_check(close)
            for symbol, bar in entries:
                self.broker.process_bar(symbol, bar)
                self.checkpoints[symbol] = asdict(bar)
                self.broker.events.append({"event": "bar", "symbol": symbol, "provider": self.provider, **asdict(bar)})
            self._risk_check(close)
        # An outage blocks/cancels new risk, while protective management remains active.
        if errors:
            self.broker.cancel_entries(now, "data_unavailable")
        for order in self.broker.pending():
            if order.expires_at < now - self.settings.data_delay_seconds:
                self.broker._finish(order, "expired", now, "order_expired")

        self._risk_check(expected)
        decisions = []
        for symbol in self.settings.paper_symbols:
            bars = histories.get(symbol, [])
            if symbol in errors or not bars:
                continue
            session = datetime.fromtimestamp(bars[-1].timestamp, timezone.utc).date().isoformat()
            if self.decisions.get(symbol) == session:
                continue
            self.decisions[symbol] = session
            decision = decide(bars, held=symbol in self.broker.positions, trend_days=self.settings.trend_days)
            result = {"symbol": symbol, "session": session, **asdict(decision), "submitted": False}
            next_session = calendar().next_session(session)
            weekly_entry_day = next_session.isocalendar()[:2] != calendar().date_to_session(session).isocalendar()[:2]
            if decision.action == "BUY" and not weekly_entry_day:
                result["reason"] = "entry_review_weekly"
            elif decision.action != "HOLD" and not self.broker.pending(symbol):
                qty, limit = (entry_quantity(self.broker, self.settings, bars[-1].close, decision.stop_loss_pct)
                              if decision.action == "BUY" else (self.broker.positions[symbol].qty, bars[-1].close))
                if decision.action == "BUY" and (self.halted or errors):
                    result["reason"] = "entries_halted"
                elif qty <= 0:
                    result["reason"] = "risk_or_cash_limit"
                elif self.settings.dry_run:
                    result["reason"] = "dry_run"
                else:
                    oid = f"{self.settings.paper_account_id}:{session}:{symbol}:{decision.action}"
                    order = PaperOrder(oid, symbol, decision.action, qty, now,
                                       calendar().session_close(next_session).timestamp(), limit,
                                       decision.stop_loss_pct, decision.take_profit_pct, decision.reason)
                    # Late-start workers must never submit an already-expired intent.
                    if now <= calendar().session_open(next_session).timestamp():
                        result["submitted"] = self.broker.submit(order)
                    else:
                        result["reason"] = "missed_next_session_open"
            decisions.append(result)
            self.broker.events.append({"event": "decision", "timestamp": now, **result})
        self.last_step = now
        return {"timestamp": now, "mode": "paper", "broker": "simulated", "currency": "USD",
                "account": self.settings.paper_account_id, "strategy": self.settings.strategy,
                "status": "degraded" if errors else ("halted" if self.halted else "ok"),
                "data_session": expected_day, "data_provider": self.provider, "errors": errors, "equity": self.broker.equity(),
                "cash": self.broker.cash, "reserved_cash": self.broker.reserved_cash(),
                "exposure": self.broker.exposure(), "unrealized_pnl": sum(
                    (p.current_price - p.entry_price) * p.qty - p.entry_fee for p in self.broker.positions.values()),
                "realized_pnl": sum(t.pnl for t in self.broker.trades),
                "positions": {k: asdict(v) for k, v in self.broker.positions.items()},
                "pending_orders": [asdict(o) for o in self.broker.pending()],
                "risk": dict(self.risk), "decisions": decisions}
