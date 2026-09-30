from dataclasses import replace
import json

import pandas as pd
import pytest

from backtest.data_feed import BarDataFeed
from backtest.runner import BacktestRunner, HistoricalDailyData
from backtest.sim_broker import PaperOrder, SimBroker
from core.config import Settings
from core.market_calendar import calendar, latest_completed_session, session_bounds
from data.bars import Bar, completed_bars
from trader.execution_adapter import entry_quantity
from trader.paper_engine import PaperEngine
from trader.paper_store import PaperStore


@pytest.fixture
def history():
    sessions = calendar().sessions_in_range("2024-01-02", "2025-03-31")
    rows = []
    for i, day in enumerate(sessions):
        price = 10 + i * 0.01
        rows.append(dict(timestamp=day.timestamp(), open=price, high=price + 0.02,
                         low=price - 0.02, close=price, volume=100_000))
    return rows


def config(**changes):
    return replace(Settings(), dry_run=False, paper_initial_cash=1000, paper_symbols=["TEST"],
                   paper_data_provider="historical", **changes)


def data_at(rows, index):
    data = HistoricalDailyData(BarDataFeed({"TEST": pd.DataFrame(rows)}))
    data.now = Bar.from_record(rows[index]).available_at
    return data, data.now + 900


def friday(rows):
    return next(i for i, row in enumerate(rows) if i > 205 and pd.Timestamp(row["timestamp"], unit="s").dayofweek == 4)


def test_daily_bars_not_available_until_close_and_holidays():
    assert session_bounds("2024-07-04") is None
    _, close = session_bounds("2024-07-03")
    assert pd.Timestamp(close, unit="s", tz="UTC").hour == 17  # early close, EDT
    assert latest_completed_session(close - 1) == "2024-07-02"
    assert latest_completed_session(close) == "2024-07-03"
    record = dict(timestamp=pd.Timestamp("2024-07-03").timestamp(), open=10, high=11, low=9, close=10, volume=100)
    assert completed_bars([record], close - 1) == []
    assert len(completed_bars([record], close)) == 1
    assert session_bounds("2024-03-08")[0] % 86400 == 14.5 * 3600
    assert session_bounds("2024-03-11")[0] % 86400 == 13.5 * 3600


def test_next_bar_execution_and_restart_idempotency(history, tmp_path):
    i = friday(history)
    data, now = data_at(history, i)
    settings = config()
    engine = PaperEngine(settings)
    first = engine.step(data, now)
    assert first["status"] == "ok"
    assert first["pending_orders"]
    assert first["positions"] == {}
    assert first["cash"] == 1000
    store = PaperStore(tmp_path / "state.sqlite3", account=settings.paper_account_id, strategy=settings.strategy)
    with store.locked():
        store.save(engine.snapshot(), engine.broker.events)
    restarted = PaperEngine(settings, store.load())
    again = restarted.step(data, now + 60)
    assert len(again["pending_orders"]) == 1
    assert restarted.broker.events == []
    data, now = data_at(history, i + 1)
    filled = restarted.step(data, now)
    assert filled["pending_orders"] == []
    assert filled["positions"]["TEST"]["entry_timestamp"] == Bar.from_record(history[i + 1]).timestamp
    assert 0 <= filled["cash"] < 1000
    twice = PaperEngine(settings, restarted.snapshot())
    assert twice.step(data, now)["equity"] == filled["equity"]
    assert twice.broker.events == []
    store.close()


def test_exits_run_during_other_symbol_outage_and_risk_halt(history):
    settings = config(max_daily_loss_pct=0.001)
    engine = PaperEngine(settings)
    i = friday(history)
    data, now = data_at(history, i)
    engine.step(data, now)
    data, now = data_at(history, i + 1)
    engine.step(data, now)
    stop = engine.broker.positions["TEST"].stop_price
    rows = [dict(r) for r in history]
    rows[i + 2].update(open=stop - 0.02, low=stop - 0.1, high=stop, close=stop - 0.05)
    # Add a failing symbol after construction for this outage-management test only.
    settings.paper_symbols.append("MISSING")
    data, now = data_at(rows, i + 2)
    report = engine.step(data, now)
    assert report["status"] == "degraded"
    assert report["positions"] == {}
    assert report["risk"]["daily_halt"]
    assert engine.broker.trades[-1].reason == "stop_loss"
    assert engine.broker.trades[-1].exit_price < stop


@pytest.mark.parametrize("fault", ["stale", "revised", "missing", "nan", "duplicate", "gap"])
def test_bad_data_blocks_entries_and_never_rewrites_fills(history, fault):
    i = friday(history)
    rows = [dict(r) for r in history]
    engine = PaperEngine(config())
    data, now = data_at(rows, i - 1)
    engine.step(data, now)
    if fault == "stale":
        rows = rows[:i]
    elif fault == "revised":
        rows[i - 1]["close"] += 0.001
    elif fault == "missing":
        rows.pop(i - 5)
    elif fault == "nan":
        rows[i]["close"] = float("nan")
    elif fault == "duplicate":
        rows.insert(i, dict(rows[i]))
    else:
        rows[i].update(open=30, high=31, low=29, close=30)
    data = HistoricalDailyData(BarDataFeed({"TEST": pd.DataFrame(history)}))
    # Inject provider payload defects after availability normalization.
    data.rows["TEST"] = rows
    data.available["TEST"] = [Bar.from_record(r).available_at for r in history][:len(rows)]
    if fault == "duplicate":
        data.available["TEST"] = [Bar.from_record(r).available_at for r in rows]
    now = Bar.from_record(history[i]).available_at + 900
    data.now = now - 900
    report = engine.step(data, now)
    assert report["status"] == "degraded"
    assert report["pending_orders"] == []
    assert report["positions"] == {}
    assert report["cash"] == 1000


def test_dry_run_and_insufficient_history(history):
    settings = config()
    settings.dry_run = True
    engine = PaperEngine(settings)
    data, now = data_at(history, friday(history))
    report = engine.step(data, now)
    assert report["decisions"][0]["reason"] == "dry_run"
    assert report["pending_orders"] == []
    young = PaperEngine(config())
    data, now = data_at(history, 10)
    assert young.step(data, now)["errors"]["TEST"] == "insufficient_history"


def test_cash_and_pending_exposure_caps():
    broker = SimBroker(1000)
    settings = config(daily_budget_usd=200, max_position_size=200, max_risk_pct=0.05,
                      max_portfolio_risk_pct=0.1, max_gross_exposure_pct=0.5)
    qty, limit = entry_quantity(broker, settings, 10, .05)
    assert broker.submit(PaperOrder("1", "AAA", "BUY", qty, 1, 10, limit))
    qty2, limit2 = entry_quantity(broker, settings, 10, .05)
    assert broker.exposure() + qty2 * limit2 <= 200
    assert broker.cash - broker.reserved_cash() >= 0
    assert entry_quantity(broker, settings, 10000, .05)[0] == 0
    assert not broker.submit(PaperOrder("2", "AAA", "BUY", 1, 1, 10, limit))


def test_ambiguous_bar_stops_first_and_duplicate_position_preserves_cash():
    broker = SimBroker(1000, slippage_bps=0, fee_bps=0, min_fee=0)
    assert broker.open_position("TEST", 10, 10, 1)
    cash = broker.cash
    assert not broker.open_position("TEST", 10, 10, 2)
    assert broker.cash == cash
    broker.process_bar("TEST", Bar(3, 4, 10, 12, 9, 11, 100))
    assert broker.trades[0].exit_price == 9.5
    assert broker.cash == 995
    broker.process_bar("TEST", Bar(3, 4, 10, 12, 9, 11, 100))
    assert len(broker.trades) == 1


def test_expired_and_gapped_orders_never_fill():
    broker = SimBroker(1000)
    broker.submit(PaperOrder("expired", "AAA", "BUY", 1, 1, 3, 10))
    broker.process_bar("AAA", Bar(4, 5, 10, 11, 9, 10, 100))
    assert broker.orders["expired"].status == "expired"
    broker.submit(PaperOrder("gapped", "AAA", "BUY", 1, 6, 9, 10))
    broker.process_bar("AAA", Bar(7, 8, 11, 12, 10, 11, 100))
    assert broker.orders["gapped"].status == "canceled"
    assert broker.cash == 1000


def test_store_lock_corruption_and_account_identity(tmp_path):
    path = tmp_path / "paper.sqlite3"
    a = PaperStore(path, account="a", strategy="daily_trend")
    b = PaperStore(path, account="b", strategy="daily_trend")
    with a.locked():
        with pytest.raises(RuntimeError), b.locked():
            pass
        a.save(PaperEngine(config()).snapshot(), [{"event": "init"}])
    with pytest.raises(ValueError, match="mismatch"):
        b.load()
    with pytest.raises(ValueError):
        a.save({"cash": float("nan")}, [])
    assert a.load()["broker"]["cash"] == 1000
    a.connection.execute("UPDATE state SET payload='broken'")
    a.connection.commit()
    with pytest.raises(json.JSONDecodeError):
        a.load()
    a.close()
    b.close()


def test_state_reconciliation_and_configuration_drift():
    engine = PaperEngine(config())
    state = engine.snapshot()
    state["broker"]["cash"] = 999
    with pytest.raises(ValueError, match="reconcile"):
        PaperEngine(config(), state)
    with pytest.raises(ValueError, match="configuration changed"):
        PaperEngine(replace(config(), paper_initial_cash=2000), engine.snapshot())
    bad = engine.snapshot()
    del bad["broker"]["orders"]
    with pytest.raises(ValueError, match="Incomplete"):
        PaperEngine(config(), bad)


def test_backtest_replays_same_engine_and_is_repeatable(history):
    feed = BarDataFeed({"TEST": pd.DataFrame(history)})
    runner = BacktestRunner(feed, settings=config())
    a = runner.run()
    b = runner.run()
    assert a.final_state == b.final_state
    data = HistoricalDailyData(feed)
    paper = PaperEngine(runner.settings)
    for close in data.available["TEST"]:
        data.now = close
        paper.step(data, close + 900)
    assert paper.snapshot() == a.final_state
    assert len(a.final_state["broker"]["orders"]) > 0
    assert all(report["cash"] >= 0 for report in a.reports)
    json.dumps(a.summary(), allow_nan=False)


def test_late_start_does_not_invent_an_opening_fill(history):
    i = friday(history)
    data, _ = data_at(history, i)
    monday_open = Bar.from_record(history[i + 1]).timestamp
    engine = PaperEngine(config())
    report = engine.step(data, monday_open + 60)
    assert report["pending_orders"] == []
    assert report["decisions"][0]["reason"] == "missed_next_session_open"


def test_provider_switch_and_future_changes_cannot_change_current_decision(history):
    i = friday(history)
    data, now = data_at(history, i)
    engine = PaperEngine(config())
    report = engine.step(data, now)
    data.daily_provider_name = "different"
    with pytest.raises(ValueError, match="source changed"):
        engine.step(data, now + 1)
    modified = [dict(r) for r in history]
    for row in modified[i + 1:]:
        for key in ("open", "high", "low", "close"):
            row[key] *= 3
    future, same_now = data_at(modified, i)
    other = PaperEngine(config()).step(future, same_now)
    assert report == other


def test_drawdown_halt_survives_restart_and_new_session(history):
    settings = config(max_drawdown_pct=0.001)
    engine = PaperEngine(settings)
    i = friday(history)
    data, now = data_at(history, i)
    engine.step(data, now)
    data, now = data_at(history, i + 1)
    engine.step(data, now)
    stop = engine.broker.positions["TEST"].stop_price
    rows = [dict(r) for r in history]
    rows[i + 2].update(open=stop, low=stop - .1, high=stop + .01, close=stop - .05)
    data, now = data_at(rows, i + 2)
    report = engine.step(data, now)
    assert report["risk"]["drawdown_halt"]
    restarted = PaperEngine(settings, engine.snapshot())
    data, now = data_at(rows, i + 3)
    assert restarted.step(data, now)["risk"]["drawdown_halt"]
    assert not restarted.broker.pending()


def test_partial_fill_cancels_remainder_and_releases_cash():
    broker = SimBroker(1000, slippage_bps=0, fee_bps=0, min_fee=0, partial_fill_ratio=.5)
    broker.submit(PaperOrder("partial", "TEST", "BUY", 10, 1, 5, 10))
    assert broker.reserved_cash() == 100
    broker.process_bar("TEST", Bar(2, 3, 10, 10.01, 9.99, 10, 100))
    assert broker.positions["TEST"].qty == 5
    assert broker.cash == 950
    assert broker.reserved_cash() == 0
    assert broker.orders["partial"].status == "filled"
    assert broker.events[-1]["reason"] == "partial_remainder_canceled"
