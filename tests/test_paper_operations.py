import json
from unittest.mock import Mock

import pytest

from core.config import get_settings
from core.io import atomic_json
from scripts.paper_backup import backup
from scripts.paper_health import check_health
from scripts.preflight import run_preflight
from trader.paper_store import PaperStore


@pytest.mark.parametrize("name,value", [("MODE", "live"), ("BROKER", "ibkr"), ("DRY_RUN", "maybe"),
    ("PAPER_INITIAL_CASH_USD", "nan"), ("MAX_POSITIONS", "0"), ("MAX_RISK_PCT", "1.5"), ("MAX_POSITION_SIZE", "-1"), ("DRY_RUN", ""),
    ("STRATEGY", "momentum"), ("STRIP_RATE_LIMITED_KEYS", "true"), ("MAX_UNIVERSE_SIZE", "1"),
    ("PAPER_SYMBOLS", "SPY,SPY"), ("HEARTBEAT_URL", "http://monitor.invalid")])
def test_preflight_rejects_invalid_configuration(monkeypatch, name, value):
    monkeypatch.setenv("TWELVEDATA_API_KEY", "test-key")
    monkeypatch.setenv(name, value)
    get_settings.cache_clear()
    try:
        errors, _ = run_preflight()
        assert errors
    finally:
        get_settings.cache_clear()


def test_railway_requires_mounted_persistent_state(monkeypatch, tmp_path):
    monkeypatch.setenv("TWELVEDATA_API_KEY", "test-key")
    monkeypatch.setenv("RAILWAY_ENVIRONMENT_ID", "test")
    monkeypatch.delenv("RAILWAY_VOLUME_MOUNT_PATH", raising=False)
    get_settings.cache_clear()
    try:
        assert any("persistent volume" in e for e in run_preflight()[0])
        monkeypatch.setenv("RAILWAY_VOLUME_MOUNT_PATH", str(tmp_path))
        monkeypatch.setenv("PAPER_STATE_PATH", str(tmp_path / "state.sqlite3"))
        get_settings.cache_clear()
        assert run_preflight()[0] == []
    finally:
        get_settings.cache_clear()


def test_healthy_stale_failed_and_missing_reports(tmp_path):
    path = tmp_path / "status.json"
    with pytest.raises(OSError):
        check_health(path, 60, now=100)
    atomic_json(path, {"timestamp": 50, "status": "ok"})
    assert check_health(path, 60, now=100)["status"] == "ok"
    with pytest.raises(ValueError):
        check_health(path, 60, now=200)
    atomic_json(path, {"timestamp": 100, "status": "degraded"})
    with pytest.raises(ValueError):
        check_health(path, 60, now=110)


def test_snapshot_and_events_rollback_together_and_backup(tmp_path):
    source = tmp_path / "paper.sqlite3"
    store = PaperStore(source, account="test", strategy="daily_trend")
    store.save({"version": 1}, [{"event": "first"}])
    store.connection.execute("CREATE TRIGGER reject_event BEFORE INSERT ON events BEGIN SELECT RAISE(ABORT, 'test failure'); END")
    with pytest.raises(Exception, match="test failure"):
        store.save({"version": 2}, [{"event": "second"}])
    assert store.load() == {"version": 1}
    assert store.connection.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 1
    destination = tmp_path / "backup.sqlite3"
    backup(source, destination)
    restored = PaperStore(destination, account="test", strategy="daily_trend")
    assert restored.load() == {"version": 1}
    with pytest.raises(FileExistsError):
        backup(source, destination)
    restored.close()
    store.close()


def test_logging_redacts_provider_urls_and_secrets(monkeypatch):
    import logging
    from core.logger import SecretFilter
    monkeypatch.setenv("TWELVEDATA_API_KEY", "super-secret-value")
    record = logging.LogRecord("test", 30, "file", 1, "error %s %s", ("https://provider.invalid?apikey=other-secret", "super-secret-value"), None)
    SecretFilter().filter(record)
    assert "super-secret" not in record.getMessage()
    assert "other-secret" not in record.getMessage()


def test_main_does_not_construct_worker_for_live_mode(monkeypatch):
    import main
    monkeypatch.setenv("MODE", "live")
    monkeypatch.setattr("sys.argv", ["main.py", "--once"])
    constructor = Mock(side_effect=AssertionError("must not reach execution"))
    monkeypatch.setattr(main, "PaperStore", constructor)
    get_settings.cache_clear()
    try:
        assert main.main() == 2
        constructor.assert_not_called()
    finally:
        get_settings.cache_clear()


def test_main_persists_status_and_ledger_without_network(monkeypatch, tmp_path):
    import main
    monkeypatch.setenv("PAPER_STATE_PATH", str(tmp_path / "paper.sqlite3"))
    get_settings.cache_clear()
    settings = get_settings()
    store = PaperStore(settings.paper_state_path, account=settings.paper_account_id, strategy=settings.strategy)
    router = Mock()
    router.daily_provider_name = "fixture"
    router.get_daily_aggregates.return_value = []
    try:
        report = main.run_once(store, router, 1730493000)
        assert report["status"] == "degraded"
        assert store.load()["broker"]["cash"] == settings.paper_initial_cash
        assert json.loads(settings.paper_state_path.with_suffix(".status.json").read_text()) == report
    finally:
        store.close()
        get_settings.cache_clear()
