from core.config import get_settings
from scripts.preflight import run_preflight


def _reset_settings():
    get_settings.cache_clear()


def test_preflight_rejects_unsafe_trading_env(monkeypatch):
    monkeypatch.setenv("APCA_API_KEY_ID", "key")
    monkeypatch.setenv("APCA_API_SECRET_KEY", "secret")
    monkeypatch.setenv("TWELVEDATA_API_KEY", "td")
    monkeypatch.setenv("ALLOW_FALLBACK_ML", "true")
    monkeypatch.setenv("UNIVERSE_FALLBACK_ONLY", "true")
    monkeypatch.setenv("MAX_UNIVERSE_SIZE", "8-12")
    _reset_settings()

    errors, _warnings = run_preflight()

    assert any(e.startswith("ALLOW_FALLBACK_ML must be false") for e in errors)
    assert any(e.startswith("UNIVERSE_FALLBACK_ONLY must be false") for e in errors)
    assert "MAX_UNIVERSE_SIZE must be a single integer, not '8-12'." in errors

    _reset_settings()


def test_preflight_accepts_minimal_paper_env(monkeypatch):
    monkeypatch.setenv("APCA_API_KEY_ID", "key")
    monkeypatch.setenv("APCA_API_SECRET_KEY", "secret")
    monkeypatch.setenv("TWELVEDATA_API_KEY", "td")
    monkeypatch.setenv("USE_SENTIMENT", "false")
    monkeypatch.setenv("DRY_RUN", "true")
    monkeypatch.setenv("ALLOW_FALLBACK_ML", "false")
    monkeypatch.setenv("UNIVERSE_FALLBACK_ONLY", "false")
    _reset_settings()

    errors, _warnings = run_preflight()

    assert errors == []

    _reset_settings()
