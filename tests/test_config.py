from core.config import Settings


def test_blank_numeric_env_values_use_defaults(monkeypatch):
    monkeypatch.setenv("MIN_PRICE", "")
    monkeypatch.setenv("MAX_DAILY_LOSS_PCT", "")
    monkeypatch.setenv("MAX_POSITION_SIZE", "")

    settings = Settings()

    assert settings.min_price == 2.0
    assert settings.max_daily_loss_pct == 0.03
    assert settings.max_position_size == settings.daily_budget_usd / 3


def test_invalid_numeric_env_values_use_defaults(monkeypatch):
    monkeypatch.setenv("MAX_RISK_PCT", "not-a-number")
    monkeypatch.setenv("MIN_DOLLAR_VOLUME", "bad")

    settings = Settings()

    assert settings.max_risk_pct == 0.005
    assert settings.min_dollar_volume == 8_000_000.0


def test_synthetic_ml_heuristic_fallback_is_opt_in(monkeypatch):
    monkeypatch.delenv("ALLOW_FALLBACK_ML", raising=False)

    settings = Settings()

    assert settings.allow_fallback_ml is False
