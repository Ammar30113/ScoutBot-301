from __future__ import annotations

import os
from pathlib import Path
import math
import re
from urllib.parse import urlparse
import sys

from core.config import _normalize_env_value, get_settings

INT_ENV_VARS = (
    "TREND_DAYS",
    "DATA_DELAY_SECONDS",
    "CACHE_TTL",
    "MARKETSTACK_CACHE_TTL",
    "CACHE_MAX_SIZE",
    "CRASH_MAX_HOLD_MINUTES",
    "CRASH_MAX_POSITIONS",
    "DEFAULT_MAX_HOLD_MINUTES",
    "EXECUTION_HALT_COOLDOWN_SECONDS",
    "INTRADAY_STALE_SECONDS",
    "MAX_POSITIONS",
    "MAX_UNIVERSE_SIZE",
    "MIN_VOLUME_HISTORY_DAYS",
    "SCHEDULER_INTERVAL_SECONDS",
    "SENTIMENT_CACHE_TTL",
    "TWITTER_MAX_POSTS_PER_DAY",
    "TWITTER_TWEETS_PER_ACCOUNT",
    "UNIVERSE_CANDIDATE_LIMIT",
    "UNIVERSE_LIQUIDITY_TOP_N",
)

FLOAT_ENV_VARS = (
    "PAPER_INITIAL_CASH_USD",
    "PAPER_SLIPPAGE_BPS",
    "PAPER_FEE_BPS",
    "PAPER_MIN_FEE_USD",
    "MAX_GROSS_EXPOSURE_PCT",
    "MAX_PORTFOLIO_RISK_PCT",
    "MAX_DRAWDOWN_PCT",
    "ATR_MULTIPLIER",
    "CRASH_STOP_LOSS_PCT",
    "CRASH_TAKE_PROFIT_PCT",
    "DAILY_BUDGET_USD",
    "DAILY_STALE_SECONDS",
    "MAX_DAILY_LOSS_PCT",
    "MAX_MKT_CAP",
    "MAX_POSITION_PCT",
    "MAX_POSITION_SIZE",
    "MAX_PRICE",
    "MAX_RISK_PCT",
    "MIN_CONFIDENCE",
    "MIN_DOLLAR_VOLUME",
    "MIN_MKT_CAP",
    "MIN_PRICE",
    "ML_HEURISTIC_WEIGHT",
    "ML_REVERSAL_THRESHOLD",
    "ML_TREND_THRESHOLD",
    "PNL_PENALTY_GAIN_THRESHOLD",
    "PNL_PENALTY_GAIN_VALUE",
    "PNL_PENALTY_LOSS_THRESHOLD",
    "PNL_PENALTY_LOSS_VALUE",
    "REGIME_GATE_MIN_SCORE",
)


def _provider_names(settings) -> list[str]:
    providers = []
    if settings.alpaca_api_key and settings.alpaca_api_secret:
        providers.append("alpaca")
    if settings.twelvedata_api_key:
        providers.append("twelvedata")
    if settings.alphavantage_api_key:
        providers.append("alphavantage")
    if settings.marketstack_api_key:
        providers.append("marketstack")
    return providers


def _raw(name: str) -> str | None:
    return _normalize_env_value(os.getenv(name))


def _validate_numeric_env(errors: list[str]) -> None:
    for name in INT_ENV_VARS:
        raw = _raw(name)
        if not raw:
            continue
        try:
            int(raw.replace("_", ""))
        except ValueError:
            errors.append(f"{name} must be a single integer, not {raw!r}.")
    for name in FLOAT_ENV_VARS:
        raw = _raw(name)
        if not raw:
            continue
        try:
            value = float(raw.replace("_", ""))
            if not math.isfinite(value):
                raise ValueError("non-finite")
        except ValueError:
            errors.append(f"{name} must be a single number, not {raw!r}.")


def run_preflight() -> tuple[list[str], list[str]]:
    settings = get_settings()
    errors: list[str] = []
    warnings: list[str] = []
    _validate_numeric_env(errors)
    bool_names = ("DRY_RUN", "ALLOW_LIVE_TRADING", "USE_SENTIMENT", "USE_TWITTER_NEWS", "ALLOW_SYNTHETIC_ML",
                  "ALLOW_FALLBACK_ML", "TRAIN_ML_ON_STARTUP", "UNIVERSE_FALLBACK_ONLY",
                  "UNIVERSE_ALLOW_UNFILTERED_FALLBACK", "REQUIRE_CRASH_DATA", "ALLOW_ALPACA_DAILY",
                  "SKIP_DAILY_ON_RATE_LIMIT", "STRIP_RATE_LIMITED_KEYS")
    for name in bool_names:
        value = _raw(name)
        if value is not None and value.lower() not in {"true", "false", "1", "0", "yes", "no", "y", "n"}:
            errors.append(f"{name} must be a boolean.")
    if settings.trading_mode != "paper" or settings.allow_live_trading:
        errors.append("Only MODE=paper with ALLOW_LIVE_TRADING=false is supported. Real-money execution is disabled.")
    if settings.broker != "simulated":
        errors.append("BROKER must be simulated; no external execution broker is integrated.")
    if settings.strategy != "daily_trend":
        errors.append("STRATEGY must be daily_trend. Legacy strategies are offline research only.")
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", settings.paper_account_id):
        errors.append("PAPER_ACCOUNT_ID must contain only letters, digits, underscores, or hyphens (1-64 characters).")
    symbols = settings.paper_symbols
    if not symbols or len(set(symbols)) != len(symbols) or any(not re.fullmatch(r"[A-Z][A-Z0-9.-]{0,14}", s) for s in symbols):
        errors.append("PAPER_SYMBOLS must be a unique comma-separated uppercase USD stock/ETF allowlist.")
    if len(symbols) > settings.max_universe_size:
        errors.append("PAPER_SYMBOLS exceeds MAX_UNIVERSE_SIZE; the allowlist will not be silently truncated.")
    providers = _provider_names(settings)
    if not providers or (providers == ["alpaca"] and settings.allow_alpaca_daily is not True):
        errors.append("No market-data providers configured. Add TWELVEDATA_API_KEY, MARKETSTACK_API_KEY, or Alpaca data credentials.")
    if settings.paper_data_provider not in {"auto", "twelvedata", "marketstack", "alpaca", "alphavantage"}:
        errors.append("PAPER_DATA_PROVIDER is invalid.")
    elif settings.paper_data_provider != "auto" and settings.paper_data_provider not in providers:
        errors.append("PAPER_DATA_PROVIDER credentials are missing.")
    elif settings.paper_data_provider == "auto":
        warnings.append("PAPER_DATA_PROVIDER=auto selects one provider. Pin it before a durable run to prevent history changes.")
    if settings.paper_data_provider == "alpaca" and settings.allow_alpaca_daily is not True:
        errors.append("Alpaca daily provider requires ALLOW_ALPACA_DAILY=true.")
    for name in ("ALPACA_API_DATA_URL",):
        value = _raw(name)
        if value and urlparse(value).scheme != "https":
            errors.append(f"{name} must use HTTPS.")
    if settings.heartbeat_url and urlparse(settings.heartbeat_url).scheme != "https":
        errors.append("HEARTBEAT_URL must use HTTPS.")
    for name in ("APCA_API_BASE_URL", "ALPACA_API_BASE_URL", "FINVIZ_TOKEN", "GEMINI_API_KEY", "USE_FINNHUB", "URL"):
        if _raw(name):
            warnings.append(f"{name} is unused by the simulated paper worker; remove it from this service.")
    for name in ("USE_SENTIMENT", "USE_TWITTER_NEWS", "TRAIN_ML_ON_STARTUP", "ALLOW_SYNTHETIC_ML", "ALLOW_FALLBACK_ML",
                 "UNIVERSE_FALLBACK_ONLY", "UNIVERSE_ALLOW_UNFILTERED_FALLBACK", "REQUIRE_CRASH_DATA"):
        if (_raw(name) or "false").lower() in {"true", "1", "yes", "y"}:
            errors.append(f"{name} must be false for the daily paper worker; implicit strategy/data fallbacks are disabled.")
    raw_position_size = _raw("MAX_POSITION_SIZE")
    if raw_position_size:
        try:
            if float(raw_position_size.replace("_", "")) <= 0:
                errors.append("MAX_POSITION_SIZE must be positive when explicitly configured.")
        except ValueError:
            pass  # Already reported by numeric validation.
    for attr in ("paper_initial_cash", "daily_budget_usd", "max_position_size"):
        if getattr(settings, attr) <= 0:
            errors.append(f"{attr} must be positive.")
    for attr in ("max_daily_loss_pct", "max_risk_pct", "max_gross_exposure_pct", "max_portfolio_risk_pct", "max_drawdown_pct"):
        if not 0 < getattr(settings, attr) <= 1:
            errors.append(f"{attr} must be in (0, 1].")
    if not 0 <= settings.max_position_pct <= 1:
        errors.append("MAX_POSITION_PCT must be in [0, 1].")
    if settings.max_risk_pct > settings.max_portfolio_risk_pct:
        errors.append("MAX_RISK_PCT cannot exceed MAX_PORTFOLIO_RISK_PCT.")
    for attr in ("paper_slippage_bps", "paper_fee_bps", "paper_min_fee"):
        if getattr(settings, attr) < 0:
            errors.append(f"{attr} cannot be negative.")
    if settings.paper_slippage_bps >= 10000 or settings.paper_fee_bps >= 10000:
        errors.append("Paper slippage/fee basis points must be below 10000.")
    for name in ("MAX_POSITIONS", "MAX_UNIVERSE_SIZE", "CACHE_MAX_SIZE", "CACHE_TTL", "MARKETSTACK_CACHE_TTL"):
        raw = _raw(name)
        if raw and raw.replace("_", "").lstrip("-").isdigit() and int(raw.replace("_", "")) <= 0:
            errors.append(f"{name} must be positive.")
    if settings.trend_days < 20 or settings.trend_days > 960:
        errors.append("TREND_DAYS must be between 20 and 960.")
    if settings.data_delay_seconds < 0:
        errors.append("DATA_DELAY_SECONDS cannot be negative.")
    if settings.scheduler_interval_seconds < 60:
        errors.append("SCHEDULER_INTERVAL_SECONDS must be at least 60.")
    if os.getenv("RAILWAY_ENVIRONMENT_ID"):
        mount = os.getenv("RAILWAY_VOLUME_MOUNT_PATH")
        if not mount or not settings.paper_state_path.resolve().is_relative_to(Path(mount).resolve()):
            errors.append("Railway requires a persistent volume and PAPER_STATE_PATH inside RAILWAY_VOLUME_MOUNT_PATH.")
    if settings.strip_rate_limited_keys:
        errors.append("STRIP_RATE_LIMITED_KEYS must be false; recover from rate limits without disabling the provider permanently.")
    if settings.max_position_size > settings.paper_initial_cash or settings.daily_budget_usd > settings.paper_initial_cash:
        warnings.append("Configured notional limits exceed initial cash; available cash and portfolio exposure still cap every order.")
    if settings.dry_run:
        warnings.append("DRY_RUN=true: decisions are recorded but no simulated orders are submitted.")
    else:
        warnings.append("DRY_RUN=false: fills are simulated only. No real brokerage orders can be sent.")
    return errors, warnings


def main() -> int:
    errors, warnings = run_preflight()
    print("ScoutBot simulated paper preflight")
    for item in warnings:
        print(f"WARNING: {item}")
    for item in errors:
        print(f"ERROR: {item}")
    if not errors:
        print("OK: configuration valid; provider entitlement and fresh data must still pass a worker cycle.")
    return int(bool(errors))


if __name__ == "__main__":
    sys.exit(main())
