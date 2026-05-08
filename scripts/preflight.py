from __future__ import annotations

import os
import sys

from core.config import _normalize_env_value, get_settings

INT_ENV_VARS = (
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


def _is_live_trading_mode(settings) -> bool:
    base_url = (settings.alpaca_base_url or "").lower()
    mode = settings.trading_mode or "paper"
    return mode == "live" or ("paper" not in base_url)


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
            float(raw.replace("_", ""))
        except ValueError:
            errors.append(f"{name} must be a single number, not {raw!r}.")


def run_preflight() -> tuple[list[str], list[str]]:
    settings = get_settings()
    errors: list[str] = []
    warnings: list[str] = []
    _validate_numeric_env(errors)

    if _raw("APCA_API_BASE_URL") and not _raw("ALPACA_API_BASE_URL"):
        warnings.append("APCA_API_BASE_URL is ignored by this codebase; use ALPACA_API_BASE_URL.")

    providers = _provider_names(settings)
    if not settings.alpaca_api_key or not settings.alpaca_api_secret:
        errors.append("Alpaca trading credentials are missing; the worker cannot submit or reconcile orders.")
    if not providers:
        errors.append("No market-data providers are configured; universe, crash, and signal generation cannot run.")
    elif providers == ["alpaca"] and settings.allow_alpaca_daily is not False:
        warnings.append("Only Alpaca data is configured; add TwelveData, AlphaVantage, or Marketstack to reduce rate-limit pressure.")

    if _is_live_trading_mode(settings):
        if not settings.allow_live_trading:
            errors.append("Live Alpaca endpoint/mode detected but ALLOW_LIVE_TRADING is false.")
        else:
            errors.append("Live trading is enabled. Keep Railway offline until paper trading evidence is reviewed.")

    if settings.allow_synthetic_ml:
        errors.append("ALLOW_SYNTHETIC_ML=true is unsafe for a trading worker.")
    if settings.allow_fallback_ml:
        errors.append("ALLOW_FALLBACK_ML=true permits heuristic ML fallback when no real model is available.")
    if settings.train_ml_on_startup:
        warnings.append("TRAIN_ML_ON_STARTUP=true can slow startup and create data-dependent behavior; prefer offline training.")
    if settings.use_sentiment and not settings.openai_api_key:
        warnings.append("USE_SENTIMENT=true but OPENAI_API_KEY is missing; sentiment will be neutral.")
    if settings.use_twitter_news:
        warnings.append("USE_TWITTER_NEWS=true adds Twitter quota/rate-limit risk; disable it for the first paper boot.")
    if settings.dry_run is False:
        warnings.append("DRY_RUN=false will submit paper orders. Use DRY_RUN=true for the first Railway boot check.")
    if settings.allow_alpaca_daily and any(provider in providers for provider in ("twelvedata", "alphavantage", "marketstack")):
        warnings.append("ALLOW_ALPACA_DAILY=true is unnecessary with external daily providers and can increase Alpaca rate pressure.")
    if settings.universe_fallback_only:
        errors.append("UNIVERSE_FALLBACK_ONLY=true bypasses liquidity/fundamental filters and is not suitable for trading.")
    if settings.daily_budget_usd > 25_000:
        warnings.append("DAILY_BUDGET_USD is high for initial paper validation; start smaller until logs prove the loop.")
    if settings.max_position_size > 10_000:
        warnings.append("MAX_POSITION_SIZE is high for initial paper validation; start smaller until fills/exits look correct.")

    if settings.daily_budget_usd <= 0:
        errors.append("DAILY_BUDGET_USD must be positive.")
    if settings.max_daily_loss_pct <= 0:
        errors.append("MAX_DAILY_LOSS_PCT must be positive.")
    if settings.max_risk_pct <= 0:
        errors.append("MAX_RISK_PCT must be positive.")
    if settings.scheduler_interval_seconds < 60:
        warnings.append("SCHEDULER_INTERVAL_SECONDS below 60 can increase API pressure.")

    return errors, warnings


def main() -> int:
    errors, warnings = run_preflight()
    print("ScoutBot preflight")
    if warnings:
        print("\nWarnings:")
        for item in warnings:
            print(f"- {item}")
    if errors:
        print("\nErrors:")
        for item in errors:
            print(f"- {item}")
        return 1
    print("\nOK: paper worker preflight passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
