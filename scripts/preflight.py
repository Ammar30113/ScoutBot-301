from __future__ import annotations

import sys

from core.config import get_settings


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


def run_preflight() -> tuple[list[str], list[str]]:
    settings = get_settings()
    errors: list[str] = []
    warnings: list[str] = []

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
        warnings.append("ALLOW_FALLBACK_ML=true permits heuristic ML fallback when no real model is available.")
    if settings.train_ml_on_startup:
        warnings.append("TRAIN_ML_ON_STARTUP=true can slow startup and create data-dependent behavior; prefer offline training.")
    if settings.use_sentiment and not settings.openai_api_key:
        warnings.append("USE_SENTIMENT=true but OPENAI_API_KEY is missing; sentiment will be neutral.")

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
