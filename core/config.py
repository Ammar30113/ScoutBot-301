from __future__ import annotations

import os
import re
import logging
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path

from dotenv import load_dotenv

logger = logging.getLogger(__name__)

load_dotenv()

ALPACA_API_KEY = os.getenv("APCA_API_KEY_ID")
ALPACA_API_SECRET = os.getenv("APCA_API_SECRET_KEY")
MODE = os.getenv("MODE", "paper")
DEFAULT_TWITTER_ALLOWED_ACCOUNTS = [
    "Benzinga",
    "MarketWatch",
    "WSJmarkets",
    "ReutersBiz",
    "Reuters",
    "BreakingMarkets",
    "federalreserve",
    "BLS_gov",
    "BEA_News",
    "EconUS",
    "bespokeinvest",
    "unusual_whales",
    "spotgamma",
    "Barchart",
    "Stocktwits",
    "YahooFinance",
]


def _normalize_env_value(value: str | None) -> str | None:
    if value is None:
        return None
    trimmed = value.strip()
    if len(trimmed) >= 2 and trimmed[0] == trimmed[-1] and trimmed[0] in ("'", '"'):
        trimmed = trimmed[1:-1].strip()
    return trimmed


def _get_str(name: str, default: str = "") -> str:
    val = _normalize_env_value(os.getenv(name))
    return val if val is not None else default


def _get_bool(name: str, default: bool) -> bool:
    val = _normalize_env_value(os.getenv(name))
    if val is None:
        return default
    return val.lower() in ("1", "true", "yes", "y")


def _get_optional_bool(name: str) -> bool | None:
    val = _normalize_env_value(os.getenv(name))
    if val is None:
        return None
    return val.lower() in ("1", "true", "yes", "y")


def _get_int(name: str, default: int) -> int:
    try:
        raw = _normalize_env_value(os.getenv(name))
        if raw is None:
            return default
        return int(raw)
    except ValueError:
        raw = _normalize_env_value(os.getenv(name))
        if not raw:
            return default
        match = re.search(r"-?\d+", raw)
        if not match:
            return default
        try:
            return int(match.group(0))
        except ValueError:
            return default


def _get_csv(name: str, default: list[str]) -> list[str]:
    raw = _normalize_env_value(os.getenv(name))
    if not raw:
        return default
    parts = [item.strip().lstrip("@") for item in raw.split(",")]
    parsed = [p for p in parts if p]
    return parsed or default


# Core sentiment/env toggles exposed for direct imports
USE_SENTIMENT = _get_bool("USE_SENTIMENT", True)
USE_TWITTER_NEWS = _get_bool("USE_TWITTER_NEWS", False)
OPENAI_MODEL = _normalize_env_value(os.getenv("OPENAI_MODEL")) or "gpt-3.5-turbo-16k"
SENTIMENT_CACHE_TTL = _get_int("SENTIMENT_CACHE_TTL", 300)


@dataclass
class Settings:
    """Central configuration object loaded from environment variables."""

    alpaca_api_key: str = field(default_factory=lambda: _get_str("ALPACA_API_KEY") or _get_str("APCA_API_KEY_ID", ""))
    alpaca_api_secret: str = field(
        default_factory=lambda: _get_str("ALPACA_API_SECRET") or _get_str("APCA_API_SECRET_KEY", "")
    )
    alpaca_base_url: str = field(
        default_factory=lambda: _get_str("ALPACA_API_BASE_URL", "https://paper-api.alpaca.markets")
    )
    alpaca_data_url: str = field(default_factory=lambda: _get_str("ALPACA_API_DATA_URL", "https://data.alpaca.markets/v2"))
    alpaca_data_feed: str = field(default_factory=lambda: _get_str("ALPACA_DATA_FEED", "iex"))
    trading_mode: str = field(default_factory=lambda: _get_str("MODE", "paper").lower())
    allow_live_trading: bool = field(default_factory=lambda: _get_bool("ALLOW_LIVE_TRADING", False))
    dry_run: bool = field(default_factory=lambda: _get_bool("DRY_RUN", False))
    allow_alpaca_daily: bool | None = field(default_factory=lambda: _get_optional_bool("ALLOW_ALPACA_DAILY"))
    strip_rate_limited_keys: bool = field(default_factory=lambda: _get_bool("STRIP_RATE_LIMITED_KEYS", False))
    skip_daily_on_rate_limit: bool = field(default_factory=lambda: _get_bool("SKIP_DAILY_ON_RATE_LIMIT", True))
    require_crash_data: bool = field(default_factory=lambda: _get_bool("REQUIRE_CRASH_DATA", False))
    execution_halt_cooldown_seconds: int = field(default_factory=lambda: _get_int("EXECUTION_HALT_COOLDOWN_SECONDS", 300))

    twelvedata_api_key: str = field(
        default_factory=lambda: _get_str("TWELVEDATA_API_KEY") or _get_str("TWELVEDATA_KEY", "")
    )
    alphavantage_api_key: str = field(
        default_factory=lambda: _get_str("ALPHAVANTAGE_API_KEY")
        or _get_str("ALPHAVANTAGE_KEY")
        or _get_str("ALPHA_VANTAGE_KEY", "")
    )
    marketstack_api_key: str = field(default_factory=lambda: _get_str("MARKETSTACK_API_KEY", ""))
    marketstack_cache_ttl: int = field(default_factory=lambda: _get_int("MARKETSTACK_CACHE_TTL", 86400))
    openai_api_key: str = field(default_factory=lambda: _get_str("OPENAI_API_KEY", ""))
    openai_model: str = field(default_factory=lambda: OPENAI_MODEL)
    use_sentiment: bool = field(default_factory=lambda: USE_SENTIMENT)
    sentiment_cache_ttl: int = field(default_factory=lambda: SENTIMENT_CACHE_TTL)
    use_twitter_news: bool = field(default_factory=lambda: USE_TWITTER_NEWS)
    twitter_bearer_token: str = field(default_factory=lambda: _get_str("TWITTER_BEARER_TOKEN", ""))
    allow_synthetic_ml: bool = field(default_factory=lambda: _get_bool("ALLOW_SYNTHETIC_ML", False))
    allow_fallback_ml: bool = field(default_factory=lambda: _get_bool("ALLOW_FALLBACK_ML", True))
    train_ml_on_startup: bool = field(default_factory=lambda: _get_bool("TRAIN_ML_ON_STARTUP", False))
    twitter_allowed_accounts: list[str] = field(
        default_factory=lambda: _get_csv("TWITTER_ALLOWED_ACCOUNTS", DEFAULT_TWITTER_ALLOWED_ACCOUNTS)
    )
    twitter_max_posts_per_day: int = field(default_factory=lambda: _get_int("TWITTER_MAX_POSTS_PER_DAY", 3))
    twitter_tweets_per_account: int = field(default_factory=lambda: _get_int("TWITTER_TWEETS_PER_ACCOUNT", 1))

    universe_fallback_csv: Path = field(
        default_factory=lambda: Path(_get_str("UNIVERSE_FALLBACK_CSV", "universe/fallback_universe.csv"))
    )
    universe_fallback_only: bool = field(default_factory=lambda: _get_bool("UNIVERSE_FALLBACK_ONLY", False))
    universe_allow_unfiltered_fallback: bool = field(
        default_factory=lambda: _get_bool("UNIVERSE_ALLOW_UNFILTERED_FALLBACK", True)
    )
    min_dollar_volume: float = field(default_factory=lambda: float(os.getenv("MIN_DOLLAR_VOLUME", 8_000_000)))
    min_mkt_cap: float = field(default_factory=lambda: float(os.getenv("MIN_MKT_CAP", 300_000_000)))
    max_mkt_cap: float = field(default_factory=lambda: float(os.getenv("MAX_MKT_CAP", 5_000_000_000)))
    min_price: float = field(default_factory=lambda: float(os.getenv("MIN_PRICE", 2.0)))
    max_price: float = field(default_factory=lambda: float(os.getenv("MAX_PRICE", 80.0)))
    max_universe_size: int = field(default_factory=lambda: _get_int("MAX_UNIVERSE_SIZE", 50))
    universe_candidate_limit: int = field(default_factory=lambda: _get_int("UNIVERSE_CANDIDATE_LIMIT", 0))
    universe_liquidity_top_n: int = field(default_factory=lambda: _get_int("UNIVERSE_LIQUIDITY_TOP_N", 300))
    cache_ttl: int = field(default_factory=lambda: _get_int("CACHE_TTL", 900))
    intraday_stale_seconds: int = field(default_factory=lambda: _get_int("INTRADAY_STALE_SECONDS", 900))
    daily_stale_seconds: int = field(default_factory=lambda: _get_int("DAILY_STALE_SECONDS", 432000))
    min_volume_history_days: int = field(default_factory=lambda: _get_int("MIN_VOLUME_HISTORY_DAYS", 3))
    allow_partial_fundamentals: bool = field(
        default_factory=lambda: _get_str("ALLOW_PARTIAL_FUNDAMENTALS", "true").lower() != "false"
    )
    allow_partial_atr: bool = field(default_factory=lambda: _get_str("ALLOW_PARTIAL_ATR", "true").lower() != "false")
    regime_gate_min_score: float = field(default_factory=lambda: float(os.getenv("REGIME_GATE_MIN_SCORE", "0.0")))

    scheduler_interval_seconds: int = field(default_factory=lambda: _get_int("SCHEDULER_INTERVAL_SECONDS", 900))
    max_positions: int = field(default_factory=lambda: _get_int("MAX_POSITIONS", 10))
    portfolio_state_path: Path = field(
        default_factory=lambda: Path(_get_str("PORTFOLIO_STATE_PATH", "data/portfolio_state.json"))
    )
    initial_equity: float = field(default_factory=lambda: float(os.getenv("INITIAL_EQUITY", "100000")))
    max_daily_loss_pct: float = field(default_factory=lambda: float(os.getenv("MAX_DAILY_LOSS_PCT", "0.03")))
    max_position_pct: float = field(
        default_factory=lambda: float(_normalize_env_value(os.getenv("MAX_POSITION_PCT")) or 0.0)
    )
    max_risk_pct: float = field(default_factory=lambda: float(os.getenv("MAX_RISK_PCT", "0.005")))
    atr_multiplier: float = field(default_factory=lambda: float(os.getenv("ATR_MULTIPLIER", "2.5")))
    min_confidence: float = field(default_factory=lambda: float(os.getenv("MIN_CONFIDENCE", "0.45")))
    default_timespan: str = field(default_factory=lambda: os.getenv("DEFAULT_TIMESPAN", "1day"))
    ml_trend_threshold: float = field(default_factory=lambda: float(os.getenv("ML_TREND_THRESHOLD", "0.20")))
    ml_reversal_threshold: float = field(default_factory=lambda: float(os.getenv("ML_REVERSAL_THRESHOLD", "0.26")))
    ml_heuristic_weight: float = field(default_factory=lambda: float(os.getenv("ML_HEURISTIC_WEIGHT", "0.8")))

    # P&L penalty thresholds (previously hardcoded in main.py)
    pnl_penalty_loss_threshold: float = field(default_factory=lambda: float(os.getenv("PNL_PENALTY_LOSS_THRESHOLD", "0.01")))
    pnl_penalty_loss_value: float = field(default_factory=lambda: float(os.getenv("PNL_PENALTY_LOSS_VALUE", "0.05")))
    pnl_penalty_gain_threshold: float = field(default_factory=lambda: float(os.getenv("PNL_PENALTY_GAIN_THRESHOLD", "0.02")))
    pnl_penalty_gain_value: float = field(default_factory=lambda: float(os.getenv("PNL_PENALTY_GAIN_VALUE", "-0.03")))

    # Crash mode overrides (previously hardcoded)
    crash_stop_loss_pct: float = field(default_factory=lambda: float(os.getenv("CRASH_STOP_LOSS_PCT", "0.005")))
    crash_take_profit_pct: float = field(default_factory=lambda: float(os.getenv("CRASH_TAKE_PROFIT_PCT", "0.015")))
    crash_max_hold_minutes: int = field(default_factory=lambda: _get_int("CRASH_MAX_HOLD_MINUTES", 60))
    crash_max_positions: int = field(default_factory=lambda: _get_int("CRASH_MAX_POSITIONS", 3))
    default_max_hold_minutes: int = field(default_factory=lambda: _get_int("DEFAULT_MAX_HOLD_MINUTES", 90))

    # Cache limits
    cache_max_size: int = field(default_factory=lambda: _get_int("CACHE_MAX_SIZE", 5000))


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    settings = Settings()
    settings.universe_fallback_csv.parent.mkdir(parents=True, exist_ok=True)
    settings.portfolio_state_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info("TWELVEDATA_API_KEY detected: %s", bool(settings.twelvedata_api_key))
    logger.info("ALPHAVANTAGE_API_KEY detected: %s", bool(settings.alphavantage_api_key))
    logger.info("MARKETSTACK_API_KEY detected: %s", bool(settings.marketstack_api_key))
    logger.info("OPENAI_API_KEY detected: %s", bool(settings.openai_api_key))
    logger.info("Trading mode=%s allow_live_trading=%s", settings.trading_mode, settings.allow_live_trading)
    if settings.use_twitter_news:
        logger.info(
            "Twitter news enabled with %d allowed accounts; bearer token: %s",
            len(settings.twitter_allowed_accounts),
            bool(settings.twitter_bearer_token),
        )
    return settings
