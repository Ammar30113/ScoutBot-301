from __future__ import annotations

import os
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


def _get_bool(name: str, default: bool) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return val.lower() in ("1", "true", "yes", "y")


def _get_optional_bool(name: str) -> bool | None:
    val = os.getenv(name)
    if val is None:
        return None
    return val.lower() in ("1", "true", "yes", "y")


def _get_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


def _get_csv(name: str, default: list[str]) -> list[str]:
    raw = os.getenv(name)
    if not raw:
        return default
    parts = [item.strip().lstrip("@") for item in raw.split(",")]
    parsed = [p for p in parts if p]
    return parsed or default


# Core sentiment/env toggles exposed for direct imports
USE_SENTIMENT = _get_bool("USE_SENTIMENT", True)
USE_TWITTER_NEWS = _get_bool("USE_TWITTER_NEWS", False)
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-3.5-turbo-16k")
try:
    SENTIMENT_CACHE_TTL = int(os.getenv("SENTIMENT_CACHE_TTL", "300"))
except ValueError:
    SENTIMENT_CACHE_TTL = 300


@dataclass
class Settings:
    """Central configuration object loaded from environment variables."""

    alpaca_api_key: str = field(default_factory=lambda: os.getenv("ALPACA_API_KEY") or os.getenv("APCA_API_KEY_ID", ""))
    alpaca_api_secret: str = field(
        default_factory=lambda: os.getenv("ALPACA_API_SECRET") or os.getenv("APCA_API_SECRET_KEY", "")
    )
    alpaca_base_url: str = field(default_factory=lambda: os.getenv("ALPACA_API_BASE_URL", "https://paper-api.alpaca.markets"))
    alpaca_data_url: str = field(default_factory=lambda: os.getenv("ALPACA_API_DATA_URL", "https://data.alpaca.markets/v2"))
    trading_mode: str = field(default_factory=lambda: os.getenv("MODE", "paper").lower())
    allow_live_trading: bool = field(default_factory=lambda: _get_bool("ALLOW_LIVE_TRADING", False))
    dry_run: bool = field(default_factory=lambda: _get_bool("DRY_RUN", False))
    allow_alpaca_daily: bool | None = field(default_factory=lambda: _get_optional_bool("ALLOW_ALPACA_DAILY"))

    twelvedata_api_key: str = field(default_factory=lambda: os.getenv("TWELVEDATA_API_KEY") or os.getenv("TWELVEDATA_KEY", ""))
    alphavantage_api_key: str = field(
        default_factory=lambda: os.getenv("ALPHAVANTAGE_API_KEY")
        or os.getenv("ALPHAVANTAGE_KEY")
        or os.getenv("ALPHA_VANTAGE_KEY", "")
    )
    marketstack_api_key: str = field(default_factory=lambda: os.getenv("MARKETSTACK_API_KEY", ""))
    marketstack_cache_ttl: int = field(default_factory=lambda: int(os.getenv("MARKETSTACK_CACHE_TTL", "86400")))
    openai_api_key: str = field(default_factory=lambda: os.getenv("OPENAI_API_KEY", ""))
    openai_model: str = field(default_factory=lambda: OPENAI_MODEL)
    use_sentiment: bool = field(default_factory=lambda: USE_SENTIMENT)
    sentiment_cache_ttl: int = field(default_factory=lambda: SENTIMENT_CACHE_TTL)
    use_twitter_news: bool = field(default_factory=lambda: USE_TWITTER_NEWS)
    twitter_bearer_token: str = field(default_factory=lambda: os.getenv("TWITTER_BEARER_TOKEN", ""))
    allow_synthetic_ml: bool = field(default_factory=lambda: _get_bool("ALLOW_SYNTHETIC_ML", False))
    twitter_allowed_accounts: list[str] = field(
        default_factory=lambda: _get_csv("TWITTER_ALLOWED_ACCOUNTS", DEFAULT_TWITTER_ALLOWED_ACCOUNTS)
    )
    twitter_max_posts_per_day: int = field(default_factory=lambda: _get_int("TWITTER_MAX_POSTS_PER_DAY", 3))
    twitter_tweets_per_account: int = field(default_factory=lambda: _get_int("TWITTER_TWEETS_PER_ACCOUNT", 1))

    universe_fallback_csv: Path = field(
        default_factory=lambda: Path(os.getenv("UNIVERSE_FALLBACK_CSV", "universe/fallback_universe.csv"))
    )
    min_dollar_volume: float = field(default_factory=lambda: float(os.getenv("MIN_DOLLAR_VOLUME", 8_000_000)))
    min_mkt_cap: float = field(default_factory=lambda: float(os.getenv("MIN_MKT_CAP", 300_000_000)))
    max_mkt_cap: float = field(default_factory=lambda: float(os.getenv("MAX_MKT_CAP", 5_000_000_000)))
    min_price: float = field(default_factory=lambda: float(os.getenv("MIN_PRICE", 2.0)))
    max_price: float = field(default_factory=lambda: float(os.getenv("MAX_PRICE", 80.0)))
    max_universe_size: int = field(default_factory=lambda: int(os.getenv("MAX_UNIVERSE_SIZE", 50)))
    universe_liquidity_top_n: int = field(default_factory=lambda: _get_int("UNIVERSE_LIQUIDITY_TOP_N", 300))
    cache_ttl: int = field(default_factory=lambda: int(os.getenv("CACHE_TTL", "900")))
    intraday_stale_seconds: int = field(default_factory=lambda: int(os.getenv("INTRADAY_STALE_SECONDS", "900")))
    daily_stale_seconds: int = field(default_factory=lambda: int(os.getenv("DAILY_STALE_SECONDS", "432000")))
    min_volume_history_days: int = field(default_factory=lambda: int(os.getenv("MIN_VOLUME_HISTORY_DAYS", "3")))
    allow_partial_fundamentals: bool = field(
        default_factory=lambda: str(os.getenv("ALLOW_PARTIAL_FUNDAMENTALS", "true")).lower() != "false"
    )
    allow_partial_atr: bool = field(default_factory=lambda: str(os.getenv("ALLOW_PARTIAL_ATR", "true")).lower() != "false")
    regime_gate_min_score: float = field(default_factory=lambda: float(os.getenv("REGIME_GATE_MIN_SCORE", "0.0")))

    scheduler_interval_seconds: int = field(default_factory=lambda: int(os.getenv("SCHEDULER_INTERVAL_SECONDS", "900")))
    max_positions: int = field(default_factory=lambda: int(os.getenv("MAX_POSITIONS", "10")))
    portfolio_state_path: Path = field(default_factory=lambda: Path(os.getenv("PORTFOLIO_STATE_PATH", "data/portfolio_state.json")))
    initial_equity: float = field(default_factory=lambda: float(os.getenv("INITIAL_EQUITY", "100000")))
    max_daily_loss_pct: float = field(default_factory=lambda: float(os.getenv("MAX_DAILY_LOSS_PCT", "0.03")))
    max_position_pct: float = field(default_factory=lambda: float(os.getenv("MAX_POSITION_PCT", "0.10")))
    max_risk_pct: float = field(default_factory=lambda: float(os.getenv("MAX_RISK_PCT", "0.005")))
    atr_multiplier: float = field(default_factory=lambda: float(os.getenv("ATR_MULTIPLIER", "2.5")))
    min_confidence: float = field(default_factory=lambda: float(os.getenv("MIN_CONFIDENCE", "0.45")))
    default_timespan: str = field(default_factory=lambda: os.getenv("DEFAULT_TIMESPAN", "1day"))


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
