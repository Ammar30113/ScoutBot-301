from __future__ import annotations

import os
from dataclasses import dataclass, field
import logging
from functools import lru_cache
from pathlib import Path
from typing import List

from dotenv import load_dotenv

logger = logging.getLogger(__name__)

load_dotenv()


@dataclass
class Settings:
    """Central configuration object loaded from environment variables."""

    alpaca_api_key: str = field(default_factory=lambda: os.getenv("ALPACA_API_KEY") or os.getenv("APCA_API_KEY_ID", ""))
    alpaca_api_secret: str = field(
        default_factory=lambda: os.getenv("ALPACA_API_SECRET") or os.getenv("APCA_API_SECRET_KEY", "")
    )
    alpaca_base_url: str = field(default_factory=lambda: os.getenv("ALPACA_API_BASE_URL", "https://paper-api.alpaca.markets"))
    alpaca_data_url: str = field(default_factory=lambda: os.getenv("ALPACA_API_DATA_URL", "https://data.alpaca.markets/v2"))

    twelvedata_api_key: str = field(default_factory=lambda: os.getenv("TWELVEDATA_API_KEY") or os.getenv("TWELVEDATA_KEY", ""))
    alphavantage_api_key: str = field(
        default_factory=lambda: os.getenv("ALPHAVANTAGE_API_KEY")
        or os.getenv("ALPHAVANTAGE_KEY")
        or os.getenv("ALPHA_VANTAGE_KEY", "")
    )
    openai_api_key: str = field(default_factory=lambda: os.getenv("OPENAI_API_KEY", ""))
    openai_model: str = field(default_factory=lambda: os.getenv("OPENAI_MODEL", "gpt-3.5-turbo-0125"))
    use_sentiment: bool = field(default_factory=lambda: str(os.getenv("USE_SENTIMENT", "true")).lower() != "false")
    use_finnhub: bool = field(default_factory=lambda: str(os.getenv("USE_FINNHUB", "false")).lower() == "true")
    sentiment_cache_ttl: int = field(default_factory=lambda: int(os.getenv("SENTIMENT_CACHE_TTL", "300")))

    universe_fallback_csv: Path = field(
        default_factory=lambda: Path(os.getenv("UNIVERSE_FALLBACK_CSV", "universe/fallback_universe.csv"))
    )
    microcap_etfs: List[str] = field(
        default_factory=lambda: [token.strip().upper() for token in os.getenv("MICROCAP_ETFS", "IWM,IWC,SMLF,VTWO,URTY").split(",") if token.strip()]
    )

    scheduler_interval_seconds: int = field(default_factory=lambda: int(os.getenv("SCHEDULER_INTERVAL_SECONDS", "900")))
    max_positions: int = field(default_factory=lambda: int(os.getenv("MAX_POSITIONS", "10")))
    portfolio_state_path: Path = field(default_factory=lambda: Path(os.getenv("PORTFOLIO_STATE_PATH", "data/portfolio_state.json")))
    initial_equity: float = field(default_factory=lambda: float(os.getenv("INITIAL_EQUITY", "100000")))
    max_daily_loss_pct: float = field(default_factory=lambda: float(os.getenv("MAX_DAILY_LOSS_PCT", "0.03")))
    max_position_pct: float = field(default_factory=lambda: float(os.getenv("MAX_POSITION_PCT", "0.10")))
    atr_multiplier: float = field(default_factory=lambda: float(os.getenv("ATR_MULTIPLIER", "2.5")))
    min_confidence: float = field(default_factory=lambda: float(os.getenv("MIN_CONFIDENCE", "0.45")))
    default_timespan: str = field(default_factory=lambda: os.getenv("DEFAULT_TIMESPAN", "1day"))


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    settings = Settings()
    if not settings.microcap_etfs:
        settings.microcap_etfs = ["IWM", "IWC", "SMLF", "VTWO", "URTY"]
    settings.universe_fallback_csv.parent.mkdir(parents=True, exist_ok=True)
    settings.portfolio_state_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info("TWELVEDATA_API_KEY detected: %s", bool(settings.twelvedata_api_key))
    logger.info("ALPHAVANTAGE_API_KEY detected: %s", bool(settings.alphavantage_api_key))
    logger.info("OPENAI_API_KEY detected: %s", bool(settings.openai_api_key))
    return settings
