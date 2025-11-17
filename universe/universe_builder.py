from __future__ import annotations

from pathlib import Path
import pandas as pd

from core.config import get_settings
from core.logger import get_logger
from universe.csv_loader import load_universe_from_csv
from universe.etf_expander import fetch_etf_holdings

logger = get_logger(__name__)
settings = get_settings()

DEFAULT_ETFS = ["SPY", "QQQ", "IWM"]
UNIVERSE_CSV = Path("universe/liquid_universe.csv")


def get_universe() -> list[str]:
    """Return a broad liquid universe from ETF constituents or CSV fallback."""

    # Try ETF constituents first (may be unavailable on Alpaca free tier)
    holdings = fetch_etf_holdings(DEFAULT_ETFS)
    if holdings:
        universe = sorted(set(holdings))
        logger.info("Loaded %s symbols via ETF holdings", len(universe))
        return universe

    # Fallback to static CSV
    df = load_universe_from_csv(UNIVERSE_CSV)
    universe = df["symbol"].dropna().astype(str).str.upper().tolist()
    logger.info("Loaded %s symbols from liquid_universe.csv", len(universe))
    return universe
