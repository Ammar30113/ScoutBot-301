from __future__ import annotations

import re

from core.config import get_settings
from core.logger import get_logger
from universe.csv_loader import load_universe_from_csv
from universe.etf_expander import fetch_etf_holdings

logger = get_logger(__name__)
settings = get_settings()

DEFAULT_ETFS = ["SPY", "QQQ", "IWM"]


def _filter_symbols(symbols: list[str]) -> list[str]:
    pattern = re.compile(r"^[A-Z0-9\.\-]+$")
    return [sym for sym in symbols if isinstance(sym, str) and pattern.match(sym.upper())]


def _csv_universe(path) -> list[str]:
    df = load_universe_from_csv(path)
    return _filter_symbols(df["symbol"].dropna().astype(str).str.upper().tolist())


def get_universe() -> list[str]:
    """Return a broad liquid universe from ETF constituents or CSV fallback."""

    etf_candidates = settings.microcap_etfs or DEFAULT_ETFS
    holdings = fetch_etf_holdings(etf_candidates)
    symbols: list[str] = []
    if holdings:
        symbols = _filter_symbols(sorted(set(holdings)))
        logger.info("Loaded %s symbols via ETF holdings", len(symbols))
    else:
        symbols = _csv_universe(settings.universe_fallback_csv)
        if symbols:
            logger.info("Loaded %s symbols from %s", len(symbols), settings.universe_fallback_csv)
        else:
            # Final safety: at least trade the ETF tickers themselves
            symbols = _filter_symbols(sorted(set(etf_candidates or DEFAULT_ETFS)))
            if symbols:
                logger.warning("Universe CSV empty; falling back to configured ETFs: %s", symbols)
            else:
                logger.warning("Universe unavailable: no ETF holdings and no CSV symbols")
    logger.info("Universe size after filtering: %s", len(symbols))
    return symbols
