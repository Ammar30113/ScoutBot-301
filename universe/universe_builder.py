from __future__ import annotations

import re
from pathlib import Path
from typing import List, Optional

import pandas as pd

from core.config import get_settings
from core.logger import get_logger
from data.price_router import PriceRouter
from data.alphavantage_provider import AlphaVantageProvider
from data.twelvedata_provider import TwelveDataProvider
from strategy.technicals import compute_atr
from universe.csv_loader import load_universe_from_csv

logger = get_logger(__name__)
settings = get_settings()
price_router = PriceRouter()

_alpha = AlphaVantageProvider() if settings.alphavantage_api_key else None
_twelve = TwelveDataProvider() if settings.twelvedata_api_key else None

CANDIDATE_FILES = [
    Path("universe/sp1500.csv"),
    Path("universe/russell3000.csv"),
    settings.universe_fallback_csv,
]


def _filter_symbols(symbols: list[str]) -> list[str]:
    pattern = re.compile(r"^[A-Z0-9\.\-]+$")
    return [sym for sym in symbols if isinstance(sym, str) and pattern.match(sym.upper())]


def _csv_universe(path) -> list[str]:
    csv_path = path if isinstance(path, Path) else Path(path)
    df = load_universe_from_csv(csv_path)
    return _filter_symbols(df["symbol"].dropna().astype(str).str.upper().tolist())


def _load_candidates() -> list[str]:
    for path in CANDIDATE_FILES:
        symbols = _csv_universe(path)
        if symbols:
            logger.info("Universe candidates loaded from %s (%s tickers)", path, len(symbols))
            return symbols
        logger.warning("Universe candidate file missing or empty: %s", path)
    return []


def _get_market_cap(symbol: str) -> Optional[float]:
    for provider in (_alpha, _twelve):
        if provider is None:
            continue
        try:
            value = provider.get_market_cap(symbol)  # type: ignore[attr-defined]
        except Exception as exc:  # pragma: no cover - network guard
            logger.warning("Market cap lookup failed for %s via %s: %s", symbol, provider.__class__.__name__, exc)
            continue
        if value:
            return float(value)
    logger.warning("Market cap unavailable for %s; skipping", symbol)
    return None


def _load_daily_frame(symbol: str, limit: int = 60) -> Optional[pd.DataFrame]:
    try:
        bars = price_router.get_daily_aggregates(symbol, limit=limit)
        frame = PriceRouter.aggregates_to_dataframe(bars)
        if frame.empty:
            logger.warning("Universe skip %s: no daily bars", symbol)
            return None
        return frame
    except Exception as exc:  # pragma: no cover - network guard
        logger.warning("Universe skip %s: price data unavailable (%s)", symbol, exc)
        return None


def _passes_filters(symbol: str, frame: pd.DataFrame) -> Optional[dict]:
    recent = frame.tail(10).copy()
    if len(recent) < 10:
        logger.info("Universe skip %s: insufficient volume history", symbol)
        return None
    recent["dollar_volume"] = recent["close"].astype(float) * recent["volume"].astype(float)
    avg_dollar_vol = float(recent["dollar_volume"].mean())
    if avg_dollar_vol < settings.min_dollar_volume:
        logger.info("Universe skip %s: avg dollar volume %.2f < threshold %.2f", symbol, avg_dollar_vol, settings.min_dollar_volume)
        return None

    price = float(frame["close"].astype(float).iloc[-1])
    if not (settings.min_price <= price <= settings.max_price):
        logger.info("Universe skip %s: price %.2f outside [%.2f, %.2f]", symbol, price, settings.min_price, settings.max_price)
        return None

    if len(frame) < 15:
        logger.info("Universe skip %s: insufficient bars for ATR", symbol)
        return None
    atr_series = compute_atr(frame, window=14)
    atr_val = float(atr_series.iloc[-1]) if len(atr_series) else 0.0
    if atr_val <= 0 or price <= 0:
        logger.info("Universe skip %s: invalid ATR/price", symbol)
        return None
    atr_pct = atr_val / price
    if not (0.02 <= atr_pct <= 0.12):
        logger.info("Universe skip %s: ATR%% %.4f outside range [0.02, 0.12]", symbol, atr_pct)
        return None

    market_cap = _get_market_cap(symbol)
    if market_cap is None:
        return None
    if not (settings.min_mkt_cap <= market_cap <= settings.max_mkt_cap):
        logger.info(
            "Universe skip %s: market cap %.0f outside [%.0f, %.0f]",
            symbol,
            market_cap,
            settings.min_mkt_cap,
            settings.max_mkt_cap,
        )
        return None

    return {"symbol": symbol, "liquidity": avg_dollar_vol}


def get_universe() -> list[str]:
    """Build universe via liquidity/volatility/market-cap filters."""

    candidates = _filter_symbols(_load_candidates())
    total_candidates = len(candidates)
    passed: List[dict] = []

    for symbol in candidates:
        frame = _load_daily_frame(symbol)
        if frame is None:
            continue
        result = _passes_filters(symbol, frame)
        if result:
            passed.append(result)

    passed = sorted(passed, key=lambda x: x["liquidity"], reverse=True)
    final_symbols = [entry["symbol"] for entry in passed[: settings.max_universe_size]]

    logger.info(
        "Universe: fetched %s candidates, %s passed filters, %s final symbols",
        total_candidates,
        len(passed),
        len(final_symbols),
    )

    if not final_symbols:
        fallback = _csv_universe(settings.universe_fallback_csv)
        if fallback:
            logger.warning("Universe empty after filters; falling back to %s (%s symbols)", settings.universe_fallback_csv, len(fallback))
            return fallback
        logger.warning("Universe unavailable: no candidates and no fallback CSV")
        return []

    return final_symbols
