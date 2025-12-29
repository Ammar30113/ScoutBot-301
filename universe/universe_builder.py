from __future__ import annotations

import re
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional

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

SKIP_LOG_SAMPLE_LIMIT = 5

CANDIDATE_FILES = [
    Path("universe/russell3000.csv"),
    settings.universe_fallback_csv,
]

_skip_counts: dict[str, int] = defaultdict(int)
_skip_sample_counts: dict[str, int] = defaultdict(int)


def _filter_symbols(symbols: list[str]) -> list[str]:
    pattern = re.compile(r"^[A-Z0-9\.\-]+$")
    return [sym for sym in symbols if isinstance(sym, str) and pattern.match(sym.upper())]


def _has_external_daily_provider() -> bool:
    return bool(settings.twelvedata_api_key or settings.alphavantage_api_key or settings.marketstack_api_key)


def _csv_universe(path) -> list[str]:
    csv_path = path if isinstance(path, Path) else Path(path)
    df = load_universe_from_csv(csv_path)
    return _filter_symbols(df["symbol"].dropna().astype(str).str.upper().tolist())


def _avg_dollar_volume(bars: Optional[List[Dict[str, float]]], lookback: int) -> Optional[float]:
    if not bars:
        return None
    try:
        sorted_bars = sorted(bars, key=lambda row: float(row.get("timestamp", 0.0)))
    except Exception:
        sorted_bars = list(bars)
    values: List[float] = []
    for row in sorted_bars:
        try:
            close = float(row.get("close", 0.0))
            volume = float(row.get("volume", 0.0))
        except (TypeError, ValueError):
            continue
        if close > 0 and volume > 0:
            values.append(close * volume)
    if len(values) < lookback:
        return None
    return float(sum(values[-lookback:]) / float(lookback))


def _load_candidates() -> list[str]:
    candidate_files = list(CANDIDATE_FILES)
    if not _has_external_daily_provider() and settings.allow_alpaca_daily is not True:
        logger.warning("Universe: no external daily providers configured; using fallback universe only")
        return _csv_universe(settings.universe_fallback_csv)
    if settings.marketstack_api_key and not settings.twelvedata_api_key and not settings.alphavantage_api_key:
        candidate_files = [settings.universe_fallback_csv] + [
            path for path in candidate_files if path != settings.universe_fallback_csv
        ]
        logger.info("Universe: Marketstack-only daily data; prioritizing fallback CSV to limit API usage")
    for path in candidate_files:
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
    logger.warning("Market cap unavailable for %s; using partial fundamentals", symbol)
    return None


def _load_daily_frame(symbol: str, limit: int = 60, preloaded: Optional[List[Dict[str, float]]] = None) -> Optional[pd.DataFrame]:
    try:
        bars = preloaded if preloaded is not None else price_router.get_daily_aggregates(symbol, limit=limit)
        frame = PriceRouter.aggregates_to_dataframe(bars)
        if frame.empty:
            _log_skip(symbol, "skip_volume_history", "no daily bars")
            return None
        return frame
    except Exception as exc:  # pragma: no cover - network guard
        _log_skip(symbol, "skip_volume_history", f"price data unavailable ({exc})")
        return None


def _log_skip(symbol: str, reason: str, detail: str = "") -> None:
    message = f"Universe skip {symbol}: {reason}"
    _skip_counts[reason] += 1
    if _skip_sample_counts[reason] < SKIP_LOG_SAMPLE_LIMIT:
        _skip_sample_counts[reason] += 1
        if detail:
            message = f"{message} ({detail})"
        logger.info(message)


def _passes_filters(symbol: str, frame: pd.DataFrame) -> Optional[dict]:
    lookback = max(settings.min_volume_history_days, 3)
    recent = frame.tail(max(lookback, 15)).copy()
    if recent.empty or len(recent.dropna(subset=["volume"])) < settings.min_volume_history_days:
        _log_skip(symbol, "skip_volume_history", f"found {len(recent.dropna(subset=['volume']))} valid days")
        return None
    recent["dollar_volume"] = recent["close"].astype(float) * recent["volume"].astype(float)
    recent_valid = recent.dropna(subset=["dollar_volume"])
    recent_valid = recent_valid[recent_valid["volume"].astype(float) > 0]
    if len(recent_valid) < settings.min_volume_history_days:
        _log_skip(symbol, "skip_volume_history", f"found {len(recent_valid)} valid days")
        return None
    avg_dollar_vol = float(recent_valid.tail(settings.min_volume_history_days)["dollar_volume"].mean())
    if avg_dollar_vol < settings.min_dollar_volume:
        _log_skip(
            symbol,
            "skip_volume_history",
            f"avg dollar vol {avg_dollar_vol:.2f} < threshold {settings.min_dollar_volume:.2f}",
        )
        return None

    price = float(frame["close"].astype(float).iloc[-1])
    if not (settings.min_price <= price <= settings.max_price):
        _log_skip(
            symbol,
            "skip_price_range",
            f"price {price:.2f} outside [{settings.min_price:.2f}, {settings.max_price:.2f}]",
        )
        return None

    atr_pct = None
    atr_ready = len(frame) >= 15
    if atr_ready:
        atr_series = compute_atr(frame, window=14)
        atr_val = float(atr_series.iloc[-1]) if len(atr_series) else 0.0
        if atr_val > 0 and price > 0:
            atr_pct = atr_val / price
    if atr_pct is None:
        if settings.allow_partial_atr:
            _log_skip(symbol, "skip_atr", "missing ATR data")
        else:
            _log_skip(symbol, "skip_atr", "insufficient ATR data")
            return None
    else:
        if not (0.02 <= atr_pct <= 0.12):
            _log_skip(symbol, "skip_atr", f"ATR% {atr_pct:.4f} outside range [0.02, 0.12]")
            return None

    market_cap = _get_market_cap(symbol)
    if market_cap is None or market_cap <= 0:
        if settings.allow_partial_fundamentals:
            _log_skip(symbol, "skip_missing_fundamentals", "market cap unavailable")
        else:
            return None
    else:
        if not (settings.min_mkt_cap <= market_cap <= settings.max_mkt_cap):
            _log_skip(
                symbol,
                "skip_market_cap",
                f"market cap {market_cap:.0f} outside [{settings.min_mkt_cap:.0f}, {settings.max_mkt_cap:.0f}]",
            )
            return None

    return {"symbol": symbol, "liquidity": avg_dollar_vol}


def get_universe() -> list[str]:
    """Build universe via liquidity/volatility/market-cap filters."""

    _skip_counts.clear()
    _skip_sample_counts.clear()
    candidates = _filter_symbols(_load_candidates())
    total_candidates = len(candidates)
    logger.info("Universe: fetched %s candidates", total_candidates)
    passed: List[dict] = []

    daily_bars_map = price_router.get_daily_bars_batch(candidates, limit=60) if candidates else {}
    top_n = max(int(settings.universe_liquidity_top_n or 0), 0)
    if top_n > 0 and candidates:
        lookback = max(settings.min_volume_history_days, 3)
        liquidity_scores: List[tuple[str, float]] = []
        for symbol in candidates:
            avg = _avg_dollar_volume(daily_bars_map.get(symbol), lookback)
            if avg is not None:
                liquidity_scores.append((symbol, avg))
        if liquidity_scores:
            liquidity_scores.sort(key=lambda row: row[1], reverse=True)
            top_count = min(top_n, len(liquidity_scores))
            top_symbols = {sym for sym, _ in liquidity_scores[:top_count]}
            candidates = [sym for sym in candidates if sym in top_symbols]
            logger.info("Universe: preselected top %s liquidity symbols (%s retained)", top_count, len(candidates))
        else:
            logger.info("Universe: liquidity prefilter found no valid bars; skipping prefilter")
    for symbol in candidates:
        preloaded = daily_bars_map.get(symbol) if daily_bars_map else None
        frame = _load_daily_frame(symbol, preloaded=preloaded)
        if frame is None:
            continue
        result = _passes_filters(symbol, frame)
        if result:
            passed.append(result)

    passed = sorted(passed, key=lambda x: x["liquidity"], reverse=True)
    final_symbols = [entry["symbol"] for entry in passed[: settings.max_universe_size]]

    logger.info(
        "%s passed initial data validation",
        len(passed),
    )
    logger.info("%s final universe symbols", len(final_symbols))

    if _skip_counts:
        summary = ", ".join(f"{reason}={count}" for reason, count in sorted(_skip_counts.items()))
        logger.info("Universe skip summary: %s", summary)
        if any(count > SKIP_LOG_SAMPLE_LIMIT for count in _skip_counts.values()):
            logger.info("Skip logs sampled (first %s per reason shown)", SKIP_LOG_SAMPLE_LIMIT)

    if not final_symbols:
        fallback = _csv_universe(settings.universe_fallback_csv)
        if fallback:
            logger.warning("Universe empty after filters; falling back to %s (%s symbols)", settings.universe_fallback_csv, len(fallback))
            return fallback
        logger.warning("Universe unavailable: no candidates and no fallback CSV")
        return []

    return final_symbols
