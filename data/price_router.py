from __future__ import annotations

from typing import Dict, List, Sequence

import pandas as pd

from core.config import get_settings
from core.logger import get_logger
from data.alpaca_provider import AlpacaProvider
from data.alphavantage_provider import AlphaVantageProvider
from data.twelvedata_provider import TwelveDataProvider
from core.cache import get_cache

logger = get_logger(__name__)
settings = get_settings()
cache = get_cache()
_providers_cache: Sequence[object] | None = None


def resample_to_5m(bars) -> pd.DataFrame:
    """Normalize raw bars to 5-minute OHLCV buckets."""

    frame = pd.DataFrame(bars)
    if frame.empty:
        return frame
    frame = frame.sort_values("timestamp").reset_index(drop=True)
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], unit="s", errors="coerce", utc=True)
    frame = frame.dropna(subset=["timestamp"]).set_index("timestamp")
    # Pandas FutureWarning fix: use '5min' instead of '5T'
    frame = frame.resample("5min").agg(
        {
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
        }
    )
    frame = frame.dropna().reset_index()
    return frame


def _build_providers() -> Sequence[object]:
    global _providers_cache
    if _providers_cache is not None:
        return _providers_cache

    providers: list[object] = []

    if settings.twelvedata_api_key:
        providers.append(TwelveDataProvider())
    else:
        logger.info("PriceRouter: TwelveData disabled (missing TWELVEDATA_API_KEY)")

    if settings.alphavantage_api_key:
        providers.append(AlphaVantageProvider())
    else:
        logger.info("PriceRouter: AlphaVantage disabled (missing ALPHAVANTAGE_API_KEY)")

    if settings.alpaca_api_key and settings.alpaca_api_secret:
        providers.append(AlpacaProvider())
    else:
        logger.info("PriceRouter: Alpaca disabled (missing API credentials)")

    logger.info("PriceRouter active providers: %s", [p.__class__.__name__ for p in providers])
    _providers_cache = providers
    return providers


class PriceRouter:
    """Funnel price + aggregate requests across multiple providers."""

    def __init__(self) -> None:
        self.providers = _build_providers()

    @staticmethod
    def _merge_records(cached: List[Dict[str, float]], fresh: List[Dict[str, float]], limit: int) -> List[Dict[str, float]]:
        """Merge cached + fresh bars by timestamp."""

        combined = {float(item["timestamp"]): item for item in cached or [] if "timestamp" in item}
        for item in fresh or []:
            ts = float(item.get("timestamp", 0))
            if ts:
                combined[ts] = item
        merged = list(combined.values())
        merged.sort(key=lambda x: x["timestamp"])
        if limit and len(merged) > limit:
            merged = merged[-limit:]
        return merged

    def get_price(self, symbol: str) -> float:
        last_error: Exception | None = None
        for provider in self.providers:
            try:
                price = provider.get_price(symbol)  # type: ignore[attr-defined]
                if price is None:
                    continue
                return price
            except Exception as exc:  # pragma: no cover - network guard
                provider_name = provider.__class__.__name__
                logger.warning("%s price lookup failed for %s: %s", provider_name, symbol, exc)
                if "429" in str(exc):
                    logger.warning("Rate limit hit on %s, skipping %s", provider_name, symbol)
                last_error = exc
        raise RuntimeError(f"All providers failed to return price for {symbol}") from last_error

    def get_aggregates(self, symbol: str, window: int = 60) -> List[Dict[str, float]]:
        """
        Return last ``window`` minutes of 5-minute bars.
        Provider priority: Alpaca → TwelveData → AlphaVantage.
        """

        last_error: Exception | None = None
        for provider in self.providers:
            provider_name = provider.__class__.__name__
            try:
                frame: pd.DataFrame
                if isinstance(provider, AlphaVantageProvider):
                    bars = provider.get_intraday_5m(symbol, limit=window)
                    frame = resample_to_5m(bars)
                elif isinstance(provider, TwelveDataProvider):
                    bars = provider.get_intraday_1m(symbol, limit=window)
                    frame = resample_to_5m(bars)
                elif isinstance(provider, AlpacaProvider):
                    bars = provider.get_intraday_1m(symbol, limit=window)
                    frame = resample_to_5m(bars)
                else:
                    continue
                if not frame.empty:
                    return frame.to_dict("records")
            except Exception as exc:  # pragma: no cover - network guard
                logger.warning("%s aggregates failed for %s: %s", provider_name, symbol, exc)
                if "429" in str(exc):
                    logger.warning("Rate limit hit on %s, skipping %s", provider_name, symbol)
                last_error = exc
        raise RuntimeError(f"All providers failed to return aggregates for {symbol}") from last_error

    def get_daily_aggregates(self, symbol: str, limit: int = 60) -> List[Dict[str, float]]:
        """
        Return up to ``limit`` daily bars.
        Provider priority: TwelveData → AlphaVantage → Alpaca (Alpaca skipped for daily bars to avoid 429s).
        """

        last_error: Exception | None = None
        limit = max(limit, 5)
        cache_key = f"daily_bars:{symbol.upper()}"
        cached_bars = cache.get(cache_key) or []
        combined: List[Dict[str, float]] = []
        for provider in self.providers:
            provider_name = provider.__class__.__name__
            if isinstance(provider, AlpacaProvider):
                # Skip Alpaca for daily bars to avoid rate limits; rely on TwelveData/AlphaVantage
                continue
            try:
                if hasattr(provider, "get_aggregates"):
                    bars = provider.get_aggregates(symbol, timespan="1day", limit=limit)  # type: ignore[arg-type]
                else:
                    continue
                frame = self.aggregates_to_dataframe(bars)
                if frame.empty:
                    continue
                records = frame.to_dict("records")
                combined = self._merge_records(cached_bars, records, limit)
                if combined:
                    cache.set(cache_key, combined, settings.cache_ttl)
                    return combined
            except Exception as exc:  # pragma: no cover - network guard
                logger.warning("%s daily aggregates failed for %s: %s", provider_name, symbol, exc)
                last_error = exc
        if cached_bars:
            return cached_bars
        if combined:
            return combined
        if last_error:
            logger.warning("Daily aggregates unavailable for %s; returning empty set: %s", symbol, last_error)
        return []

    def get_daily_bars_batch(self, symbols: Sequence[str], limit: int = 60) -> Dict[str, List[Dict[str, float]]]:
        """Batch-fetch daily bars; use provider multi endpoints when available."""

        limit = max(limit, 5)
        results: Dict[str, List[Dict[str, float]]] = {}
        remaining = []
        for sym in symbols:
            cache_key = f"daily_bars:{sym.upper()}"
            cached = cache.get(cache_key)
            if cached:
                results[sym] = cached
            else:
                remaining.append(sym)

        if remaining:
            for provider in self.providers:
                if hasattr(provider, "get_daily_bars_multi"):
                    provider_name = provider.__class__.__name__
                    try:
                        batch = provider.get_daily_bars_multi(remaining, limit=limit)  # type: ignore[attr-defined]
                        for sym, bars in batch.items():
                            merged = self._merge_records(cache.get(f"daily_bars:{sym}") or [], bars, limit)
                            cache.set(f"daily_bars:{sym}", merged, settings.cache_ttl)
                            results[sym] = merged
                    except Exception as exc:  # pragma: no cover - network guard
                        logger.warning("%s batch daily bars failed: %s", provider_name, exc)
                # no else; fall back to per-symbol below

        for sym in symbols:
            if sym in results:
                continue
            results[sym] = self.get_daily_aggregates(sym, limit=limit)
        return results

    @staticmethod
    def aggregates_to_dataframe(bars: List[Dict[str, float]]) -> pd.DataFrame:
        frame = pd.DataFrame(bars)
        if not frame.empty:
            frame = frame.sort_values("timestamp").reset_index(drop=True)
        return frame
