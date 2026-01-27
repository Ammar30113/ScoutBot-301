from __future__ import annotations

import math
import time
from datetime import datetime
from typing import Dict, List, Sequence

import pandas as pd

from core.config import get_settings
from core.logger import get_logger
from data.alpaca_provider import AlpacaProvider
from data.alphavantage_provider import AlphaVantageProvider
from data.marketstack_provider import MarketstackProvider
from data.twelvedata_provider import TwelveDataProvider
from core.cache import get_cache

logger = get_logger(__name__)
settings = get_settings()
cache = get_cache()
_providers_cache: Sequence[object] | None = None
_alpaca_daily_fallback_warned = False


def _has_external_daily_provider() -> bool:
    return bool(settings.twelvedata_api_key or settings.alphavantage_api_key or settings.marketstack_api_key)


def _allow_alpaca_daily() -> bool:
    if settings.allow_alpaca_daily is True:
        return True
    if settings.allow_alpaca_daily is False:
        return False
    if _has_external_daily_provider():
        return False
    global _alpaca_daily_fallback_warned
    if not _alpaca_daily_fallback_warned:
        logger.warning(
            "No external daily providers configured; enabling Alpaca daily fallback. Set ALLOW_ALPACA_DAILY=false to disable."
        )
        _alpaca_daily_fallback_warned = True
    return True


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

    if settings.alpaca_api_key and settings.alpaca_api_secret:
        providers.append(AlpacaProvider())
    else:
        logger.info("PriceRouter: Alpaca disabled (missing API credentials)")

    if settings.twelvedata_api_key:
        providers.append(TwelveDataProvider())
    else:
        logger.info("PriceRouter: TwelveData disabled (missing TWELVEDATA_API_KEY)")

    if settings.alphavantage_api_key:
        providers.append(AlphaVantageProvider())
    else:
        logger.info("PriceRouter: AlphaVantage disabled (missing ALPHAVANTAGE_API_KEY)")

    if settings.marketstack_api_key:
        providers.append(MarketstackProvider())
    else:
        logger.info("PriceRouter: Marketstack disabled (missing MARKETSTACK_API_KEY)")

    logger.info("PriceRouter active providers: %s", [p.__class__.__name__ for p in providers])
    _providers_cache = providers
    return providers


class PriceRouter:
    """Funnel price + aggregate requests across multiple providers."""

    def __init__(self) -> None:
        self.providers = _build_providers()
        self._last_provider: Dict[str, str] = {}

    @staticmethod
    def _normalize_timestamp(value) -> float | None:
        if value is None:
            return None
        if isinstance(value, pd.Timestamp):
            return float(value.timestamp())
        if isinstance(value, datetime):
            return float(value.timestamp())
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _latest_timestamp(self, bars) -> float | None:
        if bars is None:
            return None
        if hasattr(bars, "empty"):
            if bars.empty or "timestamp" not in bars.columns:
                return None
            return self._normalize_timestamp(bars["timestamp"].iloc[-1])
        if isinstance(bars, list):
            latest = None
            for item in bars:
                if not isinstance(item, dict):
                    continue
                ts = self._normalize_timestamp(item.get("timestamp"))
                if ts is None:
                    continue
                if latest is None or ts > latest:
                    latest = ts
            return latest
        return None

    def _bars_age_seconds(self, bars) -> float | None:
        latest = self._latest_timestamp(bars)
        if latest is None:
            return None
        return max(time.time() - latest, 0.0)

    def bars_age_seconds(self, bars) -> float | None:
        return self._bars_age_seconds(bars)

    def _set_last_provider(self, symbol: str, kind: str, provider_name: str) -> None:
        key = f"{kind}:{symbol.upper()}"
        self._last_provider[key] = provider_name

    def last_provider(self, symbol: str, kind: str = "intraday") -> str | None:
        return self._last_provider.get(f"{kind}:{symbol.upper()}")

    def _provider_rate_limited(self, provider: object) -> bool:
        checker = getattr(provider, "is_rate_limited", None)
        return bool(checker()) if callable(checker) else False

    def _daily_providers(self, allow_alpaca_daily: bool) -> list[object]:
        providers: list[object] = []
        for provider in self.providers:
            if isinstance(provider, AlpacaProvider) and not allow_alpaca_daily:
                continue
            if hasattr(provider, "get_aggregates"):
                providers.append(provider)
        return providers

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
            provider_name = provider.__class__.__name__
            try:
                price = provider.get_price(symbol)  # type: ignore[attr-defined]
                if price is None:
                    continue
                self._set_last_provider(symbol, "price", provider_name)
                return price
            except Exception as exc:  # pragma: no cover - network guard
                logger.warning("%s price lookup failed for %s: %s", provider_name, symbol, exc)
                if "429" in str(exc):
                    logger.warning("Rate limit hit on %s, skipping %s", provider_name, symbol)
                last_error = exc
        raise RuntimeError(f"All providers failed to return price for {symbol}") from last_error

    def get_aggregates(self, symbol: str, window: int = 60, *, allow_stale: bool = False) -> List[Dict[str, float]]:
        """
        Return 5-minute bars covering the last ``window`` minutes.
        Provider priority: Alpaca → TwelveData → AlphaVantage.
        If ``allow_stale`` is True, return the freshest stale bars when no provider is fresh.
        """

        last_error: Exception | None = None
        bars_needed = max(int(math.ceil(window / 5)), 1)
        stale_candidate: tuple[float, str, List[Dict[str, float]]] | None = None
        cache_key = f"intraday_bars:{symbol.upper()}:{bars_needed}"
        cached_bars = cache.get(cache_key) or []
        cached_age = self._bars_age_seconds(cached_bars)
        if cached_bars:
            if cached_age is None:
                if allow_stale:
                    stale_candidate = (0.0, "cache", cached_bars)
            elif cached_age <= settings.intraday_stale_seconds:
                self._set_last_provider(symbol, "intraday", "cache")
                return cached_bars
            elif allow_stale:
                stale_candidate = (cached_age, "cache", cached_bars)
        for provider in self.providers:
            provider_name = provider.__class__.__name__
            if self._provider_rate_limited(provider):
                logger.info("%s rate-limited; skipping intraday for %s", provider_name, symbol)
                continue
            try:
                frame: pd.DataFrame
                if isinstance(provider, AlphaVantageProvider):
                    bars = provider.get_intraday_5m(symbol, limit=bars_needed)
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
                    age = self._bars_age_seconds(frame)
                    if age is not None and age > settings.intraday_stale_seconds:
                        if allow_stale:
                            records = frame.to_dict("records")
                            if stale_candidate is None or age < stale_candidate[0]:
                                stale_candidate = (age, provider_name, records)
                        else:
                            logger.warning(
                                "%s aggregates stale for %s (age %.1f min); trying next provider",
                                provider_name,
                                symbol,
                                age / 60.0,
                            )
                        last_error = RuntimeError("stale intraday data")
                        continue
                    self._set_last_provider(symbol, "intraday", provider_name)
                    records = frame.to_dict("records")
                    cache.set(cache_key, records, settings.cache_ttl)
                    return records
            except Exception as exc:  # pragma: no cover - network guard
                logger.warning("%s aggregates failed for %s: %s", provider_name, symbol, exc)
                if "429" in str(exc):
                    logger.warning("Rate limit hit on %s, skipping %s", provider_name, symbol)
                last_error = exc
        if allow_stale and stale_candidate is not None:
            age, provider_name, records = stale_candidate
            logger.warning(
                "All providers stale for %s; using %s aggregates (age %.1f min)",
                symbol,
                provider_name,
                age / 60.0,
            )
            self._set_last_provider(symbol, "intraday", provider_name)
            return records
        raise RuntimeError(f"All providers failed to return aggregates for {symbol}") from last_error

    def get_daily_aggregates(self, symbol: str, limit: int = 60) -> List[Dict[str, float]]:
        """
        Return up to ``limit`` daily bars.
        Provider priority: TwelveData → AlphaVantage → Marketstack → Alpaca.
        Alpaca daily is used only when ALLOW_ALPACA_DAILY=true or no external daily providers are configured.
        """

        last_error: Exception | None = None
        limit = max(limit, 5)
        allow_alpaca_daily = _allow_alpaca_daily()
        daily_providers = self._daily_providers(allow_alpaca_daily)
        cache_key = f"daily_bars:{symbol.upper()}"
        cached_bars = cache.get(cache_key) or []
        cached_age = self._bars_age_seconds(cached_bars)
        if cached_age is not None and cached_age > settings.daily_stale_seconds:
            cached_bars = []
        combined: List[Dict[str, float]] = []
        if settings.skip_daily_on_rate_limit and daily_providers:
            if all(self._provider_rate_limited(provider) for provider in daily_providers):
                return cached_bars or combined
        for provider in self.providers:
            provider_name = provider.__class__.__name__
            if isinstance(provider, AlpacaProvider) and not allow_alpaca_daily:
                # Skip Alpaca for daily bars to avoid rate limits unless explicitly enabled.
                continue
            try:
                if hasattr(provider, "get_aggregates"):
                    bars = provider.get_aggregates(symbol, timespan="1day", limit=limit)  # type: ignore[arg-type]
                else:
                    continue
                frame = self.aggregates_to_dataframe(bars)
                if frame.empty:
                    continue
                age = self._bars_age_seconds(frame)
                if age is not None and age > settings.daily_stale_seconds:
                    logger.warning(
                        "%s daily aggregates stale for %s (age %.1f days); trying next provider",
                        provider_name,
                        symbol,
                        age / 86400.0,
                    )
                    last_error = RuntimeError("stale daily data")
                    continue
                records = frame.to_dict("records")
                combined = self._merge_records(cached_bars, records, limit)
                if combined:
                    self._set_last_provider(symbol, "daily", provider_name)
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
            cached_age = self._bars_age_seconds(cached) if cached else None
            if cached and (cached_age is None or cached_age <= settings.daily_stale_seconds):
                results[sym] = cached
            else:
                remaining.append(sym)

        allow_alpaca_daily = _allow_alpaca_daily()
        daily_providers = self._daily_providers(allow_alpaca_daily)
        if remaining and settings.skip_daily_on_rate_limit and daily_providers:
            if all(self._provider_rate_limited(provider) for provider in daily_providers):
                logger.warning(
                    "Daily providers rate-limited; skipping per-symbol daily fetch for %s symbols",
                    len(remaining),
                )
                for sym in remaining:
                    results.setdefault(sym, [])
                return results
        if remaining:
            for provider in self.providers:
                if hasattr(provider, "get_daily_bars_multi"):
                    provider_name = provider.__class__.__name__
                    try:
                        batch = provider.get_daily_bars_multi(remaining, limit=limit)  # type: ignore[attr-defined]
                        for sym, bars in batch.items():
                            age = self._bars_age_seconds(bars)
                            if age is not None and age > settings.daily_stale_seconds:
                                logger.warning(
                                    "%s batch daily bars stale for %s (age %.1f days); skipping",
                                    provider_name,
                                    sym,
                                    age / 86400.0,
                                )
                                continue
                            merged = self._merge_records(cache.get(f"daily_bars:{sym}") or [], bars, limit)
                            cache.set(f"daily_bars:{sym}", merged, settings.cache_ttl)
                            results[sym] = merged
                            self._set_last_provider(sym, "daily", provider_name)
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
