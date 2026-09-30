from __future__ import annotations

import math
import time
from datetime import datetime
from typing import Dict, List, Sequence

import pandas as pd

from core.config import get_settings
from core.market_calendar import calendar, latest_completed_session
from data.bars import completed_bars
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
    frame = frame[frame["timestamp"] + pd.Timedelta(minutes=5) <= pd.Timestamp.now(tz="UTC")]
    frame["timestamp"] = frame["timestamp"].astype("int64") / 1e9
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

    def _paper_provider(self):
        names = {"twelvedata": TwelveDataProvider, "marketstack": MarketstackProvider,
                 "alpaca": AlpacaProvider, "alphavantage": AlphaVantageProvider}
        selected = settings.paper_data_provider
        if selected == "auto":
            selected = next((name for name, cls in names.items()
                             if any(isinstance(p, cls) for p in self.providers)
                             and (name != "alpaca" or settings.allow_alpaca_daily is True)), "")
        provider = next((p for p in self.providers if isinstance(p, names.get(selected, type(None)))), None)
        if provider is None or (selected == "alpaca" and settings.allow_alpaca_daily is not True):
            raise RuntimeError("Selected daily provider is unavailable")
        return selected, provider

    @property
    def daily_provider_name(self) -> str:
        return self._paper_provider()[0]

    def get_daily_aggregates(self, symbol: str, limit: int = 60) -> List[Dict[str, float]]:
        """Use one pinned provider; never merge prices from different adjustment schemes.

        The engine validates exchange-session freshness, including holidays, after retrieval.
        Provider failures therefore cannot silently switch the trading strategy's data source.
        """
        name, provider = self._paper_provider()
        key = f"paper_daily:{name}:{symbol.upper()}:{limit}"
        cached = cache.get(key)
        if cached is not None:
            return cached
        bars = provider.get_aggregates(symbol, timespan="1day", limit=limit)
        if not bars:
            raise RuntimeError("Selected daily provider returned no bars")
        self._set_last_provider(symbol, "daily", name)
        now = time.time()
        expected = latest_completed_session(now - settings.data_delay_seconds)
        completed = completed_bars(bars, now - settings.data_delay_seconds)
        ttl = settings.cache_ttl
        if completed and completed[-1].available_at >= calendar().session_close(expected).timestamp():
            next_close = calendar().session_close(calendar().next_session(expected)).timestamp()
            ttl = max(ttl, int(next_close + settings.data_delay_seconds - now))
        cache.set(key, bars, ttl)
        return bars

    def get_daily_bars_batch(self, symbols: Sequence[str], limit: int = 60) -> Dict[str, List[Dict[str, float]]]:
        return {symbol: self.get_daily_aggregates(symbol, limit) for symbol in symbols}

    @staticmethod
    def aggregates_to_dataframe(bars: List[Dict[str, float]]) -> pd.DataFrame:
        frame = pd.DataFrame(bars)
        if not frame.empty:
            frame = frame.sort_values("timestamp").reset_index(drop=True)
        return frame
