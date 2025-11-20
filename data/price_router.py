from __future__ import annotations

from typing import Dict, List, Sequence

import pandas as pd

from core.config import get_settings
from core.logger import get_logger
from data.alpaca_provider import AlpacaProvider
from data.alphavantage_provider import AlphaVantageProvider
from data.twelvedata_provider import TwelveDataProvider

logger = get_logger(__name__)
settings = get_settings()
_providers_cache: Sequence[object] | None = None


def _build_providers() -> Sequence[object]:
    global _providers_cache
    if _providers_cache is not None:
        return _providers_cache

    providers: list[object] = []
    # Prefer non-Alpaca first to reduce rate-limit exposure
    if settings.alphavantage_api_key:
        providers.append(AlphaVantageProvider())
    else:
        logger.info("PriceRouter: AlphaVantage disabled (missing ALPHAVANTAGE_API_KEY)")

    if settings.twelvedata_api_key:
        providers.append(TwelveDataProvider())
    else:
        logger.info("PriceRouter: TwelveData disabled (missing TWELVEDATA_API_KEY)")

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

    def get_aggregates(self, symbol: str, timespan: str = "1day", limit: int = 60) -> List[Dict[str, float]]:
        last_error: Exception | None = None
        for provider in self.providers:
            try:
                bars = provider.get_aggregates(symbol, timespan, limit)  # type: ignore[attr-defined]
                if not bars:
                    continue
                if bars:
                    return bars
            except Exception as exc:  # pragma: no cover - network guard
                provider_name = provider.__class__.__name__
                logger.warning("%s aggregates failed for %s: %s", provider_name, symbol, exc)
                if "429" in str(exc):
                    logger.warning("Rate limit hit on %s, skipping %s", provider_name, symbol)
                last_error = exc
        raise RuntimeError(f"All providers failed to return aggregates for {symbol}") from last_error

    @staticmethod
    def aggregates_to_dataframe(bars: List[Dict[str, float]]) -> pd.DataFrame:
        frame = pd.DataFrame(bars)
        if not frame.empty:
            frame = frame.sort_values("timestamp").reset_index(drop=True)
        return frame
