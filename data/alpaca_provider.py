from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Optional
from collections import defaultdict
import time

import requests

from core.config import get_settings
from core.logger import get_logger

logger = get_logger(__name__)
LOG_SAMPLE_LIMIT = 5
_warn_counts: dict[str, int] = defaultdict(int)
RATE_LIMIT_COOLDOWN = 60


def _warn_sample(reason: str, message: str) -> None:
    count = _warn_counts[reason] + 1
    _warn_counts[reason] = count
    if count <= LOG_SAMPLE_LIMIT:
        logger.warning(message)
    elif count == LOG_SAMPLE_LIMIT + 1:
        logger.info("%s (suppressing further repeats; %s occurrences)", message, count)


class AlpacaProvider:
    """Market data provider backed by the Alpaca data API."""

    _rate_limit_until = 0.0
    _disabled = False

    def __init__(self) -> None:
        settings = get_settings()
        self.base_url = settings.alpaca_data_url.rstrip("/")
        self.api_key = settings.alpaca_api_key
        self.api_secret = settings.alpaca_api_secret
        self._strip_on_rate_limit = settings.strip_rate_limited_keys
        if not self.api_key or not self.api_secret:
            logger.warning("AlpacaProvider missing credentials; calls will fail until configured")

    def is_rate_limited(self) -> bool:
        return AlpacaProvider._disabled or time.time() < AlpacaProvider._rate_limit_until

    def _rate_limited(self) -> bool:
        return self.is_rate_limited()

    def _set_rate_limit(self, seconds: int, reason: str) -> None:
        until = time.time() + max(int(seconds), 1)
        if until > AlpacaProvider._rate_limit_until:
            AlpacaProvider._rate_limit_until = until
            logger.warning("Alpaca rate limit: cooling down %ds (%s)", int(seconds), reason or "rate limit")
        if self._strip_on_rate_limit:
            self._disable_provider(reason)

    def _disable_provider(self, reason: str) -> None:
        if AlpacaProvider._disabled:
            return
        AlpacaProvider._disabled = True
        self.api_key = ""
        self.api_secret = ""
        logger.warning("Alpaca disabled after rate limit (%s)", reason or "rate limit")

    def _headers(self) -> Dict[str, str]:
        return {"APCA-API-KEY-ID": self.api_key, "APCA-API-SECRET-KEY": self.api_secret}

    def get_price(self, symbol: str) -> Optional[float]:
        if not self.api_key or not self.api_secret:
            return None
        if self._rate_limited():
            return None
        url = f"{self.base_url}/stocks/{symbol.upper()}/trades/latest"
        try:
            response = requests.get(url, headers=self._headers(), timeout=10)
            if response.status_code == 429:
                self._set_rate_limit(RATE_LIMIT_COOLDOWN, "http 429")
                return None
            response.raise_for_status()
            payload = response.json()
            trade = payload.get("trade")
            if not trade:
                return None
            return float(trade.get("p", 0.0))
        except Exception as exc:  # pragma: no cover - network guard
            logger.warning("Alpaca price fetch failed for %s: %s", symbol, exc)
            return None

    def get_aggregates(self, symbol: str, timespan: str = "1day", limit: int = 60) -> List[Dict[str, float]]:
        if not self.api_key or not self.api_secret:
            return []
        if self._rate_limited():
            return []
        timeframe = self._normalize_timespan(timespan)
        url = f"{self.base_url}/stocks/{symbol.upper()}/bars"
        params = {"timeframe": timeframe, "limit": limit, "adjustment": "split"}
        try:
            response = requests.get(url, headers=self._headers(), params=params, timeout=10)
            if response.status_code == 429:
                self._set_rate_limit(RATE_LIMIT_COOLDOWN, "http 429")
                return []
            response.raise_for_status()
            data = response.json().get("bars", []) or []
            return [self._normalize_bar(item) for item in data]
        except Exception as exc:  # pragma: no cover - network guard
            _warn_sample("aggregates_failed", f"Alpaca aggregates failed for {symbol}: {exc}")
            return []

    def _normalize_timespan(self, timespan: str) -> str:
        mapping = {"1day": "1Day", "1hour": "1Hour", "1min": "1Min", "5min": "5Min"}
        return mapping.get(timespan.lower(), "1Day")

    def _normalize_bar(self, bar: Dict[str, float]) -> Dict[str, float]:
        return {
            "open": float(bar["o"]),
            "high": float(bar["h"]),
            "low": float(bar["l"]),
            "close": float(bar["c"]),
            "volume": float(bar["v"]),
            "timestamp": datetime.fromisoformat(bar["t"].replace("Z", "+00:00")).timestamp(),
        }

    def get_intraday_1m(self, symbol: str, limit: int = 60) -> List[Dict[str, float]]:
        """Convenience wrapper for 1-minute bars."""

        return self.get_aggregates(symbol, timespan="1min", limit=limit)
