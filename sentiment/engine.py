from __future__ import annotations

import logging
import time
from typing import Dict, List, Optional, Sequence, Tuple

from core.config import get_settings
from sentiment.providers import FinageProvider, MarketauxProvider

logger = logging.getLogger(__name__)
settings = get_settings()


def _normalize(score: float) -> float:
    """Normalize incoming sentiment into [-1, 1]."""

    if score is None:
        return 0.0
    try:
        val = float(score)
    except (TypeError, ValueError):
        return 0.0
    if val > 1.0:
        # assume already scaled 0-100
        val = val / 100.0
    return max(min(val, 1.0), -1.0)


class SentimentEngine:
    def __init__(self) -> None:
        self.enabled = settings.use_sentiment
        self.cache_ttl = settings.sentiment_cache_ttl
        self.marketaux = MarketauxProvider(settings.marketaux_api_key) if settings.marketaux_api_key else None
        self.finage = FinageProvider(settings.finage_api_key) if settings.finage_api_key else None
        self._cache: Dict[str, Dict] = {}

    def _from_cache(self, symbol: str) -> Optional[Dict]:
        entry = self._cache.get(symbol.upper())
        if not entry:
            return None
        if time.time() - entry.get("timestamp", 0) > self.cache_ttl:
            return None
        return entry

    def _set_cache(self, symbol: str, payload: Dict) -> None:
        payload["timestamp"] = time.time()
        self._cache[symbol.upper()] = payload

    def _composite(self, marketaux_score: float, finage_score: float) -> float:
        parts: List[Tuple[float, float]] = []
        if marketaux_score is not None:
            parts.append((0.70, _normalize(marketaux_score)))
        if finage_score is not None:
            parts.append((0.30, _normalize(finage_score)))
        if not parts:
            return 0.0
        weighted = sum(w * s for w, s in parts)
        total_w = sum(w for w, _ in parts)
        return weighted / total_w if total_w else 0.0

    def _fetch_symbol(self, symbol: str) -> Dict:
        symbol_u = symbol.upper()
        headlines: List[Dict] = []
        marketaux_score = None
        finage_score = None
        source_used = None

        if self.marketaux:
            try:
                res = self.marketaux.fetch_news(symbol_u)
                marketaux_score = res.get("sentiment_score")
                headlines = res.get("headlines") or []
                source_used = res.get("source") or "marketaux"
            except Exception as exc:  # pragma: no cover - defensive
                logger.warning("Sentiment[%s] marketaux failed: %s", symbol_u, exc)

        if (marketaux_score is None or marketaux_score == 0.0) and self.finage:
            try:
                res = self.finage.fetch_news(symbol_u)
                finage_score = res.get("sentiment_score")
                headlines = headlines or res.get("headlines") or []
                source_used = res.get("source") or "finage"
            except Exception as exc:  # pragma: no cover - defensive
                logger.warning("Sentiment[%s] finage failed: %s", symbol_u, exc)

        score = self._composite(marketaux_score, finage_score)
        payload = {
            "symbol": symbol_u,
            "sentiment_score": score,
            "headlines": headlines,
            "source": source_used or "none",
        }
        logger.info("Sentiment[%s] provider=%s score=%.4f", symbol_u, payload["source"], score)
        self._set_cache(symbol_u, payload)
        return payload

    def get_sentiment(self, symbol: str) -> Dict:
        if not self.enabled:
            return {"symbol": symbol.upper(), "sentiment_score": 0.0, "headlines": [], "source": "disabled"}

        cached = self._from_cache(symbol)
        if cached:
            return cached
        return self._fetch_symbol(symbol)

    def get_news(self, symbol: str) -> Dict:
        return self.get_sentiment(symbol)

    def preload(self, symbols: Sequence[str]) -> None:
        """Batch preload cache for large universes using Marketaux batch."""

        if not self.enabled or not self.marketaux:
            return
        symbols = [s.upper() for s in symbols]
        if not symbols:
            return
        try:
            batch = self.marketaux.fetch_batch(symbols)
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("Sentiment batch preload failed: %s", exc)
            return
        for sym, data in batch.items():
            score = data.get("sentiment_score") or 0.0
            payload = {
                "symbol": sym,
                "sentiment_score": _normalize(score),
                "headlines": data.get("headlines") or [],
                "source": data.get("source") or "marketaux",
                "timestamp": time.time(),
            }
            self._cache[sym] = payload
