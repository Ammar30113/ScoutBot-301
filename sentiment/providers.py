from __future__ import annotations

import time
from typing import Dict, List, Optional, Sequence

import requests

from core.config import get_settings
from core.logger import get_logger

logger = get_logger(__name__)
settings = get_settings()


def _request_with_retry(url: str, *, params: Dict[str, str], max_attempts: int = 3) -> Optional[requests.Response]:
    """Basic backoff on 429/503."""

    delay = 0.5
    for attempt in range(1, max_attempts + 1):
        try:
            resp = requests.get(url, params=params, timeout=10)
            if resp.status_code in (429, 503):
                raise requests.HTTPError(f"HTTP {resp.status_code}", response=resp)
            resp.raise_for_status()
            return resp
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            if status not in (429, 503) or attempt == max_attempts:
                logger.warning("Request failed (final) %s params=%s err=%s", url, params, exc)
                return None
            logger.warning("Request backoff attempt %s for %s (status=%s)", attempt, url, status)
            time.sleep(delay)
            delay *= 2
        except requests.RequestException as exc:
            logger.warning("Request failed %s params=%s err=%s", url, params, exc)
            return None
    return None


class MarketauxProvider:
    BASE_URL = "https://api.marketaux.com/v1/news/all"

    def __init__(self, api_key: str) -> None:
        self.api_key = api_key

    def fetch_news(self, symbol: str) -> Dict:
        params = {
            "api_token": self.api_key,
            "symbols": symbol.upper(),
            "language": "en",
            "filter_entities": "true",
            "limit": 25,
        }
        resp = _request_with_retry(self.BASE_URL, params=params)
        if resp is None:
            return {"headlines": [], "sentiment_score": 0.0, "source": "marketaux"}
        data = resp.json().get("data", []) or []
        headlines = [
            {"title": item.get("title"), "published_at": item.get("published_at")}
            for item in data
            if item.get("title")
        ]
        scores: List[float] = []
        for item in data:
            raw = (
                item.get("overall_sentiment_score")
                or item.get("sentiment_score")
                or (item.get("sentiment") or {}).get("score")
            )
            if raw is not None:
                try:
                    scores.append(float(raw))
                except (TypeError, ValueError):
                    continue
        score = sum(scores) / len(scores) if scores else 0.0
        return {"headlines": headlines, "sentiment_score": float(score), "source": "marketaux"}

    def fetch_batch(self, symbols: Sequence[str]) -> Dict[str, Dict]:
        symbols_str = ",".join(sorted({sym.upper() for sym in symbols}))
        params = {
            "api_token": self.api_key,
            "symbols": symbols_str,
            "language": "en",
            "filter_entities": "true",
            "limit": 5,
        }
        resp = _request_with_retry(self.BASE_URL, params=params)
        if resp is None:
            return {}
        data = resp.json().get("data", []) or []
        result: Dict[str, Dict] = {}
        for item in data:
            syms = item.get("entities") or item.get("symbols") or []
            if isinstance(syms, str):
                syms = [syms]
            for sym in syms:
                sym_u = str(sym).upper()
                payload = result.setdefault(sym_u, {"headlines": [], "sentiment_score": [], "source": "marketaux"})
                if item.get("title"):
                    payload.setdefault("headlines", []).append(
                        {"title": item.get("title"), "published_at": item.get("published_at")}
                    )
                raw = (
                    item.get("overall_sentiment_score")
                    or item.get("sentiment_score")
                    or (item.get("sentiment") or {}).get("score")
                )
                if raw is not None:
                    try:
                        payload["sentiment_score"].append(float(raw))
                    except (TypeError, ValueError):
                        pass
        for sym, payload in result.items():
            scores = payload.get("sentiment_score") or []
            payload["sentiment_score"] = float(sum(scores) / len(scores)) if scores else 0.0
        return result


class FinageProvider:
    NEWS_URL = "https://api.finage.co.uk/news/stock"
    SENTIMENT_URL = "https://api.finage.co.uk/sentiment/stock"

    def __init__(self, api_key: str) -> None:
        self.api_key = api_key

    def fetch_news(self, symbol: str) -> Dict:
        params = {"symbol": symbol.upper(), "apikey": self.api_key, "limit": 25}
        resp = _request_with_retry(self.NEWS_URL, params=params)
        if resp is None:
            return {"headlines": [], "sentiment_score": 0.0, "source": "finage"}
        articles: List[Dict] = (
            resp.json().get("news")
            or resp.json().get("data")
            or resp.json().get("results")
            or []
        )
        headlines = [
            {"title": item.get("title"), "published_at": item.get("published_at") or item.get("date")}
            for item in articles
            if item.get("title")
        ]
        # Attempt sentiment endpoint if available
        sent_score = self.fetch_sentiment(symbol)
        return {"headlines": headlines, "sentiment_score": sent_score, "source": "finage"}

    def fetch_sentiment(self, symbol: str) -> float:
        params = {"symbol": symbol.upper(), "apikey": self.api_key}
        resp = _request_with_retry(self.SENTIMENT_URL, params=params)
        if resp is None:
            return 0.0
        payload = resp.json() or {}
        # Finage sentiment might return score in [-1,1] or 0-1; handle both.
        raw = payload.get("sentiment") or payload.get("sentiment_score") or payload.get("score")
        try:
            return float(raw) if raw is not None else 0.0
        except (TypeError, ValueError):
            return 0.0
