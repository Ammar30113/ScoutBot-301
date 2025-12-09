from __future__ import annotations

import logging
from typing import Dict, Optional

from core.config import get_settings

try:
    from openai import OpenAI
except Exception:  # pragma: no cover - dependency missing
    OpenAI = None  # type: ignore

logger = logging.getLogger(__name__)
settings = get_settings()


class GPTProvider:
    """Thin wrapper over OpenAI for sentiment scoring with graceful fallbacks."""

    def __init__(self) -> None:
        self.api_key = settings.openai_api_key
        # Try env model first, then a couple of known cheap/public models
        candidates = [
            settings.openai_model,
            "gpt-4o-mini",
            "gpt-4o-mini-2024-07-18",
            "gpt-3.5-turbo-0125",
        ]
        # Preserve order but drop falsy/duplicates
        seen = set()
        self.model_candidates = [m for m in candidates if m and not (m in seen or seen.add(m))]

        self.enabled = bool(self.api_key) and OpenAI is not None
        self._warned_missing = False
        self._disabled_reason: Optional[str] = None
        self.client = OpenAI(api_key=self.api_key) if self.enabled else None

    def _ensure_available(self) -> bool:
        if self.enabled and self.client and not self._disabled_reason:
            return True
        if not self._warned_missing:
            logger.warning(
                "GPT sentiment disabled: %s",
                self._disabled_reason or "OPENAI_API_KEY missing or openai package unavailable",
            )
            self._warned_missing = True
        return False

    def _is_model_forbidden(self, exc: Exception) -> bool:
        msg = str(exc).lower()
        return "model" in msg and ("not found" in msg or "access" in msg or "forbidden" in msg)

    def fetch_sentiment(self, symbol: str) -> Dict:
        """
        Returns {'symbol': str, 'sentiment_score': float, 'source': 'gpt'}
        sentiment_score normalized to [-1, 1]; defaults to 0 on any failure.
        """

        symbol_u = symbol.upper()
        if not self._ensure_available():
            return {"symbol": symbol_u, "sentiment_score": 0.0, "source": "gpt"}

        prompt = (
            "You are a trading assistant. Return a single sentiment score between -1 (very bearish) and 1 (very bullish) "
            "for the given stock ticker based on recent market tone. Respond with JSON: "
            '{"ticker": "TICKER", "sentiment": number}. '
            f"Ticker: {symbol_u}"
        )

        for model_name in self.model_candidates:
            try:
                resp = self.client.chat.completions.create(
                    model=model_name,
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0,
                    max_tokens=50,
                )
                content = resp.choices[0].message.content if resp and resp.choices else ""
                score = _extract_score(content)
                score = _normalize(score)
                logger.info("GPT sentiment for %s = %.4f (model=%s)", symbol_u, score, model_name)
                return {"symbol": symbol_u, "sentiment_score": score, "source": "gpt"}
            except Exception as exc:  # pragma: no cover - network guard
                if self._is_model_forbidden(exc):
                    logger.warning("Model %s unavailable; trying next fallback", model_name)
                    continue
                logger.warning("GPT sentiment failed for %s (model=%s): %s", symbol_u, model_name, exc)
                return {"symbol": symbol_u, "sentiment_score": 0.0, "source": "gpt"}

        # If we exhausted models, disable sentiment to avoid noisy 403s
        self.enabled = False
        self._disabled_reason = "no accessible OpenAI model (403/model_not_found)"
        logger.warning("Disabling GPT sentiment: no accessible OpenAI model after trying fallbacks.")
        return {"symbol": symbol_u, "sentiment_score": 0.0, "source": "gpt"}


def _normalize(score: float) -> float:
    try:
        val = float(score)
    except (TypeError, ValueError):
        return 0.0
    return max(min(val, 1.0), -1.0)


def _extract_score(text: Optional[str]) -> float:
    if not text:
        return 0.0
    import json
    try:
        data = json.loads(text)
        if isinstance(data, dict) and "sentiment" in data:
            return float(data["sentiment"])
    except Exception:
        pass
    # fallback: find first number in text
    import re

    match = re.search(r"-?\d+(?:\.\d+)?", text)
    if match:
        try:
            return float(match.group(0))
        except ValueError:
            return 0.0
    return 0.0
