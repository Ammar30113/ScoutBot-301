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
    """Thin wrapper over OpenAI for sentiment scoring."""

    def __init__(self) -> None:
        self.api_key = settings.openai_api_key
        self.model = settings.openai_model
        self.fallback_model = "gpt-3.5-turbo-0125"
        self.enabled = bool(self.api_key) and OpenAI is not None
        self._warned_missing = False
        self.client = OpenAI(api_key=self.api_key) if self.enabled else None

    def _ensure_available(self) -> bool:
        if self.enabled and self.client:
            return True
        if not self._warned_missing:
            logger.warning("GPT sentiment disabled: OPENAI_API_KEY missing or openai package unavailable")
            self._warned_missing = True
        return False

    def fetch_sentiment(self, symbol: str) -> Dict:
        """
        Returns {'symbol': str, 'sentiment_score': float, 'source': 'gpt'}
        sentiment_score normalized to [-1, 1]; defaults to 0 on any failure.
        """

        symbol_u = symbol.upper()
        if not self._ensure_available():
            return {"symbol": symbol_u, "sentiment_score": 0.0, "source": "gpt"}

        model_to_use = self.model or self.fallback_model
        if model_to_use.lower() == "gpt-4o-mini" and not self.fallback_model:
            model_to_use = "gpt-3.5-turbo-0125"

        prompt = (
            "You are a trading assistant. Return a single sentiment score between -1 (very bearish) and 1 (very bullish) "
            "for the given stock ticker based on recent market tone. Respond with JSON: "
            '{"ticker": "TICKER", "sentiment": number}. '
            f"Ticker: {symbol_u}"
        )
        try:
            resp = self.client.chat.completions.create(
                model=model_to_use,
                messages=[{"role": "user", "content": prompt}],
                temperature=0,
                max_tokens=50,
            )
            content = resp.choices[0].message.content if resp and resp.choices else ""
            score = _extract_score(content)
        except Exception as exc:  # pragma: no cover - network guard
            msg = str(exc).lower()
            if "model" in msg and "not found" in msg and model_to_use != self.fallback_model:
                logger.warning("Model %s unavailable; retrying with %s", model_to_use, self.fallback_model)
                self.model = self.fallback_model
                return self.fetch_sentiment(symbol_u)
            logger.warning("GPT sentiment failed for %s: %s", symbol_u, exc)
            score = 0.0
        score = _normalize(score)
        logger.info("GPT sentiment for %s = %.4f", symbol_u, score)
        return {"symbol": symbol_u, "sentiment_score": score, "source": "gpt"}


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
