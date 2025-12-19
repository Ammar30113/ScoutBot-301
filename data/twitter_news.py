from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import time
from pathlib import Path
from typing import Dict, List, Optional

import requests

from core.config import get_settings
from core.logger import get_logger

logger = get_logger(__name__)

TWITTER_API_BASE = "https://api.twitter.com/2"
MONTHLY_POST_LIMIT = 100
QUOTA_STATE_PATH = Path("data/twitter_quota.json")
USER_ID_CACHE_PATH = Path("data/twitter_user_ids.json")
USER_ID_CACHE: Dict[str, Optional[str]] = {}
USER_ID_NEGATIVE_UNTIL: Dict[str, float] = {}
USER_ID_NEGATIVE_TTL = 3600
_USER_ID_CACHE_LOADED = False


def _today() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _current_month() -> str:
    now = datetime.now(timezone.utc)
    return now.strftime("%Y-%m")


def _normalize_handle(handle: str) -> str:
    return handle.strip().lstrip("@")


def _next_utc_midnight_timestamp() -> float:
    now = datetime.now(timezone.utc)
    next_midnight = datetime(now.year, now.month, now.day, tzinfo=timezone.utc) + timedelta(days=1)
    return next_midnight.timestamp()


def _load_user_id_cache() -> None:
    global _USER_ID_CACHE_LOADED
    if _USER_ID_CACHE_LOADED:
        return
    _USER_ID_CACHE_LOADED = True
    if not USER_ID_CACHE_PATH.exists():
        return
    try:
        raw = json.loads(USER_ID_CACHE_PATH.read_text())
    except Exception:
        return
    if not isinstance(raw, dict):
        return
    for handle, user_id in raw.items():
        if not user_id:
            continue
        key = _normalize_handle(str(handle)).lower()
        USER_ID_CACHE[key] = str(user_id)


def _persist_user_id_cache() -> None:
    payload = {handle: user_id for handle, user_id in USER_ID_CACHE.items() if user_id}
    if not payload:
        return
    USER_ID_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    try:
        USER_ID_CACHE_PATH.write_text(json.dumps(payload))
    except Exception:
        logger.warning("Twitter user ID cache could not be written; continuing without persistence")


def _negative_cache_active(handle: str) -> bool:
    until = USER_ID_NEGATIVE_UNTIL.get(handle)
    if not until:
        return False
    if time.time() >= until:
        USER_ID_NEGATIVE_UNTIL.pop(handle, None)
        return False
    return True


def _mentions_symbol(text: str, symbol: str) -> bool:
    sym = symbol.upper()
    upper_text = text.upper()
    if f"${sym}" in upper_text:
        return True
    pattern = r"\b" + re.escape(sym) + r"\b"
    return re.search(pattern, upper_text) is not None


def _clean_text(text: str) -> str:
    return " ".join(text.split()).strip()


@dataclass
class QuotaState:
    day: str
    month: str
    day_count: int = 0
    month_count: int = 0
    per_account: Dict[str, int] = field(default_factory=dict)

    @classmethod
    def load(cls) -> "QuotaState":
        day = _today()
        month = _current_month()
        default_state = cls(day=day, month=month)
        if not QUOTA_STATE_PATH.exists():
            return default_state
        try:
            raw = json.loads(QUOTA_STATE_PATH.read_text())
        except Exception:
            return default_state

        state = cls(day=day, month=month)
        if raw.get("month") == month:
            state.month_count = int(raw.get("month_count", 0))
        if raw.get("day") == day:
            state.day_count = int(raw.get("day_count", 0))
            state.per_account = {k: int(v) for k, v in (raw.get("per_account") or {}).items()}
        return state

    def persist(self) -> None:
        payload = {
            "day": self.day,
            "month": self.month,
            "day_count": int(self.day_count),
            "month_count": int(self.month_count),
            "per_account": {k: int(v) for k, v in self.per_account.items()},
        }
        QUOTA_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        try:
            QUOTA_STATE_PATH.write_text(json.dumps(payload))
        except Exception:
            # Persistence failure should never block trading loop
            logger.warning("Twitter quota state could not be written; continuing without persistence")


class TwitterNewsClient:
    """
    Whitelisted, quota-aware Twitter fetcher.
    Resolves user IDs once and enforces daily/monthly caps before returning tweets.
    """

    def __init__(self) -> None:
        self.settings = get_settings()
        self.enabled = bool(self.settings.twitter_bearer_token) and bool(self.settings.use_twitter_news)
        self.allowed_accounts = [_normalize_handle(h) for h in self.settings.twitter_allowed_accounts if h.strip()]
        self.allowed_accounts = list(dict.fromkeys(self.allowed_accounts))  # dedupe, preserve order
        self.max_posts_per_day = max(0, min(int(self.settings.twitter_max_posts_per_day), MONTHLY_POST_LIMIT))
        self.tweets_per_account = max(0, int(self.settings.twitter_tweets_per_account))
        self.quota = QuotaState.load()
        self._rate_limit_until: float = 0.0
        if self.enabled:
            _load_user_id_cache()

    def _remaining_daily(self) -> int:
        return max(0, self.max_posts_per_day - self.quota.day_count)

    def _remaining_monthly(self) -> int:
        return max(0, MONTHLY_POST_LIMIT - self.quota.month_count)

    def _remaining_budget(self) -> int:
        return min(self._remaining_daily(), self._remaining_monthly())

    def _per_account_remaining(self, handle: str) -> int:
        used = self.quota.per_account.get(handle.lower(), 0)
        return max(0, self.tweets_per_account - used)

    def _update_quota(self, consumed: int, handle: Optional[str] = None) -> None:
        self.quota.day_count += consumed
        self.quota.month_count += consumed
        if handle:
            key = handle.lower()
            self.quota.per_account[key] = self.quota.per_account.get(key, 0) + consumed
        self.quota.persist()
        if self._remaining_budget() <= 0:
            logger.info(
                "Twitter quota exhausted (day=%d, month=%d)",
                self.quota.day_count,
                self.quota.month_count,
            )

    def _reset_if_period_changed(self) -> None:
        today = _today()
        month = _current_month()
        if self.quota.month != month:
            self.quota.month = month
            self.quota.month_count = 0
        if self.quota.day != today:
            self.quota.day = today
            self.quota.day_count = 0
            self.quota.per_account = {}

    def _headers(self) -> Dict[str, str]:
        return {"Authorization": f"Bearer {self.settings.twitter_bearer_token}"}

    def _cooldown_until_next_day(self, reason: str) -> None:
        until = _next_utc_midnight_timestamp()
        if until > self._rate_limit_until:
            self._rate_limit_until = until
            logger.warning(
                "Twitter rate limit hit; cooling down until %s (%s)",
                datetime.fromtimestamp(until, tz=timezone.utc).isoformat(),
                reason,
            )

    def _resolve_user_id(self, handle: str) -> Optional[str]:
        key = handle.lower()
        if key in USER_ID_CACHE:
            return USER_ID_CACHE[key]
        if self._rate_limit_until and time.time() < self._rate_limit_until:
            return None
        if _negative_cache_active(key):
            return None

        url = f"{TWITTER_API_BASE}/users/by/username/{handle}"
        try:
            resp = requests.get(url, headers=self._headers(), timeout=5)
            if resp.status_code == 429:
                self._cooldown_until_next_day("user lookup 429")
                return None
            resp.raise_for_status()
            payload = resp.json().get("data") or {}
            user_id = payload.get("id")
            if user_id:
                USER_ID_CACHE[key] = user_id
                _persist_user_id_cache()
                logger.info("Twitter user resolved: @%s -> %s", handle, user_id)
            else:
                logger.warning("Twitter user ID missing for @%s", handle)
                USER_ID_NEGATIVE_UNTIL[key] = time.time() + USER_ID_NEGATIVE_TTL
            return user_id
        except Exception as exc:  # pragma: no cover - network guard
            if isinstance(exc, requests.HTTPError):
                resp = getattr(exc, "response", None)
                if resp is not None and resp.status_code == 429:
                    self._cooldown_until_next_day("user lookup 429")
                    return None
            logger.warning("Twitter user resolution failed for @%s: %s", handle, exc)
            USER_ID_NEGATIVE_UNTIL[key] = time.time() + USER_ID_NEGATIVE_TTL
            return None

    def _resolve_all_ids(self) -> Dict[str, str]:
        ids: Dict[str, str] = {}
        if self._rate_limit_until and time.time() < self._rate_limit_until:
            return ids
        for handle in self.allowed_accounts:
            if self._rate_limit_until and time.time() < self._rate_limit_until:
                break
            user_id = self._resolve_user_id(handle)
            if user_id:
                ids[handle] = user_id
        return ids

    def _fetch_user_tweets(self, user_id: str, max_results: int) -> List[Dict[str, str]]:
        params = {
            "max_results": max(1, min(max_results, 100)),
            "exclude": "retweets,replies",
            "tweet.fields": "created_at,text",
        }
        url = f"{TWITTER_API_BASE}/users/{user_id}/tweets"
        try:
            if self._rate_limit_until and time.time() < self._rate_limit_until:
                return []
            resp = requests.get(url, headers=self._headers(), params=params, timeout=6)
            if resp.status_code == 429:
                self._cooldown_until_next_day("tweets 429")
                return []
            resp.raise_for_status()
            data = resp.json().get("data") or []
            logger.info("Twitter fetch for user %s returned %d tweets", user_id, len(data))
            return data
        except Exception as exc:  # pragma: no cover - network guard
            logger.warning("Twitter fetch failed for user %s: %s", user_id, exc)
            return []

    def _format_news(self, handle: str, text: str) -> str:
        clean = _clean_text(text)
        return f"@{handle}: {clean}"

    def get_symbol_news(self, symbol: str) -> List[str]:
        """
        Fetch whitelisted tweets mentioning ``symbol``.
        Enforces per-day, per-account, and monthly caps and never raises.
        """
        if not self.enabled or not self.allowed_accounts:
            return []

        try:
            self._reset_if_period_changed()
            if self._rate_limit_until and time.time() < self._rate_limit_until:
                logger.info(
                    "Twitter rate limit active; skipping fetch until %s",
                    datetime.fromtimestamp(self._rate_limit_until, tz=timezone.utc).isoformat(),
                )
                return []

            remaining_budget = self._remaining_budget()
            if remaining_budget <= 0:
                logger.info("Twitter daily/monthly quota exhausted; skipping fetch")
                return []

            user_ids = self._resolve_all_ids()
            if not user_ids:
                return []

            news: List[str] = []
            max_accounts_to_check = min(len(user_ids), max(3, self.max_posts_per_day * 2))
            for idx, (handle, user_id) in enumerate(user_ids.items()):
                if idx >= max_accounts_to_check:
                    break
                if remaining_budget <= 0:
                    break

                per_account_remaining = self._per_account_remaining(handle)
                fetch_cap = min(per_account_remaining, remaining_budget)
                if fetch_cap <= 0:
                    continue

                tweets = self._fetch_user_tweets(user_id, fetch_cap)
                if self._rate_limit_until and time.time() < self._rate_limit_until:
                    break
                consumed = min(len(tweets), fetch_cap, self._remaining_budget())
                if consumed:
                    self._update_quota(consumed, handle)
                    remaining_budget = self._remaining_budget()
                processed = 0
                for item in tweets:
                    if processed >= consumed:
                        break
                    text = item.get("text") or ""
                    if not text:
                        continue
                    processed += 1
                    if _mentions_symbol(text, symbol):
                        news.append(self._format_news(handle, text))
                        if len(news) >= self.max_posts_per_day:
                            break
                if len(news) >= self.max_posts_per_day:
                    break

            return news[: self.max_posts_per_day]
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("Twitter news fetch failed for %s: %s", symbol, exc)
            return []


_client: Optional[TwitterNewsClient] = None


def get_symbol_news(symbol: str) -> List[str]:
    global _client
    if _client is None:
        _client = TwitterNewsClient()
    return _client.get_symbol_news(symbol)
