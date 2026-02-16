from __future__ import annotations

import logging
from datetime import datetime, time
from typing import Dict, List, Optional, Sequence

import pandas as pd
import pytz

from core.config import get_settings
from data.price_router import PriceRouter
from strategy.technicals import compute_vwap, compute_atr
from trader.risk_model import STOP_LOSS_PCT, TAKE_PROFIT_PCT

logger = logging.getLogger(__name__)
price_router = PriceRouter()
settings = get_settings()

EASTERN = pytz.timezone("America/New_York")
ORB_LOOKBACK_MINUTES = 180  # ensures the 9:30am bar is present through late morning
ORB_MIN_RANGE_PCT = 0.003  # 0.30% min opening range to avoid noise
ORB_MAX_RANGE_PCT = 0.05  # cap extremely wide opens
ORB_BUFFER_PCT = 0.002  # price must clear range by 0.20%
ORB_IMBALANCE_PCT = 0.0025  # additional distance beyond range to count as imbalance
ORB_VOLUME_RATIO_MIN = 1.25  # breakout bar volume vs prior bars
ORB_BODY_RATIO_MIN = 0.55  # candle body must be at least 55% of its range
ORB_CHOP_TOLERANCE = 0.001  # if early bars pierce both sides by this amount → choppy
ORB_STALENESS_MINUTES = 20  # ignore breakouts that happened long ago
ORB_SESSION_END = time(11, 0)  # ORB only traded in the morning


def _now_eastern() -> datetime:
    return datetime.now(EASTERN)


def _within_session(now: datetime) -> bool:
    """Trade ORB only once the first bar has closed and before midday chop."""

    return time(9, 35) <= now.time() <= ORB_SESSION_END


def _prepare_intraday(df: pd.DataFrame, today: datetime.date) -> pd.DataFrame:
    """Normalize timestamps, filter to today's session, and reset indices."""

    if df is None or df.empty or "timestamp" not in df.columns:
        return pd.DataFrame()
    frame = df.copy()
    frame["ts"] = pd.to_datetime(frame["timestamp"], unit="s", utc=True).dt.tz_convert(EASTERN)
    frame = frame[frame["ts"].dt.date == today]
    return frame.reset_index(drop=True)


def _opening_range(frame: pd.DataFrame) -> Optional[int]:
    """Return the index of the 9:30-9:35am bar, or None if missing."""

    mask = (frame["ts"].dt.time >= time(9, 30)) & (frame["ts"].dt.time < time(9, 35))
    indices = list(frame[mask].index)
    return indices[0] if indices else None


def _is_choppy(frame: pd.DataFrame, open_idx: int, or_high: float, or_low: float) -> bool:
    """Flag whipsaw opens where early bars pierce both sides of the range."""

    if open_idx + 1 >= len(frame):
        return True
    early = frame.iloc[open_idx + 1 : open_idx + 3]  # next two bars
    if early.empty:
        return True
    pierced_high = float(early["high"].max()) > or_high * (1 + ORB_CHOP_TOLERANCE)
    pierced_low = float(early["low"].min()) < or_low * (1 - ORB_CHOP_TOLERANCE)
    return pierced_high and pierced_low


def _score_breakout(breakout_extension: float, vol_ratio: float) -> float:
    """Blend distance from range and volume thrust into a 0-1 score."""

    distance_component = min(breakout_extension * 3.0, 0.25)
    volume_component = min(max(vol_ratio - 1.0, 0.0) * 0.08, 0.12)
    base = 0.55
    return float(min(base + distance_component + volume_component, 0.99))


def _evaluate_orb(symbol: str, frame: pd.DataFrame, now: datetime) -> Optional[Dict[str, float | str]]:
    frame_today = _prepare_intraday(frame, now.date())
    if frame_today.empty:
        return None

    open_idx = _opening_range(frame_today)
    if open_idx is None:
        return None

    or_bar = frame_today.iloc[open_idx]
    or_high = float(or_bar["high"])
    or_low = float(or_bar["low"])
    if or_high <= 0 or or_low <= 0:
        return None
    mid_price = max((or_high + or_low) / 2, 1e-6)
    range_pct = (or_high - or_low) / mid_price
    if not (ORB_MIN_RANGE_PCT <= range_pct <= ORB_MAX_RANGE_PCT):
        return None
    if _is_choppy(frame_today, open_idx, or_high, or_low):
        logger.info("ORB skipped for %s: choppy open", symbol)
        return None

    vwap_series = compute_vwap(frame_today)
    for idx in range(open_idx + 1, len(frame_today)):
        bar = frame_today.iloc[idx]
        bar_time = bar["ts"].time()
        if bar_time > ORB_SESSION_END:
            break

        minutes_old = (now - bar["ts"]).total_seconds() / 60
        if minutes_old > ORB_STALENESS_MINUTES:
            continue

        close = float(bar["close"])
        open_price = float(bar["open"])
        high = float(bar["high"])
        low = float(bar["low"])
        range_size = max(high - low, 1e-6)
        body_ratio = abs(close - open_price) / range_size
        if body_ratio < ORB_BODY_RATIO_MIN:
            continue

        cleared_high = close > or_high * (1 + ORB_BUFFER_PCT)
        strong_close = close > open_price
        if not (cleared_high and strong_close):
            continue

        breakout_extension = (close - or_high) / or_high
        imbalance_ok = breakout_extension >= max(range_pct * 0.25, ORB_IMBALANCE_PCT)
        vwap_val = float(vwap_series.iloc[idx]) if len(vwap_series) > idx else close
        if vwap_val <= or_high:
            imbalance_ok = False
        if not imbalance_ok:
            continue

        vol_window = frame_today.iloc[max(open_idx, idx - 3) : idx]
        base_vol = float(vol_window["volume"].mean()) if not vol_window.empty else float(or_bar["volume"])
        vol_ratio = (float(bar["volume"]) / base_vol) if base_vol else 0.0
        if vol_ratio < ORB_VOLUME_RATIO_MIN:
            continue

        score = _score_breakout(breakout_extension, vol_ratio)
        atr_series = compute_atr(frame_today, window=14)
        atr_current = float(atr_series.iloc[-1]) if len(atr_series) else 0.0
        entry_price = close
        atr_pct_intraday = (atr_current / entry_price) if entry_price > 0 and atr_current > 0 else 0.0
        base_sl_pct = STOP_LOSS_PCT
        base_tp_pct = TAKE_PROFIT_PCT
        max_sl_pct = 0.08
        max_tp_pct = 0.20
        if atr_pct_intraday > 0:
            stop_loss_pct = max(base_sl_pct, min(atr_pct_intraday * settings.atr_multiplier, max_sl_pct))
        else:
            stop_loss_pct = base_sl_pct
        take_profit_pct = max(base_tp_pct, min(stop_loss_pct * 1.8, max_tp_pct))
        logger.info(
            "ORB breakout long %s: close=%.2f range=%.3f%% ext=%.3f vol_ratio=%.2f score=%.3f",
            symbol,
            close,
            range_pct * 100,
            breakout_extension,
            vol_ratio,
            score,
        )
        return {
            "symbol": symbol,
            "type": "orb",
            "score": score,
            "vol_ratio": vol_ratio,
            "orb_range_pct": range_pct,
            "orb_extension": breakout_extension,
            "atr_pct": atr_pct_intraday,
            "stop_loss_pct": stop_loss_pct,
            "take_profit_pct": take_profit_pct,
            "reason": "5m ORB breakout long",
        }
    return None


def find_orb_setups(universe: Sequence[str], *, crash_mode: bool = False, now: Optional[datetime] = None) -> List[Dict[str, float | str]]:
    """
    Identify 5-minute ORB breakouts across the universe.
    Skips when not in the morning session or during crash_mode.
    """

    now = now or _now_eastern()
    if crash_mode:
        return []
    if not _within_session(now):
        return []

    signals: List[Dict[str, float | str]] = []
    for symbol in universe:
        try:
            bars = price_router.get_aggregates(symbol, window=ORB_LOOKBACK_MINUTES)
            frame = PriceRouter.aggregates_to_dataframe(bars)
        except Exception as exc:  # pragma: no cover - network guard
            logger.warning("ORB data unavailable for %s: %s", symbol, exc)
            continue
        provider_lookup = getattr(price_router, "last_provider", None)
        intraday_provider = provider_lookup(symbol, "intraday") if callable(provider_lookup) else None
        signal = _evaluate_orb(symbol, frame, now)
        if signal:
            if intraday_provider:
                signal["provider_intraday"] = intraday_provider
            signals.append(signal)
    return signals
