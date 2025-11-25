from __future__ import annotations

from data.price_router import PriceRouter
from core.logger import get_logger

logger = get_logger(__name__)
price_router = PriceRouter()


def is_crash_mode() -> tuple[bool, float]:
    """
    Returns (crash_mode, drop_pct) based on SPY 5-minute bars.
    Crash mode triggers when last 5-min bar drops >= 1%.
    """

    try:
        bars = price_router.get_aggregates("SPY", window=10)  # get at least two 5m bars post-resample
        if not bars or len(bars) < 2:
            return False, 0.0
        close_prev = float(bars[-2]["close"])
        close_last = float(bars[-1]["close"])
        if close_prev == 0:
            return False, 0.0
        drop = (close_last - close_prev) / close_prev
        return drop <= -0.01, drop
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Crash detector unavailable: %s", exc)
        return False, 0.0
