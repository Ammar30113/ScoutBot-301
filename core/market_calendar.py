"""Exchange sessions and bar availability; all public timestamps are UTC seconds."""
from functools import lru_cache

import exchange_calendars as xcals
import pandas as pd


@lru_cache(maxsize=1)
def calendar():
    return xcals.get_calendar("XNYS", start="1990-01-01", end="2035-12-31")


def session_bounds(day: str) -> tuple[float, float] | None:
    cal = calendar()
    if not cal.is_session(day):
        return None
    return cal.session_open(day).timestamp(), cal.session_close(day).timestamp()


def latest_completed_session(now: float) -> str:
    cal = calendar()
    day = pd.Timestamp(now, unit="s", tz="UTC").tz_convert("America/New_York").date().isoformat()
    session = cal.date_to_session(day, direction="previous")
    if cal.session_close(session).timestamp() > now:
        session = cal.previous_session(session)
    return session.date().isoformat()


def is_market_open(now: float) -> bool:
    day = pd.Timestamp(now, unit="s", tz="UTC").tz_convert("America/New_York").date().isoformat()
    bounds = session_bounds(day)
    return bool(bounds and bounds[0] <= now < bounds[1])
