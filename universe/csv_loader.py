from __future__ import annotations

from pathlib import Path
import pandas as pd

from core.logger import get_logger

logger = get_logger(__name__)


def load_universe_from_csv(path: Path) -> pd.DataFrame:
    """Load a liquid universe from CSV with a required 'symbol' column."""

    if not path.exists():
        logger.warning("Universe CSV missing: %s", path)
        return pd.DataFrame(columns=["symbol"])
    try:
        df = pd.read_csv(path)
    except Exception as exc:
        logger.warning("Unable to read universe CSV %s: %s", path, exc)
        return pd.DataFrame(columns=["symbol"])

    if "symbol" not in df.columns:
        logger.warning("Universe CSV %s missing 'symbol' column", path)
        return pd.DataFrame(columns=["symbol"])
    df["symbol"] = df["symbol"].astype(str).str.upper()
    return df[["symbol"]].dropna()
