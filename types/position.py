from __future__ import annotations

from typing import Optional

try:
    from pydantic import BaseModel  # type: ignore
except ImportError:
    class BaseModel:  # fallback when pydantic is unavailable
        def __init__(self, **data):
            for key, value in data.items():
                setattr(self, key, value)


class Position(BaseModel):
    symbol: str
    qty: float
    avg_price: float
    side: str
    entry_timestamp: Optional[float] = None  # NEW
