from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional

from pydantic import BaseModel


@dataclass(slots=True)
class Candle:
    symbol: str
    date: str
    open: float
    high: float
    low: float
    close: float
    volume: int

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class CandleRecord(BaseModel):
    date: str
    open: float
    high: float
    low: float
    close: float
    volume: int


class HistoryResponse(BaseModel):
    symbol: str
    last_close: Optional[float]
    history: List[CandleRecord]


class LastCloseResponse(BaseModel):
    symbol: str
    last_close: Optional[float]
