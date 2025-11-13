from __future__ import annotations

import logging
import threading
from typing import Dict, List, Optional

from .database import Database
from .models import Candle

logger = logging.getLogger("aggregates.cache")


class MarketCache:
    def __init__(self, database: Optional[Database] = None):
        self._data: Dict[str, List[Candle]] = {}
        self._last_close: Dict[str, float] = {}
        self._lock = threading.RLock()
        self._database = database

    def replace_all(self, data: Dict[str, List[Candle]]) -> None:
        normalized: Dict[str, List[Candle]] = {}
        last_close: Dict[str, float] = {}
        for symbol, candles in data.items():
            if not candles:
                continue
            ordered = sorted(candles, key=lambda c: c.date)
            normalized[symbol] = ordered
            last_close[symbol] = ordered[-1].close

        with self._lock:
            self._data = normalized
            self._last_close = last_close

        logger.info("Cache loaded %s symbols (%s candles)", len(normalized), sum(len(v) for v in normalized.values()))

        if self._database:
            self._database.replace_all(normalized)

    def set_symbol(self, symbol: str, candles: List[Candle]) -> None:
        if not candles:
            return
        ordered = sorted(candles, key=lambda c: c.date)
        with self._lock:
            self._data[symbol] = ordered
            self._last_close[symbol] = ordered[-1].close

    def get_symbols(self) -> List[str]:
        with self._lock:
            symbols = sorted(self._data.keys())
        if symbols:
            return symbols
        if self._database:
            return self._database.get_symbols()
        return []

    def get_history(self, symbol: str) -> List[Candle]:
        target = symbol.upper()
        with self._lock:
            data = self._data.get(target)
            if data:
                return data
        if self._database:
            candles = self._database.get_history(target)
            if candles:
                self.set_symbol(target, candles)
                return candles
        return []

    def get_last_close(self, symbol: str) -> Optional[float]:
        target = symbol.upper()
        with self._lock:
            last = self._last_close.get(target)
            if last is not None:
                return last
        if self._database:
            last = self._database.get_last_close(target)
            if last is not None:
                with self._lock:
                    self._last_close[target] = last
                return last
        return None
