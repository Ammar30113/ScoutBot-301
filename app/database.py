from __future__ import annotations

import logging
import sqlite3
import threading
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from .models import Candle

logger = logging.getLogger("aggregates.database")


class Database:
    def __init__(self, path: Path):
        self._path = path
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        try:
            with self._connect() as conn:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS candles (
                        symbol TEXT NOT NULL,
                        date TEXT NOT NULL,
                        open REAL NOT NULL,
                        high REAL NOT NULL,
                        low REAL NOT NULL,
                        close REAL NOT NULL,
                        volume INTEGER NOT NULL,
                        PRIMARY KEY (symbol, date)
                    )
                    """
                )
                conn.execute("CREATE INDEX IF NOT EXISTS idx_symbol_date ON candles(symbol, date)")
        except sqlite3.Error as exc:  # pragma: no cover - defensive logging
            logger.exception("Failed to initialize SQLite database: %s", exc)

    def replace_all(self, data: Dict[str, List[Candle]]) -> None:
        def row_iter() -> Iterable[tuple]:
            for candles in data.values():
                for candle in candles:
                    yield (
                        candle.symbol,
                        candle.date,
                        candle.open,
                        candle.high,
                        candle.low,
                        candle.close,
                        candle.volume,
                    )

        try:
            with self._lock:
                with self._connect() as conn:
                    conn.execute("DELETE FROM candles")
                    conn.executemany(
                        """
                        INSERT OR REPLACE INTO candles
                        (symbol, date, open, high, low, close, volume)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        row_iter(),
                    )
        except sqlite3.Error as exc:
            logger.exception("Failed to persist data to SQLite: %s", exc)

    def get_symbols(self) -> List[str]:
        try:
            with self._lock, self._connect() as conn:
                rows = conn.execute("SELECT DISTINCT symbol FROM candles ORDER BY symbol").fetchall()
                return [row["symbol"] for row in rows]
        except sqlite3.Error as exc:
            logger.exception("Failed to read symbols from SQLite: %s", exc)
            return []

    def get_history(self, symbol: str) -> List[Candle]:
        try:
            with self._lock, self._connect() as conn:
                rows = conn.execute(
                    "SELECT symbol, date, open, high, low, close, volume FROM candles WHERE symbol = ? ORDER BY date",
                    (symbol.upper(),),
                ).fetchall()
                return [
                    Candle(
                        symbol=row["symbol"],
                        date=row["date"],
                        open=row["open"],
                        high=row["high"],
                        low=row["low"],
                        close=row["close"],
                        volume=row["volume"],
                    )
                    for row in rows
                ]
        except sqlite3.Error as exc:
            logger.exception("Failed to read %s history from SQLite: %s", symbol, exc)
            return []

    def get_last_close(self, symbol: str) -> Optional[float]:
        try:
            with self._lock, self._connect() as conn:
                row = conn.execute(
                    "SELECT close FROM candles WHERE symbol = ? ORDER BY date DESC LIMIT 1",
                    (symbol.upper(),),
                ).fetchone()
                return row["close"] if row else None
        except sqlite3.Error as exc:
            logger.exception("Failed to read %s last close from SQLite: %s", symbol, exc)
            return None
