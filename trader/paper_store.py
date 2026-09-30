"""Versioned, transactional paper state. Never silently reset unreadable state."""
from contextlib import contextmanager
import fcntl
import json
from pathlib import Path
import sqlite3


class PaperStore:
    def __init__(self, path: Path, *, account: str, strategy: str) -> None:
        path = path.resolve()
        self.path = path
        self.identity = {"account": account, "strategy": strategy, "currency": "USD", "mode": "paper"}
        path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(path, timeout=5)
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=FULL")
        version = self.connection.execute("PRAGMA user_version").fetchone()[0]
        if version not in (0, 1):
            raise ValueError(f"Unsupported paper schema version: {version}")
        with self.connection:
            self.connection.execute("CREATE TABLE IF NOT EXISTS state (id INTEGER PRIMARY KEY CHECK(id=1), payload TEXT NOT NULL)")
            self.connection.execute("CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)")
            self.connection.execute("PRAGMA user_version=1")

    @contextmanager
    def locked(self):
        with self.path.with_suffix(".lock").open("a") as handle:
            try:
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise RuntimeError("Another paper worker owns this database") from exc
            try:
                yield self
            finally:
                fcntl.flock(handle, fcntl.LOCK_UN)

    def load(self) -> dict | None:
        row = self.connection.execute("SELECT payload FROM state WHERE id=1").fetchone()
        if row is None:
            if self.connection.execute("SELECT COUNT(*) FROM events").fetchone()[0]:
                raise ValueError("Ledger snapshot missing; refusing to reset capital")
            return None
        data = json.loads(row[0])
        if data.get("identity") != self.identity:
            raise ValueError("Paper account/strategy mismatch; use a separate database")
        return data["state"]

    def save(self, state: dict, events: list[dict]) -> None:
        payload = json.dumps({"identity": self.identity, "state": state}, allow_nan=False)
        serialized_events = [(json.dumps(event, allow_nan=False),) for event in events]
        with self.connection:
            self.connection.execute("INSERT INTO state(id,payload) VALUES(1,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload", (payload,))
            self.connection.executemany("INSERT INTO events(payload) VALUES(?)", serialized_events)

    def close(self) -> None:
        self.connection.close()
