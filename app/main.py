from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException

from .cache import MarketCache
from .config import Settings, get_settings
from .database import Database
from .loader import load_all
from .models import Candle, CandleRecord, HistoryResponse, LastCloseResponse
from .s3_client import build_s3_client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("aggregates.app")

settings: Settings = get_settings()
database: Optional[Database] = Database(settings.sqlite_path) if settings.should_use_sqlite() else None
cache = MarketCache(database=database)
_s3_client: Optional[Any] = None
_reload_lock = asyncio.Lock()
_periodic_task: Optional[asyncio.Task] = None

app = FastAPI(
    title="Massive Flat File Cache",
    description="Caches Massive flat-file aggregates for the microcap scout bot.",
    version="1.0.0",
)


def _get_s3_client() -> Optional[Any]:
    global _s3_client
    if _s3_client is not None:
        return _s3_client
    try:
        _s3_client = build_s3_client(settings)
    except Exception as exc:  # pragma: no cover - network guard
        logger.error("Unable to initialize S3 client: %s", exc)
        return None
    return _s3_client


async def _perform_reload(reason: str) -> bool:
    client = _get_s3_client()
    if client is None:
        logger.error("Reload skipped (%s) because S3 client is unavailable", reason)
        return False

    if _reload_lock.locked():
        logger.info("Reload already running; skipping %s request", reason)
        return False

    async with _reload_lock:
        logger.info("Starting %s reload", reason)
        success = await asyncio.to_thread(load_all, cache, client, settings)
        logger.info("Reload %s", "succeeded" if success else "failed")
        return success


async def _periodic_reload_loop() -> None:
    try:
        while True:
            await asyncio.sleep(settings.refresh_interval_seconds)
            await _perform_reload("scheduled")
    except asyncio.CancelledError:  # pragma: no cover - task shutdown
        logger.info("Periodic reload loop cancelled")


@app.on_event("startup")
async def on_startup() -> None:
    await _perform_reload("startup")
    global _periodic_task
    _periodic_task = asyncio.create_task(_periodic_reload_loop())


@app.on_event("shutdown")
async def on_shutdown() -> None:
    if _periodic_task:
        _periodic_task.cancel()
        try:
            await _periodic_task
        except asyncio.CancelledError:
            pass


def _serialize_history(history: List[Candle]) -> List[CandleRecord]:
    return [
        CandleRecord(
            date=candle.date,
            open=candle.open,
            high=candle.high,
            low=candle.low,
            close=candle.close,
            volume=candle.volume,
        )
        for candle in history
    ]


@app.get("/symbols")
async def list_symbols() -> Dict[str, List[str]]:
    symbols = cache.get_symbols()
    logger.info("Symbols requested (%s found)", len(symbols))
    return {"symbols": symbols}


@app.get("/history/{symbol}", response_model=HistoryResponse)
async def get_history(symbol: str) -> HistoryResponse:
    history = cache.get_history(symbol)
    if not history:
        logger.warning("History not found for %s", symbol.upper())
        raise HTTPException(status_code=404, detail="Symbol not found")
    payload = HistoryResponse(
        symbol=symbol.upper(),
        last_close=cache.get_last_close(symbol),
        history=_serialize_history(history),
    )
    return payload


@app.get("/last_close/{symbol}", response_model=LastCloseResponse)
async def get_last_close(symbol: str) -> LastCloseResponse:
    last_close = cache.get_last_close(symbol)
    if last_close is None:
        logger.warning("Last close not found for %s", symbol.upper())
        raise HTTPException(status_code=404, detail="Symbol not found")
    return LastCloseResponse(symbol=symbol.upper(), last_close=last_close)


@app.get("/reload")
async def trigger_reload() -> Dict[str, Any]:
    success = await _perform_reload("manual")
    return {"status": "ok" if success else "failed"}
