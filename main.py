"""Paper-only worker. IBKR integration is intentionally not enabled yet."""
from __future__ import annotations

import argparse
import json
import logging
import signal
import threading
import time

import requests

from core.config import get_settings
from core.io import atomic_json
from core.logger import get_logger
from data.price_router import PriceRouter
from scripts.preflight import run_preflight
from trader.paper_engine import PaperEngine
from trader.paper_store import PaperStore

logger = get_logger(__name__)


def run_once(store: PaperStore, router: PriceRouter, now: float | None = None) -> dict:
    settings = get_settings()
    with store.locked():
        engine = PaperEngine(settings, store.load())
        report = engine.step(router, time.time() if now is None else now)
        store.save(engine.snapshot(), engine.broker.events)
        atomic_json(settings.paper_state_path.with_suffix(".status.json"), report)
        return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--once", action="store_true", help="Run one cycle and exit; nonzero if unhealthy")
    args = parser.parse_args()
    errors, warnings = run_preflight()
    for warning in warnings:
        logger.warning("Preflight: %s", warning)
    for error in errors:
        logger.error("Preflight: %s", error)
    if errors:
        return 2
    settings = get_settings()
    store = PaperStore(settings.paper_state_path, account=settings.paper_account_id, strategy=settings.strategy)
    router = PriceRouter()
    stopped = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stopped.set())
    try:
        while not stopped.is_set():
            start = time.monotonic()
            try:
                report = run_once(store, router)
                logger.info("Paper cycle: %s", json.dumps(report, allow_nan=False))
                if report["status"] == "ok" and settings.heartbeat_url:
                    try:
                        requests.get(settings.heartbeat_url, timeout=5).raise_for_status()
                    except requests.RequestException:
                        logger.warning("Heartbeat delivery failed")
                code = 0 if report["status"] == "ok" else 1
            except Exception as exc:
                # Provider exception messages can contain credential-bearing URLs.
                logger.error("Paper cycle failed (%s); persisted ledger was not reset", type(exc).__name__)
                atomic_json(settings.paper_state_path.with_suffix(".status.json"),
                            {"timestamp": time.time(), "status": "error", "error": type(exc).__name__})
                code = 1
            if args.once:
                return code
            stopped.wait(max(settings.scheduler_interval_seconds - (time.monotonic() - start), 1))
    finally:
        store.close()
    return 0


if __name__ == "__main__":
    logging.getLogger("httpx").setLevel(logging.WARNING)
    raise SystemExit(main())
