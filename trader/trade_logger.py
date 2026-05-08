from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

from core.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()
LOG_PATH = settings.portfolio_state_path.parent / "trade_log.jsonl"


def log_trade(event: dict[str, Any]) -> None:
    payload = dict(event)
    payload.setdefault("timestamp", datetime.now(timezone.utc).isoformat())
    try:
        LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(LOG_PATH, "a") as handle:
            handle.write(json.dumps(payload, ensure_ascii=True) + "\n")
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Trade log write failed: %s", exc)
