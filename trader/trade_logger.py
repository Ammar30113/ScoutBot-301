from __future__ import annotations

import json
import logging
from enum import Enum
from uuid import UUID
from datetime import datetime, timezone
from typing import Any

from core.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()
LOG_PATH = settings.portfolio_state_path.parent / "trade_log.jsonl"


def _json_value(value):
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, Enum):
        return value.value
    raise TypeError(f"Unsupported journal value: {type(value).__name__}")


def log_trade(event: dict[str, Any]) -> None:
    payload = dict(event)
    payload.setdefault("timestamp", datetime.now(timezone.utc).isoformat())
    try:
        LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(LOG_PATH, "a") as handle:
            handle.write(json.dumps(payload, ensure_ascii=True, allow_nan=False, default=_json_value) + "\n")
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Trade log write failed: %s", exc)
