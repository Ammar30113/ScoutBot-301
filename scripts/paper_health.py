"""Read the last paper report; exit nonzero on errors, risk halts, or a stale heartbeat."""
import json
import math
from pathlib import Path
import time

from core.config import get_settings


def check_health(path: Path, max_age: float, *, now: float | None = None) -> dict:
    report = json.loads(path.read_text())
    timestamp = float(report["timestamp"])
    age = (time.time() if now is None else now) - timestamp
    if not math.isfinite(age) or not 0 <= age <= max_age:
        raise ValueError("Paper heartbeat is stale or has an invalid clock")
    if report.get("status") != "ok":
        raise ValueError("Paper worker is degraded, halted, or failed; inspect its status report")
    return report


def main() -> int:
    settings = get_settings()
    try:
        report = check_health(settings.paper_state_path.with_suffix(".status.json"),
                              max(2 * settings.scheduler_interval_seconds + 60, 180))
        print(json.dumps({key: report[key] for key in ("status", "timestamp", "account", "data_session", "equity")}))
        return 0
    except (OSError, ValueError, KeyError, TypeError) as exc:
        print(f"UNHEALTHY: {type(exc).__name__}; inspect paper status and worker logs")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
