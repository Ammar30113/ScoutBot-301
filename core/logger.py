from __future__ import annotations

import logging
import os
import re
import sys
from functools import lru_cache


class SecretFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        message = re.sub(r"https?://[^\s]+", "[redacted-url]", message)
        for name, value in os.environ.items():
            if any(part in name for part in ("KEY", "SECRET", "TOKEN")) and len(value) >= 6:
                message = message.replace(value, "[redacted]")
        record.msg, record.args = message, ()
        return True


def _configure_root_logger() -> None:
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)

    root = logging.getLogger()
    root.setLevel(logging.INFO)
    if not root.handlers:
        root.addHandler(handler)
    for active in root.handlers:
        if not any(isinstance(f, SecretFilter) for f in active.filters):
            active.addFilter(SecretFilter())


@lru_cache(maxsize=None)
def get_logger(name: str) -> logging.Logger:
    _configure_root_logger()
    return logging.getLogger(name)
