from __future__ import annotations

import csv
import gzip
import io
import logging
from collections import defaultdict
from typing import Any, DefaultDict, Dict, List

from .cache import MarketCache
from .config import Settings
from .models import Candle
from .utils import safe_float, safe_int

logger = logging.getLogger("aggregates.loader")


def load_all(cache: MarketCache, s3_client: Any, settings: Settings) -> bool:
    """Download all flat files, parse them, and refresh the cache."""

    try:
        aggregated = _fetch_and_parse_all(s3_client, settings)
        if not aggregated:
            logger.warning("No data fetched from Massive flat files; cache left untouched")
            return False
        cache.replace_all(aggregated)
        return True
    except Exception as exc:  # pragma: no cover - network guard
        logger.exception("Failed to refresh cache from Massive flat files: %s", exc)
        return False


def _fetch_and_parse_all(s3_client: Any, settings: Settings) -> Dict[str, List[Candle]]:
    aggregated: DefaultDict[str, List[Candle]] = defaultdict(list)
    bucket = settings.massive_s3_bucket
    prefix = settings.s3_prefix
    keys = _list_keys(s3_client, bucket, prefix)

    for key in keys:
        body = _download_object(s3_client, bucket, key)
        if not body:
            continue
        file_data = _parse_csv_gz(body)
        for symbol, candles in file_data.items():
            aggregated[symbol].extend(candles)

    # Deduplicate by date per symbol
    normalized: Dict[str, List[Candle]] = {}
    for symbol, candles in aggregated.items():
        by_date: Dict[str, Candle] = {}
        for candle in candles:
            by_date[candle.date] = candle
        normalized[symbol] = sorted(by_date.values(), key=lambda c: c.date)

    logger.info("Fetched %s symbols across %s files", len(normalized), len(keys))
    return normalized


def _list_keys(s3_client: Any, bucket: str, prefix: str) -> List[str]:
    keys: List[str] = []
    paginator = s3_client.get_paginator("list_objects_v2")
    try:
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj.get("Key")
                if key and key.endswith(".csv.gz"):
                    keys.append(key)
    except Exception as exc:
        logger.exception("Failed to list flat files: %s", exc)
    return keys


def _download_object(s3_client: Any, bucket: str, key: str) -> bytes | None:
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        return response["Body"].read()
    except Exception as exc:
        logger.exception("Failed to download %s: %s", key, exc)
        return None


def _parse_csv_gz(payload: bytes) -> Dict[str, List[Candle]]:
    result: DefaultDict[str, List[Candle]] = defaultdict(list)
    try:
        with gzip.GzipFile(fileobj=io.BytesIO(payload)) as gz:
            wrapper = io.TextIOWrapper(gz, encoding="utf-8")
            reader = csv.DictReader(wrapper)
            for row in reader:
                symbol = (row.get("ticker") or row.get("symbol") or "").strip().upper()
                date = (row.get("date") or row.get("timestamp") or "").strip()
                if not symbol or not date:
                    continue

                open_price = safe_float(row.get("open"))
                high_price = safe_float(row.get("high"))
                low_price = safe_float(row.get("low"))
                close_price = safe_float(row.get("close"))
                volume = safe_int(row.get("volume")) or 0

                if None in (open_price, high_price, low_price, close_price):
                    continue

                result[symbol].append(
                    Candle(
                        symbol=symbol,
                        date=date,
                        open=open_price,
                        high=high_price,
                        low=low_price,
                        close=close_price,
                        volume=volume,
                    )
                )
    except Exception as exc:
        logger.exception("Failed to parse csv.gz payload: %s", exc)

    return result
