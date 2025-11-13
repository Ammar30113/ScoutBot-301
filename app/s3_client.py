from __future__ import annotations

import logging
from typing import Any

import boto3
from botocore.config import Config

from .config import Settings

logger = logging.getLogger("aggregates.s3")


def build_s3_client(settings: Settings) -> Any:
    """Create a boto3 S3 client configured for Massive flat files."""

    try:
        session = boto3.session.Session()
        client = session.client(
            "s3",
            endpoint_url=settings.massive_s3_endpoint,
            region_name="us-east-1",
            aws_access_key_id=settings.massive_s3_access_key_id,
            aws_secret_access_key=settings.massive_s3_secret_access_key,
            config=Config(signature_version="s3v4", retries={"max_attempts": 3, "mode": "standard"}),
        )
        return client
    except Exception as exc:  # pragma: no cover - network guard
        logger.exception("Failed to build S3 client: %s", exc)
        raise
