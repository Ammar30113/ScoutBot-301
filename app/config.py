from __future__ import annotations

from pathlib import Path
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application configuration resolved from environment variables."""

    massive_s3_access_key_id: str = Field(validation_alias="MASSIVE_S3_ACCESS_KEY_ID")
    massive_s3_secret_access_key: str = Field(validation_alias="MASSIVE_S3_SECRET_ACCESS_KEY")
    massive_s3_endpoint: str = Field(default="https://files.massive.com", validation_alias="MASSIVE_S3_ENDPOINT")
    massive_s3_bucket: str = Field(default="flatfiles", validation_alias="MASSIVE_S3_BUCKET")
    refresh_interval_minutes: int = Field(default=60, validation_alias="REFRESH_INTERVAL_MINUTES")
    sqlite_path: Path = Field(default=Path("data/aggregates.db"), validation_alias="SQLITE_PATH")
    use_sqlite: bool = Field(default=True, validation_alias="USE_SQLITE_STORAGE")

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    @property
    def refresh_interval_seconds(self) -> int:
        return max(self.refresh_interval_minutes, 1) * 60

    @property
    def s3_prefix(self) -> str:
        return "daily/aggregates/stocks/"

    def should_use_sqlite(self) -> bool:
        return self.use_sqlite


def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]
