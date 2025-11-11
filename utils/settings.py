from functools import lru_cache
from typing import Optional

from pydantic import AliasChoices, AnyHttpUrl, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    environment: str = Field(default="development", validation_alias="ENVIRONMENT")
    finviz_api_key: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("FINVIZ_API_KEY", "FINVIZ_TOKEN"),
    )
    stockdata_api_key: Optional[str] = Field(
        default=None,
        validation_alias="STOCKDATA_API_KEY",
    )
    massive_api_key: Optional[str] = Field(
        default=None,
        validation_alias="MASSIVE_API_KEY",
    )
    finnhub_api_key: Optional[str] = Field(
        default=None,
        validation_alias="FINNHUB_API_KEY",
    )
    alpaca_api_key: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("ALPACA_API_KEY", "APCA_API_KEY_ID"),
    )
    alpaca_secret_key: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("ALPACA_SECRET_KEY", "APCA_API_SECRET_KEY"),
    )
    alpaca_base_url: AnyHttpUrl = Field(
        default="https://data.alpaca.markets/v2",
        validation_alias=AliasChoices("ALPACA_BASE_URL", "APCA_API_DATA_URL"),
    )
    alpaca_trading_url: AnyHttpUrl = Field(
        default="https://paper-api.alpaca.markets",
        validation_alias=AliasChoices("ALPACA_TRADING_BASE_URL", "APCA_API_BASE_URL"),
    )
    default_symbol: str = Field(default="SPY", validation_alias="DEFAULT_SYMBOL")
    trading_budget: float = Field(default=1000.0, validation_alias="TRADING_BUDGET")
    daily_budget_usd: float = Field(default=10000.0, validation_alias="DAILY_BUDGET_USD")
    mode: str = Field(default="paper", validation_alias="MODE")

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")


@lru_cache
def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]
