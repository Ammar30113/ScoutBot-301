from __future__ import annotations

import logging

from alpaca.trading.client import TradingClient

from core.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

_base_url = (settings.alpaca_base_url or "").lower()
_mode = settings.trading_mode or "paper"
is_live_trading_mode = _mode == "live" or ("paper" not in _base_url)

if is_live_trading_mode and not settings.allow_live_trading:
    trading_client = None
    logger.error(
        "Live trading blocked: set ALLOW_LIVE_TRADING=true to enable live execution (MODE=%s, ALPACA_API_BASE_URL=%s)",
        settings.trading_mode,
        settings.alpaca_base_url,
    )
elif settings.alpaca_api_key and settings.alpaca_api_secret:
    trading_client = TradingClient(
        settings.alpaca_api_key,
        settings.alpaca_api_secret,
        paper=not is_live_trading_mode,
    )
else:
    trading_client = None
    logger.warning("Alpaca credentials missing; trading operations will be skipped.")
