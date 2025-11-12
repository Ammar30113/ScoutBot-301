import asyncio
import os

from fastapi import FastAPI

from routes.health import router as health_router
from routes.products import router as products_router
from services.market_data import get_daily_universe
from services.trading import daily_summary, maybe_trade
from utils.logger import configure_logging
from utils.settings import get_settings

logger = configure_logging()

settings = get_settings()
STRATEGY_INTERVAL_SECONDS = int(os.getenv("STRATEGY_INTERVAL_SECONDS", 24 * 60 * 60))
_strategy_task: asyncio.Task | None = None

app = FastAPI(
    title="Microcap Scout Bot",
    description="Microcap Scout Bot - clean FastAPI rebuild with Finviz, Massive, Finnhub, Alpaca, and hybrid trade logic.",
    version="0.2.0",
    contact={"name": "Microcap Scout Bot", "url": "https://github.com/Ammar30113/microcap-scout-bot-clean"},
)


@app.on_event("startup")
async def startup_event() -> None:
    global _strategy_task
    logger.info("Starting Microcap Scout Bot in %s mode", settings.environment)
    if _strategy_task is None or _strategy_task.done():
        _strategy_task = asyncio.create_task(_run_daily_strategy())
    else:
        logger.info("Strategy task already running; skipping reinitialization")


async def _run_daily_strategy() -> None:
    """
    Build the daily universe and execute potential trades without blocking the main loop.
    """
    while True:
        try:
            symbols = await get_daily_universe()
        except Exception as exc:  # pragma: no cover - defensive log
            logger.exception("Unable to build daily universe: %s", exc)
        else:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, _trade_sync, symbols)
            await loop.run_in_executor(None, daily_summary)

        await asyncio.sleep(STRATEGY_INTERVAL_SECONDS)


def _trade_sync(symbols: list[str]) -> None:
    for symbol in symbols:
        should_continue = maybe_trade(symbol)
        if not should_continue:
            break


app.include_router(health_router)
app.include_router(products_router)
