import asyncio

from fastapi import FastAPI

from routes.health import router as health_router
from routes.products import router as products_router
from services.market_data import get_daily_universe
from services.trading import maybe_trade
from utils.logger import configure_logging
from utils.settings import get_settings

logger = configure_logging()

settings = get_settings()

app = FastAPI(
    title="Microcap Scout Bot",
    description="Microcap Scout Bot - clean FastAPI rebuild with Finviz, StockData, Alpaca, and hybrid trade logic.",
    version="0.2.0",
    contact={"name": "Microcap Scout Bot", "url": "https://github.com/Ammar30113/microcap-scout-bot-clean"},
)


@app.on_event("startup")
async def startup_event() -> None:
    logger.info("Starting Microcap Scout Bot in %s mode", settings.environment)
    asyncio.create_task(_run_daily_strategy())


async def _run_daily_strategy() -> None:
    """
    Build the daily universe and execute potential trades without blocking the main loop.
    """
    try:
        symbols = await get_daily_universe()
    except Exception as exc:  # pragma: no cover - defensive log
        logger.exception("Unable to build daily universe: %s", exc)
        return

    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, _trade_sync, symbols)


def _trade_sync(symbols: list[str]) -> None:
    for symbol in symbols:
        maybe_trade(symbol)


app.include_router(health_router)
app.include_router(products_router)
