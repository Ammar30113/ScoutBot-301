from __future__ import annotations

import logging

from fastapi import FastAPI, HTTPException

from services.aggregator import PriceAggregator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("market_data_proxy")

app = FastAPI(title="Market Data Proxy", version="1.0.0")
aggregator = PriceAggregator()


@app.get("/price/{symbol}")
async def get_price(symbol: str):
    result = aggregator.get_price(symbol)
    if not result:
        logger.error("Unable to retrieve price for %s", symbol.upper())
        raise HTTPException(status_code=502, detail="No data sources available")
    return result
