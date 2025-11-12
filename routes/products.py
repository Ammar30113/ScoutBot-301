from typing import List, Optional

from fastapi import APIRouter, Query
from pydantic import BaseModel, Field

from services.market_data import gather_symbol_insights, get_cached_universe
from services.trading import get_trade_log
from utils.settings import get_settings

router = APIRouter()


class Product(BaseModel):
    name: str
    slug: str
    description: str
    integrations: List[str]
    sample_symbol: Optional[str] = Field(default=None, description="Symbol best suited for demoing this product")


class TradeAction(BaseModel):
    symbol: str
    status: str
    reason: str
    details: dict
    timestamp: str


class ProductListing(BaseModel):
    products: List[Product]
    sample_data: dict
    universe: List[str]
    trade_log: List[TradeAction]


PRODUCT_CATALOG: List[Product] = [
    Product(
        name="Microcap Screener",
        slug="microcap-screener",
        description="Async screener powered by Finviz filters for near-real-time scans.",
        integrations=["Finviz"],
        sample_symbol="IWM",
    ),
    Product(
        name="Momentum Radar",
        slug="momentum-radar",
        description="Multi-source momentum signals using Alpaca ticks married with Finviz heatmaps.",
        integrations=["Alpaca", "Finviz"],
        sample_symbol="QQQ",
    ),
    Product(
        name="Daily Hybrid Strategy",
        slug="daily-hybrid-strategy",
        description="Budget-aware allocator blending Finviz/Finnhub sentiment with Massive quotes and Alpaca execution.",
        integrations=["Alpaca", "Finviz", "Massive", "Finnhub"],
        sample_symbol="AAPL",
    ),
]


@router.get(
    "/products.json",
    response_model=ProductListing,
    summary="Product catalog",
    tags=["Products"],
)
async def get_products(symbol: Optional[str] = Query(default=None, description="Override default symbol for sample data")) -> ProductListing:
    """
    Return the product catalog plus the latest universe snapshot and trade log.
    """
    settings = get_settings()
    lookup_symbol = symbol or PRODUCT_CATALOG[0].sample_symbol or settings.default_symbol
    sample_data = await gather_symbol_insights(lookup_symbol)
    universe_snapshot = get_cached_universe()
    trade_log = get_trade_log()

    return ProductListing(
        products=PRODUCT_CATALOG,
        sample_data=sample_data,
        universe=universe_snapshot.get("symbols", []),
        trade_log=trade_log,
    )
