from __future__ import annotations

from typing import Dict, Optional

from .alpha_client import AlphaClient
from .massive_client import MassiveClient
from .yahoo_client import YahooClient


class PriceAggregator:
    def __init__(self) -> None:
        self.massive = MassiveClient()
        self.yahoo = YahooClient()
        self.alpha = AlphaClient()

    def get_price(self, symbol: str) -> Optional[Dict[str, float]]:
        result = self.massive.get_latest_price(symbol)
        if result:
            return result

        result = self.yahoo.get_latest_price(symbol)
        if result:
            return result

        return self.alpha.get_latest_price(symbol)
