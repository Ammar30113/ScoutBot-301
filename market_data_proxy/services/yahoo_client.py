from __future__ import annotations

from typing import Dict, Optional

import yfinance as yf


class YahooClient:
    def get_latest_price(self, symbol: str) -> Optional[Dict[str, float]]:
        try:
            ticker = yf.Ticker(symbol)
            data = ticker.history(period="1d")
        except Exception:
            return None

        if data.empty:
            return None

        last_row = data.tail(1).iloc[0]
        price = last_row.get("Close")
        if price is None:
            return None

        return {"symbol": symbol.upper(), "price": float(price), "source": "yahoo"}
