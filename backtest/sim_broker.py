from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class Position:
    symbol: str
    qty: int
    entry_price: float
    entry_timestamp: float
    current_price: float

    @property
    def notional(self) -> float:
        return float(self.qty) * float(self.current_price)


@dataclass
class Trade:
    symbol: str
    qty: int
    entry_price: float
    exit_price: float
    entry_timestamp: float
    exit_timestamp: float
    pnl: float


@dataclass
class SimBroker:
    cash: float
    positions: Dict[str, Position] = field(default_factory=dict)
    trades: List[Trade] = field(default_factory=list)

    def equity(self) -> float:
        position_value = sum(pos.notional for pos in self.positions.values())
        return float(self.cash) + position_value

    def mark_to_market(self, price_map: Dict[str, float]) -> None:
        for symbol, price in price_map.items():
            pos = self.positions.get(symbol)
            if pos is not None:
                pos.current_price = float(price)

    def open_position(self, symbol: str, qty: int, price: float, timestamp: float) -> bool:
        if qty <= 0:
            return False
        notional = float(qty) * float(price)
        if notional <= 0 or notional > self.cash:
            return False
        self.cash -= notional
        self.positions[symbol] = Position(
            symbol=symbol,
            qty=int(qty),
            entry_price=float(price),
            entry_timestamp=float(timestamp),
            current_price=float(price),
        )
        return True

    def close_position(self, symbol: str, price: float, timestamp: float) -> Optional[Trade]:
        pos = self.positions.pop(symbol, None)
        if pos is None:
            return None
        exit_price = float(price)
        notional = float(pos.qty) * exit_price
        self.cash += notional
        pnl = (exit_price - pos.entry_price) * float(pos.qty)
        trade = Trade(
            symbol=symbol,
            qty=pos.qty,
            entry_price=pos.entry_price,
            exit_price=exit_price,
            entry_timestamp=pos.entry_timestamp,
            exit_timestamp=float(timestamp),
            pnl=float(pnl),
        )
        self.trades.append(trade)
        return trade
