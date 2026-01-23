from __future__ import annotations

from dataclasses import dataclass, field
import math
from typing import Dict, List, Optional


@dataclass
class Position:
    symbol: str
    qty: int
    entry_price: float
    entry_timestamp: float
    current_price: float
    entry_fee: float = 0.0

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
    slippage_bps: float = 0.0
    fee_bps: float = 0.0
    partial_fill_ratio: float = 1.0
    positions: Dict[str, Position] = field(default_factory=dict)
    trades: List[Trade] = field(default_factory=list)

    def _apply_slippage(self, price: float, *, side: str) -> float:
        bps = max(float(self.slippage_bps), 0.0)
        if bps <= 0:
            return price
        slip = price * (bps / 10000.0)
        if side.lower() == "sell":
            return max(price - slip, 0.0)
        return price + slip

    def _apply_fee(self, notional: float) -> float:
        bps = max(float(self.fee_bps), 0.0)
        if bps <= 0:
            return 0.0
        return notional * (bps / 10000.0)

    def _apply_partial_fill(self, qty: int) -> int:
        ratio = min(max(float(self.partial_fill_ratio), 0.0), 1.0)
        if ratio >= 0.999:
            return int(qty)
        return int(math.floor(float(qty) * ratio))

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
        fill_qty = self._apply_partial_fill(qty)
        if fill_qty <= 0:
            return False
        fill_price = self._apply_slippage(float(price), side="buy")
        notional = float(fill_qty) * float(fill_price)
        entry_fee = self._apply_fee(notional)
        total_cost = notional + entry_fee
        if total_cost <= 0 or total_cost > self.cash:
            return False
        self.cash -= total_cost
        self.positions[symbol] = Position(
            symbol=symbol,
            qty=int(fill_qty),
            entry_price=float(fill_price),
            entry_timestamp=float(timestamp),
            current_price=float(fill_price),
            entry_fee=float(entry_fee),
        )
        return True

    def close_position(self, symbol: str, price: float, timestamp: float) -> Optional[Trade]:
        pos = self.positions.pop(symbol, None)
        if pos is None:
            return None
        exit_price = self._apply_slippage(float(price), side="sell")
        notional = float(pos.qty) * exit_price
        exit_fee = self._apply_fee(notional)
        self.cash += max(notional - exit_fee, 0.0)
        pnl = (exit_price - pos.entry_price) * float(pos.qty) - float(pos.entry_fee) - float(exit_fee)
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
