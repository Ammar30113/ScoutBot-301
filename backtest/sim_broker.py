"""Cash-only simulated execution, shared by historical and daily paper runs.

Orders fill at the next bar's open, only after that entire bar is available.
Stops win ambiguous stop/target bars. This is a conservative OHLC model, not
an exchange emulator: no queue priority, intrabar path, or liquidity guarantee.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
import math
from typing import Any

from data.bars import Bar


@dataclass
class Position:
    symbol: str
    qty: int
    entry_price: float
    entry_timestamp: float
    current_price: float
    entry_fee: float = 0.0
    stop_price: float = 0.0
    target_price: float = 0.0
    owner: str = "daily_trend"

    @property
    def notional(self) -> float:
        return self.qty * self.current_price


@dataclass
class Trade:
    symbol: str
    qty: int
    entry_price: float
    exit_price: float
    entry_timestamp: float
    exit_timestamp: float
    pnl: float
    reason: str = "exit"
    fees: float = 0.0


@dataclass
class PaperOrder:
    id: str
    symbol: str
    action: str
    qty: int
    submitted_at: float
    expires_at: float
    limit_price: float
    stop_loss_pct: float = 0.05
    take_profit_pct: float = 0.10
    reason: str = "signal"
    status: str = "pending"


@dataclass
class SimBroker:
    cash: float
    slippage_bps: float = 10.0
    fee_bps: float = 5.0
    partial_fill_ratio: float = 1.0
    min_fee: float = 0.35
    positions: dict[str, Position] = field(default_factory=dict)
    trades: list[Trade] = field(default_factory=list)
    orders: dict[str, PaperOrder] = field(default_factory=dict)
    processed: dict[str, float] = field(default_factory=dict)
    events: list[dict[str, Any]] = field(default_factory=list)

    def __post_init__(self) -> None:
        values = (self.cash, self.slippage_bps, self.fee_bps, self.partial_fill_ratio, self.min_fee)
        if not all(math.isfinite(v) for v in values) or min(values) < 0 or not 0 < self.partial_fill_ratio <= 1 or max(self.slippage_bps, self.fee_bps) >= 10000:
            raise ValueError("Invalid simulation capital or execution costs")

    def _apply_slippage(self, price: float, *, side: str) -> float:
        return price * (1 + (1 if side == "buy" else -1) * self.slippage_bps / 10000)

    def _apply_fee(self, notional: float) -> float:
        return max(self.min_fee, notional * self.fee_bps / 10000)

    def equity(self) -> float:
        return self.cash + sum(pos.notional for pos in self.positions.values())

    def mark_to_market(self, price_map: dict[str, float]) -> None:
        for symbol, price in price_map.items():
            if not math.isfinite(price) or price <= 0:
                raise ValueError("Invalid mark price")
            if symbol in self.positions:
                self.positions[symbol].current_price = float(price)

    def pending(self, symbol: str | None = None) -> list[PaperOrder]:
        return [o for o in self.orders.values() if o.status == "pending" and (symbol is None or o.symbol == symbol)]

    def reserved_cash(self) -> float:
        return sum(o.qty * o.limit_price + self._apply_fee(o.qty * o.limit_price)
                   for o in self.pending() if o.action == "BUY")

    def exposure(self) -> float:
        return sum(p.notional for p in self.positions.values()) + sum(
            o.qty * o.limit_price for o in self.pending() if o.action == "BUY")

    def stop_risk(self) -> float:
        return sum(max(p.current_price - p.stop_price, 0) * p.qty for p in self.positions.values()) + sum(
            o.qty * o.limit_price * o.stop_loss_pct for o in self.pending() if o.action == "BUY")

    def submit(self, order: PaperOrder) -> bool:
        if order.id in self.orders or self.pending(order.symbol):
            return False
        numeric = (order.submitted_at, order.expires_at, order.limit_price, order.stop_loss_pct, order.take_profit_pct)
        if not all(math.isfinite(v) for v in numeric) or order.expires_at <= order.submitted_at:
            raise ValueError("Invalid order timestamps or prices")
        if order.action not in {"BUY", "SELL"} or type(order.qty) is not int or order.qty <= 0 or order.limit_price <= 0 or order.status != "pending":
            raise ValueError("Invalid order")
        if not 0 < order.stop_loss_pct < 1 or not 0 < order.take_profit_pct < 1:
            raise ValueError("Invalid protective prices")
        if order.action == "BUY":
            cost = order.qty * order.limit_price
            if order.symbol in self.positions or cost + self._apply_fee(cost) > self.cash - self.reserved_cash():
                return False
        elif order.symbol not in self.positions or order.qty != self.positions[order.symbol].qty:
            return False
        self.orders[order.id] = order
        self.events.append({"timestamp": order.submitted_at, "event": "submitted", **asdict(order)})
        return True

    def cancel_entries(self, now: float, reason: str) -> None:
        for order in self.pending():
            if order.action == "BUY":
                self._finish(order, "canceled", now, reason)

    def _finish(self, order: PaperOrder, status: str, timestamp: float, reason: str) -> None:
        order.status = status
        self.events.append({"timestamp": timestamp, "event": status, "order_id": order.id,
                            "symbol": order.symbol, "reason": reason})

    def open_position(self, symbol: str, qty: int, price: float, timestamp: float,
                      *, stop_loss_pct: float = 0.05, take_profit_pct: float = 0.10) -> bool:
        if symbol in self.positions or type(qty) is not int or qty <= 0 or not math.isfinite(price) or price <= 0:
            return False
        qty = math.floor(qty * self.partial_fill_ratio)
        if qty <= 0:
            return False
        fill = self._apply_slippage(price, side="buy")
        notional = qty * fill
        fee = self._apply_fee(notional)
        if notional + fee > self.cash:
            return False
        self.cash -= notional + fee
        self.positions[symbol] = Position(symbol, qty, fill, timestamp, price, fee,
                                          fill * (1 - stop_loss_pct), fill * (1 + take_profit_pct))
        self.events.append({"timestamp": timestamp, "event": "fill", "symbol": symbol,
                            "action": "BUY", "qty": qty, "price": fill, "fee": fee})
        return True

    def close_position(self, symbol: str, price: float, timestamp: float, *, reason: str = "exit") -> Trade | None:
        if not math.isfinite(price) or price <= 0:
            raise ValueError("Invalid exit price")
        pos = self.positions.pop(symbol, None)
        if pos is None:
            return None
        fill = self._apply_slippage(price, side="sell")
        notional = pos.qty * fill
        fee = self._apply_fee(notional)
        self.cash += notional - fee
        pnl = (fill - pos.entry_price) * pos.qty - pos.entry_fee - fee
        trade = Trade(symbol, pos.qty, pos.entry_price, fill, pos.entry_timestamp, timestamp,
                      pnl, reason, pos.entry_fee + fee)
        self.trades.append(trade)
        self.events.append({"timestamp": timestamp, "event": "fill", "symbol": symbol,
                            "action": "SELL", "qty": pos.qty, "price": fill, "fee": fee, "pnl": pnl,
                            "reason": reason})
        return trade

    def process_bar(self, symbol: str, bar: Bar) -> None:
        if bar.timestamp <= self.processed.get(symbol, 0):
            return
        self.processed[symbol] = bar.timestamp
        for order in self.pending(symbol):
            if bar.timestamp < order.submitted_at:
                continue
            if bar.timestamp > order.expires_at:
                self._finish(order, "expired", bar.available_at, "order_expired")
                continue
            if order.action == "BUY":
                if self._apply_slippage(bar.open, side="buy") > order.limit_price:
                    self._finish(order, "canceled", bar.timestamp, "gap_above_limit")
                    continue
                filled = self.open_position(symbol, order.qty, bar.open, bar.timestamp,
                                            stop_loss_pct=order.stop_loss_pct, take_profit_pct=order.take_profit_pct)
            else:
                filled = self.close_position(symbol, bar.open, bar.timestamp, reason=order.reason) is not None
            self._finish(order, "filled" if filled else "rejected", bar.timestamp,
                         "partial_remainder_canceled" if self.partial_fill_ratio < 1 else order.reason)
        pos = self.positions.get(symbol)
        if pos is not None:
            # Full-bar timestamps disclose uncertainty; stop first if both levels touched.
            if bar.low <= pos.stop_price:
                self.close_position(symbol, min(bar.open, pos.stop_price), bar.available_at, reason="stop_loss")
            elif bar.high >= pos.target_price:
                self.close_position(symbol, pos.target_price, bar.available_at, reason="take_profit")
            else:
                pos.current_price = bar.close
        if symbol not in self.positions:
            for order in self.pending(symbol):
                if order.action == "SELL":
                    self._finish(order, "canceled", bar.available_at, "position_already_closed")

    def to_dict(self) -> dict:
        data = asdict(self)
        data.pop("events")
        return data

    @classmethod
    def from_dict(cls, data: dict) -> "SimBroker":
        required = {"cash", "slippage_bps", "fee_bps", "partial_fill_ratio", "min_fee", "positions", "trades", "orders", "processed"}
        if set(data) != required:
            raise ValueError("Incomplete or unsupported paper ledger")
        values = dict(data)
        values["positions"] = {k: Position(**v) for k, v in values.get("positions", {}).items()}
        values["orders"] = {k: PaperOrder(**v) for k, v in values.get("orders", {}).items()}
        values["trades"] = [Trade(**v) for v in values.get("trades", [])]
        broker = cls(**values)
        for symbol, pos in broker.positions.items():
            if symbol != pos.symbol or pos.owner != "daily_trend" or type(pos.qty) is not int or pos.qty <= 0:
                raise ValueError("Invalid persisted position ownership/quantity")
            if not 0 < pos.stop_price < pos.entry_price < pos.target_price or pos.current_price <= 0 or pos.entry_fee < 0:
                raise ValueError("Invalid persisted position prices")
        for key, order in broker.orders.items():
            if key != order.id or order.status not in {"pending", "filled", "rejected", "canceled", "expired"}:
                raise ValueError("Invalid persisted order")
            if order.action not in {"BUY", "SELL"} or type(order.qty) is not int or order.qty <= 0:
                raise ValueError("Invalid persisted order quantity/action")
            if order.expires_at <= order.submitted_at or order.limit_price <= 0:
                raise ValueError("Invalid persisted order time/price")
        if len({o.symbol for o in broker.pending()}) != len(broker.pending()) or broker.reserved_cash() > broker.cash + 1e-8:
            raise ValueError("Invalid persisted cash reservations")
        # Serializing with allow_nan=False rejects non-finite nested values too.
        import json
        json.dumps(broker.to_dict(), allow_nan=False)
        return broker
