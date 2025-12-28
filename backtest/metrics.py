from __future__ import annotations

import math
from statistics import median
from typing import Dict, List, TYPE_CHECKING

if TYPE_CHECKING:
    from backtest.runner import BacktestResult

TRADING_DAY_SECONDS = 6.5 * 60 * 60


def _max_drawdown(equity_curve: List[Dict[str, float]]) -> float:
    peak = None
    max_dd = 0.0
    for point in equity_curve:
        equity = float(point.get("equity", 0.0))
        if equity <= 0:
            continue
        if peak is None or equity > peak:
            peak = equity
        if peak:
            dd = (equity - peak) / peak
            if dd < max_dd:
                max_dd = dd
    return max_dd


def _returns(equity_curve: List[Dict[str, float]]) -> List[float]:
    returns: List[float] = []
    for prev, cur in zip(equity_curve, equity_curve[1:]):
        prev_eq = float(prev.get("equity", 0.0))
        cur_eq = float(cur.get("equity", 0.0))
        if prev_eq > 0:
            returns.append((cur_eq - prev_eq) / prev_eq)
    return returns


def _annualization_factor(equity_curve: List[Dict[str, float]]) -> float:
    if len(equity_curve) < 2:
        return 1.0
    deltas = []
    for prev, cur in zip(equity_curve, equity_curve[1:]):
        try:
            delta = float(cur.get("timestamp", 0.0)) - float(prev.get("timestamp", 0.0))
        except (TypeError, ValueError):
            continue
        if delta > 0:
            deltas.append(delta)
    if not deltas:
        return 1.0
    step_seconds = median(deltas)
    if step_seconds <= 0:
        return 1.0
    periods_per_year = (252.0 * TRADING_DAY_SECONDS) / step_seconds
    return math.sqrt(max(periods_per_year, 1.0))


def summarize_backtest(result: BacktestResult) -> Dict[str, float]:
    trades = result.trades
    trade_count = len(trades)
    wins = [t for t in trades if t.pnl > 0]
    losses = [t for t in trades if t.pnl <= 0]

    win_rate = (len(wins) / trade_count) if trade_count else 0.0
    avg_win = (sum(t.pnl for t in wins) / len(wins)) if wins else 0.0
    avg_loss = (sum(t.pnl for t in losses) / len(losses)) if losses else 0.0
    gross_profit = sum(t.pnl for t in wins)
    gross_loss = sum(t.pnl for t in losses)
    profit_factor = (gross_profit / abs(gross_loss)) if gross_loss < 0 else (math.inf if gross_profit > 0 else 0.0)

    durations = [(t.exit_timestamp - t.entry_timestamp) / 60.0 for t in trades if t.exit_timestamp >= t.entry_timestamp]
    avg_hold_minutes = (sum(durations) / len(durations)) if durations else 0.0
    median_hold_minutes = median(durations) if durations else 0.0

    equity_curve = result.equity_curve or []
    max_drawdown = _max_drawdown(equity_curve)
    returns = _returns(equity_curve)
    mean_ret = sum(returns) / len(returns) if returns else 0.0
    variance = sum((r - mean_ret) ** 2 for r in returns) / (len(returns) - 1) if len(returns) > 1 else 0.0
    std_ret = math.sqrt(variance) if variance > 0 else 0.0
    sharpe = (mean_ret / std_ret) * _annualization_factor(equity_curve) if std_ret > 0 else 0.0

    return {
        "final_equity": float(result.final_equity),
        "total_return": float(result.total_return),
        "trade_count": int(trade_count),
        "win_rate": float(win_rate),
        "avg_win": float(avg_win),
        "avg_loss": float(avg_loss),
        "profit_factor": float(profit_factor),
        "max_drawdown_pct": float(max_drawdown),
        "sharpe": float(sharpe),
        "avg_hold_minutes": float(avg_hold_minutes),
        "median_hold_minutes": float(median_hold_minutes),
    }
