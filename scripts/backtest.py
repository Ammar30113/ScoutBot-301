"""Replay daily OHLCV CSV files through the paper engine. No brokerage or network access."""
import argparse
from dataclasses import asdict, replace
import hashlib
import json
from pathlib import Path

from backtest.data_feed import BarDataFeed, load_bars_directory
from backtest.runner import BacktestRunner
from core.config import get_settings
from core.io import atomic_json


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data", type=Path, help="Directory of SYMBOL.csv files (daily bars)")
    parser.add_argument("--output", type=Path, default=Path("artifacts/backtest.json"))
    parser.add_argument("--cash", type=float, help="Simulated USD cash; defaults to PAPER_INITIAL_CASH_USD")
    args = parser.parse_args()
    settings = get_settings()
    if args.cash is not None:
        settings = replace(settings, paper_initial_cash=args.cash)
    result = BacktestRunner(BarDataFeed(load_bars_directory(args.data)), settings=settings).run()
    manifest = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(args.data.glob("*.csv"))}
    atomic_json(args.output, {"summary": result.summary(), "input_sha256": manifest,
                             "strategy": "daily_trend", "currency": "USD", "equity_curve": result.equity_curve,
                             "trades": [asdict(t) for t in result.trades], "final_state": result.final_state,
                             "reports": result.reports,
                             "limitations": "OHLC simulation; no dividends, taxes, FX, or liquidity/queue model. Open positions marked at last close."})
    print(json.dumps(result.summary(), allow_nan=False))


if __name__ == "__main__":
    main()
