import os
import json
import datetime
import logging
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import GetPortfolioHistoryRequest

from core.config import ALPACA_API_KEY, ALPACA_API_SECRET, MODE

log = logging.getLogger(__name__)

client = TradingClient(ALPACA_API_KEY, ALPACA_API_SECRET, paper=(MODE == "paper"))


STATE_FILE = "data/portfolio_state.json"
PNL_DIR = "data/pnl"


def _ensure_dirs():
    if not os.path.exists("data"):
        os.makedirs("data")
    if not os.path.exists(PNL_DIR):
        os.makedirs(PNL_DIR)


def _load_state():
    if not os.path.exists(STATE_FILE):
        return {}
    try:
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    except:
        return {}


def _save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def _write_daily_pnl(day: str, data: dict):
    path = os.path.join(PNL_DIR, f"{day}.json")
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    log.info(f"pnl_tracker | Wrote daily P/L log to {path}")


def compute_unrealized_and_equity():
    """
    Return:
      unrealized_pnl
      equity_value
      positions list
    """
    try:
        positions = client.get_all_positions()

        unrealized = 0.0
        value = 0.0

        for p in positions:
            qty = float(p.qty)
            price = float(p.current_price)
            market_value = qty * price
            value += market_value

            unrealized += float(p.unrealized_pl)

        account = client.get_account()
        cash = float(account.cash)

        equity = cash + value

        return unrealized, equity, positions
    except Exception as e:
        log.error(f"pnl_tracker | Failed to compute unrealized/equity: {e}")
        return 0.0, 0.0, []
    

def compute_realized_today():
    """
    Uses portfolio history API to compute today's realized P/L.
    """
    try:
        today = datetime.date.today()
        req = GetPortfolioHistoryRequest(period="1D", timeframe="1D")
        hist = client.get_portfolio_history(req)

        if hist.profit_loss:
            return float(hist.profit_loss[-1])
        return 0.0
    except Exception as e:
        log.error(f"pnl_tracker | Failed to compute realized P/L: {e}")
        return 0.0


def update_daily_pl():
    """
    Runs ONCE per day.
    Called by main.py after market close, or at next morning startup.
    """
    _ensure_dirs()

    today_str = str(datetime.date.today())
    state = _load_state()

    last_run = state.get("last_pnl_day")
    if last_run == today_str:
        # Already logged today
        return

    unrealized, equity, positions = compute_unrealized_and_equity()
    realized = compute_realized_today()

    snapshot = {
        "date": today_str,
        "realized_pnl": realized,
        "unrealized_pnl": unrealized,
        "equity": equity,
        "open_positions": [
            {
                "symbol": p.symbol,
                "qty": p.qty,
                "market_value": p.market_value,
                "unrealized_pl": p.unrealized_pl
            }
            for p in positions
        ],
    }

    _write_daily_pnl(today_str, snapshot)

    state["last_pnl_day"] = today_str
    _save_state(state)

    log.info(f"pnl_tracker | Daily P/L computed: realized={realized}, unrealized={unrealized}, equity={equity}")
