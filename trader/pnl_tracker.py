"""Legacy account analytics only; not used by the isolated paper worker."""
import logging
import math
from datetime import datetime, timezone

from data.portfolio_state import load_state, save_state

logger = logging.getLogger(__name__)


def update_daily_pnl(alpaca_client, *, realized_pnl: float | None = None):
    state = load_state()

    if alpaca_client is None:
        return
    try:
        account = alpaca_client.get_account()
        positions = alpaca_client.get_all_positions()
    except Exception as exc:
        logger.warning("Failed to fetch account/positions for P&L update: %s", exc)
        return

    equity = float(account.equity)
    unrealized = sum(float(p.unrealized_pl) for p in positions)
    if realized_pnl is None or not math.isfinite(realized_pnl):
        raise ValueError("Realized P&L must be supplied from a reconciled fill ledger")
    realized = float(realized_pnl)

    today = datetime.now(timezone.utc).date().isoformat()
    if state.day_start_date != today or state.day_start_equity <= 0:
        state.day_start_date = today
        state.day_start_equity = equity

    baseline = state.day_start_equity or equity
    if baseline <= 0:
        logger.warning("Day start equity is non-positive (%.2f); using current equity as baseline", baseline)
        baseline = equity if equity > 0 else 1.0

    realized_pct = realized / baseline if baseline > 0 else 0.0
    unrealized_pct = unrealized / baseline if baseline > 0 else 0.0
    equity_return_pct = (equity - baseline) / baseline if baseline > 0 else 0.0

    state.equity = equity
    state.realized_pnl = realized
    state.unrealized_pnl = unrealized
    state.realized_pct = realized_pct
    state.unrealized_pct = unrealized_pct
    state.equity_return_pct = equity_return_pct

    state.prior_equity = equity

    save_state(state)
    return state
