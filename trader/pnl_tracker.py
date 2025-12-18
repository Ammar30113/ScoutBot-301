from datetime import datetime, timezone

from data.portfolio_state import load_state, save_state


def update_daily_pnl(alpaca_client):
    state = load_state()

    if alpaca_client is None:
        return
    try:
        account = alpaca_client.get_account()
        positions = alpaca_client.get_all_positions()
    except Exception:
        return

    equity = float(account.equity)
    unrealized = sum(float(p.unrealized_pl) for p in positions)
    realized_raw = getattr(account, "realized_pl", 0.0)
    try:
        realized = float(realized_raw)
    except Exception:
        realized = 0.0

    today = datetime.now(timezone.utc).date().isoformat()
    if state.day_start_date != today or state.day_start_equity <= 0:
        state.day_start_date = today
        state.day_start_equity = equity

    baseline = state.day_start_equity or equity
    if baseline <= 0:
        baseline = equity if equity > 0 else 1.0

    realized_pct = realized / baseline
    unrealized_pct = unrealized / baseline
    equity_return_pct = (equity - baseline) / baseline

    state.equity = equity
    state.realized_pnl = realized
    state.unrealized_pnl = unrealized
    state.realized_pct = realized_pct
    state.unrealized_pct = unrealized_pct
    state.equity_return_pct = equity_return_pct

    state.prior_equity = equity

    save_state(state)
    return state
