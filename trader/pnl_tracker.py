from data.portfolio_state import load_state, save_state, PortfolioState


def update_daily_pnl(alpaca_client):
    state = load_state()

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

    if state.prior_equity == 0:
        state.prior_equity = equity

    realized_pct = realized / state.prior_equity
    unrealized_pct = unrealized / state.prior_equity
    equity_return_pct = (equity - state.prior_equity) / state.prior_equity

    state.equity = equity
    state.realized_pnl = realized
    state.unrealized_pnl = unrealized
    state.realized_pct = realized_pct
    state.unrealized_pct = unrealized_pct
    state.equity_return_pct = equity_return_pct

    state.prior_equity = equity

    save_state(state)
    return state
