# portfolio_state.py
# This file is intentionally light after refactor; pnl_tracker handles persistence.
# Keeping this for compatibility with older imports.

portfolio_state = {
    "equity": None,
    "daily_pnl": None,
    "updated_at": None,
}
