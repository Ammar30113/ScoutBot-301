import json
from pathlib import Path
from dataclasses import dataclass, asdict

STATE_PATH = Path("data/portfolio_state.json")


@dataclass
class PortfolioState:
    equity: float = 0.0
    prior_equity: float = 0.0
    realized_pnl: float = 0.0
    unrealized_pnl: float = 0.0
    realized_pct: float = 0.0
    unrealized_pct: float = 0.0
    equity_return_pct: float = 0.0

    def to_dict(self):
        return asdict(self)


def load_state() -> PortfolioState:
    if not STATE_PATH.exists():
        return PortfolioState()
    try:
        with open(STATE_PATH, "r") as f:
            data = json.load(f)
        return PortfolioState(**data)
    except Exception:
        return PortfolioState()


def save_state(state: PortfolioState):
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(STATE_PATH, "w") as f:
        json.dump(state.to_dict(), f, indent=2)
