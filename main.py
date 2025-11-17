import logging

from universe.universe_builder import get_universe
from strategy.momentum import compute_momentum_scores
from strategy.signal_router import route_signals
from trader.allocation import allocate_positions
from trader.order_executor import execute_trades, close_position, list_positions
from trader import risk_model
from data.price_router import PriceRouter

logging.basicConfig(level=logging.INFO, format="%Y-%m-%d %H:%M:%S | %(levelname)s | %(name)s | %(message)s")
logger = logging.getLogger(__name__)
price_router = PriceRouter()


def microcap_cycle():
    logger.info("=== Starting Trading Cycle ===")
    universe = get_universe()

    momentum_ranked = compute_momentum_scores(universe)
    signals = route_signals([sym for sym, _ in momentum_ranked])

    allocations = allocate_positions(signals)

    # Enforce max position caps before submitting
    filtered_allocations = {}
    open_positions = list_positions()
    open_count = len(open_positions)
    for symbol, shares in allocations.items():
        try:
            price = price_router.get_price(symbol)
        except Exception as exc:  # pragma: no cover - network guard
            logger.warning("Skipping %s for risk check; price unavailable: %s", symbol, exc)
            continue
        notional = shares * price
        if risk_model.can_open_position(open_count + len(filtered_allocations), notional):
            filtered_allocations[symbol] = shares
        else:
            logger.info("Risk cap blocked %s (notional %.2f)", symbol, notional)

    execute_trades(filtered_allocations)

    # Exit checks for existing positions
    for pos in list_positions():
        try:
            current_price = float(pos.current_price)
            entry_price = float(pos.avg_entry_price)
        except Exception:
            continue
        position_payload = {
            "symbol": pos.symbol,
            "current_price": current_price,
            "entry_price": entry_price,
            "open_date": None,  # Alpaca positions do not expose open date directly
        }
        if risk_model.should_exit(position_payload):
            close_position(pos.symbol)

    logger.info("=== Cycle Complete ===")


if __name__ == "__main__":
    microcap_cycle()
