import logging
from universe.universe_builder import get_universe
from strategy.ml_classifier import generate_predictions
from strategy.signal_router import route_signals
from trader.allocation import allocate_positions
from trader.order_executor import execute_trades

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
logger = logging.getLogger(__name__)

def microcap_cycle():
    logger.info("=== Starting Trading Cycle ===")

    # 1. Load universe
    universe = get_universe()

    # 2. ML predictions
    predictions = generate_predictions(universe)

    # 3. Select top performers
    selected = route_signals(predictions)

    # 4. Allocate $10,000 across them
    allocation = allocate_positions(selected)

    # 5. Execute trades via Alpaca bracket orders
    execute_trades(allocation)

    logger.info("=== Cycle Complete ===")


if __name__ == "__main__":
    microcap_cycle()
