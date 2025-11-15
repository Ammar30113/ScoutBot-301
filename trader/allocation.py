import logging
import os

logger = logging.getLogger(__name__)

DAILY_BUDGET = float(os.getenv("DAILY_BUDGET_USD", 10000))

def allocate_positions(selected):
    if not selected:
        logger.warning("No selected symbols to allocate capital")
        return {}

    per_asset = DAILY_BUDGET / len(selected)

    logger.info(
        f"Allocating ${per_asset:.2f} per asset "
        f"across {len(selected)} positions"
    )

    return {symbol: per_asset for symbol in selected}
