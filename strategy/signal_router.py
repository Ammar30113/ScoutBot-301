import logging

logger = logging.getLogger(__name__)

def route_signals(predictions, keep_ratio=0.70):
    """
    predictions: [(symbol, score), ...]
    Keep top 70% performers based on ML score.
    """

    if not predictions:
        logger.warning("No ML predictions — cannot route signals")
        return []

    # Sort highest score → lowest
    sorted_preds = sorted(predictions, key=lambda x: x[1], reverse=True)
    keep_cnt = max(1, int(len(sorted_preds) * keep_ratio))

    selected = sorted_preds[:keep_cnt]

    logger.info(
        f"Selected {keep_cnt}/{len(sorted_preds)} symbols "
        f"({keep_ratio*100:.0f}% keep ratio)"
    )

    return [s for s, score in selected]
