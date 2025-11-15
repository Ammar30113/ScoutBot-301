import logging

logger = logging.getLogger(__name__)

STATIC_UNIVERSE = [
    "AAPL","MSFT","NVDA","AMD","GOOG","META","AMZN","TSLA",
    "JPM","V","MA","BRK.B","JNJ","HD","NFLX","AVGO",
    "COST","PEP","KO","MCD","XOM","CVX","TSM",
    "QQQ","SPY","IWM","SMH","SOXX"
]

def get_universe():
    logger.info(f"Loaded static liquid universe ({len(STATIC_UNIVERSE)} symbols)")
    return STATIC_UNIVERSE
