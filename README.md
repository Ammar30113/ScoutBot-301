# Microcap Scout Bot — ML + Multi-Provider Trading Engine

This repository contains a from-scratch rewrite of the Microcap Scout Bot. The new system discards the legacy dependencies and introduces an AI-driven, multi-provider market data and trading stack tuned for Railway deployments.

## Highlights
- **Market data router** prioritizes Alpaca → TwelveData → AlphaVantage with automatic failover.
- **Universe engine** expands ETF constituents (IWM, IWC, SMLF, VTWO, URTY), filters on market-cap, price, and liquidity, and can fall back to a bundled CSV snapshot.
- **ML classifier** (XGBoost) scores upside probability using momentum, volatility, sentiment, liquidity, and ETF-relative strength inputs backed by the bundled `models/microcap_model.pkl` file.
- **Strategies**: momentum breakout, mean-reversion snapback, and ETF/semiconductor arbitrage pairs, all merged via a signal router that enforces ATR-based take-profit/stop-loss targets.
- **Trader engine**: allocation, risk limits (max 10% position / 3% daily loss), Alpaca bracket orders, and persisted portfolio state.

## Repository Layout
```
microcap-scout-bot/
├── core/                # configuration, logging, scheduler utilities
├── data/                # market-data providers + price router
├── universe/            # ETF expansion, CSV fallback, and microcap filtering
├── strategy/            # ML classifier + trading strategies + signal router
├── trader/              # allocation, risk, order execution, and portfolio state
├── models/              # microcap_model.pkl placeholder (trained on mock data)
├── main.py              # orchestrates the full pipeline + scheduler
└── requirements.txt
```

## Environment Variables
Set the following variables inside Railway (or a local `.env` file – the project loads them via `python-dotenv`):

| Variable | Description |
|----------|-------------|
| `APCA_API_KEY_ID` / `ALPACA_API_KEY` | Alpaca trading/data key |
| `APCA_API_SECRET_KEY` / `ALPACA_API_SECRET` | Alpaca secret |
| `ALPACA_API_BASE_URL` | Default `https://paper-api.alpaca.markets` |
| `ALPACA_API_DATA_URL` | Default `https://data.alpaca.markets/v2` |
| `TWELVEDATA_API_KEY` | Optional fallback data |
| `ALPHAVANTAGE_API_KEY` | Optional fallback data |
| `OPENAI_API_KEY` | Required for GPT sentiment engine |
| `OPENAI_MODEL` | Optional model override for sentiment (default `gpt-3.5-turbo-0125`) |
| `USE_SENTIMENT` | Toggle sentiment system (default `true`) |
| `SENTIMENT_CACHE_TTL` | Sentiment cache TTL seconds (default `300`) |
| `USE_FINNHUB` | Legacy toggle (ignored by current sentiment engine) |
| `MICROCAP_ETFS` | Comma-separated ETF tickers (default `IWM,IWC,SMLF,VTWO,URTY`) |
| `INITIAL_EQUITY` | Portfolio equity baseline (default 100000) |
| `MAX_POSITION_PCT` | Position cap per trade (default 0.10) |
| `MAX_DAILY_LOSS_PCT` | Risk guardrails (default 0.03) |
| `SCHEDULER_INTERVAL_SECONDS` | Re-run cadence (default 900) |

## Running Locally
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python main.py
```

## Railway Deployment
1. Attach this repo to Railway and select the Python/Docker buildpack.
2. Paste the required environment variables in the Railway dashboard (Bulk Edit recommended).
3. Railway executes `python main.py` which boots the scheduler, builds the universe, generates ML signals, and routes orders through Alpaca.

## Notes
- The bundled ML model ships as a placeholder trained on synthetic data. For production, retrain `models/microcap_model.pkl` with historical features + outcomes.
- The ETF arbitrage engine currently tracks IWM/URTY, AMD/SMH, and NVDA/SOXX. Extend `strategy/etf_arbitrage.py` if additional pairs are required.
