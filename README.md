# Microcap Scout Bot — ML + Multi-Provider Trading Engine

This repository contains a from-scratch rewrite of the Microcap Scout Bot. The new system discards the legacy dependencies and introduces an AI-driven, multi-provider market data and trading stack tuned for Railway deployments.

## Highlights
- **Market data router** prioritizes Alpaca → TwelveData → AlphaVantage with automatic failover.
- **Universe engine** pulls S&P1500/Russell3000 candidates, filters by liquidity, volatility, market-cap, and price, and falls back to the bundled CSV snapshot when data is missing.
- **ML classifier** (XGBoost) scores upside probability using momentum, volatility, sentiment, and liquidity inputs backed by the bundled `models/microcap_model.pkl` file.
- **Strategies**: momentum breakout and mean-reversion snapback, merged via a signal router that enforces ATR-based take-profit/stop-loss targets.
- **Trader engine**: allocation, risk limits (max 10% position / 3% daily loss), Alpaca bracket orders, and persisted portfolio state.

## Repository Layout
```
microcap-scout-bot/
├── core/                # configuration, logging, scheduler utilities
├── data/                # market-data providers + price router
├── universe/            # universe building via liquidity/vol/market-cap filters + CSV fallback
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
| `OPENAI_MODEL` | Optional model override for sentiment (default `gpt-3.5-turbo-16k`) |
| `USE_SENTIMENT` | Toggle sentiment system (default `true`) |
| `SENTIMENT_CACHE_TTL` | Sentiment cache TTL seconds (default `300`) |
| `MIN_DOLLAR_VOLUME` | Min avg daily dollar volume last 10 days (default 8000000) |
| `MIN_MKT_CAP` | Min market cap filter (default 300000000) |
| `MAX_MKT_CAP` | Max market cap filter (default 5000000000) |
| `MIN_PRICE` | Min price filter (default 2) |
| `MAX_PRICE` | Max price filter (default 80) |
| `MAX_UNIVERSE_SIZE` | Max symbols returned (default 50) |
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

## Sentiment (GPT-only)
- `OPENAI_API_KEY`: OpenAI project key with permission to call chat models.
- `OPENAI_MODEL`: Primary model for sentiment. Default: `gpt-3.5-turbo-16k`. Allowed for this project: `gpt-3.5-turbo-16k`, `gpt-4o-2024-05-13`, `gpt-4.1-2025-04-14`, `gpt-5`.
- `USE_SENTIMENT`: If false, sentiment is skipped and treated as 0.
- `SENTIMENT_CACHE_TTL`: Per-symbol cache TTL (seconds). Default: `300`.

P/L Tracking (Hybrid)
---------------------
P/L is logged once per trading day into `data/pnl/YYYY-MM-DD.json`.

Env variables needed:
- `APCA_API_KEY_ID`
- `APCA_API_SECRET_KEY`
- `MODE` (`paper` or `live`)
