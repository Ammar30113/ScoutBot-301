# Microcap Scout Bot - ML + Multi-Provider Trading Engine

This repository contains a from-scratch rewrite of the Microcap Scout Bot. The system uses a multi-provider market-data stack, an intraday ML classifier, and a signal router tuned for small-cap momentum workflows.

## Highlights
- **Market data router** prioritizes Alpaca -> TwelveData -> AlphaVantage for prices/intraday; daily bars use TwelveData/AlphaVantage/Marketstack with caching + rate-limit backoff.
- **Universe engine** loads CSV candidates (Russell3000 + fallback), filters by liquidity, ATR%, price, and market cap, with optional partial fundamentals/ATR.
- **ML classifier** (XGBoost) saved at `models/momentum_sentiment_model.pkl` predicts next-bar upside from 5-minute features (RSI, MACD, VWAP diff, slope, volume ratio, ATR, ATR-band position).
- **Strategies**: 5-minute ORB (morning only), momentum breakout, reversal; router blends ML prob, momentum rank, sentiment, and P&L penalty.
- **Trader engine**: DAILY_BUDGET allocations, caps via `MAX_POSITIONS`/`MAX_POSITION_SIZE`, Alpaca bracket orders, time-stop + technical exits, crash mode triggered on SPY 5-min drop >= 1%.

## Repository Layout
```
ScoutBot-301/
|-- core/                # configuration, logging, scheduler utilities
|-- data/                # market-data providers + price router
|-- universe/            # universe building via liquidity/vol/market-cap filters + CSV fallback
|-- strategy/            # ML classifier + trading strategies + signal router
|-- trader/              # allocation, risk, order execution, and portfolio state
|-- models/              # auto-trained XGBoost model (momentum_sentiment_model.pkl)
|-- main.py              # orchestrates the full pipeline + scheduler
`-- requirements.txt
```

## Environment Variables
Set the following variables inside Railway (or a local `.env` file - the project loads them via `python-dotenv`):

| Variable | Description |
|----------|-------------|
| `APCA_API_KEY_ID` / `ALPACA_API_KEY` | Alpaca trading/data key |
| `APCA_API_SECRET_KEY` / `ALPACA_API_SECRET` | Alpaca secret |
| `ALPACA_API_BASE_URL` | Default `https://paper-api.alpaca.markets` |
| `ALPACA_API_DATA_URL` | Default `https://data.alpaca.markets/v2` |
| `MODE` | Trading mode (`paper` or `live`, default `paper`) |
| `ALLOW_LIVE_TRADING` | Explicitly enable live trading (default `false`) |
| `TWELVEDATA_API_KEY` | Optional fallback data |
| `ALPHAVANTAGE_API_KEY` | Optional fallback data |
| `MARKETSTACK_API_KEY` | Optional Marketstack EOD daily bars |
| `MARKETSTACK_CACHE_TTL` | Marketstack daily cache TTL seconds (default `86400`) |
| `OPENAI_API_KEY` | Required for GPT sentiment engine |
| `OPENAI_MODEL` | Primary model for sentiment (default `gpt-3.5-turbo-16k`) |
| `USE_SENTIMENT` | Toggle sentiment system (default `true`) |
| `USE_TWITTER_NEWS` | Toggle Twitter headlines in sentiment (default `false`) |
| `TWITTER_BEARER_TOKEN` | Required if `USE_TWITTER_NEWS=true` |
| `TWITTER_ALLOWED_ACCOUNTS` | Comma-separated handles to scan (defaults in `core/config.py`) |
| `TWITTER_MAX_POSTS_PER_DAY` | Daily tweet budget (default `3`) |
| `TWITTER_TWEETS_PER_ACCOUNT` | Max tweets per account per day (default `1`) |
| `SENTIMENT_CACHE_TTL` | Sentiment cache TTL seconds (default `300`) |
| `UNIVERSE_FALLBACK_CSV` | CSV fallback path (default `universe/fallback_universe.csv`) |
| `MIN_DOLLAR_VOLUME` | Min avg daily dollar volume (default `8000000`) |
| `MIN_VOLUME_HISTORY_DAYS` | Lookback days for dollar-volume filter (default `3`) |
| `MIN_MKT_CAP` | Min market cap filter (default `300000000`) |
| `MAX_MKT_CAP` | Max market cap filter (default `5000000000`) |
| `MIN_PRICE` | Min price filter (default `2`) |
| `MAX_PRICE` | Max price filter (default `80`) |
| `MAX_UNIVERSE_SIZE` | Max symbols returned (default `50`) |
| `ALLOW_PARTIAL_FUNDAMENTALS` | Allow symbols with missing market cap (default `true`) |
| `ALLOW_PARTIAL_ATR` | Allow symbols with missing ATR (default `true`) |
| `DAILY_BUDGET_USD` | Per-cycle allocation budget (default `10000`) |
| `MAX_POSITIONS` | Max open positions (default `5`) |
| `MAX_POSITION_SIZE` | Max notional per position (defaults to DAILY_BUDGET/3) |
| `SCHEDULER_INTERVAL_SECONDS` | Re-run cadence (default `900`) |
| `PORTFOLIO_STATE_PATH` | JSON state path (default `data/portfolio_state.json`) |
| `CACHE_TTL` | Price cache TTL seconds (default `900`) |
| `INTRADAY_STALE_SECONDS` | Max intraday bar staleness in seconds (default `900`) |
| `DAILY_STALE_SECONDS` | Max daily bar staleness in seconds (default `432000`) |

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
- The ML model auto-trains on first run from recent intraday data and is cached at `models/momentum_sentiment_model.pkl`. If no market data is available, it falls back to a synthetic model; consider retraining offline for production.

## Sentiment (GPT-only)
- `OPENAI_API_KEY`: OpenAI project key with permission to call chat models.
- `OPENAI_MODEL`: Primary model for sentiment (default `gpt-3.5-turbo-16k`). Fallbacks: `gpt-4o-2024-05-13`, `gpt-5`.
- `USE_SENTIMENT`: If false, sentiment is skipped and treated as 0.
- `SENTIMENT_CACHE_TTL`: Per-symbol cache TTL (seconds). Default: `300`.
- `USE_TWITTER_NEWS` / `TWITTER_BEARER_TOKEN`: Optional Twitter headlines from whitelisted accounts.

## P/L Tracking
P/L, equity baseline, and entry timestamps are stored in `data/portfolio_state.json` (or `PORTFOLIO_STATE_PATH`) and updated each cycle when the Alpaca client is available.
