# Market Data Proxy

FastAPI microservice that proxies price lookups through Massive.com, Yahoo Finance, and AlphaVantage in sequence.

## Features
- GET `/price/{symbol}` returns `{symbol, price, source}`
- Primary data source: Massive REST (requires `MASSIVE_API_KEY`)
- Fallbacks: Yahoo Finance (`yfinance`), AlphaVantage (`ALPHAVANTAGE_API_KEY`)
- Simple retry/backoff and logging wrappers

## Requirements
```
pip install -r requirements.txt
```

## Environment Variables
- `MASSIVE_API_KEY` – Massive REST token (**TODO: set in deployment**)
- `ALPHAVANTAGE_API_KEY` – AlphaVantage token (**TODO: set in deployment**)

## Run locally
```
uvicorn app:app --host 0.0.0.0 --port 8000
```

Deploy the folder independently (e.g., Railway service) so it stays isolated from the trading bot codebase.
