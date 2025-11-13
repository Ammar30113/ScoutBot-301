# Market Data Proxy

FastAPI microservice that proxies price lookups through Massive.com, Yahoo Finance, and AlphaVantage in sequence.

## Features
- GET `/price/{symbol}` returns `{symbol, price, source}`
- GET `/health` returns `{ "status": "ok" }` for health checks
- Fallback chain: Massive REST → Yahoo Finance → AlphaVantage
- Simple retry/backoff and logging wrappers

## Requirements
```
pip install -r requirements.txt
```

## Environment Variables (TODO: configure in Railway)
- `MASSIVE_API_KEY` – Massive REST token
- `ALPHAVANTAGE_API_KEY` (or `ALPHA_VANTAGE_KEY`) – AlphaVantage token

## Fallback Flow
1. Attempt Massive REST (requires `MASSIVE_API_KEY`).
2. If Massive fails/timeouts/returns empty, fetch from Yahoo Finance via `yfinance`.
3. If Yahoo fails, call AlphaVantage using `ALPHAVANTAGE_API_KEY`.
4. Response body always includes the data source (`massive`, `yahoo`, or `alpha`).

## Deploying to Railway
1. Create a new Railway service and set the root directory to `market_data_proxy/`.
2. Railway can build using the included `Dockerfile` (Python 3.11) that installs `requirements.txt` and starts uvicorn.
3. Configure env vars (`MASSIVE_API_KEY`, `ALPHAVANTAGE_API_KEY`).
4. The runtime command (`uvicorn app:app --host 0.0.0.0 --port 8000`) is defined in the Dockerfile/Procfile.

## Run locally
```
uvicorn app:app --host 0.0.0.0 --port 8000
```

## Example Request
```
curl http://localhost:8000/price/AAPL
# => {"symbol":"AAPL","price":123.45,"source":"massive"}
```

Deploy the folder independently (e.g., Railway service) so it stays isolated from the trading bot codebase.
