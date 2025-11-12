# Microcap Scout Bot (Clean FastAPI Rebuild)

Microcap Scout Bot is a FastAPI-based backend that aggregates data from Finviz, Massive, Finnhub, and Alpaca to power microcap-focused scouting tools. This repo is a clean rebuild of the previous app, aligned with the confirmed architecture and deployment targets (Railway + Docker).

## Features
- FastAPI app with `uvicorn main:app --host 0.0.0.0 --port ${PORT:-8000}` (Railway/Heroku set `PORT` automatically).
- `/` health and `/products.json` catalog endpoints stay stable while exposing the latest universe + trade log.
- Hybrid scanner merges Finviz microcaps with filtered large caps (AAPL, NVDA, TSLA, etc.) and runs daily.
- Trading engine uses Alpaca bracket orders (8% TP / 4% SL) with utilization guardrails, Massive-powered pricing, and a $10k configurable daily budget allocator plus end-of-day summaries.
- Service layer integrates Finviz, Massive, Finnhub, and Alpaca, backed by in-memory caching and throttling.
- Environment-driven configuration with `.env.example`, Dockerfile + Procfile for Railway.

## Project Structure
```
.
├── Dockerfile
├── Procfile
├── README.md
├── main.py
├── requirements.txt
├── routes/
│   ├── __init__.py
│   ├── health.py
│   └── products.py
├── services/
│   ├── __init__.py
│   ├── alpaca.py
│   ├── finnhub_client.py
│   ├── finviz.py
│   ├── market_data.py
│   ├── massive_client.py
│   └── trading.py
├── utils/
│   ├── __init__.py
│   ├── http_client.py
│   ├── logger.py
│   └── settings.py
└── .env.example
```

## Getting Started

### 1. Requirements
- Python 3.11+
- pip / venv (recommended)

### 2. Install dependencies
```bash
python -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

### 3. Configure environment variables
Copy the sample file and edit with real API credentials:
```bash
cp .env.example .env
# edit .env
```

Required keys:
- `FINVIZ_TOKEN` (Finviz screener)
- `MASSIVE_API_KEY`
- `FINNHUB_API_KEY`
- `APCA_API_KEY_ID`, `APCA_API_SECRET_KEY`
- `APCA_API_BASE_URL`, `APCA_API_DATA_URL` (leave off `/v2`; the SDK appends it automatically)
- `TRADING_BUDGET` (e.g., `1000`) and `DAILY_BUDGET_USD` (defaults to `10000`)

Optional overrides: `ALPACA_BASE_URL`, `DEFAULT_SYMBOL`, `ENVIRONMENT`, `PORT`, `MODE`.

### 4. Run the app locally
```bash
uvicorn main:app --host 0.0.0.0 --port ${PORT:-8000}
```

Health check:
```bash
curl http://127.0.0.1:8000/
```

Product schema:
```bash
curl http://127.0.0.1:8000/products.json
```

## Docker Workflow
```bash
docker build -t microcap-scout-bot .
docker run --env-file .env -p 8000:8000 microcap-scout-bot
```

## Railway Deployment
1. Create a new Railway project and attach this repo.
2. In the Railway dashboard, set the environment variables from `.env.example`.
3. Railway detects the `Dockerfile` or `Procfile`. The Procfile command (`sh -c 'uvicorn main:app --host 0.0.0.0 --port ${PORT:-8000}'`) respects Railway’s `PORT` env or defaults to 8000 locally.
4. Deploy. The health route (`/`) is ideal for Railway health checks.

## API Overview
| Method | Route           | Description                                        |
|--------|-----------------|----------------------------------------------------|
| GET    | `/`             | Health/status ping                                 |
| HEAD   | `/`             | Head-only health                                   |
| GET    | `/products.json`| Product catalog + blended data + universe + trades |

## Next Steps
- Persist trade logs/universe snapshots in a backing store (Redis/Postgres) for inspection.
- Run the hybrid strategy on a scheduled worker (Celery/Temporal) instead of app startup.
- Wire the trading signals into your Discord/Telegram bots for real-time pushes.
