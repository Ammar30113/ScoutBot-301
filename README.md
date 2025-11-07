# Microcap Scout Bot (Clean FastAPI Rebuild)

Microcap Scout Bot is a FastAPI-based backend that aggregates data from Finviz, StockData.org, Alpaca, and `yfinance` to power microcap-focused scouting tools. This repo is a clean rebuild of the previous app, aligned with the confirmed architecture and deployment targets (Railway + Docker).

## Features
- FastAPI app with `uvicorn main:app --host 0.0.0.0 --port ${PORT:-8000}` (Railway/Heroku set `PORT` automatically).
- Health and catalog endpoints (`/` and `/products.json`).
- Service layer integrating Finviz, StockData.org, Alpaca, and `yfinance` (latest required version pinned at `0.2.52`).
- Environment-driven configuration with `.env.example`.
- Dockerfile + Procfile compatible with Railway.

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
│   ├── finviz.py
│   ├── market_data.py
│   └── stockdata.py
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
- `FINVIZ_API_KEY`
- `STOCKDATA_API_KEY`
- `ALPACA_API_KEY`
- `ALPACA_SECRET_KEY`

Optional overrides: `ALPACA_BASE_URL`, `DEFAULT_SYMBOL`, `ENVIRONMENT`, `PORT`.

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
| Method | Route           | Description                      |
|--------|-----------------|----------------------------------|
| GET    | `/`             | Health/status ping               |
| HEAD   | `/`             | Head-only health                 |
| GET    | `/products.json`| Product catalog + sample payload |

## Next Steps
- Add persistence/cache layers for expensive API calls.
- Expand `services` to handle auth refresh and retries.
- Wire the service into the existing front-end or bots.
