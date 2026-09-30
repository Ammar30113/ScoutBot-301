# ScoutBot-301

A daily, **simulated paper trading worker** for testing a transparent strategy with a fixed USD cash allocation. It does not connect to a brokerage account or send real orders. IBKR integration is a later, separate milestone. Wealthsimple TFSA/RRSP holdings remain outside this application.

This version replaces the unsafe Alpaca execution loop. The daily paper engine and historical replay now share sizing, orders, costs, stops, and account state. The earlier intraday/ML modules remain research utilities and cannot be selected by the worker. No profitable edge or passive income has been established.

## Run locally

Python 3.11 or 3.12 on Linux/macOS; the production image uses 3.12.

```sh
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements-paper.lock
cp .env.example .env
```

Set one market-data key in `.env`. The example selects TwelveData; change `PAPER_DATA_PROVIDER=marketstack` to use Marketstack instead. The provider must supply at least 201 consecutive completed daily sessions for every selected symbol. Key presence alone does not establish subscription entitlement or data freshness.

```sh
python -m scripts.preflight
python main.py --once
python -m scripts.paper_health
python main.py
```

`--once` exits 0 only for a healthy cycle, 1 for a data/risk/runtime problem, and 2 for invalid configuration. An empty signal list or unaffordable whole share is a normal result. `DRY_RUN=false` enables simulated fills; `DRY_RUN=true` records decisions without submitting new orders. Existing simulated orders and positions continue to be managed in either mode. A dry-run decision is not resubmitted for that same session when the flag changes.

Run the credential-free engineering smoke test:

```sh
python -m scripts.paper_smoke
```

It uses invented prices in a temporary database. Its result tests software behavior, not investment performance.

## Strategy and execution model

- Explicit allowlist, default SPY/QQQ/IWM. USD, whole shares, long only, cash only. These are test instruments, not individualized investment recommendations.
- After a completed daily close, buy candidates must be above their 200-session simple moving average and above their close 20 sessions earlier.
- Review new entries at the end of the last trading session of each week, targeting the next week's first session. Review existing holdings every daily session; a close below the moving average queues an exit.
- Signals queue orders for a **later session's open**. The entire daily bar must become available before the simulator records its modeled opening fill and protective exit. A worker starting after the intended session's open cannot invent a fill at that past open.
- Entry prices are bounded: reference close plus modeled slippage plus a 0.5% gap allowance. A higher opening price cancels the entry. Unfilled intents expire at the next session's close.
- Stops are 5% below the modeled fill and targets 10% above it. If both are touched in one OHLC bar, the stop wins. Downward opening gaps fill below the stop. Fees and slippage apply on both legs.
- The exchange calendar handles holidays, daylight saving time, and early closes. `DATA_DELAY_SECONDS` is a publication allowance after close; it does not certify a provider's delivery latency.

This is an OHLC simulation, **not IBKR paper execution**. It does not model intrabar sequencing, queue priority, actual liquidity, dividends, taxes, FX, or a complete corporate-action ledger. Large gaps, detected splits, revised historical prices, missing sessions, or insufficient recovery overlap stop that symbol's processing and block new entries. Inspect the error and preserve the account before starting a corrected run. A data failure cannot guarantee protective management while prices are unavailable.

## Cash, risk, and persistence

`PAPER_INITIAL_CASH_USD` creates virtual cash once. The example uses USD 10,000 only to exercise whole-share fills; it is not a recommendation to fund an account with that amount. Lower capital and risk limits can legitimately produce zero trades.

`DAILY_BUDGET_USD` is retained for migration but now means a **total deployed-notional cap**, including pending buys. It is not new money each day or a daily spending allowance. `MAX_POSITION_SIZE` is USD notional per holding, not a number of shares. Gross exposure, position count, available cash, pending reservations, per-trade stop risk, and total stop risk all constrain sizing. Profits do not automatically increase the initial risk allocation. Price appreciation can move existing holdings above an entry cap; the cap blocks additional exposure rather than forcing a rebalance.

Daily loss and peak drawdown limits latch a halt on new entries. Protective exits continue. The daily halt resets at the next processed session; the drawdown halt persists. Stop risk is an estimate, not a guaranteed maximum loss in a gap.

SQLite stores the account snapshot and append-only bar/decision/order/fill events in one transaction. Restart recovery reloads pending orders, cash, holdings, processed bars, and risk state. A process lock prevents overlapping cycles against the same database. Run **one replica with one persistent volume**. This is not a distributed execution system.

Changing capital, costs, strategy, symbols, provider, or risk parameters requires a new database/account, preserving the old run. Startup rejects unreadable state, account mismatches, cash that does not reconcile, and incompatible configuration; it never silently resets a damaged account. `PAPER_STATE_PATH` and `PAPER_ACCOUNT_ID` must remain stable across normal deployments. Do not delete state to clear a risk halt.

The status report sits beside the database: `data/paper.status.json` for the default path. It includes data freshness/errors, equity, cash, pending orders, holdings, P&L, risk halts, and new decisions. Healthy repeats can have no new decisions. The SQLite events retain earlier decisions and the bars used by the engine. An optional HTTPS `HEARTBEAT_URL` receives a ping only after healthy cycles; alert delivery must be configured and tested with your monitoring service.

Back up committed WAL contents consistently:

```sh
python -m scripts.paper_backup /path/to/new-backup.sqlite3
```

Stop the worker before a restore. Restore to a new path and verify the matching account/configuration before restarting. Copying only an active `.sqlite3` file can omit WAL transactions; use the backup command.

## Historical replay and validation

```sh
python -m scripts.backtest /path/to/daily-csv-directory --output artifacts/backtest.json
```

Supply one `SYMBOL.csv` per instrument with `timestamp,open,high,low,close,volume`; `date` or `datetime` can replace `timestamp`. Timestamps label exchange dates, not data availability. Numeric epoch seconds or milliseconds are accepted. Inputs must be daily bars with positive, finite prices, consistent OHLC, no duplicates, and consecutive exchange sessions. Use one consistent adjustment convention and identify corporate actions before evaluation.

The runner filters by historical availability, calls the same paper engine, starts a fresh account on each run, and never patches global routers or calls a provider. Output includes input-file hashes, trades, daily equity, diagnostics, and final state. Open holdings are marked at the last close and are not artificially liquidated. Sharpe uses 252 daily observations per year and a zero risk-free rate; profit factor is null when wins exist without losses. Data errors remain visible in per-session reports, so inspect them before interpreting metrics.

For development and regression tests:

```sh
pip install -r requirements-dev.lock
python -m pytest -q
ruff check .
```

Production dependencies are fully pinned separately from the larger research stack. The development lock pins tested packages; XGBoost additionally resolves its Linux-only NCCL dependency. CI tests Python 3.11/3.12 and builds/runs the minimal production image without networking. Do not enable the old ML or sentiment paths on Railway; those research strategies still need provenance, chronological holdouts, calibration, and cost-aware benchmarking.

## Deployment and acceptance

Follow [the Railway setup and variable migration](docs/PAPER_TRADING.md). There is no web server or public HTTP health endpoint; deploy as a worker. A repository test pass is not proof that the hosted worker is healthy.

Before calling a hosted run operational, verify: preflight, entitled fresh data, mounted state, successful scheduled cycles, a deployment/restart preserving that same state, and a deliberately missed heartbeat producing an alert. Before any IBKR integration, review a substantial forward paper record against a comparable buy-and-hold benchmark after realistic costs, including drawdowns and losing periods. Profit is not a deployment acceptance criterion or a promised outcome.

The [repository review](REPOSITORY_REVIEW.md) retains the original findings with their implementation disposition.
