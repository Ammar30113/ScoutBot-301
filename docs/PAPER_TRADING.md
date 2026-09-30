# Railway paper worker setup

This is ScoutBot's own simulated account. It requires **market data**, not an Alpaca or IBKR trading account. No real-money execution is available in this version.

## Set up the service

1. Deploy the repository using its Dockerfile as a persistent worker. Start command: `python main.py`. Use one replica, disable application sleeping, and do not configure an HTTP health path or public domain for this worker.
2. Attach a Railway volume mounted at `/data`. Set `PAPER_STATE_PATH=/data/paper.sqlite3`. Railway should supply `RAILWAY_VOLUME_MOUNT_PATH`; preflight requires the state file to be within that actual mount. Set `PAPER_ACCOUNT_ID=scout-paper-v1` and keep both stable across deployments.
3. Copy nonsecret values from `.env.example` into the service, with the `/data` path override. Choose one provider. Start with `PAPER_DATA_PROVIDER=twelvedata` and the TwelveData key you already have, subject to your plan's daily-history access. If that provider cannot supply the required history, use Marketstack with a separate new run and `PAPER_DATA_PROVIDER=marketstack`.
4. Confirm `MODE=paper`, `BROKER=simulated`, `ALLOW_LIVE_TRADING=false`, and `DRY_RUN=false`. In this version the last setting enables simulated orders only.
5. Run `python -m scripts.preflight`, then `python main.py --once` in the same environment with the mounted volume. Avoid running a manual cycle concurrently with the worker. Check the first report, then run the persistent worker.
6. Verify `python -m scripts.paper_health` after startup and after a subsequent session. Restart/redeploy and verify cash, existing orders, positions, and event history survive. Configure your external heartbeat service and test an alert by stopping the worker temporarily.

The example's USD 10,000 is **virtual test cash**. Your CAD 60,000 salary does not determine a suitable real trading allocation. No transfer from Wealthsimple or IBKR is needed.

## Migrate the variables you supplied

Only variable names were available for this review; masked values and current Railway deployment behavior were not inspected. Shared variables showing “0 of 6 in use” are not inherited automatically. A same-named service variable can still exist independently.

| Existing variable | Action for this worker |
| --- | --- |
| `MODE` | Set `paper`. |
| `DRY_RUN` | Set `false` for simulated orders; `true` for new decisions only. Existing simulated positions/orders still run. |
| `DAILY_BUDGET_USD` | Example `5000`; total position plus pending-buy notional cap, not daily fresh capital. |
| `MAX_POSITION_SIZE` | Example `1000`; USD notional per position. |
| `MAX_POSITIONS` | Set `2`. |
| `MAX_UNIVERSE_SIZE` | Set `3` for the three-symbol example allowlist. Must not be a range such as `8-12`. |
| `TWELVEDATA_API_KEY` | Keep if selecting TwelveData. Enter privately in Railway. |
| `MARKETSTACK_API_KEY` | Keep only if selecting Marketstack; HTTPS v2 endpoint is used. |
| `ALLOW_ALPACA_DAILY` | Set `false` for TwelveData/Marketstack. |
| `APCA_API_KEY_ID`, `APCA_API_SECRET_KEY` | Remove from this service when using TwelveData/Marketstack. They are only optional Alpaca market-data credentials now. |
| `APCA_API_BASE_URL` (shared), `ALPACA_API_BASE_URL` | Remove from this service; no Alpaca order API is used. |
| `ALLOW_FALLBACK_ML`, `TRAIN_ML_ON_STARTUP` | Set `false`. |
| `REQUIRE_CRASH_DATA`, `UNIVERSE_FALLBACK_ONLY` | Set `false`; explicit daily strategy/allowlist replaces these paths. |
| `USE_SENTIMENT`, `USE_TWITTER_NEWS` | Set `false`. |
| `STRIP_RATE_LIMITED_KEYS` | Set `false`; preflight rejects permanent provider disabling. |
| `SKIP_DAILY_ON_RATE_LIMIT` | Remove; provider cooldown applies without switching providers. |
| `INTRADAY_STALE_SECONDS` | Remove; the worker uses daily session freshness. |
| `SENTIMENT_CACHE_TTL`, `OPENAI_API_KEY`, `OPENAI_MODEL`, `TWITTER_BEARER_TOKEN` | Remove from this worker; no LLM or Twitter requests are made. |
| `USE_FINNHUB`, `FINVIZ_TOKEN`, `GEMINI_API_KEY`, `URL` | Remove from this service; unused. Do not attach the unused shared variables. |

Add the variables in `.env.example` that are not present yet, particularly:

```dotenv
BROKER=simulated
STRATEGY=daily_trend
ALLOW_LIVE_TRADING=false
PAPER_ACCOUNT_ID=scout-paper-v1
PAPER_STATE_PATH=/data/paper.sqlite3
PAPER_INITIAL_CASH_USD=10000
PAPER_SYMBOLS=SPY,QQQ,IWM
PAPER_DATA_PROVIDER=twelvedata
MAX_POSITION_PCT=0.10
MAX_GROSS_EXPOSURE_PCT=0.50
MAX_RISK_PCT=0.005
MAX_PORTFOLIO_RISK_PCT=0.01
MAX_DAILY_LOSS_PCT=0.01
MAX_DRAWDOWN_PCT=0.05
PAPER_SLIPPAGE_BPS=10
PAPER_FEE_BPS=5
PAPER_MIN_FEE_USD=0.35
TREND_DAYS=200
DATA_DELAY_SECONDS=900
SCHEDULER_INTERVAL_SECONDS=900
CACHE_TTL=60
MARKETSTACK_CACHE_TTL=900
ALLOW_SYNTHETIC_ML=false
UNIVERSE_ALLOW_UNFILTERED_FALLBACK=false
```

Costs are simulator assumptions, not verified IBKR rates. Do not change them mid-account. A provider or capital/risk/strategy change requires a new state path and account ID so records remain comparable. Do not point the new worker at the old `portfolio_state.json`; no brokerage holdings are imported.

## Expected daily behavior

The loop wakes every 15 minutes. Fresh daily history is cached until the next expected completed session, so a healthy run generally retrieves history once per symbol per session; restarts, stale data, or failed requests can consume additional quota. No intraday feed is needed. New entries are reviewed weekly; management runs when each completed daily bar arrives.

A Friday close signal normally queues a Monday opening simulation (Tuesday after a Monday holiday), whose result becomes visible after that session closes and data arrives. A Monday morning startup after the open cannot backfill that morning's entry. Weekends and exchange holidays retain the last completed session without falsely labeling it stale.

Read `/data/paper.status.json` and the worker logs:

| Result | Meaning / response |
| --- | --- |
| `ok`, no new decisions | Same session already evaluated, no signal, or risk/cash limits permit no whole shares. Inspect earlier SQLite decision events. |
| `insufficient_history` | Fewer than 201 bars; confirm provider entitlement, symbol and requested history. Do not lower history merely to force a trade. |
| `stale_daily_data` | Latest expected completed session missing. Check provider status, publication delay, quota and plan. |
| `data_unavailable:...` | Provider returned no usable data or validation failed. Logs redact URLs/secrets; inspect provider dashboard privately. |
| `history_revised...` / `corporate_action...` / `missing_daily_sessions` | Data integrity issue. New entries are blocked; affected positions cannot be modeled safely until resolved. Preserve/backup the account before correction or a fresh run. |
| `halted` | Daily loss or persistent drawdown threshold reached. Existing protection continues with valid data. Investigate; do not delete state to resume. |
| Configuration/account mismatch | Restore the prior settings or begin a deliberately separate run. |
| Heartbeat stale | Worker stopped, hung or failed. Inspect Railway and volume; do not assume the last `ok` report is current. |

`paper_health` is a CLI read of the report, not an HTTP server. The optional HTTPS heartbeat is sent only after healthy cycles. External monitoring configuration, notification delivery, Railway volume/restart behavior, and provider entitlements remain deployment acceptance tasks.

## Before IBKR

Retain the Wealthsimple long-term accounts separately. The next integration needs an explicit IBKR paper account allowlist, contract resolution, broker order IDs and lifecycle reconciliation, broker-held protective orders, connection recovery, and verified account/product permissions. Nothing in this repository update authorizes or enables real trading. Strategy evidence must be assessed separately from software reliability.
