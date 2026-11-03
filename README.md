# Binance Quant Arbitrage

Read-only Binance Spot LONG + USD-M perpetual SHORT funding-rate market intelligence engine.

## Current milestone

Phase 1 research foundation and Phase 2 public market-data scanner are present. The application discovers active USDT Spot/perpetual pairs, filters by 24-hour liquidity and spread, fetches bounded order-book depth and funding history, estimates costs and basis risk, ranks candidates, and persists snapshots/funding settlements to SQLite.

**No live trading:** there are no authenticated Binance endpoints, order-placement methods, or automated position operations. Estimates are not realized returns or guarantees.

## Install and run

Python 3.12+:

    python -m venv .venv
    # Windows: .venv\\Scripts\\activate
    # macOS/Linux: source .venv/bin/activate
    python -m pip install -e .[dev]
    uvicorn app.api.main:app --reload

Run tests:

    pytest

Copy .env.example to .env to customize scan cadence, liquidity thresholds, cost assumptions, ranking weights and SQLite retention. Do not put secrets in this project; public data does not require API keys.

## API

- GET /api/v1/health
- GET /api/v1/market/overview
- GET /api/v1/market/spot
- GET /api/v1/market/futures
- GET /api/v1/funding/current
- GET /api/v1/funding/history/{symbol}
- GET /api/v1/scanner/opportunities
- GET /api/v1/scanner/status
- GET /api/v1/market/orderbook/{symbol}

Interactive API documentation is available at /docs when the service is running.

## Architecture and limitations

See docs/PHASE_2_ARCHITECTURE.md and docs/PHASE_2_LIMITATIONS.md. The current scanner uses REST snapshots. A reconnecting combined-WebSocket helper is included but is not wired into scanner state. Detailed depth/history requests are bounded to avoid rate-limit storms.
