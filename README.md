# Binance Quant Arbitrage

Read-only Binance Spot LONG + USD-M perpetual SHORT funding-rate market intelligence engine.

## Current milestone

Phase 1 research foundation, Phase 2 market-data scanner and Phase 2.1 integrated market-intelligence improvements are present. Phase 2.1 adds normalized public WebSocket streams, REST snapshot recovery, per-field freshness, exchange filter validation, paginated funding history, VWAP execution estimates, explainable hedge-aware ranking and optional observation persistence.

**No live trading:** there are no authenticated Binance endpoints, order-placement methods, or automated position operations. Estimates are not realized returns or guarantees.

## Install and run

Python 3.12+:

    python -m venv .venv
    # Windows: .venv\Scripts\activate
    # macOS/Linux: source .venv/bin/activate
    python -m pip install -e .[dev]
    uvicorn app.api.main:app --reload

Run tests:

    python -m compileall -q app tests
    pytest -q

Copy .env.example to .env to customize scan cadence, liquidity thresholds, costs, ranking weights, funding history limits and SQLite retention. Public data requires no API keys.

To enable continuous public WebSocket streams, set WEBSOCKET_ENABLED=true. Streams are disabled by default. Set OBSERVATION_MODE=true to record candidate snapshots and appearance/disappearance events. Observation mode does not simulate orders or realized PnL.

Optional public-only Binance smoke tests (no credentials): set BINANCE_PUBLIC_INTEGRATION=1 before pytest. The test skips gracefully if public network access is unavailable.

## API

- GET /api/v1/health
- GET /api/v1/market/overview
- GET /api/v1/market/spot
- GET /api/v1/market/futures
- GET /api/v1/funding/current
- GET /api/v1/funding/history/{symbol}
- GET /api/v1/scanner/opportunities
- GET /api/v1/scanner/status
- GET /api/v1/scanner/rejections
- GET /api/v1/market/orderbook/{symbol}
- GET /api/v1/market/stream-health
- GET /api/v1/market/data-quality
- GET /api/v1/exchange/filters/{symbol}
- GET /api/v1/analytics/profitability/{symbol}
- GET /api/v1/observation/summary

Interactive API documentation is available at /docs.

## Architecture and limitations

See docs/PHASE_2_ARCHITECTURE.md, docs/PHASE_2_LIMITATIONS.md and docs/PHASE_2_1_ARCHITECTURE.md. Phase 2.1 remains a read-only research engine; visible order-book VWAP and funding estimates do not guarantee execution or profit.
