# Binance Quant Arbitrage

Read-only Binance Spot LONG + USD-M perpetual SHORT funding-rate market intelligence engine.

## Current milestone

Phase 1 research foundation, Phase 2 market-data scanner and Phase 2.1 integrated market-intelligence improvements are present. Phase 2.1 adds normalized public WebSocket streams, REST snapshot recovery, per-field freshness, exchange filter validation, paginated funding history, VWAP execution estimates, explainable hedge-aware ranking and optional observation persistence. Phase 2.2 adds public-only validation, session-scoped evidence and a local read-only research dashboard.

**No live trading:** there are no authenticated Binance endpoints, order-placement methods, or automated position operations. Estimates are not realized returns or guarantees.

## Install and run

Python 3.12+:

    python -m venv .venv
    # Windows: .venv\Scripts\activate
    # macOS/Linux: source .venv/bin/activate
    python -m pip install -e '.[dev]'
    uvicorn app.api.main:app --reload

Run tests:

    python -m compileall -q app tests
    pytest -q
    ruff check --select E4,E7,E9,F --ignore E701,E702 app tests

Copy .env.example to .env to customize scan cadence, liquidity thresholds, costs, ranking weights, funding history limits and SQLite retention. Public data requires no API keys.

Continuous public WebSocket streams, observation mode and periodic public validation are independent opt-ins. Keep WEBSOCKET_ENABLED=false, OBSERVATION_MODE=false and LIVE_VALIDATION_ENABLED=false unless separately authorized. LIVE_TRADING_ENABLED must remain false.

## Research dashboard

After starting the existing FastAPI application, open:

    http://127.0.0.1:8000/dashboard

The dashboard is a self-contained HTML/CSS/JavaScript page served by the same FastAPI process. It uses existing read-only GET endpoints, refreshes every 15 seconds and does not require Node.js, a second backend, a new database or external CDN assets. It does not trigger validation POST actions. Session JSON export links directly to the existing session export endpoint.

For remote viewing, keep Uvicorn bound to 127.0.0.1 and use an authenticated VPN or SSH local port forward. Do not expose port 8000 publicly. The API and dashboard do not include authentication middleware.

## API

- GET /dashboard
- GET /api/v1/health
- GET /api/v1/market/overview
- GET /api/v1/market/spot
- GET /api/v1/market/futures
- GET /api/v1/funding/current
- GET /api/v1/funding/history/{symbol}
- GET /api/v1/scanner/opportunities
- GET /api/v1/scanner/status
- GET /api/v1/scanner/rejections
- GET /api/v1/scanner/eligibility
- GET /api/v1/validation/summary
- GET /api/v1/validation/connectivity
- GET /api/v1/validation/websocket
- GET /api/v1/validation/clock
- GET /api/v1/validation/data-quality
- GET /api/v1/validation/funding
- GET /api/v1/observation/events
- GET /api/v1/observation/report
- GET /api/v1/observation/sessions
- GET /api/v1/observation/sessions/{session_id}
- GET /api/v1/observation/sessions/{session_id}/report.json

Interactive API documentation is available at /docs.

## Architecture and limitations

See docs/PHASE_2_ARCHITECTURE.md, docs/PHASE_2_LIMITATIONS.md, docs/PHASE_2_1_ARCHITECTURE.md, docs/PHASE_2_2_IMPLEMENTATION_PLAN.md and docs/PHASE_2_2_LIMITATIONS.md. Public connectivity checks, one-message WebSocket probes and hypothetical funding/basis/slippage estimates do not guarantee execution or profit. SQLite is intended for a single application process.
