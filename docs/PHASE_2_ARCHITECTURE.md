# Phase 2 — Market data and funding scanner

## Runtime flow

1. The read-only REST adapter fetches Spot and USD-M Futures exchange metadata, 24-hour tickers, best bid/ask and Futures premium index.
2. The scanner intersects active USDT Spot trading pairs with active USDT perpetual contracts.
3. Configured volume and spread filters produce a bounded detailed-data shortlist.
4. The engine fetches Spot/Futures order books, Futures open interest and funding history for shortlisted pairs.
5. It estimates executable depth, four-leg slippage, fee drag, basis penalty and funding consistency.
6. Valid snapshots and historical funding settlements are deduplicated into SQLite.
7. Opportunities are ranked and exposed through read-only API routes.

## Binance public endpoints

Spot:
- GET /api/v3/exchangeInfo
- GET /api/v3/ticker/24hr
- GET /api/v3/ticker/bookTicker
- GET /api/v3/depth

USD-M Futures:
- GET /fapi/v1/exchangeInfo
- GET /fapi/v1/ticker/24hr
- GET /fapi/v1/ticker/bookTicker
- GET /fapi/v1/premiumIndex
- GET /fapi/v1/fundingInfo
- GET /fapi/v1/fundingRate
- GET /fapi/v1/depth
- GET /fapi/v1/openInterest

All calls are public and require no API key. No authenticated endpoints or order routes exist.

## WebSockets

Phase 2.1 integrates the sharded combined-stream manager into the scanner source-of-truth path when WEBSOCKET_ENABLED=true. See docs/PHASE_2_1_ARCHITECTURE.md for the current data flow. Phase 2.2 adds independent opt-in public connectivity and clock validation. Neither a one-message connectivity probe nor deterministic CI certifies long-run WebSocket stability.

## Scanner and ranking

The scanner scans the intersection of TRADING Spot USDT pairs and TRADING USD-M USDT PERPETUAL contracts. High-volume, tight-spread pairs are shortlisted before depth/history calls. Detailed requests are bounded by MAX_DEPTH_SYMBOLS_PER_CYCLE and concurrency limits to reduce rate-limit risk.

Funding is only a potential cash flow for the short leg when the rate is positive. The expected net estimate subtracts configured entry/exit taker fees, a simplified four-side slippage estimate and a conservative absolute-basis haircut. Ranking uses net edge, funding consistency/volatility, depth, spread, slippage, basis and freshness. A score is not a probability or guaranteed return.

## Storage

SQLite tables:
- market_observations: one market snapshot per (symbol, observed_at_ms).
- funding_observations: one historical settlement per (symbol, funding_time_ms).

Indexes support symbol/time history retrieval. Old observations are pruned using RETENTION_DAYS.

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

Legacy /health and /market/overview aliases are retained.

## Configuration

See .env.example. Important settings include scan interval, data-age threshold, quote-volume and spread filters, notional size, fee assumptions, bounded depth/history shortlist, order-book depth and SQLite retention.

## Run

    python -m pip install -e .[dev]
    uvicorn app.api.main:app --reload
    pytest

The application performs a scan on startup and then on the configured interval. For production use, supervise the process and monitor /api/v1/scanner/status.
