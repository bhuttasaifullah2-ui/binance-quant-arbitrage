# Phase 2.1 — Integrated read-only market intelligence

## Runtime architecture

- Public REST is used for exchange metadata, first-run bootstrap, reconnect recovery, bounded order-book depth, and paginated funding settlements.
- A MarketDataHub normalizes Spot bookTicker/ticker and USD-M Futures bookTicker/ticker/markPrice@1s events.
- BinanceWebSocketManager shards combined subscriptions, lowercases symbols, reconnects with exponential backoff, relies on the WebSocket library's ping/pong handling, and rebuilds shards when the eligible symbol universe changes.
- On each connection/reconnection, a rate-throttled bulk REST recovery snapshot refreshes the active symbol universe.
- With WebSockets enabled, the scanner performs one REST bulk bootstrap, then reads normalized per-symbol hub state on subsequent cycles instead of repeatedly polling all tickers. Missing/stale symbols are rejected; reconnect recovery refreshes the active universe. It continues to refresh reference metadata hourly and requests bounded depth/history for shortlisted symbols.
- Event time is corrected using startup Spot/Futures server-time offsets. Delayed/future events, duplicate update IDs, duplicate events, out-of-order events, and stale per-field values are rejected or marked.

## WebSocket configuration

Copy .env.example to .env. Set WEBSOCKET_ENABLED=true to start public streams. Default is false to avoid unexpected persistent connections. WEBSOCKET_STREAMS_PER_CONNECTION bounds each combined subscription shard. The manager automatically reconnects and resubscribes after a universe change or disconnect.

Spot streams:
- <symbol>@bookTicker
- <symbol>@ticker

USD-M Futures streams:
- <symbol>@bookTicker
- <symbol>@ticker
- <symbol>@markPrice@1s

REST recovery uses bulk book-ticker, 24-hour ticker, and premium-index endpoints. Public connectivity requires no API key.

## Exchange filters and executable hedge validation

app/domain/exchange_filters.py parses LOT_SIZE, MARKET_LOT_SIZE, PRICE_FILTER, MIN_NOTIONAL, and NOTIONAL. Decimal-only helpers round quantities down to the applicable lot step and prices to tick sizes. Market-order validation does not apply limit-price tick constraints; it still checks quantity, notional and symbol status. The hedge calculator returns exchange-valid quantities, actual leg notionals, ratio, residual exposure and explicit rejection reasons. An opportunity is not marked executable unless both legs pass the configured market-order filters and mismatch threshold.

USD-M quantity is treated as base-asset quantity with a default contract multiplier of one; contract-size overrides are used only if the exchange metadata supplies them.

## Freshness and event integrity

Each normalized field carries exchange event time, local receive time and source. Freshness is checked independently for Spot bid/ask/last/volume and Futures bid/ask/last/volume/mark/index/funding/next-funding time. Spot book-ticker events do not carry an exchange event timestamp; those use local receive time plus the exchange update ID for duplicate/out-of-order protection. This limitation is exposed in the feed model. Clock offsets are synchronized at startup with a bounded timeout.

## Funding history

fetch_funding_history uses bounded startTime/endTime pagination, a configurable page cap, and timestamp deduplication. New settlements are fetched incrementally from the latest stored settlement. Detected gaps in stored history are backfilled within the configured page budget. Funding statistics include mean, median, min/max, standard deviation, positive/negative frequency, recent trend, consecutive sign streaks and gap count. These are historical observations, not forecasts.

## Order-book and profitability model

estimate_vwap consumes quote-notional depth and returns VWAP, executed quantity, actual quote notional, slippage/impact bps and a complete/incomplete flag. Both entry and exit directions are estimated for Spot and Futures. Net funding estimate uses matched leg notionals, each leg's configured taker fee assumption, four VWAP slippage costs and an explicit absolute-basis risk haircut. The break-even funding rate is based on estimated costs. It assumes one upcoming funding interval and is not realized PnL; basis convergence/drift, margin, liquidation, taxes, account-specific fee tiers and execution certainty remain outside scope.

## Ranking

Ranking exposes a weighted factor breakdown for net edge, funding consistency, liquidity, spread, slippage, basis, freshness, hedge precision and residual exposure. Critical stale data, inadequate depth, excessive hedge mismatch or residual exposure forces score zero. The score is an ordinal heuristic, not a probability.

## Persistence and observation mode

SQLite schema upgrades are additive and preserve existing Phase 1/2 tables. New tables persist exchange metadata, sampled normalized WebSocket events, WebSocket health, scanner runs, opportunity snapshots/lifecycle and data-quality events. WAL mode, unique keys and indexes support restart recovery and deduplication. OBSERVATION_MODE=true records candidate snapshots and APPEARED/DISAPPEARED events; no orders are simulated and no realized PnL is claimed.

## API additions

- GET /api/v1/market/stream-health
- GET /api/v1/market/data-quality
- GET /api/v1/scanner/rejections
- GET /api/v1/exchange/filters/{symbol}
- GET /api/v1/analytics/profitability/{symbol}
- GET /api/v1/observation/summary

Existing API routes remain. /docs provides interactive OpenAPI documentation.

## Tests

Install and run:

    python -m pip install -e '.[dev]'
    python -m compileall -q app tests
    pytest -q

The public API smoke test is skipped by default. To opt in, set BINANCE_PUBLIC_INTEGRATION=1; it uses only public exchange-info endpoints and skips gracefully when public network access is unavailable. No credentials are required.

## Known limitations

- WebSocket streams are disabled by default and must be explicitly enabled.
- Spot book-ticker does not include an exchange event timestamp; local receive time and update ID are used.
- Startup clock synchronization is a one-time offset estimate, not a continuously disciplined clock.
- Metadata refresh is hourly; changes can take up to that interval to be reflected.
- Detailed depth and funding requests remain bounded to the liquidity/spread shortlist and configured per-cycle limits. This is rate-limit protection, not full-depth coverage for every pair.
- Historical backfill is bounded by FUNDING_HISTORY_MAX_PAGES; older gaps may remain and are surfaced as warnings.
- Exchange filters validate public exchange metadata, not account-specific balances, fee tier, position mode, leverage or margin availability.
- USD-M contract multiplier defaults to one where metadata omits it; exotic contract specifications require further validation.
- Visible order-book VWAP is not a fill guarantee and does not model queue priority, hidden liquidity or adverse selection.
- Estimated profitability assumes one upcoming settlement and configured fee assumptions. Basis paths, funding changes before settlement, margin/liquidation, capital costs, taxes and execution failures are not fully modelled.
- SQLite is intended for a single application process; multi-process write coordination is not implemented.
- No authenticated account endpoints, order placement, position operations, transfers or withdrawals exist.
