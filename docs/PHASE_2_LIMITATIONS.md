# Phase 2 known limitations

- The scanner is a REST snapshot poller. The optional WebSocket consumer is not connected to scanner state or persistence.
- Detailed depth and funding history requests are bounded to a configurable shortlist, not every common pair in every cycle.
- Spot and Futures snapshots are not atomically synchronized; local receive time is used for observation time.
- Order-book slippage is a visible-depth estimate, not a fill guarantee; hidden liquidity, queue priority, market impact and adverse selection are not modelled.
- Fee assumptions are configurable placeholders. Account-specific fee tiers and discounts are not fetched.
- Net estimate models one funding interval, simple entry/exit taker fees and simplified slippage. It does not yet fully model basis convergence, basis drift scenarios, margin, liquidation, capital opportunity cost, taxes or execution risk.
- Funding interval is inferred from the returned settlement timestamps; default fallback is eight hours when insufficient data exists.
- SQLite is a single-process local store; no migrations framework or multi-process write coordination is included.
- /api/v1/market/spot, /api/v1/market/futures and /api/v1/funding/current now fetch live bulk public REST snapshots and paginate the eligible USDT symbols. The historical SQLite endpoints retain only symbols that passed the scanner's detailed enrichment stage.
- The scanner does not place orders and does not connect to authenticated Binance account APIs.
- Automated test suite passed in GitHub Actions: 16 tests passed. See docs/PHASE_2_TEST_REPORT.md for the exact run.
