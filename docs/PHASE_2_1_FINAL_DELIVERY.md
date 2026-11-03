# Phase 2.1 — Final delivery report

## Delivery status

Phase 2.1 has been merged into main. This is an upgraded read-only market intelligence foundation, not a production-certified trading system.

- Pull request: https://github.com/bhuttasaifullah2-ui/binance-quant-arbitrage/pull/3
- Merge commit: 04f03f16979af923ac420ffa93c70dd3961089b9
- CI-verified head: 75ae592c195ea920e1557ece59995d10e0075675
- CI: Python 3.12 and 3.13 both passed (35 passed, 1 skipped each).
- Detailed results: docs/PHASE_2_1_TEST_REPORT.md

## Main features delivered

- Public Spot and USD-M Futures combined WebSocket stream manager with dynamic sharding, reconnect/backoff, ping/pong, REST recovery, clock offsets and health status.
- Normalized per-symbol state with event/receive timestamps, duplicate/out-of-order/delayed-event rejection and field-level freshness.
- Decimal parsing/rounding/validation for LOT_SIZE, MARKET_LOT_SIZE, PRICE_FILTER, MIN_NOTIONAL and NOTIONAL.
- Matched Spot LONG/Futures SHORT sizing using exchange-rounded quantities, entry VWAP and post-VWAP mismatch checks.
- Exact-quantity order-book VWAP and visible-depth slippage estimates for both entry and exit legs.
- Incremental, paginated and deduplicated funding history with bounded historical gap backfill and expanded statistical summaries.
- Fee/slippage/basis-adjusted one-settlement net estimate, break-even rate and basis sensitivity.
- Explainable ranking factors including hedge precision and residual exposure.
- Additive SQLite persistence for metadata, sampled WebSocket events, feed health, scanner runs, opportunity snapshots/lifecycle and data-quality events.
- Optional observation mode; no simulated orders or realized PnL.
- New read-only APIs for stream health, data quality, scanner rejections, exchange filters, profitability history and observation summary.

## Known limitations and outstanding risks

- WebSockets are disabled by default; set WEBSOCKET_ENABLED=true to enable.
- Spot bookTicker has no exchange event timestamp; receive time and update ID are used.
- Clock offset is sampled at startup only; there is no continuous time-discipline service.
- Exchange metadata refreshes hourly.
- Detailed depth and funding retrieval are bounded by shortlist/page settings; the full market is not deeply enriched on every scan.
- Historical funding backfill is page-capped; remaining gaps are surfaced.
- Fee assumptions are configured and not account-specific. Balances, fee tiers, margin and liquidation constraints are not checked.
- USD-M contract multiplier defaults to one if not supplied in symbol metadata.
- Visible-book VWAP is an estimate, not a fill guarantee; queue priority, hidden liquidity and adverse selection are not modeled.
- Profitability assumes one upcoming funding settlement and includes a conservative basis haircut; funding changes, basis path, taxes, capital costs, margin and liquidation are not fully modeled.
- SQLite is designed for one application process; multi-process write coordination is not included.
- The public Binance integration smoke test is opt-in and was skipped in CI. Live WebSocket operation was not verified in CI.

## Safety

No API-key collection, authenticated account endpoints, order placement, position operations, transfers, withdrawals or live trading are present. Do not interpret candidate scores or estimated net PnL as guaranteed returns.
