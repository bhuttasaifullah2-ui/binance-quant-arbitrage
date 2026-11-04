# Quantitative audit remediation — read-only research

This draft extends the existing PR #5 feature branch without changing PR #5.

## Implemented in this draft

- Four-leg Spot/Futures entry and exit fee assumptions remain explicit.
- Spread-crossing cost is calculated separately from visible-book depth slippage.
- Funding cash flow uses linear USD-M Futures quantity, contract multiplier and current mark price. The current mark is a proxy; the future settlement mark is unknown.
- The public USD-M `/fapi/v1/fundingInfo` endpoint supplies adjusted funding-interval overrides. Inferred intervals are restricted to the documented 1h, 4h and 8h schedules.
- The latest realized settlement must align with the expected previous settlement implied by the next funding time and interval. A settlement that is too old or later than the expected schedule is rejected.
- Funding freshness uses the premium-index exchange source timestamp where available, rather than only local HTTP receipt time.
- Spot/Futures hedge validation checks base asset, quote asset, USD-M margin asset, perpetual contract type and positive contract multipliers.
- Rejection states distinguish missing history, stale history, incomplete intervals, rate reconciliation failure, settlement timestamp mismatch and insufficient net edge.
- Manual validation does not initiate WebSocket probes when `WEBSOCKET_ENABLED=false`.
- Tests cover positive/negative funding, mark-notional funding, spread costs, interval inference, interval overrides, settlement timestamp mismatch and incompatible hedge assets.

## Unresolved limitations

- Binance funding history includes `markPrice` and `rateType`; the current database still stores only settlement timestamp and funding rate. Historical mark-price cash-flow reconstruction and special-rate tagging remain follow-up work.
- Funding-history retrieval is bounded. When no time bounds are supplied, Binance may return only the most recent 200 records even when a larger limit is requested.
- `fundingInfo` reports adjusted intervals but not a historical effective timestamp for every schedule transition. Mixed historical schedules may remain ambiguous; unresolved gaps block eligibility.
- Duplicate timestamp counts do not yet distinguish identical duplicates from conflicting funding-rate records at the same timestamp.
- Static fee settings are assumptions, not account-specific commissions. No private account data is accessed.
- Basis-risk haircut, visible order-book impact and the next funding rate remain estimates, not realized PnL or a forecast.
- Theoretical checks do not model balances, margin, liquidation, actual fills or funding receipts.

## Safety boundary

Public unauthenticated market-data GET endpoints only. No credentials, account endpoints, orders, paper trading, margin or leverage management, position management, transfers or withdrawals. Disabled modes remain disabled. No VPS, firewall, SSH, systemd, service or deployment changes. PR #5 remains unmodified and unmerged.
