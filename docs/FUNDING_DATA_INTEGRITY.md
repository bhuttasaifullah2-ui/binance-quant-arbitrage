# Funding data integrity and settlement reconciliation

## Public exchange inputs

The scanner uses only unauthenticated USDⓈ-M market-data endpoints:

- `GET /fapi/v1/fundingInfo` for symbol-specific adjusted funding intervals and adjusted rate cap/floor. Binance documents zero request weight for this endpoint, but it shares the 500-requests-per-5-minute-per-IP limit with `GET /fapi/v1/fundingRate`; history pagination therefore remains bounded by the scanner's configured symbol/page budget.
- `GET /fapi/v1/fundingRate` for realized settlement timestamp, rate, settlement mark price and rate type.
- `GET /fapi/v1/premiumIndex` for the latest funding-rate observation, next settlement time and current mark-price proxy.

Binance documents that `fundingInfo` returns symbols with adjusted funding parameters; an absent symbol is not treated as proof that the default 8-hour interval is authoritative. If fundingInfo is unavailable, the scanner may infer only 1h, 4h or 8h from at least three distinct settlement timestamps with a consistent observed cadence. That fallback is labelled as history-inferred and is not exchange-authoritative. If it cannot be inferred safely, eligibility is blocked.

Adjusted cap/floor values are checked only when supplied by Binance. An entry with missing or malformed cap/floor data does not pass the adjusted-limit check.

## Settlement history states

The research scanner distinguishes:

- `HISTORY_MISSING`: no realized settlement rows.
- `HISTORY_INCOMPLETE`: insufficient distinct settlements, unknown interval, or missing intervals / coverage.
- `HISTORY_STALE`: latest settlement predates the expected previous settlement.
- `HISTORY_RECONCILIATION_FAILED`: timestamp, rate, or same-time payload conflict.
- `FUNDING_RATE_RECONCILED`: the latest settlement timestamp and rate reconcile within configured tolerances.

No missing settlement is synthesized as a zero-rate event. A minimum of three distinct settlement timestamps is required. Coverage start/end timestamps are reported, but the bounded stored window does not claim that older history is complete.

## SQLite v4 migration and conflict policy

The migration is additive at the application level and preserves all pre-existing funding rows. Before rebuilding the old timestamp-unique table, the store writes a consistent online backup to `<database>.pre_v4.bak`. The table rebuild and schema-version update occur in one SQLite transaction.

The new event identity is the complete public settlement payload:
`symbol + funding timestamp + funding rate + settlement mark + rateType + source`.
An exact repeat is idempotent even if observed again later. A different rate, mark, rateType or source at the same timestamp is retained as a separate row and surfaced through `funding_history_conflicts`; it is not overwritten or silently deduplicated. The scanner treats conflicting same-time settlement payloads as a reconciliation failure and blocks eligibility pending review.

Online backup and integrity-checked restore helpers are available on the SQLite store. Legacy rows are preserved with null settlement mark and rateType because those fields were not stored by the previous schema; missing historical values are not fabricated.

## Cash-flow interpretation

Historical settlement cash flow can be normalized using the historical settlement mark from funding history and the current hypothetical Futures quantity. It is explicitly labelled as a calculation for a hypothetical quantity, not actual account PnL.

The next settlement mark is unknown. Forward funding remains an estimate using the current Futures mark as a proxy and the latest observed funding rate carried forward; it is not a forecast or a realized return.

## Market compatibility and timestamps

Spot/Futures symbols must agree on explicit base and quote assets. USDⓈ-M contract multipliers are included in quantity, notional and funding calculations. Spot and Futures REST snapshot request midpoints are compared against the configured clock-skew bound; WebSocket field receive timestamps are also checked cross-market. A divergence blocks the candidate; thresholds are not relaxed.

## Safety

All work remains public unauthenticated read-only research. No private account endpoint, credential, balance, order, paper-trading, leverage, margin, transfer, withdrawal, deployment or VPS operation is used. Keep `LIVE_TRADING_ENABLED=false`, `WEBSOCKET_ENABLED=false`, `LIVE_VALIDATION_ENABLED=false` and `OBSERVATION_MODE=false`.
