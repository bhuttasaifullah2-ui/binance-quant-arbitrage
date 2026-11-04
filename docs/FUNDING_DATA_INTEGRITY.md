# Funding data integrity and settlement reconciliation

## Public exchange inputs

The scanner uses only unauthenticated USDⓈ-M market-data endpoints:

- `GET /fapi/v1/fundingInfo` for symbol-specific adjusted funding intervals and adjusted rate cap/floor. Binance documents zero request weight for this endpoint, but it shares the 500-requests-per-5-minute-per-IP limit with `GET /fapi/v1/fundingRate`; history pagination therefore remains bounded by the scanner's configured symbol/page budget.
- `GET /fapi/v1/fundingRate` for realized settlement timestamp, rate, settlement mark price and rate type.
- `GET /fapi/v1/premiumIndex` for the latest funding-rate observation, next settlement time and current mark-price proxy.

Binance documents that `fundingInfo` returns symbols with adjusted funding parameters; an absent symbol is not treated as proof that the default 8-hour interval is authoritative. If fundingInfo is unavailable, the scanner may infer only 1h, 4h or 8h when three Regular settlements provide two adjacent exact-cadence gaps within tolerance. A multi-interval gap, short gap or Special event cannot be used to infer the Regular interval. That fallback is labelled as history-inferred and is not exchange-authoritative. If it cannot be inferred safely, eligibility is blocked.

Adjusted cap/floor values are checked only when supplied by Binance. An entry with missing or malformed cap/floor data does not pass the adjusted-limit check.

## Settlement history states

The research scanner distinguishes:

- `HISTORY_MISSING`: no realized settlement rows.
- `HISTORY_INCOMPLETE`: insufficient distinct settlements, unknown interval, or missing intervals / coverage.
- `HISTORY_STALE`: latest settlement predates the expected previous settlement.
- `HISTORY_RECONCILIATION_FAILED`: timestamp, rate, or same-time payload conflict.
- `FUNDING_RATE_RECONCILED`: the latest settlement timestamp and rate reconcile within configured tolerances.

No missing settlement is synthesized as a zero-rate event. A minimum of three distinct settlement timestamps is required. Coverage start/end timestamps are reported, but the bounded stored window does not claim that older history is complete.

## SQLite v5 migration and conflict policy

The migration is additive at the application level and preserves all pre-existing funding rows. Before rebuilding the old timestamp-unique table, the store writes a consistent SQLite online backup to a unique, non-overwriting `<database>.pre_v5.<id>.bak` path and verifies `PRAGMA integrity_check` on that backup. The table rebuild and schema-version update occur in one `BEGIN IMMEDIATE` transaction; exceptions roll it back. Repeated initialization does not overwrite the prior backup. Restore refuses an existing destination or WAL/SHM sidecars, validates the source and restored database, and only writes to a newly created destination.

Schema v5 stores one economic event row per settlement payload identity and keeps source/observation provenance in the separate `funding_observation_sources` table. The economic event identity is `symbol + funding timestamp + rateType`; rate and settlement mark are payload fields used to detect disagreement. Provenance is stored separately. Exact economic payload repeats from multiple sources remain available as provenance records but collapse to one observation in scanner statistics. Different rates or marks for the same timestamp and rateType remain visible as conflicts and block reconciliation. Legacy NULL `rate_type` values are preserved in the row but normalized to Regular only for identity comparisons. Regular and Special events at the same timestamp are distinct event types when their provenance overlaps (for example, both are reported by the same public endpoint). If the event types are reported only by disjoint sources, the label is ambiguous and reconciliation fails closed. Special rows are retained and reported separately, excluded from Regular cadence inference and Regular funding-rate summary statistics, and are not substituted for the Regular rate used for current-rate reconciliation.

Online backup and integrity-checked restore helpers are available on the SQLite store. Legacy rows are preserved with null settlement mark and rateType because those fields were not stored by the previous schema; missing historical values are not fabricated. The migration tests use isolated temporary SQLite files only; no production database is used.

## Cash-flow interpretation

Historical settlement cash flow can be normalized using the historical settlement mark from funding history and the current hypothetical Futures quantity. It is explicitly labelled as a calculation for a hypothetical quantity, not actual account PnL.

The next settlement mark is unknown. Forward funding remains an estimate using the current Futures mark as a proxy and the latest observed funding rate carried forward; it is not a forecast or a realized return.

## Market compatibility and timestamps

Spot/Futures symbols must agree on explicit base and quote assets. USDⓈ-M contract multipliers are included in quantity, notional and funding calculations. Spot and Futures REST snapshot request midpoints are measured per endpoint and the full required-field timestamp range is compared against the configured age and skew bounds. These are local request-time proxies, not exchange-generated quote timestamps; network latency and server-side response generation can still make a midpoint differ from the true market-data time. WebSocket receive timestamps are checked for every required field, including each field's freshness and the entire Spot/Futures timestamp range. A divergence blocks the candidate; thresholds are not relaxed.

## Funding request budget

A shared in-process rolling 5-minute budget limits combined fundingInfo/fundingRate calls to 450, leaving a 50-request reserve for current settlement retrieval. Historical backfills use a lower-priority allocation and cannot consume that reserve. Budget exhaustion defers work to a later scan. A 418/429 response immediately activates a shared cooldown for these endpoints instead of retrying concurrently and creating a retry storm. The budget is shared across symbols and repeated scans for the lifetime of the public client process.

## Safety

All work remains public unauthenticated read-only research. No private account endpoint, credential, balance, order, paper-trading, leverage, margin, transfer, withdrawal, deployment or VPS operation is used. Keep `LIVE_TRADING_ENABLED=false`, `WEBSOCKET_ENABLED=false`, `LIVE_VALIDATION_ENABLED=false` and `OBSERVATION_MODE=false`.
