# Phase 2.2 validation limitations

- Public WebSocket probes establish a connection and receive one event; they are not long-duration stability certification.
- Periodic validation is opt-in through LIVE_VALIDATION_ENABLED. WEBSOCKET_ENABLED and OBSERVATION_MODE remain independently opt-in.
- Clock checks use request midpoint offset estimates. They do not discipline the host clock; NTP/chrony remains an operating-system responsibility.
- WebSocket counters are process-lifetime metrics; downtime before process start cannot be measured.
- Spot bookTicker has no exchange event timestamp; existing receive-time/update-ID protection remains.
- Data-quality classification covers required normalized fields in the hub; order-book snapshots and exchange metadata require explicit REST validation.
- Funding gap detection is a conservative interval-gap heuristic, not proof that every exchange settlement was received.
- Theoretical hedge validation uses public metadata and top-of-book prices; it does not model balances, commissions, margin, liquidation or actual fills.
- Profitability sensitivity uses illustrative fee/slippage assumptions and a one-settlement model. It is not realized PnL or a forecast.
- Phase 2.2.1 adds daily UTC and cumulative reports from persisted scanner runs, opportunity snapshots and lifecycle/connection events. Their distributions describe eligible opportunity snapshots rather than the entire exchange universe; uptime is limited to persisted connection-transition evidence.
- Observation event persistence emphasizes WebSocket interruption/recovery. Existing scanner lifecycle snapshots remain in the Phase 2.1 store; detailed ranking/funding/basis/liquidity change-diff events are not yet emitted.
- SQLite remains single-process. Protect the API with VPN or authenticated reverse proxy before remote access.
- No real-world uptime, fill quality, funding income or trading performance is established by deterministic CI.


See docs/PHASE_2_2_1_ACCEPTANCE.md for the Phase 2.2.1 implementation summary, deterministic test coverage and remaining limitations.
