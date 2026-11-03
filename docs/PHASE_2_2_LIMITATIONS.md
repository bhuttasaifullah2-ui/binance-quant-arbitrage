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
- Legacy daily/cumulative reports remain cross-session historical reports. Session-specific exports are separate and must be used when isolating one process run.
- Session WebSocket uptime is deliberately reported as unavailable until a complete, defensible state interval can be established; a connection event alone is not a complete denominator.
- Session clock distributions are based only on successful offset measurements; failed checks remain in the observations with missing offsets.
- Session eligibility coverage includes final decisions supplied to the lifecycle snapshot writer. Early pre-filter rejections that never reach that final-decision map remain in legacy scanner rejection records, not in the session final-eligibility denominator.
- Session opportunity snapshots and transitions are session-scoped. Historical legacy opportunity state remains intact and is not rewritten.
- Session recovery marks any still-RUNNING session INTERRUPTED at the next process startup. It cannot determine the exact instant of an unclean process death; the interruption timestamp is the next startup detection time.
- SQLite remains single-process. Protect the API with VPN or authenticated reverse proxy before remote access.
- No real-world uptime, fill quality, funding income or trading performance is established by deterministic CI.

See docs/PHASE_2_2_1_ACCEPTANCE.md for implementation details and the current acceptance boundary.
