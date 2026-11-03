# Phase 2.2.1 — Critical completion and acceptance hardening

This document records the read-only implementation on PR #5. It is not a claim of production certification.

## Implemented

### Central scanner eligibility
- Added app.domain.scanner_eligibility.validate_scanner_eligibility as the shared final eligibility decision.
- Every MarketScanner candidate is passed through the gate immediately before it is persisted and appended to the eligible opportunity list.
- The gate evaluates required Spot/Futures fields, finite positive prices and volumes, field age, WebSocket health when enabled, clock synchronization/drift, exchange filter completeness, funding freshness, current-versus-latest-realized funding reconciliation and unresolved funding gaps, theoretical hedge validation, residual exposure and visible order-book depth.
- Decisions include stable rejection codes and human-readable explanations. The legacy candidate builder cannot independently label a candidate eligible without full gate evidence.

### Session evidence added in Phase 2.2.2.1
- Added an additive SQLite schema for observation_sessions, session_observations, session_events, session_quality, session_eligibility, session_opportunity_state and session_opportunity_snapshots. Existing validation and opportunity tables are not dropped or rewritten.
- Every process start creates a new UUID session and a process identity. A prior session left RUNNING is marked INTERRUPTED when the next process starts; it is never resumed or merged.
- Graceful shutdown records a UTC-equivalent millisecond end timestamp and COMPLETED status. Session metadata exposes ISO-8601 UTC boundaries.
- Session-linked records cover validation samples (including REST, clock and funding), WebSocket transitions, data-quality decisions, final scanner eligibility decisions and opportunity snapshots/transitions.
- Identical repeated WebSocket events at the same timestamp are suppressed in session event storage; meaningful state changes at later timestamps are retained.
- JSON export includes metadata, raw session observations/events, quality decisions, eligibility decisions, opportunity snapshots, denominators, missing-data indicators and warnings.

### Session endpoints
- GET /api/v1/observation/sessions
- GET /api/v1/observation/sessions/{session_id}
- GET /api/v1/observation/sessions/{session_id}/report.json
- GET /api/v1/observation/sessions/{session_id}/rest
- GET /api/v1/observation/sessions/{session_id}/websocket
- GET /api/v1/observation/sessions/{session_id}/clock
- GET /api/v1/observation/sessions/{session_id}/data-quality
- GET /api/v1/observation/sessions/{session_id}/eligibility
- GET /api/v1/observation/sessions/{session_id}/funding

All endpoints are read-only. Session reports return null for unsupported uptime calculations rather than inventing a denominator.

### Existing validation and observation
- Public unauthenticated Spot/Futures REST validation and public WebSocket probes.
- WebSocket health/reconnect metrics, clock synchronization, data quality, bounded funding reconciliation and a shared scanner eligibility gate.
- Existing daily UTC and cumulative reports remain available as historical cross-session reports.

## Tests
- Existing deterministic tests cover scanner eligibility, SQLite lifecycle persistence, report calculations, mocked WebSocket recovery and funding reconciliation.
- Phase 2.2.2.1 adds deterministic session separation, interruption detection, record isolation, quality/eligibility export, duplicate event suppression, legacy-data preservation and database reopen tests.
- CI runs Python 3.12 and 3.13, Ruff, compilation and pytest. The current commit-specific workflow result must be checked before accepting this phase.

## Read-only boundary
No API credentials, authenticated account endpoints, balances, positions, order placement/cancellation, leverage/margin changes, transfers, withdrawals, paper trading or Phase 3 execution features are implemented. Public market-data access only.

## Remaining limitations
- Public Binance integration tests remain opt-in. Unless explicitly enabled and executed, CI is not evidence of real Binance connectivity.
- A one-message WebSocket probe and deterministic mocked recovery tests do not certify long-duration connection stability.
- Funding interval detection remains heuristic and bounded; unresolved intervals remain warnings and block scanner eligibility.
- Session WebSocket uptime is not emitted as a percentage unless complete connection-state boundaries support a valid denominator.
- Unexpected termination time is detected at next startup, not reconstructed precisely.
- Theoretical hedge checks do not include account balances, margin, liquidation, account fee tiers or actual fills.
- SQLite is intended for a single application process. Use a VPN or authenticated reverse proxy before remote API access.
- No real-money or paper-trading performance is established by this phase.
