# Phase 2.2 implementation plan

Baseline: a72f71de383a78f3c6ace310ccf4f459d8eb3b98.

1. Extend existing public-only REST/WebSocket services with validation, clock checks and per-field data-quality classification.
2. Persist validation samples and recovery events in SQLite using WAL and bounded retention.
3. Add read-only validation and observation reporting endpoints.
4. Add Decimal-based hypothetical funding/basis/slippage sensitivity and theoretical exchange-filter hedge checks.
5. Add deterministic tests, opt-in public integration checks and Linux VPS deployment guidance.

No signed endpoints, API keys, account endpoints, orders, transfers, withdrawals, leverage or margin controls are in scope. Validation remains disabled by default; LIVE_VALIDATION_ENABLED=true opts into periodic public connectivity probes. WEBSOCKET_ENABLED and OBSERVATION_MODE remain explicit opt-ins.
