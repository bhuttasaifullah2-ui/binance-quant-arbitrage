# Architecture

Dependency direction: API / adapters -> services -> domain. Domain calculations use Decimal and have no exchange dependencies.

- domain: pure funding, hedge, profitability and risk rules.
- exchange/binance: public REST adapter only in this milestone.
- services: scanner orchestration and decision explanation.
- api: read-only HTTP surface.
- tests: deterministic unit tests.

Execution, persistence, accounting and frontend are intentionally absent rather than represented by fake implementations.
