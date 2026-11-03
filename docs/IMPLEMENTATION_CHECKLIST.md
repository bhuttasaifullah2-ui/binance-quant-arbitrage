# Implementation checklist

## Delivered in Phase 1
- [x] Python package scaffold and dependency configuration
- [x] Environment-based configuration with research mode defaults
- [x] Decimal utilities and quantity step rounding
- [x] Funding payment and funding-history summary calculations
- [x] Spot/futures hedge quantity sizing and mismatch calculation
- [x] Basic risk decision rules
- [x] Simplified fee/slippage profitability estimate
- [x] Public Binance Spot and USDⓈ-M Futures REST market-data adapter
- [x] Read-only FastAPI health, funding and market overview endpoints
- [x] Deterministic unit-test files

## Not delivered yet
- [ ] Authenticated account and balances
- [ ] Order execution, order state machine and recovery
- [ ] Paper fill simulator
- [ ] Persistent storage and immutable accounting ledger
- [ ] WebSocket market streams and stale-data watchdog
- [ ] Backtester and production dashboard
- [ ] Live-trading release review
