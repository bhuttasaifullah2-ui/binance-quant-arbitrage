# Known limitations

This is a Phase 1 engineering foundation, not a complete arbitrage bot.

1. Market data calls use public REST only; WebSockets and stale-stream watchdogs are not implemented.
2. No authenticated Binance account, balance, fee-tier, user-data or funding-income integration.
3. No order placement, cancellation, idempotency, partial-fill handling or hedge recovery.
4. Scanner helper estimates one funding interval and simplified costs; it does not yet model basis PnL, depth-based slippage, borrow costs, margin, liquidation or account-specific fees.
5. No database, immutable ledger, persistent positions or reconciliation.
6. No paper fill simulator or historical backtester.
7. No UI dashboard.
8. No forecast model.
9. Unit tests are supplied but must be run in a configured Python environment.
10. Binance API schemas and rate limits must be checked against current official documentation before connector certification.
