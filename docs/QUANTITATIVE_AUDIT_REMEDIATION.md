# Quantitative audit remediation — read-only research

This branch contains a limited set of evidence-backed quantitative corrections. It does not change PR #5.

## Implemented

- Scanner funding cash flow uses Futures position quantity multiplied by the Futures mark price and the exchange contract multiplier, rather than the smaller entry VWAP notional.
- Four-leg execution shortfall is measured against Spot and Futures mid-prices. Each buy/sell leg includes spread crossing and visible order-book impact; favorable execution is not credited.
- The scanner reads the public USD-M `/fapi/v1/fundingInfo` endpoint for adjusted funding intervals. Where a symbol has no override, historical inference is restricted to the documented 1h, 4h and 8h interval set.
- Funding eligibility now checks that the latest realized settlement timestamp aligns with the expected previous settlement implied by the next funding time and interval.
- Scanner rejections distinguish missing history, historical gaps, rate mismatch, stale settlement timestamps, insufficient net edge and theoretical hedge-filter failures.
- Deterministic tests cover four-leg spread/impact costs, mark-notional funding cash flow, unsupported inferred intervals, stale latest settlement timestamps and interval overrides.

## Deliberately unchanged

- No liquidity, spread, hedge-mismatch, funding-consistency or net-edge thresholds were relaxed.
- `LIVE_TRADING_ENABLED=false`, `WEBSOCKET_ENABLED=false`, `LIVE_VALIDATION_ENABLED=false` and `OBSERVATION_MODE=false` remain the intended defaults.
- No account endpoints, credentials, balances, positions, orders, transfers, withdrawals, paper trading or position-management code was added.
- No VPS configuration, firewall, SSH, systemd, service process or deployment was changed.
- PR #5 was not modified or merged.

## Remaining limitations

- Funding history remains bounded. Binance's funding-history endpoint returns at most the latest 200 records when no time bounds are supplied; deeper backfill requires explicit time pagination.
- The current funding-info endpoint supplies adjusted intervals, but does not provide a historical effective timestamp for every interval transition. Mixed historical schedules can therefore remain ambiguous and should be treated conservatively.
- The current net estimate still depends on static configured fee assumptions, visible book depth, a basis-risk haircut and the latest realized funding rate as a proxy. It is not a forecast or execution guarantee.
- No account-specific commissions, balances, margin, liquidation, actual fills or funding receipts are available or queried.
- The source audit identified additional work not included in this limited remediation: explicit asset compatibility checks, conflicting duplicate settlement detection, complete retention policy, per-market clock freshness and scanner task supervision.

## Validation status

GitHub Actions run [41810938426](https://github.com/bhuttasaifullah2-ui/binance-quant-arbitrage/actions/runs/41810938426) passed on commit `ea74f3fa50545e6aa9990d4c3d392f86ec1bc289`:

- Python 3.12: Ruff passed, compile passed, **77 passed, 2 skipped**.
- Python 3.13: Ruff passed, compile passed, **77 passed, 2 skipped**.

The two skipped tests are opt-in public Binance integration tests. They were not run; no live connectivity is claimed.
