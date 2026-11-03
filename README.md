# Binance Quant Arbitrage — Phase 1

A safety-first Python scaffold for researching Binance Spot LONG + USDⓈ-M Perpetual SHORT funding-rate arbitrage.

## Status

**Research/scanner scaffold only. No live trading.** This phase includes Decimal-based calculations, funding and hedge domain logic, basic risk checks, a public REST market-data client, a read-only FastAPI API, and unit-test files.

Not implemented in this phase: authenticated account access, order placement, a full paper-fill simulator, persistent database, WebSocket feeds, production dashboard, or historical backtester.

## Safety

- Never commit API keys, secrets, or `.env` files.
- Keep trading disabled until independent testing and review are complete.
- Estimated funding income is uncertain; fees, slippage, basis movement, liquidation, funding changes, and execution risk can outweigh it.

## Quick start

Use Python 3.12+ and install dependencies:

```bash
python -m venv .venv
# Windows: .venv\\Scripts\\activate
# macOS/Linux: source .venv/bin/activate
python -m pip install -e .[dev]
```

Run the API:

```bash
uvicorn app.api.main:app --reload
```

Run tests:

```bash
pytest
```

Test execution has not yet been independently verified for this delivery.
