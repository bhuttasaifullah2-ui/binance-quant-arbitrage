# Security report

- No API keys or secrets are required by this Phase 1 scaffold.
- `.env` is excluded by `.gitignore`; `.env.example` contains non-secret defaults only.
- Binance adapter uses public REST endpoints and exposes no order methods.
- Live trading is disabled by default and there are no order execution endpoints.
- Before future authenticated integration, use least-privilege API keys, IP restrictions, withdrawals disabled, secret manager storage, rotation and audit logging.
- Do not enable live trading without independent security review and paper-trading acceptance.
