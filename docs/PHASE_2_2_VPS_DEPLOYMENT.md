# Phase 2.2 — Linux VPS deployment (read-only)

## Requirements
- Python 3.12 or 3.13.
- Linux VPS with outbound HTTPS/WSS access to Binance public market-data hosts.
- Start with 2 vCPU / 2 GB RAM and monitor actual memory, SQLite growth and network usage; this is a conservative starting point, not a benchmark or guarantee.
- One application process only: SQLite WAL is not a multi-process write-coordination design.

## Install
    sudo apt update
    sudo apt install -y python3.12 python3.12-venv python3-pip
    sudo useradd --system --create-home --shell /usr/sbin/nologin quantbot
    sudo mkdir -p /opt/binance-quant-arbitrage /var/lib/binance-quant-arbitrage
    sudo chown -R quantbot:quantbot /opt/binance-quant-arbitrage /var/lib/binance-quant-arbitrage

Deploy the reviewed Git commit into /opt/binance-quant-arbitrage, then:
    sudo -u quantbot python3.12 -m venv /opt/binance-quant-arbitrage/.venv
    sudo -u quantbot /opt/binance-quant-arbitrage/.venv/bin/pip install -e '/opt/binance-quant-arbitrage[dev]'

## Environment
Create /etc/binance-quant-arbitrage.env readable only by the service user. Keep existing opt-in settings false unless separately approved:

    APP_ENV=production
    APP_MODE=RESEARCH
    DATABASE_PATH=/var/lib/binance-quant-arbitrage/market_data.sqlite3
    RETENTION_DAYS=30
    WEBSOCKET_ENABLED=false
    OBSERVATION_MODE=false
    LIVE_VALIDATION_ENABLED=false
    LIVE_TRADING_ENABLED=false

All Binance requests are public and unauthenticated. Do not add API keys. LIVE_TRADING_ENABLED must remain false.

## Research dashboard
The dashboard is served by the existing FastAPI process at GET /dashboard; no second service, frontend build step, database or new port is required. It reads existing endpoints using GET requests only and refreshes every 15 seconds. It does not call validation POST endpoints or initiate scans, validation probes, WebSocket connections or observation mode.

Local verification on the VPS:

    curl --fail http://127.0.0.1:8000/dashboard
    curl --fail http://127.0.0.1:8000/api/v1/health

For remote viewing, use an authenticated VPN or an SSH local port forward to loopback. Example from an operator workstation:

    ssh -L 8000:127.0.0.1:8000 -p SSH_PORT USER@VPS_HOST

Then open http://127.0.0.1:8000/dashboard locally. Substitute the operator's SSH connection details. Do not bind Uvicorn to 0.0.0.0, expose port 8000, change firewall rules or add an unauthenticated public reverse proxy. The application API has no authentication middleware.

## Session lifecycle and restart behavior
- Each application process creates a new UUID session in the same SQLite database; sessions are never resumed or merged.
- A clean service shutdown marks the active session COMPLETED and records its end timestamp.
- If a process disappears without clean shutdown, the next startup marks its still-RUNNING session INTERRUPTED and records which new session detected the interruption. The exact crash instant is not knowable.
- List sessions at GET /api/v1/observation/sessions. Select the returned session_id to inspect or export it.
- Export a session JSON document with GET /api/v1/observation/sessions/{session_id}/report.json.
- REST, WebSocket, clock, data-quality, scanner-eligibility and funding summaries are available under the same session path.
- Existing legacy daily/cumulative reports are cross-session and must not be substituted for a session-specific report.

## systemd
Create /etc/systemd/system/binance-quant-arbitrage.service:

    [Unit]
    Description=Binance Quant Arbitrage — read-only observer
    After=network-online.target
    Wants=network-online.target

    [Service]
    Type=simple
    User=quantbot
    Group=quantbot
    WorkingDirectory=/opt/binance-quant-arbitrage
    EnvironmentFile=/etc/binance-quant-arbitrage.env
    ExecStart=/opt/binance-quant-arbitrage/.venv/bin/uvicorn app.api.main:app --host 127.0.0.1 --port 8000
    Restart=on-failure
    RestartSec=5
    KillSignal=SIGINT
    TimeoutStopSec=30
    NoNewPrivileges=true
    PrivateTmp=true
    ProtectSystem=strict
    ProtectHome=true
    ReadWritePaths=/var/lib/binance-quant-arbitrage

    [Install]
    WantedBy=multi-user.target

Run systemctl daemon-reload and systemctl enable --now only under a separately approved deployment procedure. Do not change service startup or boot enablement as part of dashboard implementation.

Bind to loopback. Do not expose port 8000 directly to the internet; use authenticated VPN access if remote access is required. No authentication middleware is included in this phase.

## Smoke test and evidence capture
1. Confirm deployed commit SHA and record opt-in flags.
2. Start the service only after approval and inspect health, validation summary and observation sessions.
3. Record the new session_id. Run a 30–60 minute smoke test.
4. Export its JSON report and preserve the raw file with deployment commit/configuration.
5. Restart only after approval. Confirm the prior session is INTERRUPTED if shutdown was unclean, or COMPLETED if graceful; confirm a new session_id.
6. Document DNS/network interruptions, REST failures, WebSocket disconnect/recovery events, clock warnings, stale/missing fields, unresolved funding gaps and scanner rejections.

## Health, logs and shutdown
- curl http://127.0.0.1:8000/api/v1/health
- curl http://127.0.0.1:8000/api/v1/validation/summary
- journalctl -u binance-quant-arbitrage -f
- systemctl stop binance-quant-arbitrage requests graceful shutdown.

## Backup and upgrade
Stop the service before copying SQLite database and WAL/SHM companions, or use SQLite online backup API. Keep encrypted off-host backups and test restore. For upgrades: stop service, back up database, deploy reviewed commit, install dependencies, run python -m compileall -q app tests and pytest -q, start service, check health and retain previous release for rollback.

## Constraints and troubleshooting
Public REST/WebSocket limits are exchange-controlled. Reconnects, slow DNS, VPS clock drift, network egress filtering and rate limits can degrade feeds. The validator reports failures; it cannot guarantee Binance availability. If Spot/Futures data is stale or clock status is warning, do not treat opportunities as eligible. Observation mode records estimates only. There is no account/balance/margin/liquidation visibility and no execution.
