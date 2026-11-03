# Phase 2.2 — Linux VPS deployment (read-only)

## Requirements
- Python 3.12 or 3.13 (CI matrix covers both).
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
Create /etc/binance-quant-arbitrage.env readable only by the service user:
    APP_ENV=production
    APP_MODE=RESEARCH
    DATABASE_PATH=/var/lib/binance-quant-arbitrage/market_data.sqlite3
    RETENTION_DAYS=30
    WEBSOCKET_ENABLED=true
    OBSERVATION_MODE=true
    LIVE_VALIDATION_ENABLED=true
    VALIDATION_INTERVAL_SECONDS=300
    MAX_CLOCK_DRIFT_MS=5000
    MAX_VALIDATION_MESSAGE_AGE_MS=15000
    MAX_RECONNECT_ATTEMPTS=10
    MAX_RECOVERY_DURATION_MS=60000
    MAX_MESSAGE_PROCESSING_LATENCY_MS=100
    MAX_STALE_SYMBOLS=5
    REQUEST_TIMEOUT_SECONDS=10
    REQUEST_MAX_RETRIES=3

All Binance requests are public and unauthenticated. Do not add API keys. Keep the three opt-in settings false unless intentionally starting observation. LIVE_TRADING_ENABLED is not a supported execution control and must remain false if present in legacy configuration.

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

Then run systemctl daemon-reload and systemctl enable --now binance-quant-arbitrage.

Bind to loopback. Do not expose port 8000 directly to the internet; place it behind authenticated VPN/reverse proxy access and TLS if remote access is required. No authentication middleware is included in this phase.

## Health, logs and shutdown
- curl http://127.0.0.1:8000/api/v1/health
- curl http://127.0.0.1:8000/api/v1/validation/summary
- journalctl -u binance-quant-arbitrage -f
- systemctl stop binance-quant-arbitrage requests graceful shutdown.
- Configure journald retention or host logrotate; application logs go to stdout/stderr.

## Backup and upgrade
Stop the service before copying SQLite database and WAL/SHM companions, or use SQLite online backup API. Keep encrypted off-host backups and test restore. For upgrades: stop service, back up database, deploy reviewed commit, install dependencies, run python -m compileall -q app tests and pytest -q, start service, check health and validation endpoints, and retain previous release for rollback.

## Constraints and troubleshooting
Public REST/WebSocket limits are exchange-controlled. Reconnects, slow DNS, VPS clock drift, network egress filtering and rate limits can degrade feeds. The validator reports failures; it cannot guarantee Binance availability. If Spot/Futures data is stale or clock status is warning, do not treat opportunities as eligible. Observation mode records estimates only. There is no account/balance/margin/liquidation visibility and no execution.
