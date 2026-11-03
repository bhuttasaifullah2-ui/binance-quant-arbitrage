# Phase 2.2.2 — Ubuntu VPS deployment runbook (public read-only)

**Target application commit:** `3d0c5d27f5520827388ea70761d6f9232416a993`  
**Existing PR:** #5 — must remain open and unmerged  
**Purpose:** prepare a single-process, read-only public market-data observer. This document does not assert that a VPS has been deployed or that Binance connectivity has been tested.

## 0. Safety boundary and stop conditions

This application build is intended to use public, unauthenticated Binance Spot and USDⓈ-M Futures market-data endpoints. The REST client uses HTTP GET requests and the public WebSocket manager uses public stream URLs. It does not require Binance API keys.

Do not add credentials, API keys, secrets, account endpoints, balances, positions, margin/leverage operations, orders, cancellations, transfers or withdrawals. Do not enable paper trading or Phase 3. Keep `LIVE_TRADING_ENABLED=false`.

**Stop before starting the service** if any of these are true:

- The checked-out Git SHA is not the target SHA above.
- CI for the target SHA is not successful on Python 3.12 and 3.13.
- The source or configuration differs from the reviewed target commit.
- The service cannot be constrained to one Uvicorn worker on loopback only.
- The environment file contains unreviewed keys or credentials.
- SQLite's database directory is not writable by the service user.
- A firewall or network configuration would expose TCP/8000 to the public internet.

The API includes POST validation-trigger routes. They are not trading routes, but **do not publish or proxy the API publicly**. Keep the listener on `127.0.0.1`; use an SSH tunnel for operator access. No authentication middleware is included in this phase.

## 1. Reviewed repository facts

The reviewed source uses:

- Python `>=3.12`; CI matrix covers 3.12 and 3.13.
- Dependencies from `pyproject.toml`; install the editable project with the `dev` extra for pre-deployment checks.
- Settings loaded by Pydantic Settings from environment variables / `.env`, with `extra="ignore"`.
- SQLite path from `DATABASE_PATH`; session persistence and market observations share the configured database.
- A single FastAPI lifespan which starts the scanner, optional public WebSocket supervisor and optional periodic public validation.
- Uvicorn invoked directly with `--host 127.0.0.1 --port 8000`; do not add `--workers`, Gunicorn workers, multiple systemd instances or a second copy of the service.

The application currently reports version `0.4.0` in the FastAPI object. The Python package metadata in `pyproject.toml` reports `0.3.0`. Record both rather than treating them as the same version.

## 2. Ubuntu host preparation

These commands target Ubuntu 24.04 LTS, where Python 3.12 is available from the standard Ubuntu repositories. Run as an administrator with sudo access.

```bash
sudo apt-get update
sudo apt-get install -y git ca-certificates curl python3.12 python3.12-venv python3.12-dev build-essential sqlite3
python3.12 --version
git --version
sqlite3 --version
```

Do not install or configure a desktop, database server, reverse proxy or container runtime for this runbook. A reverse proxy is deliberately not used because the API has no authentication middleware.

## 3. Dedicated service identity and directories

Create a system account with no interactive login. These commands are safe to re-run:

```bash
if ! id quantbot >/dev/null 2>&1; then
  sudo useradd --system --create-home --home-dir /var/lib/quantbot --shell /usr/sbin/nologin quantbot
fi
sudo install -d -o root -g root -m 0755 /opt/binance-quant-arbitrage
sudo install -d -o quantbot -g quantbot -m 0750 /var/lib/binance-quant-arbitrage
sudo install -d -o root -g root -m 0755 /opt/releases
```

The code checkout and virtual environment are root-owned and not writable by the service. Only the persistent data directory is writable by `quantbot`.

## 4. Fetch and verify the exact target commit

Use the existing PR feature branch as the fetch source, but check out the exact immutable commit in detached-HEAD mode. This avoids accidentally deploying a later branch head.

```bash
sudo git clone --no-checkout https://github.com/bhuttasaifullah2-ui/binance-quant-arbitrage.git /opt/binance-quant-arbitrage
sudo git -C /opt/binance-quant-arbitrage fetch --no-tags origin feature/phase-2-2-live-validation
sudo git -C /opt/binance-quant-arbitrage checkout --detach 3d0c5d27f5520827388ea70761d6f9232416a993
sudo git -C /opt/binance-quant-arbitrage rev-parse HEAD
sudo git -C /opt/binance-quant-arbitrage status --short
```

The `rev-parse` output must be exactly:

```text
3d0c5d27f5520827388ea70761d6f9232416a993
```

The working tree should be clean. If the fetch or checkout fails, stop. Do not substitute the branch's current head or another SHA.

## 5. Create the Python environment and run pre-deployment checks

```bash
sudo python3.12 -m venv /opt/binance-quant-arbitrage/.venv
sudo /opt/binance-quant-arbitrage/.venv/bin/python -m pip install --upgrade pip
sudo /opt/binance-quant-arbitrage/.venv/bin/python -m pip install -e '/opt/binance-quant-arbitrage[dev]'
cd /opt/binance-quant-arbitrage
sudo .venv/bin/python -m compileall -q app tests
sudo .venv/bin/ruff check app tests
sudo .venv/bin/pytest -q
```

Do not continue if installation, compile, lint or tests fail. The expected test result for this reviewed revision is the successful CI result; CI has reported 67 passed and 2 skipped for each supported Python version. The skipped tests are opt-in integration tests and are not evidence of live connectivity. A local VPS test run may differ by environment; record its actual output.

## 6. Configuration — exact source-backed variable names

The names below are defined by `app/core/config.py` (Pydantic Settings maps the uppercase environment names to the lowercase field names). The service command sets the listener address directly; there is no `HOST` or `PORT` setting in the reviewed Settings class.

### Required minimal observation environment

Create `/etc/binance-quant-arbitrage.env` with only these reviewed settings:

```ini
APP_ENV=production
APP_MODE=RESEARCH
DATABASE_PATH=/var/lib/binance-quant-arbitrage/market_data.sqlite3
RETENTION_DAYS=30
WEBSOCKET_ENABLED=true
OBSERVATION_MODE=true
LIVE_VALIDATION_ENABLED=true
LIVE_TRADING_ENABLED=false
VALIDATION_INTERVAL_SECONDS=300
REQUEST_TIMEOUT_SECONDS=10
REQUEST_MAX_RETRIES=4
MAX_CLOCK_DRIFT_MS=5000
MAX_VALIDATION_MESSAGE_AGE_MS=15000
WEBSOCKET_PROBE_TIMEOUT_SECONDS=5
MAX_RECONNECT_ATTEMPTS=10
MAX_RECOVERY_DURATION_MS=60000
MAX_MESSAGE_PROCESSING_LATENCY_MS=100
MAX_STALE_SYMBOLS=5
MAX_CLOCK_SYNC_AGE_SECONDS=300
```

Install it with restrictive permissions:

```bash
sudo install -o root -g quantbot -m 0640 /dev/null /etc/binance-quant-arbitrage.env
sudoedit /etc/binance-quant-arbitrage.env
sudo chown root:quantbot /etc/binance-quant-arbitrage.env
sudo chmod 0640 /etc/binance-quant-arbitrage.env
```

Do not put Binance API keys or secrets in this file. Do not add variables copied from unrelated bot projects. In particular, there is no need to add a key, secret, account mode, leverage, margin, order or trading-enable credential.

### Optional source-defined settings

Other accepted settings are declared in `app/core/config.py` and mirrored in `.env.example`, including the public Spot/Futures REST and WebSocket base URLs, scanner thresholds, fees used in estimates, depth/history limits, concurrency, funding reconciliation tolerance, and WebSocket stream shard size. Keep the reviewed defaults unless a separate, documented review approves a change. Do not adjust thresholds to improve eligibility statistics.

### Verify loaded values without printing secrets

The reviewed configuration contains no secret fields. This command prints only the safe deployment values needed for operator verification:

```bash
cd /opt/binance-quant-arbitrage
sudo -u quantbot env $(sudo cat /etc/binance-quant-arbitrage.env | grep -v '^[[:space:]]*#' | grep -v '^[[:space:]]*$' | xargs) \
  /opt/binance-quant-arbitrage/.venv/bin/python -c 'from app.core.config import settings; print({"app_env":settings.app_env,"app_mode":settings.app_mode,"database_path":settings.database_path,"websocket_enabled":settings.websocket_enabled,"observation_mode":settings.observation_mode,"live_validation_enabled":settings.live_validation_enabled,"live_trading_enabled":settings.live_trading_enabled,"validation_interval_seconds":settings.validation_interval_seconds})'
```

Expected values include `websocket_enabled=True`, `observation_mode=True`, `live_validation_enabled=True`, and `live_trading_enabled=False`, with the database path exactly as shown above. If the command fails, do not start the service. The command is intended for this simple key=value file; do not use it if values contain shell-special characters.

## 7. SQLite directory and permissions

The configured database is `/var/lib/binance-quant-arbitrage/market_data.sqlite3`. SQLite may also create `-wal` and `-shm` companion files in that same directory.

```bash
sudo chown -R quantbot:quantbot /var/lib/binance-quant-arbitrage
sudo chmod 0750 /var/lib/binance-quant-arbitrage
sudo -u quantbot test -w /var/lib/binance-quant-arbitrage
```

Do not place the database inside the read-only application checkout. Do not run a second app instance against this database. The session store is designed for one application process; multiple Uvicorn workers or duplicate services can interfere with session lifecycle and SQLite writes.

## 8. Exact systemd unit

Create `/etc/systemd/system/binance-quant-arbitrage.service`:

```ini
[Unit]
Description=Binance Quant Arbitrage — public read-only observer
Wants=network-online.target
After=network-online.target

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
```

Install and inspect the unit:

```bash
sudo systemctl daemon-reload
sudo systemd-analyze verify /etc/systemd/system/binance-quant-arbitrage.service
sudo systemctl enable binance-quant-arbitrage
```

There is deliberately no `--workers` option. Do not add one. Do not run Uvicorn manually while systemd is managing the service.

## 9. Network exposure controls

The API must listen on loopback only. Confirm this in the unit's `ExecStart` and later with `ss`.

If UFW is already enabled, ensure TCP/8000 is not allowed from external interfaces. Do not enable UFW remotely until SSH access is explicitly allowed and the operator understands the impact.

```bash
sudo ufw status verbose
sudo ufw deny 8000/tcp
sudo ss -ltnp | grep ':8000'
```

Expected listener address: `127.0.0.1:8000`. A listener on `0.0.0.0:8000`, `[::]:8000` or a public IP is a stop condition. Do not create a public reverse-proxy route to this API. For remote operator access, use an SSH tunnel from the operator workstation:

```bash
ssh -L 8000:127.0.0.1:8000 <operator-user>@<vps-host>
```

Then access `http://127.0.0.1:8000` on the operator workstation while the tunnel is active.

## 10. Start, inspect and stop

Start only after all preceding checks pass:

```bash
sudo systemctl start binance-quant-arbitrage
sudo systemctl status binance-quant-arbitrage --no-pager
sudo journalctl -u binance-quant-arbitrage -n 100 --no-pager
sudo ss -ltnp | grep ':8000'
```

Useful operations:

```bash
sudo systemctl stop binance-quant-arbitrage
sudo systemctl restart binance-quant-arbitrage
sudo systemctl status binance-quant-arbitrage --no-pager
sudo journalctl -u binance-quant-arbitrage -f
sudo journalctl -u binance-quant-arbitrage --since '1 hour ago' --no-pager
```

A normal `systemctl stop` sends SIGINT and should allow graceful shutdown; the active session should be marked `COMPLETED`. A subsequent start creates a different session UUID. Do not use `kill -9` for routine restart testing.

## 11. Local smoke-test checklist

Run these commands on the VPS or through the SSH tunnel. They use GET endpoints only.

```bash
curl --fail --silent --show-error http://127.0.0.1:8000/api/v1/health
curl --fail --silent --show-error http://127.0.0.1:8000/api/v1/validation/summary
curl --fail --silent --show-error http://127.0.0.1:8000/api/v1/validation/connectivity
curl --fail --silent --show-error http://127.0.0.1:8000/api/v1/validation/websocket
curl --fail --silent --show-error http://127.0.0.1:8000/api/v1/validation/clock
curl --fail --silent --show-error http://127.0.0.1:8000/api/v1/validation/data-quality
curl --fail --silent --show-error http://127.0.0.1:8000/api/v1/validation/funding
curl --fail --silent --show-error http://127.0.0.1:8000/api/v1/observation/sessions
```

Expected response characteristics (not fixed numeric values):

- `/api/v1/health`: JSON with `status` (`ok` or `degraded`), mode, `live_trading_enabled:false`, `execution_enabled:false`, and scanner status.
- `/api/v1/validation/summary`: validation summary; initial checks may report `NOT_RUN` or warning/unhealthy while feeds initialize.
- `/api/v1/validation/connectivity`: public Spot/Futures REST checks; successful checks report `HEALTHY` and latency; failures must remain visible.
- `/api/v1/validation/websocket`: probe/stream status; do not assume healthy merely because the process started.
- `/api/v1/validation/clock`: Spot/Futures offsets and statuses; missing or unhealthy clock evidence is not a pass.
- `/api/v1/validation/data-quality`: current quality classifications; stale/missing/invalid data must be investigated.
- `/api/v1/validation/funding`: funding reconciliation state; unresolved gaps remain reported, not filled with synthetic zero values.
- `/api/v1/observation/sessions`: list containing the new session UUID and UTC start timestamp.

The service also has POST validation-trigger routes. Do not invoke them as part of this read-only smoke test; periodic validation is enabled by the service configuration. Never expose those POST routes to a public network.

Record the exact UTC start time, Git SHA, Python version, package version, FastAPI version, configuration flags, session UUID and all actual responses. Run the 30–60 minute observation only after startup checks are reviewed. Do not infer missing measurements or claim a healthy stream from an empty report.

## 12. Session JSON evidence export

After identifying the session UUID from `/api/v1/observation/sessions`, set it explicitly:

```bash
SESSION_ID='<paste-session-uuid-here>'
curl --fail --silent --show-error \
  "http://127.0.0.1:8000/api/v1/observation/sessions/$SESSION_ID/report.json" \
  | sudo -u quantbot tee "/var/lib/binance-quant-arbitrage/session-$SESSION_ID.json" >/dev/null
sudo chmod 0600 "/var/lib/binance-quant-arbitrage/session-$SESSION_ID.json"
```

The export is tied to the selected session and includes metadata, observations, events, data-quality decisions, eligibility decisions, opportunity snapshots and summary counts. Missing data is not evidence of health. WebSocket uptime may be null when complete boundaries are not available.

Verify the file parses and identify its session:

```bash
sudo -u quantbot /opt/binance-quant-arbitrage/.venv/bin/python -m json.tool \
  "/var/lib/binance-quant-arbitrage/session-$SESSION_ID.json" >/dev/null
sudo -u quantbot /opt/binance-quant-arbitrage/.venv/bin/python -c \
  'import json,sys; d=json.load(open(sys.argv[1])); print({"session_id":d["session"]["session_id"],"status":d["session"]["status"],"started_at_utc":d["session"]["started_at_utc"],"ended_at_utc":d["session"]["ended_at_utc"],"summary":d["summary"]})' \
  "/var/lib/binance-quant-arbitrage/session-$SESSION_ID.json"
```

Transfer evidence securely using SCP over SSH, not public web hosting or an unauthenticated endpoint:

```bash
scp <operator-user>@<vps-host>:/var/lib/binance-quant-arbitrage/session-<session-uuid>.json ./evidence/
```

Before sharing logs or reports, inspect them for hostnames, public IPs, usernames, filesystem paths or other operational details. Do not share environment files, SSH keys, credentials or private system logs. Preserve the original JSON unchanged; create a separate redacted copy if needed.

## 13. Database integrity and backup

Stop the service before making a simple file copy so SQLite has no active writer:

```bash
sudo systemctl stop binance-quant-arbitrage
sudo sqlite3 /var/lib/binance-quant-arbitrage/market_data.sqlite3 'PRAGMA integrity_check;'
sudo install -d -o root -g root -m 0700 /var/backups/binance-quant-arbitrage
sudo cp --preserve=mode,timestamps \
  /var/lib/binance-quant-arbitrage/market_data.sqlite3 \
  /var/backups/binance-quant-arbitrage/market_data-$(date -u +%Y%m%dT%H%M%SZ).sqlite3
sudo chmod 0600 /var/backups/binance-quant-arbitrage/*.sqlite3
sudo systemctl start binance-quant-arbitrage
```

Expected integrity result is `ok`. If it is not, keep the service stopped and preserve the original database and any WAL/SHM companions for investigation. Do not delete WAL/SHM files to try to repair the database.

For an online backup while the service is running, use SQLite's backup API rather than copying only the main database file:

```bash
sudo -u quantbot /opt/binance-quant-arbitrage/.venv/bin/python - <<'PY'
import sqlite3
source = sqlite3.connect("/var/lib/binance-quant-arbitrage/market_data.sqlite3")
target = sqlite3.connect("/var/lib/binance-quant-arbitrage/market_data-backup.sqlite3")
with target:
    source.backup(target)
target.close()
source.close()
PY
sudo chmod 0600 /var/lib/binance-quant-arbitrage/market_data-backup.sqlite3
```

After a backup, check it independently:

```bash
sudo sqlite3 /var/lib/binance-quant-arbitrage/market_data-backup.sqlite3 'PRAGMA integrity_check;'
```

Keep encrypted off-host backups under the operator's normal secure backup policy. Do not transfer the SQLite database to a public location.

## 14. Restart recovery test

First perform a graceful restart:

1. Record the active session UUID and start timestamp.
2. Run `sudo systemctl restart binance-quant-arbitrage`.
3. Query `GET /api/v1/observation/sessions`.
4. Confirm the prior session status is `COMPLETED`.
5. Confirm a new UUID and a later UTC start timestamp.
6. Export both sessions and verify their `session_id` values and records are isolated.

Unexpected termination is a separate test. It should be performed only after a successful smoke test, with a database backup and explicit operator approval. Prefer a controlled SIGKILL test in a maintenance window; it is not a routine restart method. After the process is confirmed stopped, start the service normally. The next startup should mark the prior still-`RUNNING` session `INTERRUPTED`, set its detection timestamp, and link it to the new session. The exact crash instant is unknowable. Do not manufacture a crash timestamp or interpret the detection timestamp as the actual failure time.

## 15. Rollback

For an application rollback, stop the service first and preserve the current database and evidence exports.

```bash
sudo systemctl stop binance-quant-arbitrage
sudo cp -a /opt/binance-quant-arbitrage /opt/releases/binance-quant-arbitrage-before-rollback-$(date -u +%Y%m%dT%H%M%SZ)
```

Restore the previously approved application checkout from the operator's recorded release backup (do not guess a commit), reinstall its locked/reviewed dependencies in its own virtual environment, and run its documented tests before starting it. Keep the existing SQLite database unless a tested migration/rollback plan explicitly requires otherwise; do not replace it with an older database copy just to roll back application code.

If rollback requires restoring a database backup, stop the service, preserve the current database plus WAL/SHM files, verify the chosen backup with `PRAGMA integrity_check`, then restore it with the service stopped and reset ownership:

```bash
sudo chown quantbot:quantbot /var/lib/binance-quant-arbitrage/market_data.sqlite3
sudo chmod 0640 /var/lib/binance-quant-arbitrage/market_data.sqlite3
```

Start the service and repeat the local health/session checks. Keep the failed release and its logs for diagnosis. Do not deploy an untested rollback target.

## 16. Operator evidence checklist and acceptance record

Preserve the following, all tied to the exact deployed SHA and UTC timestamps:

- `git rev-parse HEAD`, clean/dirty working-tree result and PR #5 state.
- Python version, package metadata version and FastAPI app version.
- Sanitized effective configuration (the flags and database path only; never secrets).
- systemd unit, service status and listener binding showing `127.0.0.1:8000`.
- Startup health and validation endpoint responses.
- Session UUID, UTC start/end, session JSON export and SHA-256 checksum.
- REST check counts, statuses and latency observations.
- WebSocket connection/disconnection/reconnect events and measured recovery intervals; do not invent uptime.
- Clock offset measurements, per-symbol freshness and quality classifications.
- Funding reconciliation results and unresolved gaps.
- Scanner eligibility/rejection decisions and opportunity snapshots.
- Graceful restart and, if approved, unexpected-termination recovery evidence.
- SQLite `PRAGMA integrity_check` result and backup verification.
- Incident log: outages, DNS failures, rate limits, stale symbols, missing observations, database errors and operator actions.

Compute an export checksum:

```bash
sha256sum "/var/lib/binance-quant-arbitrage/session-$SESSION_ID.json"
```

Do not move to 24–72-hour extended observation until the 30–60 minute smoke-test evidence has been reviewed. Do not change strategy thresholds to improve acceptance rates.

## 17. Known limitations

- This is preparation only. The operator must execute every command and provide sanitized evidence.
- Successful CI does not establish VPS connectivity, market-data freshness or live reliability.
- Public Binance services, DNS, routing, VPS clock discipline and rate limits are external dependencies.
- No account state, balances, positions, margin, liquidation, account fee tier or fills are queried.
- Session restart detection occurs on the next process startup; the precise crash time is unavailable.
- Session WebSocket uptime is not calculated when complete connection-state boundaries are missing.
- Scanner session eligibility captures the final eligibility decisions; early pre-filter rejections may remain only in legacy scanner rejection evidence.
- SQLite is single-process oriented. Do not use multiple Uvicorn workers or duplicate service instances.
- The API has no authentication middleware. Loopback binding and SSH tunnelling are mandatory for this runbook.
- No paper trading, execution or Phase 3 functionality is part of this phase.
