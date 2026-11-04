"""SQLite persistence with additive, restart-safe schema upgrades."""
import json
import os
import sqlite3
import uuid
from decimal import Decimal
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 5
SCHEMA = """
CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS market_observations (
 id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT NOT NULL, observed_at_ms INTEGER NOT NULL,
 spot_bid TEXT, spot_ask TEXT, spot_last TEXT, spot_quote_volume TEXT,
 futures_bid TEXT, futures_ask TEXT, futures_mark TEXT, futures_index TEXT,
 futures_quote_volume TEXT, funding_rate TEXT, next_funding_time_ms INTEGER,
 open_interest TEXT, spot_depth_usdt TEXT, futures_depth_usdt TEXT,
 spot_slippage_bps TEXT, futures_slippage_bps TEXT, basis_bps TEXT,
 source TEXT NOT NULL DEFAULT 'binance_rest', UNIQUE(symbol, observed_at_ms));
CREATE INDEX IF NOT EXISTS idx_market_symbol_time ON market_observations(symbol, observed_at_ms DESC);
CREATE INDEX IF NOT EXISTS idx_market_time ON market_observations(observed_at_ms DESC);
CREATE TABLE IF NOT EXISTS funding_observations (
 id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT NOT NULL, funding_time_ms INTEGER NOT NULL,
 funding_rate TEXT NOT NULL, settlement_mark_price TEXT, rate_type TEXT,
 observed_at_ms INTEGER NOT NULL, source TEXT NOT NULL DEFAULT 'binance_fapi_fundingRate');
CREATE INDEX IF NOT EXISTS idx_funding_symbol_time ON funding_observations(symbol, funding_time_ms DESC);
CREATE TABLE IF NOT EXISTS funding_observation_sources (
 funding_observation_id INTEGER NOT NULL REFERENCES funding_observations(id) ON DELETE CASCADE,
 source TEXT NOT NULL, observed_at_ms INTEGER NOT NULL,
 PRIMARY KEY(funding_observation_id, source, observed_at_ms));
CREATE TABLE IF NOT EXISTS exchange_symbol_metadata (
 market TEXT NOT NULL, symbol TEXT NOT NULL, updated_at_ms INTEGER NOT NULL,
 metadata_json TEXT NOT NULL, PRIMARY KEY(market, symbol));
CREATE TABLE IF NOT EXISTS market_data_events (
 id INTEGER PRIMARY KEY AUTOINCREMENT, market TEXT NOT NULL, symbol TEXT NOT NULL,
 stream TEXT NOT NULL, event_time_ms INTEGER NOT NULL, received_at_ms INTEGER NOT NULL,
 payload_json TEXT NOT NULL, UNIQUE(market, symbol, stream, event_time_ms));
CREATE INDEX IF NOT EXISTS idx_market_events_symbol_time ON market_data_events(symbol, received_at_ms DESC);
CREATE TABLE IF NOT EXISTS websocket_health (
 market TEXT PRIMARY KEY, updated_at_ms INTEGER NOT NULL, connected INTEGER NOT NULL,
 streams INTEGER NOT NULL, last_event_ms INTEGER, error TEXT);
CREATE TABLE IF NOT EXISTS scanner_runs (
 id INTEGER PRIMARY KEY AUTOINCREMENT, started_at_ms INTEGER NOT NULL,
 completed_at_ms INTEGER, matched_pairs INTEGER NOT NULL DEFAULT 0,
 eligible_count INTEGER NOT NULL DEFAULT 0, rejected_count INTEGER NOT NULL DEFAULT 0,
 error TEXT);
CREATE INDEX IF NOT EXISTS idx_scanner_runs_time ON scanner_runs(started_at_ms DESC);
CREATE TABLE IF NOT EXISTS opportunity_observations (
 id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT NOT NULL, observed_at_ms INTEGER NOT NULL,
 score TEXT NOT NULL, expected_net_usdt TEXT NOT NULL, eligible INTEGER NOT NULL,
 payload_json TEXT NOT NULL, UNIQUE(symbol, observed_at_ms));
CREATE INDEX IF NOT EXISTS idx_opportunities_symbol_time ON opportunity_observations(symbol, observed_at_ms DESC);
CREATE TABLE IF NOT EXISTS opportunity_lifecycle (
 id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT NOT NULL, event_type TEXT NOT NULL,
 observed_at_ms INTEGER NOT NULL, score TEXT, expected_net_usdt TEXT);
CREATE INDEX IF NOT EXISTS idx_lifecycle_symbol_time ON opportunity_lifecycle(symbol, observed_at_ms DESC);
CREATE TABLE IF NOT EXISTS data_quality_events (
 id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT NOT NULL, event_time_ms INTEGER NOT NULL,
 reason TEXT NOT NULL, payload_json TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS idx_quality_time ON data_quality_events(event_time_ms DESC);
"""


class SQLiteMarketStore:
    def __init__(self, database_path: str, retention_days: int = 90):
        self.path = Path(database_path)
        self.retention_days = retention_days
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=15)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def _create_verified_backup(self, conn: sqlite3.Connection, destination: Path) -> str:
        """Create a non-overwriting online backup and verify it before migration."""
        destination.parent.mkdir(parents=True, exist_ok=True)
        if any(Path(f"{destination}{suffix}").exists() for suffix in ("-wal", "-shm", "-journal")):
            raise FileExistsError("backup destination SQLite sidecar already exists")
        descriptor = os.open(destination, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        os.close(descriptor)
        target = None
        try:
            target = sqlite3.connect(destination)
            conn.backup(target)
            integrity = target.execute("PRAGMA integrity_check").fetchone()
            if integrity is None or integrity[0] != "ok":
                raise sqlite3.DatabaseError("migration backup failed SQLite integrity_check")
            target.commit()
        except Exception:
            if target is not None:
                target.close()
                target = None
            destination.unlink(missing_ok=True)
            raise
        finally:
            if target is not None:
                target.close()
        return str(destination)

    def _initialize(self) -> None:
        with self._connect() as conn:
            tables = {row["name"] for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
            has_funding = "funding_observations" in tables
            has_version = "schema_version" in tables
            version_row = (conn.execute("SELECT version FROM schema_version LIMIT 1").fetchone()
                           if has_version else None)
            current_version = 0 if version_row is None else int(version_row["version"])
            columns = ({item["name"] for item in conn.execute(
                "PRAGMA table_info(funding_observations)").fetchall()} if has_funding else set())
            needs_rebuild = has_funding and (
                current_version < SCHEMA_VERSION
                or "settlement_mark_price" not in columns
                or "rate_type" not in columns
                or "funding_observation_sources" not in tables
            )

            if needs_rebuild:
                backup_path = Path(f"{self.path}.pre_v5.{uuid.uuid4().hex}.bak")
                self.last_migration_backup_path = self._create_verified_backup(conn, backup_path)
                mark_expr = "settlement_mark_price" if "settlement_mark_price" in columns else "NULL"
                type_expr = "rate_type" if "rate_type" in columns else "NULL"
                source_expr = "source" if "source" in columns else "'legacy_unknown'"
                conn.execute("BEGIN IMMEDIATE")
                try:
                    if not has_version:
                        conn.execute("CREATE TABLE schema_version (version INTEGER NOT NULL)")
                    conn.execute("""CREATE TABLE funding_observations_v5 (
                        id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT NOT NULL,
                        funding_time_ms INTEGER NOT NULL, funding_rate TEXT NOT NULL,
                        settlement_mark_price TEXT, rate_type TEXT, observed_at_ms INTEGER NOT NULL,
                        source TEXT NOT NULL DEFAULT 'binance_fapi_fundingRate')""")
                    conn.execute(f"""INSERT INTO funding_observations_v5
                        (id,symbol,funding_time_ms,funding_rate,settlement_mark_price,rate_type,
                         observed_at_ms,source)
                        SELECT id,symbol,funding_time_ms,funding_rate,{mark_expr},{type_expr},
                               observed_at_ms,{source_expr} FROM funding_observations""")
                    conn.execute("DROP TABLE funding_observations")
                    conn.execute("ALTER TABLE funding_observations_v5 RENAME TO funding_observations")
                    conn.execute("""CREATE TABLE IF NOT EXISTS funding_observation_sources (
                        funding_observation_id INTEGER NOT NULL
                            REFERENCES funding_observations(id) ON DELETE CASCADE,
                        source TEXT NOT NULL, observed_at_ms INTEGER NOT NULL,
                        PRIMARY KEY(funding_observation_id, source, observed_at_ms))""")
                    conn.execute("""INSERT OR IGNORE INTO funding_observation_sources
                        (funding_observation_id,source,observed_at_ms)
                        SELECT (SELECT MIN(c.id) FROM funding_observations c
                                WHERE c.symbol=o.symbol AND c.funding_time_ms=o.funding_time_ms
                                  AND c.funding_rate=o.funding_rate
                                  AND COALESCE(c.settlement_mark_price,'')=COALESCE(o.settlement_mark_price,'')
                                  AND COALESCE(c.rate_type,'Regular')=COALESCE(o.rate_type,'Regular')),
                               o.source,o.observed_at_ms
                        FROM funding_observations o""")
                    conn.execute("""DELETE FROM funding_observations WHERE id NOT IN (
                        SELECT MIN(id) FROM funding_observations
                        GROUP BY symbol,funding_time_ms,funding_rate,
                                 COALESCE(settlement_mark_price,''),COALESCE(rate_type,'Regular'))""")
                    conn.execute("""CREATE INDEX idx_funding_symbol_time
                        ON funding_observations(symbol, funding_time_ms DESC)""")
                    conn.execute("""CREATE UNIQUE INDEX uq_funding_economic_event
                        ON funding_observations(symbol,funding_time_ms,funding_rate,
                            COALESCE(settlement_mark_price,''),COALESCE(rate_type,'Regular'))""")
                    conn.execute("DELETE FROM schema_version")
                    conn.execute("INSERT INTO schema_version(version) VALUES (?)", (SCHEMA_VERSION,))
                    conn.commit()
                except Exception:
                    conn.rollback()
                    raise

            # Ancillary schema creation is restart-safe after the transactional migration.
            conn.executescript(SCHEMA)
            version_row = conn.execute("SELECT version FROM schema_version LIMIT 1").fetchone()
            if version_row is None:
                conn.execute("INSERT INTO schema_version(version) VALUES (?)", (SCHEMA_VERSION,))
            elif int(version_row["version"]) < SCHEMA_VERSION:
                conn.execute("UPDATE schema_version SET version=?", (SCHEMA_VERSION,))
            conn.execute("""CREATE UNIQUE INDEX IF NOT EXISTS uq_funding_economic_event
                ON funding_observations(symbol, funding_time_ms, funding_rate,
                    COALESCE(settlement_mark_price, ''), COALESCE(rate_type, 'Regular'))""")

    def backup_to(self, destination_path: str) -> str:
        """Create a consistent, verified online backup without overwriting a prior file."""
        destination = Path(destination_path)
        if destination.resolve() == self.path.resolve():
            raise ValueError("backup destination must differ from the active database")
        source = self._connect()
        try:
            return self._create_verified_backup(source, destination)
        finally:
            source.close()

    @staticmethod
    def restore_from_backup(backup_path: str, database_path: str) -> "SQLiteMarketStore":
        """Restore atomically to a new path; never overwrite a database or sidecar."""
        source_path, destination = Path(backup_path), Path(database_path)
        if source_path.resolve() == destination.resolve():
            raise ValueError("backup and restore paths must differ")
        if not source_path.is_file():
            raise FileNotFoundError(f"SQLite backup does not exist: {source_path}")
        sidecars = (Path(f"{destination}-wal"), Path(f"{destination}-shm"),
                    Path(f"{destination}-journal"))
        if destination.exists() or any(path.exists() for path in sidecars):
            raise FileExistsError("restore destination or SQLite sidecar already exists")
        source = sqlite3.connect(source_path)
        temp_path = destination.with_name(f"{destination.name}.restore.{uuid.uuid4().hex}.tmp")
        linked = False
        try:
            integrity = source.execute("PRAGMA integrity_check").fetchone()
            if integrity is None or integrity[0] != "ok":
                raise sqlite3.DatabaseError("backup failed SQLite integrity_check")
            tables = {row[0] for row in source.execute(
                "SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
            if "funding_observations" not in tables:
                raise sqlite3.DatabaseError("backup is missing the funding_observations table")
            destination.parent.mkdir(parents=True, exist_ok=True)
            descriptor = os.open(temp_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            os.close(descriptor)
            target = sqlite3.connect(temp_path)
            try:
                source.backup(target)
                restored_integrity = target.execute("PRAGMA integrity_check").fetchone()
                if restored_integrity is None or restored_integrity[0] != "ok":
                    raise sqlite3.DatabaseError("restored database failed SQLite integrity_check")
            finally:
                target.close()
            # Hard-link installation is atomic and fails rather than replacing a
            # destination created by another process during the restore.
            os.link(temp_path, destination)
            linked = True
        except Exception:
            if linked:
                destination.unlink(missing_ok=True)
                for sidecar in sidecars:
                    sidecar.unlink(missing_ok=True)
            raise
        finally:
            source.close()
            temp_path.unlink(missing_ok=True)
        return SQLiteMarketStore(str(destination))

    def save_observation(self, row: dict[str, Any]) -> bool:
        keys = ["symbol","observed_at_ms","spot_bid","spot_ask","spot_last","spot_quote_volume",
                "futures_bid","futures_ask","futures_mark","futures_index","futures_quote_volume",
                "funding_rate","next_funding_time_ms","open_interest","spot_depth_usdt",
                "futures_depth_usdt","spot_slippage_bps","futures_slippage_bps","basis_bps","source"]
        values = [str(row.get(k)) if row.get(k) is not None and k not in {"observed_at_ms","next_funding_time_ms"} else row.get(k) for k in keys]
        with self._connect() as conn:
            cur = conn.execute(f"INSERT OR IGNORE INTO market_observations ({','.join(keys)}) VALUES ({','.join('?' for _ in keys)})", values)
            return cur.rowcount == 1

    def save_funding_observations(self, symbol: str, rows: list[dict[str, Any]], observed_at_ms: int) -> int:
        """Persist one economic event and append source/observation provenance."""
        inserted = 0
        with self._connect() as conn:
            for row in rows:
                funding_time = row.get("fundingTime", row.get("funding_time_ms"))
                funding_rate = row.get("fundingRate", row.get("funding_rate"))
                if funding_time is None or funding_rate is None:
                    continue
                mark = row.get("markPrice", row.get("settlement_mark_price"))
                rate_type = row.get("rateType", row.get("rate_type")) or "Regular"
                source = str(row.get("source") or "binance_fapi_fundingRate")
                cur = conn.execute("""INSERT OR IGNORE INTO funding_observations
                    (symbol,funding_time_ms,funding_rate,settlement_mark_price,rate_type,
                     observed_at_ms,source) VALUES (?,?,?,?,?,?,?)""",
                    (symbol.upper(), int(funding_time), str(funding_rate),
                     None if mark is None else str(mark), str(rate_type),
                     int(observed_at_ms), source))
                event = conn.execute("""SELECT id FROM funding_observations
                    WHERE symbol=? AND funding_time_ms=? AND funding_rate=?
                      AND COALESCE(settlement_mark_price,'')=COALESCE(?,'')
                      AND COALESCE(rate_type,'Regular')=COALESCE(?,'Regular')""",
                    (symbol.upper(), int(funding_time), str(funding_rate),
                     None if mark is None else str(mark), str(rate_type))).fetchone()
                if event is None:
                    raise sqlite3.DatabaseError("funding economic event insert could not be resolved")
                conn.execute("""INSERT OR IGNORE INTO funding_observation_sources
                    (funding_observation_id,source,observed_at_ms) VALUES (?,?,?)""",
                    (int(event["id"]), source, int(observed_at_ms)))
                inserted += cur.rowcount
        return inserted

    def save_exchange_metadata(self, market: str, symbols: list[dict[str, Any]], updated_at_ms: int) -> None:
        with self._connect() as conn:
            conn.executemany("""INSERT INTO exchange_symbol_metadata(market,symbol,updated_at_ms,metadata_json)
                VALUES(?,?,?,?) ON CONFLICT(market,symbol) DO UPDATE SET
                updated_at_ms=excluded.updated_at_ms, metadata_json=excluded.metadata_json""",
                [(market, str(x["symbol"]), updated_at_ms, json.dumps(x, separators=(",", ":"), default=str))
                 for x in symbols if x.get("symbol")])

    def exchange_filter(self, market: str, symbol: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute("SELECT metadata_json FROM exchange_symbol_metadata WHERE market=? AND symbol=?",
                               (market, symbol.upper())).fetchone()
        return None if row is None else json.loads(row["metadata_json"])

    def save_market_event(self, market: str, symbol: str, stream: str, event_ms: int,
                          received_ms: int, payload: dict[str, Any]) -> bool:
        with self._connect() as conn:
            cur = conn.execute("""INSERT OR IGNORE INTO market_data_events
                (market,symbol,stream,event_time_ms,received_at_ms,payload_json) VALUES(?,?,?,?,?,?)""",
                (market, symbol.upper(), stream, event_ms, received_ms, json.dumps(payload, default=str)))
            return cur.rowcount == 1

    def save_ws_health(self, market: str, health: dict[str, Any], now_ms: int) -> None:
        with self._connect() as conn:
            conn.execute("""INSERT INTO websocket_health(market,updated_at_ms,connected,streams,last_event_ms,error)
                VALUES(?,?,?,?,?,?) ON CONFLICT(market) DO UPDATE SET updated_at_ms=excluded.updated_at_ms,
                connected=excluded.connected,streams=excluded.streams,last_event_ms=excluded.last_event_ms,error=excluded.error""",
                (market, now_ms, int(bool(health.get("connected"))), int(health.get("streams", 0)),
                 health.get("last_event_ms"), health.get("error")))

    def websocket_health(self) -> list[dict[str, Any]]:
        with self._connect() as conn:
            return [dict(x) for x in conn.execute("SELECT * FROM websocket_health ORDER BY market").fetchall()]

    def save_scanner_run(self, started_ms: int, completed_ms: int, matched: int,
                         eligible: int, rejected: int, error: str | None) -> None:
        with self._connect() as conn:
            conn.execute("""INSERT INTO scanner_runs(started_at_ms,completed_at_ms,matched_pairs,
                eligible_count,rejected_count,error) VALUES(?,?,?,?,?,?)""",
                (started_ms, completed_ms, matched, eligible, rejected, error))

    def save_opportunities(self, rows: list[dict[str, Any]], observed_at_ms: int) -> None:
        with self._connect() as conn:
            for row in rows:
                conn.execute("""INSERT OR IGNORE INTO opportunity_observations
                    (symbol,observed_at_ms,score,expected_net_usdt,eligible,payload_json)
                    VALUES(?,?,?,?,?,?)""",
                    (row["symbol"], observed_at_ms, str(row.get("score", "0")),
                     str(row.get("estimated_net_usdt", "0")), 1, json.dumps(row, default=str)))

    def save_opportunity_lifecycle(self, symbol: str, event_type: str, observed_at_ms: int,
                                  score: str | None = None, expected_net_usdt: str | None = None) -> None:
        with self._connect() as conn:
            conn.execute("""INSERT INTO opportunity_lifecycle
                (symbol,event_type,observed_at_ms,score,expected_net_usdt) VALUES(?,?,?,?,?)""",
                (symbol.upper(), event_type, observed_at_ms, score, expected_net_usdt))

    def observation_summary(self) -> dict[str, Any]:
        with self._connect() as conn:
            total = conn.execute("SELECT COUNT(*) FROM opportunity_observations").fetchone()[0]
            events = conn.execute("SELECT COUNT(*) FROM opportunity_lifecycle").fetchone()[0]
            latest = conn.execute("SELECT MAX(observed_at_ms) FROM opportunity_observations").fetchone()[0]
            active = conn.execute("""SELECT COUNT(*) FROM (
                SELECT symbol, MAX(observed_at_ms) ts FROM opportunity_observations GROUP BY symbol)""").fetchone()[0]
            rows = conn.execute("""SELECT score, expected_net_usdt, payload_json
                FROM opportunity_observations ORDER BY observed_at_ms DESC LIMIT 1000""").fetchall()
        scores = [Decimal(str(x["score"])) for x in rows]
        net = [Decimal(str(x["expected_net_usdt"])) for x in rows]
        funding = []
        basis = []
        for row in rows:
            payload = json.loads(row["payload_json"])
            if payload.get("funding_rate") is not None:
                funding.append(Decimal(str(payload["funding_rate"])))
            if payload.get("basis_bps") is not None:
                basis.append(Decimal(str(payload["basis_bps"])))
        count = Decimal(len(rows)) if rows else Decimal("1")
        return {"opportunity_snapshots": int(total), "lifecycle_events": int(events),
                "symbols_observed": int(active), "last_observation_ms": latest,
                "recent_sample_count": len(rows),
                "average_score_recent": str(sum(scores, Decimal("0"))/count) if scores else None,
                "average_estimated_net_usdt_recent": str(sum(net, Decimal("0"))/count) if net else None,
                "average_funding_rate_recent": str(sum(funding, Decimal("0"))/Decimal(len(funding))) if funding else None,
                "average_basis_bps_recent": str(sum(basis, Decimal("0"))/Decimal(len(basis))) if basis else None}

    def save_quality_event(self, event: dict[str, Any], event_time_ms: int) -> None:
        with self._connect() as conn:
            conn.execute("""INSERT INTO data_quality_events(symbol,event_time_ms,reason,payload_json)
                VALUES(?,?,?,?)""",
                (str(event.get("symbol", "UNKNOWN")), event_time_ms,
                 str(event.get("reason", "unspecified")), json.dumps(event, default=str)))

    def rejections(self, limit: int = 100) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute("SELECT * FROM data_quality_events ORDER BY event_time_ms DESC LIMIT ?",
                                (limit,)).fetchall()
        return [dict(x) for x in rows]

    def latest_funding_time(self, symbol: str) -> int | None:
        with self._connect() as conn:
            row = conn.execute("SELECT MAX(funding_time_ms) AS ts FROM funding_observations WHERE symbol=?",
                               (symbol.upper(),)).fetchone()
        return None if row is None or row["ts"] is None else int(row["ts"])

    def funding_history(self, symbol: str, limit: int = 100, offset: int = 0) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute("""SELECT id,symbol,funding_time_ms,funding_rate,settlement_mark_price,
                rate_type,observed_at_ms,source FROM funding_observations WHERE symbol=?
                ORDER BY funding_time_ms DESC,rate_type,settlement_mark_price LIMIT ? OFFSET ?""",
                (symbol.upper(), limit, offset)).fetchall()
            result = []
            for row in rows:
                item = dict(row)
                provenance = conn.execute("""SELECT source,observed_at_ms FROM funding_observation_sources
                    WHERE funding_observation_id=? ORDER BY observed_at_ms,source""",
                    (int(row["id"]),)).fetchall()
                item["source_observations"] = [dict(source_row) for source_row in provenance]
                item["sources"] = sorted({source_row["source"] for source_row in provenance}) or [item["source"]]
                result.append(item)
        return result

    def funding_history_conflicts(self, symbol: str) -> list[dict[str, Any]]:
        """Report same-time, same-rateType economic payload disagreements only."""
        with self._connect() as conn:
            rows = conn.execute("""SELECT funding_time_ms,funding_rate,settlement_mark_price,rate_type
                FROM funding_observations WHERE symbol=? ORDER BY funding_time_ms DESC""",
                (symbol.upper(),)).fetchall()
        variants: dict[tuple[int, str], set[tuple[str, str]]] = {}
        for row in rows:
            key = (int(row["funding_time_ms"]), str(row["rate_type"] or "Regular"))
            payload = (str(row["funding_rate"]), str(row["settlement_mark_price"] or ""))
            variants.setdefault(key, set()).add(payload)
        conflicts: dict[int, int] = {}
        for (stamp, _event_type), payloads in variants.items():
            if len(payloads) > 1:
                conflicts[stamp] = conflicts.get(stamp, 0) + len(payloads)
        return [{"funding_time_ms": stamp, "record_count": count}
                for stamp, count in sorted(conflicts.items(), reverse=True)]

    def history(self, symbol: str, limit: int = 100, offset: int = 0) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute("SELECT * FROM market_observations WHERE symbol=? ORDER BY observed_at_ms DESC LIMIT ? OFFSET ?",
                                (symbol.upper(), limit, offset)).fetchall()
        return [dict(row) for row in rows]

    def latest(self, limit: int = 100, offset: int = 0) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute("""SELECT m.* FROM market_observations m
                JOIN (SELECT symbol, MAX(observed_at_ms) ts FROM market_observations GROUP BY symbol) x
                ON m.symbol=x.symbol AND m.observed_at_ms=x.ts ORDER BY m.symbol LIMIT ? OFFSET ?""",
                (limit, offset)).fetchall()
        return [dict(row) for row in rows]

    def profitability_history(self, symbol: str, limit: int = 100) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute("""SELECT * FROM opportunity_observations WHERE symbol=?
                ORDER BY observed_at_ms DESC LIMIT ?""", (symbol.upper(), limit)).fetchall()
        return [dict(x) for x in rows]

    def prune(self, now_ms: int) -> int:
        cutoff = now_ms - self.retention_days * 86400000
        with self._connect() as conn:
            a = conn.execute("DELETE FROM market_observations WHERE observed_at_ms < ?", (cutoff,)).rowcount
            conn.execute("DELETE FROM funding_observations WHERE funding_time_ms < ?", (cutoff,))
            conn.execute("DELETE FROM market_data_events WHERE received_at_ms < ?", (cutoff,))
            conn.execute("DELETE FROM opportunity_observations WHERE observed_at_ms < ?", (cutoff,))
            conn.execute("DELETE FROM opportunity_lifecycle WHERE observed_at_ms < ?", (cutoff,))
            conn.execute("DELETE FROM data_quality_events WHERE event_time_ms < ?", (cutoff,))
            return a

    def count(self) -> int:
        with self._connect() as conn:
            return int(conn.execute("SELECT COUNT(*) FROM market_observations").fetchone()[0])
