"""SQLite persistence with additive, restart-safe schema upgrades."""
import json
import sqlite3
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 2
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
 funding_rate TEXT NOT NULL, observed_at_ms INTEGER NOT NULL, source TEXT NOT NULL DEFAULT 'binance_rest',
 UNIQUE(symbol, funding_time_ms));
CREATE INDEX IF NOT EXISTS idx_funding_symbol_time ON funding_observations(symbol, funding_time_ms DESC);
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

    def _initialize(self) -> None:
        with self._connect() as conn:
            conn.executescript(SCHEMA)
            row = conn.execute("SELECT version FROM schema_version LIMIT 1").fetchone()
            if row is None:
                conn.execute("INSERT INTO schema_version(version) VALUES (?)", (SCHEMA_VERSION,))
            elif int(row["version"]) < SCHEMA_VERSION:
                # All changes in v2 are additive CREATE TABLE/INDEX operations above.
                conn.execute("UPDATE schema_version SET version=?", (SCHEMA_VERSION,))

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
        inserted = 0
        with self._connect() as conn:
            for row in rows:
                if "fundingTime" not in row or "fundingRate" not in row:
                    continue
                cur = conn.execute("""INSERT OR IGNORE INTO funding_observations
                    (symbol,funding_time_ms,funding_rate,observed_at_ms,source) VALUES (?,?,?,?,?)""",
                    (symbol.upper(), int(row["fundingTime"]), str(row["fundingRate"]), observed_at_ms, "binance_rest"))
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

    def funding_history(self, symbol: str, limit: int = 100, offset: int = 0) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute("""SELECT symbol,funding_time_ms,funding_rate,observed_at_ms,source
                FROM funding_observations WHERE symbol=? ORDER BY funding_time_ms DESC LIMIT ? OFFSET ?""",
                (symbol.upper(), limit, offset)).fetchall()
        return [dict(row) for row in rows]

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
            conn.execute("DELETE FROM data_quality_events WHERE event_time_ms < ?", (cutoff,))
            return a

    def count(self) -> int:
        with self._connect() as conn:
            return int(conn.execute("SELECT COUNT(*) FROM market_observations").fetchone()[0])
