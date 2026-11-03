"""Restart-safe persistence for read-only validation and observation events."""
import json
import sqlite3
from pathlib import Path
from typing import Any


class ValidationStore:
    def __init__(self, path: str, retention_days: int = 90) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.retention_days = max(1, retention_days)
        with self._connect() as db:
            db.executescript("""
            CREATE TABLE IF NOT EXISTS validation_samples(
              id INTEGER PRIMARY KEY, category TEXT NOT NULL, name TEXT NOT NULL,
              checked_at_ms INTEGER NOT NULL, status TEXT NOT NULL, payload TEXT NOT NULL);
            CREATE INDEX IF NOT EXISTS ix_validation_time ON validation_samples(category, checked_at_ms);
            CREATE TABLE IF NOT EXISTS observation_events(
              id INTEGER PRIMARY KEY, event_at_ms INTEGER NOT NULL, event_type TEXT NOT NULL,
              symbol TEXT, status TEXT, payload TEXT NOT NULL);
            CREATE INDEX IF NOT EXISTS ix_observation_events_time ON observation_events(event_at_ms);
            CREATE TABLE IF NOT EXISTS opportunity_state(symbol TEXT PRIMARY KEY,state_json TEXT NOT NULL,updated_at_ms INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS opportunity_snapshots(id INTEGER PRIMARY KEY,symbol TEXT NOT NULL,observed_at_ms INTEGER NOT NULL,rank INTEGER,payload TEXT NOT NULL,UNIQUE(symbol,observed_at_ms));
            CREATE INDEX IF NOT EXISTS ix_opportunity_snapshots_time ON opportunity_snapshots(observed_at_ms);
            """)
    def _connect(self) -> sqlite3.Connection:
        db=sqlite3.connect(self.path, timeout=20)
        db.row_factory=sqlite3.Row
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA synchronous=FULL")
        return db
    def record(self, category: str, name: str, checked_at_ms: int, status: str,
               payload: dict[str, Any]) -> None:
        with self._connect() as db:
            db.execute("INSERT INTO validation_samples(category,name,checked_at_ms,status,payload) VALUES(?,?,?,?,?)",
                       (category,name,checked_at_ms,status,json.dumps(payload,default=str)))
    def event(self, event_at_ms: int, event_type: str, symbol: str | None,
              status: str | None, payload: dict[str, Any]) -> None:
        with self._connect() as db:
            db.execute("INSERT INTO observation_events(event_at_ms,event_type,symbol,status,payload) VALUES(?,?,?,?,?)",
                       (event_at_ms,event_type,symbol,status,json.dumps(payload,default=str)))
    def latest(self, category: str | None = None) -> list[dict[str, Any]]:
        with self._connect() as db:
            if category:
                rows=db.execute("""SELECT * FROM validation_samples v WHERE id=(SELECT MAX(id) FROM validation_samples x
                    WHERE x.category=v.category AND x.name=v.name) AND category=? ORDER BY name""",(category,)).fetchall()
            else:
                rows=db.execute("""SELECT * FROM validation_samples v WHERE id=(SELECT MAX(id) FROM validation_samples x
                    WHERE x.category=v.category AND x.name=v.name) ORDER BY category,name""").fetchall()
        return [{**dict(r),"payload":json.loads(r["payload"])} for r in rows]
    def events(self, limit: int = 200) -> list[dict[str, Any]]:
        with self._connect() as db:
            rows=db.execute("SELECT * FROM observation_events ORDER BY event_at_ms DESC,id DESC LIMIT ?",
                            (max(1,min(limit,2000)),)).fetchall()
        return [{**dict(r),"payload":json.loads(r["payload"])} for r in rows]
    def report(self) -> dict[str, Any]:
        with self._connect() as db:
            count=db.execute("SELECT COUNT(*) FROM observation_events").fetchone()[0]
            first=db.execute("SELECT MIN(event_at_ms) FROM observation_events").fetchone()[0]
            last=db.execute("SELECT MAX(event_at_ms) FROM observation_events").fetchone()[0]
            by_type=db.execute("SELECT event_type,COUNT(*) n FROM observation_events GROUP BY event_type").fetchall()
        return {"event_count":count,"first_event_ms":first,"last_event_ms":last,
                "duration_ms":None if first is None or last is None else max(0,last-first),
                "event_counts":{r["event_type"]:r["n"] for r in by_type},
                "actual_trading_performance":"NOT_AVAILABLE — read-only observation; no orders placed"}
    def record_snapshot(self, opportunities: list[dict[str, Any]], decisions: dict[str, dict[str, Any]],
                        observed_at_ms: int, websocket_status: dict[str, Any] | None = None) -> int:
        """Persist stable symbol identity and emit only actual state transitions."""
        active = {str(row["symbol"]).upper(): (rank, row) for rank, row in enumerate(opportunities, 1)}
        symbols = set(active) | {str(key).upper() for key in decisions}
        emitted = 0
        with self._connect() as db:
            prior_rows = db.execute("SELECT symbol,state_json FROM opportunity_state").fetchall()
            prior = {r["symbol"]: json.loads(r["state_json"]) for r in prior_rows}
            symbols.update(prior)
            for symbol in sorted(symbols):
                ranked = active.get(symbol)
                row = None if ranked is None else ranked[1]
                decision = decisions.get(symbol, {})
                current = {
                    "active": row is not None,
                    "eligible": bool(decision.get("eligible", row is not None)),
                    "rejection_codes": sorted(decision.get("rejection_codes", [])),
                    "ranking": None if ranked is None else ranked[0],
                    "funding_rate": None if row is None else row.get("funding_rate"),
                    "basis_bps": None if row is None else row.get("basis_bps"),
                    "liquidity": None if row is None else [row.get("spot_depth_usdt"), row.get("futures_depth_usdt")],
                    "data_quality": None if row is None else [row.get("data_freshness"), row.get("max_field_age_ms")],
                    "websocket_health": websocket_status or {},
                }
                old = prior.get(symbol)
                changes = []
                if old is None:
                    if row is not None:
                        changes.append(("OPPORTUNITY_APPEARED", {"current": current}))
                else:
                    if not old.get("active") and row is not None:
                        changes.append(("OPPORTUNITY_APPEARED", {"current": current}))
                    if old.get("active") and row is None:
                        changes.append(("OPPORTUNITY_DISAPPEARED", {"previous": old}))
                    for key, event_type in (
                        ("eligible", "ELIGIBILITY_CHANGED"),
                        ("rejection_codes", "REJECTION_REASONS_CHANGED"),
                        ("ranking", "RANKING_CHANGED"),
                        ("funding_rate", "FUNDING_RATE_CHANGED"),
                        ("basis_bps", "BASIS_CHANGED"),
                        ("liquidity", "LIQUIDITY_CHANGED"),
                        ("data_quality", "DATA_QUALITY_CHANGED"),
                        ("websocket_health", "WEBSOCKET_HEALTH_CHANGED"),
                    ):
                        if old.get(key) != current.get(key):
                            changes.append((event_type, {"previous": old.get(key), "current": current.get(key)}))
                for event_type, payload in changes:
                    db.execute("INSERT INTO observation_events(event_at_ms,event_type,symbol,status,payload) VALUES(?,?,?,?,?)",
                        (observed_at_ms,event_type,symbol,"ELIGIBLE" if current["eligible"] else "REJECTED",
                         json.dumps(payload,sort_keys=True,default=str)))
                    emitted += 1
                db.execute("INSERT INTO opportunity_state(symbol,state_json,updated_at_ms) VALUES(?,?,?) "
                    "ON CONFLICT(symbol) DO UPDATE SET state_json=excluded.state_json,updated_at_ms=excluded.updated_at_ms",
                    (symbol,json.dumps(current,sort_keys=True,default=str),observed_at_ms))
                if row is not None:
                    db.execute("INSERT OR IGNORE INTO opportunity_snapshots(symbol,observed_at_ms,rank,payload) VALUES(?,?,?,?)",
                        (symbol,observed_at_ms,ranked[0],json.dumps(row,default=str)))
        return emitted

    def prune(self, now_ms: int) -> int:
        cutoff=now_ms-self.retention_days*86400000
        with self._connect() as db:
            a=db.execute("DELETE FROM validation_samples WHERE checked_at_ms < ?",(cutoff,)).rowcount
            db.execute("DELETE FROM observation_events WHERE event_at_ms < ?",(cutoff,))
        return a
