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
    def prune(self, now_ms: int) -> int:
        cutoff=now_ms-self.retention_days*86400000
        with self._connect() as db:
            a=db.execute("DELETE FROM validation_samples WHERE checked_at_ms < ?",(cutoff,)).rowcount
            db.execute("DELETE FROM observation_events WHERE event_at_ms < ?",(cutoff,))
        return a
