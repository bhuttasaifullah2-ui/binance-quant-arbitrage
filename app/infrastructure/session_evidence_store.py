"""Session-scoped evidence persistence layered over the legacy SQLite validation store.

Schema changes are additive. Historical rows remain available through the legacy reports;
new session exports contain only evidence recorded during the selected process session.
"""
from __future__ import annotations

import json
import os
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _utc(ms: int | None) -> str | None:
    if ms is None:
        return None
    return datetime.fromtimestamp(ms / 1000, timezone.utc).isoformat().replace("+00:00", "Z")


class SessionEvidenceStore:
    def __init__(self, path: str, legacy_store: Any) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.legacy = legacy_store
        self.process_id = f"{os.getpid()}:{uuid.uuid4()}"
        self.session_id = self._start_session()

    def _connect(self) -> sqlite3.Connection:
        db = sqlite3.connect(self.path, timeout=20)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA synchronous=FULL")
        return db

    def _start_session(self) -> str:
        now = __import__("time").time_ns() // 1_000_000
        session_id = str(uuid.uuid4())
        with self._connect() as db:
            db.executescript("""
            CREATE TABLE IF NOT EXISTS observation_sessions(
              session_id TEXT PRIMARY KEY, process_id TEXT NOT NULL,
              started_at_ms INTEGER NOT NULL, ended_at_ms INTEGER,
              status TEXT NOT NULL, interrupted_by_session_id TEXT);
            CREATE TABLE IF NOT EXISTS session_observations(
              id INTEGER PRIMARY KEY, session_id TEXT NOT NULL, category TEXT NOT NULL,
              name TEXT NOT NULL, observed_at_ms INTEGER NOT NULL, status TEXT NOT NULL,
              payload TEXT NOT NULL);
            CREATE INDEX IF NOT EXISTS ix_session_obs ON session_observations(session_id,category,observed_at_ms);
            CREATE TABLE IF NOT EXISTS session_events(
              id INTEGER PRIMARY KEY, session_id TEXT NOT NULL, event_at_ms INTEGER NOT NULL,
              event_type TEXT NOT NULL, symbol TEXT, status TEXT, payload TEXT NOT NULL);
            CREATE INDEX IF NOT EXISTS ix_session_events ON session_events(session_id,event_at_ms);
            CREATE TABLE IF NOT EXISTS session_quality(
              id INTEGER PRIMARY KEY, session_id TEXT NOT NULL, observed_at_ms INTEGER NOT NULL,
              symbol TEXT NOT NULL, market TEXT, quality_state TEXT NOT NULL,
              codes TEXT NOT NULL, measurements TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS session_eligibility(
              id INTEGER PRIMARY KEY, session_id TEXT NOT NULL, observed_at_ms INTEGER NOT NULL,
              symbol TEXT NOT NULL, eligible INTEGER NOT NULL, rejection_codes TEXT NOT NULL,
              payload TEXT NOT NULL, UNIQUE(session_id,symbol,observed_at_ms));
            CREATE TABLE IF NOT EXISTS session_opportunity_state(
              session_id TEXT NOT NULL, symbol TEXT NOT NULL, state_json TEXT NOT NULL,
              updated_at_ms INTEGER NOT NULL, PRIMARY KEY(session_id,symbol));
            CREATE TABLE IF NOT EXISTS session_opportunity_snapshots(
              id INTEGER PRIMARY KEY, session_id TEXT NOT NULL, symbol TEXT NOT NULL,
              observed_at_ms INTEGER NOT NULL, rank INTEGER, payload TEXT NOT NULL,
              UNIQUE(session_id,symbol,observed_at_ms));
            """)
            # A process that disappeared without a clean shutdown is interrupted,
            # never silently resumed or combined with the new process session.
            running = db.execute(
                "SELECT session_id FROM observation_sessions WHERE status='RUNNING'"
            ).fetchall()
            for row in running:
                db.execute(
                    "UPDATE observation_sessions SET status='INTERRUPTED',ended_at_ms=? WHERE session_id=?",
                    (now, row["session_id"]),
                )
                db.execute(
                    "INSERT INTO session_events(session_id,event_at_ms,event_type,status,payload) VALUES(?,?,?,?,?)",
                    (row["session_id"], now, "PROCESS_INTERRUPTED", "INTERRUPTED",
                     json.dumps({"detected_by_process_id": self.process_id})),
                )
            db.execute(
                "INSERT INTO observation_sessions(session_id,process_id,started_at_ms,status) VALUES(?,?,?,'RUNNING')",
                (session_id, self.process_id, now),
            )
            db.execute(
                "INSERT INTO session_events(session_id,event_at_ms,event_type,status,payload) VALUES(?,?,?,?,?)",
                (session_id, now, "SESSION_STARTED", "RUNNING",
                 json.dumps({"process_id": self.process_id, "recovered_interrupted_sessions": len(running)})),
            )
            for row in running:
                db.execute(
                    "UPDATE observation_sessions SET interrupted_by_session_id=? WHERE session_id=?",
                    (session_id, row["session_id"]),
                )
        return session_id

    def close_session(self, status: str = "COMPLETED") -> None:
        if status not in {"COMPLETED", "FAILED"}:
            raise ValueError("session close status must be COMPLETED or FAILED")
        now = __import__("time").time_ns() // 1_000_000
        with self._connect() as db:
            row = db.execute("SELECT status FROM observation_sessions WHERE session_id=?",
                             (self.session_id,)).fetchone()
            if row and row["status"] == "RUNNING":
                db.execute("UPDATE observation_sessions SET status=?,ended_at_ms=? WHERE session_id=?",
                           (status, now, self.session_id))
                db.execute(
                    "INSERT INTO session_events(session_id,event_at_ms,event_type,status,payload) VALUES(?,?,?,?,?)",
                    (self.session_id, now, "SESSION_ENDED", status, json.dumps({"process_id": self.process_id})),
                )

    def record(self, category: str, name: str, checked_at_ms: int, status: str,
               payload: dict[str, Any]) -> None:
        enriched = {**payload, "session_id": self.session_id}
        self.legacy.record(category, name, checked_at_ms, status, enriched)
        with self._connect() as db:
            db.execute("INSERT INTO session_observations(session_id,category,name,observed_at_ms,status,payload) VALUES(?,?,?,?,?,?)",
                       (self.session_id, category, name, checked_at_ms, status, json.dumps(enriched, default=str)))

    def event(self, event_at_ms: int, event_type: str, symbol: str | None,
              status: str | None, payload: dict[str, Any]) -> None:
        enriched = {**payload, "session_id": self.session_id}
        self.legacy.event(event_at_ms, event_type, symbol, status, enriched)
        with self._connect() as db:
            # Suppress only an identical repeated event at the same timestamp;
            # state transitions at different timestamps are retained.
            db.execute("""INSERT INTO session_events(session_id,event_at_ms,event_type,symbol,status,payload)
                SELECT ?,?,?,?,?,? WHERE NOT EXISTS(
                SELECT 1 FROM session_events WHERE session_id=? AND event_at_ms=? AND event_type=?
                AND COALESCE(symbol,'')=COALESCE(?,'') AND payload=?)""",
                (self.session_id,event_at_ms,event_type,symbol,status,json.dumps(enriched,sort_keys=True,default=str),
                 self.session_id,event_at_ms,event_type,symbol,json.dumps(enriched,sort_keys=True,default=str)))

    def record_scan(self, *args: Any, **kwargs: Any) -> None:
        self.legacy.record_scan(*args, **kwargs)
        started = int(args[0] if args else kwargs["started_at_ms"])
        ended = int(args[1] if len(args)>1 else kwargs["completed_at_ms"])
        self.record("scanner_run", "scan", ended, "COMPLETED",
                    {"started_at_ms": started, "completed_at_ms": ended})

    def record_snapshot(self, opportunities: list[dict[str, Any]], decisions: dict[str, dict[str, Any]],
                        observed_at_ms: int, websocket_status: dict[str, Any] | None = None) -> int:
        emitted = self.legacy.record_snapshot(opportunities, decisions, observed_at_ms, websocket_status)
        ranked = {str(row["symbol"]).upper(): (rank, row) for rank, row in enumerate(opportunities, 1)}
        symbols = set(ranked) | {str(key).upper() for key in decisions}
        with self._connect() as db:
            for symbol in sorted(symbols):
                decision = decisions.get(symbol, {})
                item = ranked.get(symbol)
                current = {"active": item is not None, "eligible": bool(decision.get("eligible")),
                           "rejection_codes": sorted(decision.get("rejection_codes", [])),
                           "rank": None if item is None else item[0],
                           "opportunity": None if item is None else item[1]}
                codes = current["rejection_codes"]
                payload = {**decision, "session_id": self.session_id}
                db.execute("""INSERT OR IGNORE INTO session_eligibility
                    (session_id,observed_at_ms,symbol,eligible,rejection_codes,payload) VALUES(?,?,?,?,?,?)""",
                    (self.session_id, observed_at_ms, symbol, int(current["eligible"]),
                     json.dumps(codes), json.dumps(payload, default=str)))
                db.execute("""INSERT OR IGNORE INTO session_opportunity_snapshots
                    (session_id,symbol,observed_at_ms,rank,payload) VALUES(?,?,?,?,?)""",
                    (self.session_id, symbol, observed_at_ms, current["rank"], json.dumps(current, default=str)))
                previous_row = db.execute(
                    "SELECT state_json FROM session_opportunity_state WHERE session_id=? AND symbol=?",
                    (self.session_id, symbol)).fetchone()
                previous = None if previous_row is None else json.loads(previous_row["state_json"])
                if previous is not None and previous != current:
                    if not previous["active"] and current["active"]:
                        event_type = "OPPORTUNITY_APPEARED"
                    elif previous["active"] and not current["active"]:
                        event_type = "OPPORTUNITY_DISAPPEARED"
                    else:
                        event_type = "OPPORTUNITY_STATE_CHANGED"
                    event_payload = {"previous": previous, "current": current}
                    db.execute("""INSERT INTO session_events
                        (session_id,event_at_ms,event_type,symbol,status,payload) VALUES(?,?,?,?,?,?)""",
                        (self.session_id, observed_at_ms, event_type, symbol,
                         "ELIGIBLE" if current["eligible"] else "REJECTED",
                         json.dumps(event_payload, sort_keys=True, default=str)))
                db.execute("""INSERT INTO session_opportunity_state(session_id,symbol,state_json,updated_at_ms)
                    VALUES(?,?,?,?) ON CONFLICT(session_id,symbol) DO UPDATE SET
                    state_json=excluded.state_json,updated_at_ms=excluded.updated_at_ms""",
                    (self.session_id, symbol, json.dumps(current, sort_keys=True, default=str), observed_at_ms))
        return emitted

    def record_data_quality(self, observed_at_ms: int, symbols: dict[str, dict[str, Any]]) -> None:
        with self._connect() as db:
            for symbol, item in symbols.items():
                codes = []
                for field, code in (("missing_fields","MISSING_FIELD"),("invalid_fields","INVALID_FIELD"),
                                    ("stale_fields","STALE_FIELD")):
                    codes.extend(f"{code}:{name}" for name in item.get(field, []))
                db.execute("""INSERT INTO session_quality
                    (session_id,observed_at_ms,symbol,market,quality_state,codes,measurements)
                    VALUES(?,?,?,?,?,?,?)""",
                    (self.session_id, observed_at_ms, symbol, "spot+futures", item.get("status","UNKNOWN"),
                     json.dumps(codes), json.dumps({"field_age_ms":item.get("field_age_ms",{})})))

    def list_sessions(self, limit: int = 100) -> list[dict[str, Any]]:
        with self._connect() as db:
            rows = db.execute("SELECT * FROM observation_sessions ORDER BY started_at_ms DESC LIMIT ?",
                              (max(1,min(limit,500)),)).fetchall()
        return [{**dict(r), "started_at_utc":_utc(r["started_at_ms"]),
                 "ended_at_utc":_utc(r["ended_at_ms"])} for r in rows]

    def _session(self, session_id: str) -> dict[str, Any]:
        with self._connect() as db:
            row = db.execute("SELECT * FROM observation_sessions WHERE session_id=?",(session_id,)).fetchone()
        if row is None:
            raise KeyError(session_id)
        return {**dict(row), "started_at_utc":_utc(row["started_at_ms"]),
                "ended_at_utc":_utc(row["ended_at_ms"])}

    def export_session(self, session_id: str) -> dict[str, Any]:
        metadata = self._session(session_id)
        with self._connect() as db:
            observations = db.execute("SELECT * FROM session_observations WHERE session_id=? ORDER BY observed_at_ms,id",(session_id,)).fetchall()
            events = db.execute("SELECT * FROM session_events WHERE session_id=? ORDER BY event_at_ms,id",(session_id,)).fetchall()
            quality = db.execute("SELECT * FROM session_quality WHERE session_id=? ORDER BY observed_at_ms,id",(session_id,)).fetchall()
            eligibility = db.execute("SELECT * FROM session_eligibility WHERE session_id=? ORDER BY observed_at_ms,id",(session_id,)).fetchall()
            opportunity_snapshots = db.execute("SELECT * FROM session_opportunity_snapshots WHERE session_id=? ORDER BY observed_at_ms,id",(session_id,)).fetchall()
        obs = [{**dict(x), "payload":json.loads(x["payload"])} for x in observations]
        ev = [{**dict(x), "payload":json.loads(x["payload"])} for x in events]
        q = [{**dict(x), "codes":json.loads(x["codes"]), "measurements":json.loads(x["measurements"])} for x in quality]
        el = [{**dict(x), "rejection_codes":json.loads(x["rejection_codes"]), "payload":json.loads(x["payload"])} for x in eligibility]
        snapshots = [{**dict(x), "payload":json.loads(x["payload"])} for x in opportunity_snapshots]
        rest = [x for x in obs if x["category"]=="connectivity"]
        clocks = [x["payload"].get("offset_ms") for x in obs if x["category"]=="clock"]
        ws = [x for x in ev if x["event_type"] in {"WEBSOCKET_CONNECTED","WEBSOCKET_DISCONNECTED","WEBSOCKET_RECOVERED"}]
        accepted = sum(x["eligible"] for x in el)
        rejected = len(el)-accepted
        return {
            "schema_version": 1, "session": metadata,
            "summary": {
                "rest_checks": {"count":len(rest), "healthy":sum(x["status"]=="HEALTHY" for x in rest),
                                "unhealthy":sum(x["status"]!="HEALTHY" for x in rest)},
                "websocket": {"transition_count":len(ws),
                              "reconnect_success_count":sum(x["event_type"]=="WEBSOCKET_RECOVERED" for x in ws),
                              "uptime_fraction":None,
                              "warning":"Uptime is not calculated unless complete connection-state boundaries are available."},
                "clock": {"measurement_count":len([x for x in clocks if x is not None]),
                          "offset_min_ms":min([x for x in clocks if x is not None],default=None),
                          "offset_max_ms":max([x for x in clocks if x is not None],default=None)},
                "data_quality": {"decision_count":len(q),
                                 "states":{s:sum(x["quality_state"]==s for x in q) for s in sorted({x["quality_state"] for x in q})}},
                "scanner_eligibility": {"decision_count":len(el),"accepted":accepted,"rejected":rejected,
                                        "acceptance_denominator":len(el),
                                        "acceptance_fraction":None if not el else accepted/len(el)},
                "funding": {"reconciliation_count":sum(x["category"]=="funding_reconciliation" for x in obs),
                            "unresolved_gap_count":sum(len(x["payload"].get("unresolved_gaps",[])) for x in obs if x["category"]=="funding_reconciliation")},
                "missing_data": {"rest_checks":len(rest)==0,"clock_measurements":not any(x is not None for x in clocks),
                                 "quality_decisions":len(q)==0,"eligibility_decisions":len(el)==0},
            },
            "observations":obs, "events":ev, "data_quality_decisions":q,
            "eligibility_decisions":el, "opportunity_snapshots":snapshots,
            "warnings":[ "No uptime is inferred without persisted connection transitions.",
                         "A missing observation is represented as missing, not as a healthy result." ]
        }

    def session_summary(self, session_id: str) -> dict[str, Any]:
        report = self.export_session(session_id)
        return {"session":report["session"],"summary":report["summary"],"warnings":report["warnings"]}

    def __getattr__(self, name: str) -> Any:
        return getattr(self.legacy, name)
