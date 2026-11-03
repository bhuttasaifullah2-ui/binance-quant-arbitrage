"""Restart-safe persistence for read-only validation and observation events."""
import json
import sqlite3
from pathlib import Path
from decimal import Decimal
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
            CREATE TABLE IF NOT EXISTS observation_scans(id INTEGER PRIMARY KEY,started_at_ms INTEGER NOT NULL,completed_at_ms INTEGER NOT NULL,matched_count INTEGER NOT NULL,eligible_count INTEGER NOT NULL,rejected_count INTEGER NOT NULL,expected_interval_ms INTEGER NOT NULL);
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
                    elif current["rejection_codes"]:
                        changes.append(("REJECTION_REASONS_CHANGED", {"previous": [], "current": current["rejection_codes"]}))
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

    def record_scan(self, started_at_ms: int, completed_at_ms: int, matched_count: int,
                    eligible_count: int, rejected_count: int, expected_interval_ms: int) -> None:
        with self._connect() as db:
            db.execute("INSERT INTO observation_scans(started_at_ms,completed_at_ms,matched_count,eligible_count,rejected_count,expected_interval_ms) VALUES(?,?,?,?,?,?)",
                (started_at_ms,completed_at_ms,matched_count,eligible_count,rejected_count,expected_interval_ms))

    def statistical_report(self, start_ms: int | None = None, end_ms: int | None = None) -> dict[str, Any]:
        import time
        event_where = []
        event_params: list[Any] = []
        snap_where = []
        snap_params: list[Any] = []
        if start_ms is not None:
            event_where.append("event_at_ms>=?")
            event_params.append(start_ms)
            snap_where.append("observed_at_ms>=?")
            snap_params.append(start_ms)
        if end_ms is not None:
            event_where.append("event_at_ms<?")
            event_params.append(end_ms)
            snap_where.append("observed_at_ms<?")
            snap_params.append(end_ms)
        ew = (" WHERE " + " AND ".join(event_where)) if event_where else ""
        sw = (" WHERE " + " AND ".join(snap_where)) if snap_where else ""
        with self._connect() as db:
            events = db.execute("SELECT * FROM observation_events" + ew + " ORDER BY event_at_ms,id", event_params).fetchall()
            snapshots = db.execute("SELECT * FROM opportunity_snapshots" + sw + " ORDER BY observed_at_ms", snap_params).fetchall()
            all_events = db.execute("SELECT * FROM observation_events ORDER BY event_at_ms,id").fetchall()
            scan_clauses = []
            scan_params: list[Any] = []
            if start_ms is not None:
                scan_clauses.append("completed_at_ms>=?")
                scan_params.append(start_ms)
            if end_ms is not None:
                scan_clauses.append("completed_at_ms<?")
                scan_params.append(end_ms)
            scan_where = (" WHERE " + " AND ".join(scan_clauses)) if scan_clauses else ""
            scans = db.execute("SELECT * FROM observation_scans" + scan_where + " ORDER BY completed_at_ms", scan_params).fetchall()
        counts: dict[str, int] = {}
        for item in events:
            counts[item["event_type"]] = counts.get(item["event_type"], 0) + 1
        payloads = [json.loads(item["payload"]) for item in snapshots]
        rejection_counts: dict[str, int] = {}
        for event in events:
            if event["event_type"] in {"REJECTION_REASONS_CHANGED", "ELIGIBILITY_CHANGED"}:
                detail = json.loads(event["payload"])
                codes = detail.get("current") if isinstance(detail.get("current"), list) else []
                for code in codes:
                    rejection_counts[str(code)] = rejection_counts.get(str(code), 0) + 1

        def distribution(key: str) -> dict[str, Any]:
            values = []
            for payload in payloads:
                value = payload.get(key)
                if value is not None:
                    try:
                        values.append(Decimal(str(value)))
                    except Exception:
                        continue
            return {
                "count": len(values),
                "min": str(min(values)) if values else None,
                "max": str(max(values)) if values else None,
                "mean": str(sum(values, Decimal("0")) / len(values)) if values else None,
            }

        opened: dict[str, int] = {}
        durations = []
        for item in all_events:
            symbol = item["symbol"] or ""
            if item["event_type"] == "OPPORTUNITY_APPEARED":
                opened[symbol] = int(item["event_at_ms"])
            elif item["event_type"] == "OPPORTUNITY_DISAPPEARED" and symbol in opened:
                duration = max(0, int(item["event_at_ms"]) - opened.pop(symbol))
                if (start_ms is None or int(item["event_at_ms"]) >= start_ms) and (end_ms is None or int(item["event_at_ms"]) < end_ms):
                    durations.append(duration)

        now = int(time.time() * 1000)
        uptime: dict[str, Any] = {}
        recovery_durations = []
        for market in ("spot", "futures"):
            transitions = []
            for item in all_events:
                payload = json.loads(item["payload"])
                if payload.get("market") == market and item["event_type"] in {
                    "WEBSOCKET_CONNECTED", "WEBSOCKET_RECOVERED", "WEBSOCKET_DISCONNECTED"
                }:
                    transitions.append((int(item["event_at_ms"]), item["event_type"], payload))
            start = None
            up_ms = 0
            first = transitions[0][0] if transitions else None
            last = transitions[-1][0] if transitions else None
            for stamp, event_type, payload in transitions:
                if event_type in {"WEBSOCKET_CONNECTED", "WEBSOCKET_RECOVERED"}:
                    if start is None:
                        start = stamp
                    if payload.get("recovery_duration_ms") is not None:
                        recovery_durations.append(int(payload["recovery_duration_ms"]))
                elif start is not None:
                    up_ms += max(0, stamp - start)
                    start = None
            if start is not None:
                up_ms += max(0, now - start)
                last = now
            uptime[market] = {
                "measured_connected_ms": up_ms,
                "measured_window_ms": None if first is None or last is None else max(0, last - first),
                "uptime_fraction": None if first is None or last is None or last <= first
                    else str(Decimal(up_ms) / Decimal(last - first)),
                "evidence": "persisted connection transitions; no uptime inferred before first event",
            }

        scan_gaps = []
        for previous, current in zip(scans, scans[1:]):
            delta = int(current["completed_at_ms"]) - int(previous["completed_at_ms"])
            if delta > int(previous["expected_interval_ms"]) * 2:
                scan_gaps.append({"from_ms": int(previous["completed_at_ms"]),
                                  "to_ms": int(current["completed_at_ms"]),
                                  "gap_ms": delta,
                                  "expected_interval_ms": int(previous["expected_interval_ms"])})
        return {
            "period_start_ms": start_ms,
            "period_end_ms": end_ms,
            "generated_at_ms": now,
            "observation_duration_ms": None if not events else max(0, int(events[-1]["event_at_ms"]) - int(events[0]["event_at_ms"])),
            "scan_count": len(scans),
            "matched_pair_count_total": sum(int(x["matched_count"]) for x in scans),
            "eligible_opportunity_count_total": sum(int(x["eligible_count"]) for x in scans),
            "rejected_opportunity_count_total": sum(int(x["rejected_count"]) for x in scans),
            "event_count": len(events),
            "snapshot_count": len(snapshots),
            "event_counts": counts,
            "opportunity_appearance_count": counts.get("OPPORTUNITY_APPEARED", 0),
            "opportunity_disappearance_count": counts.get("OPPORTUNITY_DISAPPEARED", 0),
            "eligibility_transition_count": counts.get("ELIGIBILITY_CHANGED", 0),
            "ranking_change_count": counts.get("RANKING_CHANGED", 0),
            "average_completed_opportunity_duration_ms": None if not durations else sum(durations) // len(durations),
            "maximum_completed_opportunity_duration_ms": max(durations) if durations else None,
            "funding_rate_distribution": distribution("funding_rate"),
            "basis_bps_distribution": distribution("basis_bps"),
            "spot_depth_distribution": distribution("spot_depth_usdt"),
            "futures_depth_distribution": distribution("futures_depth_usdt"),
            "data_freshness_age_ms_distribution": distribution("max_field_age_ms"),
            "rejection_reason_counts": rejection_counts,
            "stale_rejection_count": sum(v for k, v in rejection_counts.items() if "STALE" in k),
            "invalid_rejection_count": sum(v for k, v in rejection_counts.items() if "INVALID" in k),
            "websocket_uptime": uptime,
            "average_recovery_duration_ms": None if not recovery_durations else sum(recovery_durations) / len(recovery_durations),
            "maximum_recovery_duration_ms": max(recovery_durations) if recovery_durations else None,
            "data_gaps": {"scanner_intervals": scan_gaps, "count": len(scan_gaps),
                          "funding_gaps_are_reported_by_funding_reconciliation": True},
            "observed_data_only": True,
            "actual_trading_performance": "NONE — public read-only; no orders placed",
        }

    def prune(self, now_ms: int) -> int:
        cutoff=now_ms-self.retention_days*86400000
        with self._connect() as db:
            a=db.execute("DELETE FROM validation_samples WHERE checked_at_ms < ?",(cutoff,)).rowcount
            db.execute("DELETE FROM observation_events WHERE event_at_ms < ?",(cutoff,))
            db.execute("DELETE FROM opportunity_snapshots WHERE observed_at_ms < ?",(cutoff,))
            db.execute("DELETE FROM observation_scans WHERE completed_at_ms < ?",(cutoff,))
        return a
