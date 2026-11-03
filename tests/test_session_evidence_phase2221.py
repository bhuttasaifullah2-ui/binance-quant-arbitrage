import json
import sqlite3

from app.infrastructure.session_evidence_store import SessionEvidenceStore


class LegacyStore:
    def __init__(self):
        self.records = []
        self.events = []
        self.scans = []
        self.snapshots = []

    def record(self, *args):
        self.records.append(args)

    def event(self, *args):
        self.events.append(args)

    def record_scan(self, *args, **kwargs):
        self.scans.append((args, kwargs))

    def record_snapshot(self, *args, **kwargs):
        self.snapshots.append((args, kwargs))
        return 0


def test_session_lifecycle_separates_runs_and_marks_unclean_exit(tmp_path):
    path = str(tmp_path / "sessions.sqlite3")
    first = SessionEvidenceStore(path, LegacyStore())
    first_id = first.session_id
    second = SessionEvidenceStore(path, LegacyStore())
    second_id = second.session_id
    assert first_id != second_id
    sessions = {x["session_id"]: x for x in second.list_sessions()}
    assert sessions[first_id]["status"] == "INTERRUPTED"
    assert sessions[first_id]["ended_at_utc"] is not None
    assert sessions[second_id]["status"] == "RUNNING"
    assert sessions[first_id]["interrupted_by_session_id"] == second_id
    second.close_session()
    assert {x["session_id"]: x for x in second.list_sessions()}[second_id]["status"] == "COMPLETED"


def test_session_records_and_json_export_are_isolated(tmp_path):
    path = str(tmp_path / "sessions.sqlite3")
    store = SessionEvidenceStore(path, LegacyStore())
    store.record("connectivity", "spot_rest", 1000, "HEALTHY", {"latency_ms": 12})
    store.record("clock", "spot", 1100, "WARNING", {"offset_ms": 17})
    store.record_data_quality(1200, {"BTCUSDT": {
        "status": "STALE", "missing_fields": [], "invalid_fields": [],
        "stale_fields": ["spot_bid"], "field_age_ms": {"spot_bid": 18000},
    }})
    store.record_snapshot([], {"BTCUSDT": {
        "eligible": False, "rejection_codes": ["MARKET_DATA_STALE"],
    }}, 1300)
    store.event(1400, "WEBSOCKET_DISCONNECTED", None, "UNHEALTHY", {"market": "spot"})
    store.event(1400, "WEBSOCKET_DISCONNECTED", None, "UNHEALTHY", {"market": "spot"})
    report = store.export_session(store.session_id)
    assert report["summary"]["rest_checks"] == {"count": 1, "healthy": 1, "unhealthy": 0}
    assert report["summary"]["clock"]["offset_min_ms"] == 17
    assert report["summary"]["data_quality"]["decision_count"] == 1
    assert report["summary"]["scanner_eligibility"]["rejected"] == 1
    assert report["summary"]["scanner_eligibility"]["acceptance_fraction"] == 0.0
    assert len([x for x in report["events"] if x["event_type"] == "WEBSOCKET_DISCONNECTED"]) == 1
    assert report["summary"]["websocket"]["uptime_fraction"] is None
    assert report["session"]["session_id"] == store.session_id


def test_session_persistence_survives_database_reopen_and_old_rows_remain(tmp_path):
    path = str(tmp_path / "sessions.sqlite3")
    db = sqlite3.connect(path)
    db.execute("CREATE TABLE legacy_history(value TEXT)")
    db.execute("INSERT INTO legacy_history VALUES('keep-me')")
    db.commit()
    db.close()
    first = SessionEvidenceStore(path, LegacyStore())
    first.record("funding_reconciliation", "BTCUSDT", 1000, "WARNING",
                 {"unresolved_gaps": [{"previous_settlement_ms": 1, "next_settlement_ms": 2}]})
    session_id = first.session_id
    second = SessionEvidenceStore(path, LegacyStore())
    exported = second.export_session(session_id)
    assert exported["summary"]["funding"]["unresolved_gap_count"] == 1
    with sqlite3.connect(path) as db:
        assert db.execute("SELECT value FROM legacy_history").fetchone()[0] == "keep-me"
        assert db.execute("SELECT COUNT(*) FROM observation_sessions").fetchone()[0] == 2
    assert json.loads(json.dumps(exported, default=str))["session"]["status"] == "INTERRUPTED"
