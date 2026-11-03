from app.infrastructure.validation_store import ValidationStore


def test_report_uses_persisted_scan_windows_and_detects_large_scan_gaps(tmp_path):
    store = ValidationStore(str(tmp_path / "scans.sqlite3"))
    store.record_scan(1000, 1100, 4, 2, 2, 1000)
    store.record_scan(1200, 1300, 4, 3, 1, 1000)
    store.record_scan(5000, 5100, 4, 1, 3, 1000)
    report = store.statistical_report()
    assert report["scan_count"] == 3
    assert report["observation_duration_ms"] == 4100
    assert report["data_gaps"]["scanner_gap_count"] == 1
    assert report["eligible_opportunity_count_total"] == 6
