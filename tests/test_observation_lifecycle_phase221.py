from app.infrastructure.validation_store import ValidationStore


def test_lifecycle_events_are_deduplicated_and_survive_restart(tmp_path):
    path = str(tmp_path / "observe.sqlite3")
    store = ValidationStore(path)
    opportunity = {
        "symbol": "BTCUSDT", "score": "0.9", "estimated_net_usdt": "1.2",
        "funding_rate": "0.0001", "basis_bps": "2.5",
        "spot_depth_usdt": "5000", "futures_depth_usdt": "6000",
        "data_freshness": "FRESH", "max_field_age_ms": 250,
    }
    decision = {"BTCUSDT": {"eligible": True, "rejection_codes": []}}
    assert store.record_snapshot([opportunity], decision, 1000, {"spot": True, "futures": True}) == 1
    assert store.record_snapshot([opportunity], decision, 2000, {"spot": True, "futures": True}) == 0
    reopened = ValidationStore(path)
    changed = {**opportunity, "score": "0.8", "funding_rate": "0.0002"}
    assert reopened.record_snapshot([changed], decision, 3000, {"spot": True, "futures": True}) == 2
    assert reopened.record_snapshot([], {"BTCUSDT": {"eligible": False, "rejection_codes": ["STALE"]}}, 4000) >= 2
    event_types = [x["event_type"] for x in reopened.events(100)]
    assert event_types.count("OPPORTUNITY_APPEARED") == 1
    assert "FUNDING_RATE_CHANGED" in event_types
    assert "OPPORTUNITY_DISAPPEARED" in event_types
    assert reopened.statistical_report()["snapshot_count"] == 2


def test_daily_report_filters_persisted_events(tmp_path):
    store = ValidationStore(str(tmp_path / "daily.sqlite3"))
    row = {"symbol": "ETHUSDT", "score": "1", "estimated_net_usdt": "0.5",
           "funding_rate": "0.0002", "basis_bps": "-1", "spot_depth_usdt": "3000",
           "futures_depth_usdt": "3500", "data_freshness": "FRESH", "max_field_age_ms": 10}
    store.record_snapshot([row], {"ETHUSDT": {"eligible": True}}, 1000)
    store.record_snapshot([], {"ETHUSDT": {"eligible": False}}, 2000)
    report = store.statistical_report(1500, 2500)
    assert report["opportunity_disappearance_count"] == 1
    assert report["snapshot_count"] == 0
