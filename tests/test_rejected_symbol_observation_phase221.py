from app.infrastructure.validation_store import ValidationStore


def test_rejected_symbol_data_quality_and_feed_transitions_are_deduplicated(tmp_path):
    store = ValidationStore(str(tmp_path / "quality-events.sqlite3"))
    stale = {"eligible": False, "rejection_codes": ["MARKET_DATA_STALE"],
             "data_quality": {"status": "STALE", "stale_fields": ["spot_bid"]},
             "websocket_health": {"spot": False, "futures": True}}
    assert store.record_snapshot([], {"BTCUSDT": stale}, 1000) == 1
    assert store.record_snapshot([], {"BTCUSDT": stale}, 2000) == 0
    healthy = {"eligible": False, "rejection_codes": ["OTHER"],
               "data_quality": {"status": "HEALTHY", "stale_fields": []},
               "websocket_health": {"spot": True, "futures": True}}
    emitted = store.record_snapshot([], {"BTCUSDT": healthy}, 3000)
    assert emitted >= 2
    kinds = [x["event_type"] for x in store.events(100)]
    assert "DATA_QUALITY_CHANGED" in kinds
    assert "WEBSOCKET_HEALTH_CHANGED" in kinds
