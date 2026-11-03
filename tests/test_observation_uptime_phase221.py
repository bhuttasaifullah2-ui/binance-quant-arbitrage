from app.infrastructure.validation_store import ValidationStore


def test_websocket_uptime_uses_only_persisted_transition_intervals(tmp_path):
    store = ValidationStore(str(tmp_path / "uptime.sqlite3"))
    store.event(1000, "WEBSOCKET_CONNECTED", None, "HEALTHY", {"market": "spot"})
    store.event(4000, "WEBSOCKET_DISCONNECTED", None, "UNHEALTHY", {"market": "spot"})
    report = store.statistical_report()
    assert report["websocket_uptime"]["spot"]["measured_connected_ms"] == 3000
    assert report["websocket_uptime"]["spot"]["uptime_fraction"] == "1"
    assert report["websocket_uptime"]["futures"]["uptime_fraction"] is None
