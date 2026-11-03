import sqlite3
from app.infrastructure.sqlite_store import SQLiteMarketStore


def test_additive_schema_upgrade_preserves_phase2_market_history(tmp_path):
    path = tmp_path / "legacy.sqlite"
    conn = sqlite3.connect(path)
    conn.execute("""CREATE TABLE market_observations (
        id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT NOT NULL, observed_at_ms INTEGER NOT NULL,
        source TEXT NOT NULL DEFAULT 'binance_rest', UNIQUE(symbol, observed_at_ms))""")
    conn.execute("INSERT INTO market_observations(symbol, observed_at_ms, source) VALUES(?,?,?)",
                 ("BTCUSDT", 1000, "legacy"))
    conn.commit()
    conn.close()
    store = SQLiteMarketStore(str(path))
    assert store.history("BTCUSDT")[0]["source"] == "legacy"
    assert store.observation_summary()["opportunity_snapshots"] == 0
    assert store.latest_funding_time("BTCUSDT") is None


def test_latest_offset_and_new_persistence_tables(tmp_path):
    store = SQLiteMarketStore(str(tmp_path / "phase21.sqlite"))
    for symbol, stamp in (("BTCUSDT", 1000), ("ETHUSDT", 2000), ("SOLUSDT", 3000)):
        store.save_observation({"symbol": symbol, "observed_at_ms": stamp, "source": "test"})
    assert store.latest(1, 1)[0]["symbol"] == "ETHUSDT"
    store.save_exchange_metadata("spot", [{"symbol": "BTCUSDT", "status": "TRADING"}], 4000)
    assert store.exchange_filter("spot", "BTCUSDT")["status"] == "TRADING"
    assert store.save_market_event("spot", "BTCUSDT", "spot:bookticker", 5000, 5001, {"bid": "10"})
    assert not store.save_market_event("spot", "BTCUSDT", "spot:bookticker", 5000, 5001, {"bid": "10"})
    store.save_ws_health("spot", {"connected": True, "streams": 10}, 5001)
    assert store.websocket_health()[0]["connected"] == 1
    store.save_opportunity_lifecycle("BTCUSDT", "APPEARED", 6000, "80", "0.5")
    assert store.observation_summary()["lifecycle_events"] == 1
