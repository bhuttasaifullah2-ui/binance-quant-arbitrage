import sqlite3

from app.infrastructure.sqlite_store import SQLiteMarketStore


def test_v3_funding_migration_preserves_legacy_record_and_creates_backup(tmp_path):
    database = tmp_path / "legacy.sqlite3"
    with sqlite3.connect(database) as conn:
        conn.executescript("""
            CREATE TABLE schema_version(version INTEGER NOT NULL);
            INSERT INTO schema_version VALUES (3);
            CREATE TABLE funding_observations (
                id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT NOT NULL,
                funding_time_ms INTEGER NOT NULL, funding_rate TEXT NOT NULL,
                observed_at_ms INTEGER NOT NULL, source TEXT NOT NULL DEFAULT 'binance_rest',
                UNIQUE(symbol, funding_time_ms)
            );
            INSERT INTO funding_observations
                (symbol,funding_time_ms,funding_rate,observed_at_ms,source)
                VALUES ('BTCUSDT',1700000000000,'0.0001',1700000001000,'legacy');
        """)
    store = SQLiteMarketStore(str(database))
    history = store.funding_history("BTCUSDT")
    assert len(history) == 1
    assert history[0]["funding_rate"] == "0.0001"
    assert history[0]["settlement_mark_price"] is None
    assert history[0]["rate_type"] is None
    assert history[0]["source"] == "legacy"
    with sqlite3.connect(database) as conn:
        assert conn.execute("SELECT version FROM schema_version").fetchone()[0] == 4
        assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    backup = tmp_path / "legacy.sqlite3.pre_v4.bak"
    assert backup.exists()
    restored = SQLiteMarketStore.restore_from_backup(
        str(backup), str(tmp_path / "restored-from-pre-v4.sqlite3")
    )
    assert restored.funding_history("BTCUSDT")[0]["source"] == "legacy"


def test_exact_duplicates_are_ignored_but_mark_and_rate_type_conflicts_are_retained(tmp_path):
    store = SQLiteMarketStore(str(tmp_path / "events.sqlite3"))
    regular = {"fundingTime": 1700000000000, "fundingRate": "0.0001",
               "markPrice": "50000", "rateType": "Regular"}
    special = {**regular, "rateType": "Special"}
    changed_mark = {**regular, "markPrice": "50001"}
    changed_source = {**regular, "source": "binance_fapi_fundingRate_backfill"}
    assert store.save_funding_observations("BTCUSDT", [regular], 1700000001000) == 1
    assert store.save_funding_observations("BTCUSDT", [regular], 1700000002000) == 0
    assert store.save_funding_observations("BTCUSDT", [special, changed_mark, changed_source], 1700000003000) == 3
    rows = store.funding_history("BTCUSDT")
    assert len(rows) == 4
    assert {row["rate_type"] for row in rows} == {"Regular", "Special"}
    assert {row["settlement_mark_price"] for row in rows} == {"50000", "50001"}
    assert {row["source"] for row in rows} == {
        "binance_fapi_fundingRate", "binance_fapi_fundingRate_backfill"
    }
    assert {row["observed_at_ms"] for row in rows} == {1700000001000, 1700000003000}
    assert store.funding_history_conflicts("BTCUSDT") == [
        {"funding_time_ms": 1700000000000, "record_count": 4}
    ]


def test_backup_restore_preserves_settlement_fields(tmp_path):
    store = SQLiteMarketStore(str(tmp_path / "original.sqlite3"))
    store.save_funding_observations("ETHUSDT", [{
        "fundingTime": 1700000000000, "fundingRate": "-0.0002",
        "markPrice": "2000.5", "rateType": "Regular"
    }], 1700000001000)
    backup = store.backup_to(str(tmp_path / "backup" / "market.sqlite3"))
    restored = SQLiteMarketStore.restore_from_backup(backup, str(tmp_path / "restored.sqlite3"))
    row = restored.funding_history("ETHUSDT")[0]
    assert row["funding_rate"] == "-0.0002"
    assert row["settlement_mark_price"] == "2000.5"
    assert row["rate_type"] == "Regular"


def test_legacy_database_without_version_row_is_migrated(tmp_path):
    database = tmp_path / "no-version.sqlite3"
    with sqlite3.connect(database) as conn:
        conn.executescript("""
            CREATE TABLE funding_observations (
                id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT NOT NULL,
                funding_time_ms INTEGER NOT NULL, funding_rate TEXT NOT NULL,
                observed_at_ms INTEGER NOT NULL, source TEXT NOT NULL,
                UNIQUE(symbol,funding_time_ms)
            );
            INSERT INTO funding_observations
                (symbol,funding_time_ms,funding_rate,observed_at_ms,source)
                VALUES ('SOLUSDT',1700000000000,'0.0003',1700000001000,'legacy');
        """)
    store = SQLiteMarketStore(str(database))
    assert store.funding_history("SOLUSDT")[0]["settlement_mark_price"] is None
    with sqlite3.connect(database) as conn:
        assert conn.execute("SELECT version FROM schema_version").fetchone()[0] == 4
