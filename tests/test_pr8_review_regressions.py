from decimal import Decimal
import sqlite3

import pytest

from app.domain.funding import (
    funding_payment,
    linear_futures_funding_notional,
    summarize_funding,
)
from app.domain.funding_integrity import (
    FUNDING_RATE_RECONCILED,
    HISTORY_INCOMPLETE,
    HISTORY_RECONCILIATION_FAILED,
    assess_funding_history,
    snapshot_fields_are_acceptable,
    deduplicate_economic_settlements,
)
from app.domain.models import FundingObservation
from app.exchange.binance.funding_history import fetch_funding_history_with_stats
from app.exchange.binance.public_client import BinanceAPIError, BinancePublicClient, FundingRequestBudget
from app.infrastructure.sqlite_store import SQLiteMarketStore


HOUR = 3_600_000
BASE = 1_700_000_000_000


def regular_rows(gaps=(8 * HOUR, 8 * HOUR)):
    stamps = [BASE]
    for gap in gaps:
        stamps.append(stamps[-1] + gap)
    return [
        {"fundingTime": stamp, "fundingRate": "0.0001", "markPrice": "50000",
         "rateType": "Regular"}
        for stamp in stamps
    ]


def test_seven_hour_regular_gap_is_not_reconciled():
    rows = regular_rows((7 * HOUR, 8 * HOUR))
    result = assess_funding_history(
        rows, interval_hours=8, next_funding_time_ms=rows[-1]["fundingTime"] + 8 * HOUR,
        current_funding_rate="0.0001",
    )
    assert result["state"] == HISTORY_INCOMPLETE
    assert result["reason_code"] == "REGULAR_SETTLEMENT_CADENCE_INVALID"


def test_special_event_is_separate_from_regular_cadence():
    rows = regular_rows()
    rows.insert(1, {"fundingTime": rows[0]["fundingTime"] + 3 * HOUR,
                    "fundingRate": "0.00002", "markPrice": "50020", "rateType": "Special"})
    result = assess_funding_history(
        rows, interval_hours=8, next_funding_time_ms=rows[-1]["fundingTime"] + 8 * HOUR,
        current_funding_rate="0.0001",
    )
    assert result["state"] == FUNDING_RATE_RECONCILED
    assert result["special_event_count"] == 1
    assert result["regular_settlement_count"] == 3


def test_source_only_duplicates_count_once_and_keep_provenance():
    rows = [
        {"fundingTime": BASE, "fundingRate": "0.0001", "markPrice": "50000",
         "rateType": "Regular", "source": "primary"},
        {"fundingTime": BASE, "fundingRate": "0.0001", "markPrice": "50000",
         "rateType": "Regular", "source": "backfill"},
        {"fundingTime": BASE + 8 * HOUR, "fundingRate": "0.0002", "markPrice": "50100",
         "rateType": "Regular", "source": "primary"},
    ]
    collapsed = deduplicate_economic_settlements(rows)
    assert len(collapsed) == 2
    assert set(collapsed[0]["sources"]) == {"primary", "backfill"}
    observations = [FundingObservation("BTCUSDT", Decimal(row["fundingRate"]),
                                       int(row["fundingTime"])) for row in collapsed]
    assert summarize_funding(observations + observations[:1])["count"] == 2


@pytest.mark.asyncio
async def test_same_timestamp_special_split_across_page_boundary_is_not_skipped():
    class SplitBoundaryClient:
        def __init__(self):
            self.calls = []

        async def futures_funding_history(self, symbol, limit, start_time_ms=None, end_time_ms=None):
            self.calls.append(start_time_ms)
            if start_time_ms is None:
                return [
                    {"fundingTime": 100, "fundingRate": "0.001", "rateType": "Regular"},
                    {"fundingTime": 200, "fundingRate": "0.001", "rateType": "Regular"},
                ]
            if start_time_ms == 200:
                return [
                    {"fundingTime": 200, "fundingRate": "0.0002", "rateType": "Special"},
                    {"fundingTime": 300, "fundingRate": "0.002", "rateType": "Regular"},
                ]
            return []

    client = SplitBoundaryClient()
    rows, _ = await fetch_funding_history_with_stats(
        client, "BTCUSDT", page_size=2, max_pages=3
    )
    assert {(r["fundingTime"], r.get("rateType")) for r in rows} == {
        (100, "Regular"), (200, "Regular"), (200, "Special"), (300, "Regular")
    }
    assert client.calls[:2] == [None, 200]


def test_shared_funding_budget_reserves_capacity_for_current_data():
    budget = FundingRequestBudget(limit=10, reserve_for_current=2, window_seconds=300)
    for _ in range(8):
        budget.try_acquire("backfill", now=100)
    with pytest.raises(RuntimeError, match="budget"):
        budget.try_acquire("backfill", now=100)
    assert budget.try_acquire("current", now=100)
    assert budget.try_acquire("current", now=100)
    with pytest.raises(RuntimeError, match="budget"):
        budget.try_acquire("current", now=100)


def test_snapshot_checks_every_required_field_timestamp():
    fresh = {
        "spot_bid": BASE, "spot_ask": BASE + 100, "spot_last": BASE + 200,
        "spot_quote_volume": BASE + 300, "futures_bid": BASE + 400,
        "futures_ask": BASE + 500, "futures_last": BASE + 600,
        "futures_quote_volume": BASE + 700, "futures_mark": BASE + 800,
        "futures_index": BASE + 900, "funding_rate": BASE + 1000,
        "next_funding_time_ms": BASE + 1100,
    }
    assert snapshot_fields_are_acceptable(fresh, now_ms=BASE + 2000,
                                           max_age_ms=5000, max_skew_ms=2000)
    stale_bid = dict(fresh, spot_bid=BASE - 10_000)
    assert not snapshot_fields_are_acceptable(stale_bid, now_ms=BASE + 2000,
                                              max_age_ms=5000, max_skew_ms=20_000)
    staggered = dict(fresh, futures_index=BASE + 10_000)
    assert not snapshot_fields_are_acceptable(staggered, now_ms=BASE + 11_000,
                                              max_age_ms=20_000, max_skew_ms=2000)


def test_wal_online_backup_preserves_committed_rows(tmp_path):
    db = tmp_path / "wal.sqlite"
    store = SQLiteMarketStore(str(db))
    store.save_funding_observations("BTCUSDT", [
        {"fundingTime": BASE, "fundingRate": "0.001", "markPrice": "50000",
         "rateType": "Regular"}
    ], BASE + 1)
    backup = store.backup_to(str(tmp_path / "backup.sqlite"))
    restored = SQLiteMarketStore.restore_from_backup(backup, str(tmp_path / "restored.sqlite"))
    assert len(restored.funding_history("BTCUSDT")) == 1


def test_migration_backup_is_unique_and_integrity_checked_before_rebuild(tmp_path):
    db = tmp_path / "legacy.sqlite"
    with sqlite3.connect(db) as conn:
        conn.executescript("""
            CREATE TABLE schema_version(version INTEGER NOT NULL);
            INSERT INTO schema_version VALUES (3);
            CREATE TABLE funding_observations (
                id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT NOT NULL,
                funding_time_ms INTEGER NOT NULL, funding_rate TEXT NOT NULL,
                observed_at_ms INTEGER NOT NULL, source TEXT NOT NULL,
                UNIQUE(symbol, funding_time_ms)
            );
            INSERT INTO funding_observations VALUES
                (1, 'BTCUSDT', 100, '0.001', 101, 'legacy');
        """)
    store = SQLiteMarketStore(str(db))
    first_backup = next(tmp_path.glob("legacy.sqlite.pre_v5.*.bak"))
    assert sqlite3.connect(first_backup).execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    store._initialize()
    assert first_backup.exists()
    assert store.funding_history("BTCUSDT")[0]["settlement_mark_price"] is None


def test_migration_rollback_and_retry_preserve_legacy_row(tmp_path, monkeypatch):
    db = tmp_path / "rollback.sqlite"
    with sqlite3.connect(db) as conn:
        conn.executescript("""
            CREATE TABLE schema_version(version INTEGER NOT NULL);
            INSERT INTO schema_version VALUES (3);
            CREATE TABLE funding_observations (
                id INTEGER PRIMARY KEY, symbol TEXT NOT NULL, funding_time_ms INTEGER NOT NULL,
                funding_rate TEXT NOT NULL, observed_at_ms INTEGER NOT NULL, source TEXT NOT NULL
            );
            INSERT INTO funding_observations VALUES (1, 'ETHUSDT', 200, '0.002', 201, 'legacy');
        """)
    original_connect = sqlite3.connect
    failed = {"value": False}

    class FailCopyConnection(sqlite3.Connection):
        def execute(self, sql, parameters=()):
            if "INSERT INTO funding_observations_v4" in sql and not failed["value"]:
                failed["value"] = True
                raise sqlite3.OperationalError("injected interrupted migration")
            return super().execute(sql, parameters)

    def connect_with_failure(path, *args, **kwargs):
        if str(path) == str(db):
            kwargs["factory"] = FailCopyConnection
        return original_connect(path, *args, **kwargs)

    monkeypatch.setattr(sqlite3, "connect", connect_with_failure)
    with pytest.raises(sqlite3.OperationalError, match="injected"):
        SQLiteMarketStore(str(db))
    monkeypatch.setattr(sqlite3, "connect", original_connect)

    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT version FROM schema_version").fetchone()[0] == 3
        assert conn.execute("SELECT funding_rate FROM funding_observations").fetchone()[0] == "0.002"
        assert conn.execute("SELECT name FROM sqlite_master WHERE name='funding_observations_v4'").fetchone() is None
    store = SQLiteMarketStore(str(db))
    assert store.funding_history("ETHUSDT")[0]["funding_rate"] == "0.002"


def test_restore_refuses_existing_destination_and_wal_sidecars(tmp_path):
    store = SQLiteMarketStore(str(tmp_path / "source.sqlite"))
    backup = store.backup_to(str(tmp_path / "source-backup.sqlite"))
    destination = tmp_path / "existing.sqlite"
    destination.write_bytes(b"keep")
    with pytest.raises(FileExistsError):
        SQLiteMarketStore.restore_from_backup(backup, str(destination))
    assert destination.read_bytes() == b"keep"
    sidecar_destination = tmp_path / "sidecar.sqlite"
    (tmp_path / "sidecar.sqlite-wal").write_bytes(b"")
    with pytest.raises(FileExistsError):
        SQLiteMarketStore.restore_from_backup(backup, str(sidecar_destination))


def test_corrupt_backup_is_rejected_without_creating_destination(tmp_path):
    backup = tmp_path / "corrupt.sqlite"
    backup.write_bytes(b"not a sqlite database")
    destination = tmp_path / "never-created.sqlite"
    with pytest.raises(sqlite3.DatabaseError):
        SQLiteMarketStore.restore_from_backup(str(backup), str(destination))
    assert not destination.exists()


def test_cashflow_sign_multiplier_and_historical_mark():
    notional = linear_futures_funding_notional(Decimal("0.2"), Decimal("10"), Decimal("50000"))
    assert notional == Decimal("100000")
    assert funding_payment(notional, Decimal("0.0001")) == Decimal("10")
    assert funding_payment(notional, Decimal("-0.0001")) == Decimal("-10")
    historical = linear_futures_funding_notional(Decimal("0.2"), Decimal("10"), Decimal("49000"))
    current = linear_futures_funding_notional(Decimal("0.2"), Decimal("10"), Decimal("50000"))
    assert historical != current


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [418, 429])
async def test_rate_limit_applies_shared_cooldown_without_retry_storm(status):
    import httpx

    calls = {"count": 0}

    async def handler(request):
        calls["count"] += 1
        return httpx.Response(status, headers={"Retry-After": "30"}, request=request)

    client = BinancePublicClient("https://spot.invalid", "https://futures.invalid")
    client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    try:
        await client.acquire_funding_request("current")
        with pytest.raises(BinanceAPIError, match="shared cooldown"):
            await client._get("futures", "/fapi/v1/fundingRate")
        with pytest.raises(BinanceAPIError, match="cooldown"):
            await client.acquire_funding_request("current")
        assert calls["count"] == 1
    finally:
        await client.close()


def test_scanner_merge_and_summary_deduplicate_source_only_repeat():
    from app.services.market_scanner import _merge_funding_history

    new_rows = [{
        "fundingTime": BASE, "fundingRate": "0.0001", "markPrice": "50000",
        "rateType": "Regular", "source": "primary",
    }]
    stored_rows = [{
        "funding_time_ms": BASE, "funding_rate": "0.0001", "settlement_mark_price": "50000",
        "rate_type": "Regular", "source": "backfill",
    }]
    merged = _merge_funding_history(new_rows, stored_rows)
    assert len(merged) == 1
    assert set(merged[0]["sources"]) == {"primary", "backfill"}
    observations = [FundingObservation(
        "BTCUSDT", Decimal(row["fundingRate"]), int(row["fundingTime"]),
        Decimal(row["markPrice"]), row["rateType"], ",".join(row["sources"])
    ) for row in merged]
    assert summarize_funding(observations + observations)["count"] == 1


def test_same_time_regular_and_special_are_separate_not_conflicting():
    rows = regular_rows()
    rows.append({**rows[0], "rateType": "Special", "fundingRate": "0.00003"})
    result = assess_funding_history(
        rows, interval_hours=8, next_funding_time_ms=rows[-1]["fundingTime"] + 8 * HOUR,
        current_funding_rate="0.0001",
    )
    assert result["state"] == FUNDING_RATE_RECONCILED
    assert result["regular_settlement_count"] == 3
    assert result["special_event_count"] == 1


def test_database_source_provenance_duplicates_are_not_economic_conflicts(tmp_path):
    store = SQLiteMarketStore(str(tmp_path / "source-duplicates.sqlite"))
    row = {"fundingTime": BASE, "fundingRate": "0.0001",
           "markPrice": "50000", "rateType": "Regular"}
    store.save_funding_observations("BTCUSDT", [{**row, "source": "primary"}], BASE + 1)
    store.save_funding_observations("BTCUSDT", [{**row, "source": "backfill"}], BASE + 2)
    history = store.funding_history("BTCUSDT")
    assert len(history) == 1
    assert set(history[0]["sources"]) == {"primary", "backfill"}
    assert store.funding_history_conflicts("BTCUSDT") == []


def test_backup_destination_is_never_overwritten(tmp_path):
    store = SQLiteMarketStore(str(tmp_path / "backup-source.sqlite"))
    destination = tmp_path / "backup-once.sqlite"
    store.backup_to(str(destination))
    before = destination.read_bytes()
    with pytest.raises(FileExistsError):
        store.backup_to(str(destination))
    assert destination.read_bytes() == before


def test_special_event_is_not_in_regular_funding_rate_statistics():
    observations = [
        FundingObservation("BTCUSDT", Decimal("0.0001"), BASE, None, "Regular"),
        FundingObservation("BTCUSDT", Decimal("0.5"), BASE, None, "Special"),
        FundingObservation("BTCUSDT", Decimal("0.0002"), BASE + 8 * HOUR, None, "Regular"),
    ]
    stats = summarize_funding(observations)
    assert stats["count"] == 2
    assert stats["mean"] == Decimal("0.00015")


def test_funding_budget_uses_sliding_not_fixed_window():
    budget = FundingRequestBudget(limit=3, reserve_for_current=1, window_seconds=300)
    budget.try_acquire("backfill", now=0)
    budget.try_acquire("backfill", now=100)
    with pytest.raises(RuntimeError, match="budget"):
        budget.try_acquire("backfill", now=299)
    budget.try_acquire("current", now=299)
    with pytest.raises(RuntimeError, match="budget"):
        budget.try_acquire("current", now=299)
    assert budget.try_acquire("backfill", now=301)


def test_restore_rejects_missing_and_empty_sqlite_backup(tmp_path):
    missing = tmp_path / "missing.sqlite"
    destination = tmp_path / "missing-restore.sqlite"
    with pytest.raises(FileNotFoundError):
        SQLiteMarketStore.restore_from_backup(str(missing), str(destination))
    assert not missing.exists()
    assert not destination.exists()

    empty = tmp_path / "empty.sqlite"
    with sqlite3.connect(empty):
        pass
    with pytest.raises(sqlite3.DatabaseError, match="funding_observations table"):
        SQLiteMarketStore.restore_from_backup(str(empty), str(destination))
    assert not destination.exists()
