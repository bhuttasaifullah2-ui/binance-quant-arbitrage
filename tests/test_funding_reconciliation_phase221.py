import asyncio
import time

from app.infrastructure.validation_store import ValidationStore
from app.services.live_validation import LiveValidationService


class FundingClient:
    def __init__(self, rows, backfill_rows=None):
        self.rows = rows
        self.backfill_rows = backfill_rows or []
        self.backfill_calls = 0

    async def futures_premium_index(self):
        return [{"symbol": "BTCUSDT", "lastFundingRate": self.rows[-1]["fundingRate"],
                 "nextFundingTime": int(self.rows[-1]["fundingTime"]) + 8 * 3600000 + 1000,
                 "time": int(time.time() * 1000)}]

    async def futures_funding_info(self):
        return []

    async def futures_funding_history(self, symbol, limit, start_time_ms=None, end_time_ms=None):
        if start_time_ms is None:
            return list(self.rows)
        self.backfill_calls += 1
        return list(self.backfill_rows)


class Dummy:
    pass


def test_funding_reconciliation_backfills_missing_interval(tmp_path):
    now = int(time.time() * 1000)
    step = 8 * 3600000
    first = now - 32 * 3600000
    rows = [
        {"fundingTime": first, "fundingRate": "0.0001"},
        {"fundingTime": first + step, "fundingRate": "0.0002"},
        {"fundingTime": first + 3 * step, "fundingRate": "0.0003"},
        {"fundingTime": first + 3 * step, "fundingRate": "0.0003"},
    ]
    backfill = [{"fundingTime": first + 2 * step, "fundingRate": "0.00025"}]
    client = FundingClient(rows, backfill)
    store = ValidationStore(str(tmp_path / "funding.sqlite3"))
    service = LiveValidationService(client, Dummy(), Dummy(), store)
    result = asyncio.run(service.validate_funding("BTCUSDT"))
    assert client.backfill_calls == 1
    assert result["duplicate_interval_count"] == 1
    assert result["unresolved_gaps"] == []
    assert result["status"] == "HEALTHY"
    assert result["actual_future_funding"] == "UNKNOWN_UNTIL_REALIZED"


def test_funding_reconciliation_keeps_unresolved_gaps_explicit(tmp_path):
    now = int(time.time() * 1000)
    step = 8 * 3600000
    first = now - 32 * 3600000
    rows = [
        {"fundingTime": first, "fundingRate": "0.0001"},
        {"fundingTime": first + step, "fundingRate": "0.0002"},
        {"fundingTime": first + 3 * step, "fundingRate": "0.0003"},
        {"fundingTime": first + 4 * step, "fundingRate": "0.0004"},
    ]
    client = FundingClient(rows, [])
    service = LiveValidationService(client, Dummy(), Dummy(),
                                     ValidationStore(str(tmp_path / "gap.sqlite3")))
    result = asyncio.run(service.validate_funding("BTCUSDT"))
    assert result["status"] == "WARNING"
    assert result["unresolved_gaps"]
    assert result["missing_intervals_are_not_zero"] is True



def test_funding_reconciliation_flags_current_vs_historical_mismatch(tmp_path):
    now = int(time.time() * 1000)
    rows = [{"fundingTime": now - 8 * 3600000, "fundingRate": "0.0001"},
            {"fundingTime": now, "fundingRate": "0.0001"}]
    client = FundingClient(rows, [])
    async def mismatched_premium():
        return [{"symbol": "BTCUSDT", "lastFundingRate": "0.005",
                 "nextFundingTime": int(time.time() * 1000) + 3600000}]
    client.futures_premium_index = mismatched_premium
    service = LiveValidationService(client, Dummy(), Dummy(),
                                    ValidationStore(str(tmp_path / "mismatch.sqlite3")))
    result = asyncio.run(service.validate_funding("BTCUSDT"))
    assert result["status"] == "WARNING"
    assert result["current_vs_latest_historical_match"] is False



def test_funding_reconciliation_rejects_stale_latest_settlement_timestamp(tmp_path):
    now = int(time.time() * 1000)
    rows = [
        {"fundingTime": now - 16 * 3600000, "fundingRate": "0.0001"},
        {"fundingTime": now - 8 * 3600000, "fundingRate": "0.0001"},
    ]
    client = FundingClient(rows, [])
    async def stale_next_funding():
        return [{"symbol": "BTCUSDT", "lastFundingRate": "0.0001",
                 "nextFundingTime": now + 3600000, "time": now}]
    client.futures_premium_index = stale_next_funding
    service = LiveValidationService(client, Dummy(), Dummy(),
                                    ValidationStore(str(tmp_path / "stale-settlement.sqlite3")))
    result = asyncio.run(service.validate_funding("BTCUSDT"))
    assert result["current_vs_latest_historical_match"] is True
    assert result["latest_settlement_time_match"] is False
    assert result["status"] == "WARNING"


def test_funding_reconciliation_uses_exchange_interval_override(tmp_path):
    now = int(time.time() * 1000)
    rows = [
        {"fundingTime": now - 2 * 3600000, "fundingRate": "0.0001"},
        {"fundingTime": now - 1 * 3600000, "fundingRate": "0.0001"},
    ]
    client = FundingClient(rows, [])
    async def one_hour_next_funding():
        return [{"symbol": "BTCUSDT", "lastFundingRate": "0.0001",
                 "nextFundingTime": int(rows[-1]["fundingTime"]) + 3600000 + 1000,
                 "time": now}]
    async def one_hour_funding_info():
        return [{"symbol": "BTCUSDT", "fundingIntervalHours": 1}]
    client.futures_premium_index = one_hour_next_funding
    client.futures_funding_info = one_hour_funding_info
    service = LiveValidationService(client, Dummy(), Dummy(),
                                    ValidationStore(str(tmp_path / "one-hour.sqlite3")))
    result = asyncio.run(service.validate_funding("BTCUSDT"))
    assert result["funding_interval_hours_estimate"] == 1
    assert result["funding_interval_source"] == "BINANCE_FUNDING_INFO"
    assert result["latest_settlement_time_match"] is True
    assert result["current_premium_index_fresh"] is True
    assert result["status"] == "HEALTHY"
