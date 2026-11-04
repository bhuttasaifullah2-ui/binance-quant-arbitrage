import pytest
from app.exchange.binance.funding_history import fetch_funding_history


class FakeClient:
    def __init__(self):
        self.calls = []
    async def futures_funding_history(self, symbol, limit, start_time_ms=None, end_time_ms=None):
        self.calls.append((symbol, limit, start_time_ms, end_time_ms))
        if start_time_ms is None:
            return [{"fundingTime": 100, "fundingRate": "0.001"},
                    {"fundingTime": 200, "fundingRate": "0.002"}]
        if start_time_ms == 201:
            return [{"fundingTime": 200, "fundingRate": "0.002"},
                    {"fundingTime": 300, "fundingRate": "0.003"}]
        return []


@pytest.mark.asyncio
async def test_funding_history_paginates_and_deduplicates():
    client = FakeClient()
    rows = await fetch_funding_history(client, "BTCUSDT", page_size=2, max_pages=3)
    assert [x["fundingTime"] for x in rows] == [100, 200, 300]
    assert len(client.calls) == 3



@pytest.mark.asyncio
async def test_history_keeps_same_timestamp_special_rate_and_mark_conflicts():
    from app.exchange.binance.funding_history import fetch_funding_history_with_stats

    class ConflictClient:
        async def futures_funding_history(self, symbol, limit, start_time_ms=None, end_time_ms=None):
            return [
                {"fundingTime": 500, "fundingRate": "0.0001", "markPrice": "100", "rateType": "Regular"},
                {"fundingTime": 500, "fundingRate": "0.0001", "markPrice": "100", "rateType": "Regular"},
                {"fundingTime": 500, "fundingRate": "0.0001", "markPrice": "101", "rateType": "Regular"},
                {"fundingTime": 500, "fundingRate": "0.0002", "markPrice": "100", "rateType": "Special"},
            ]

    rows, duplicates = await fetch_funding_history_with_stats(
        ConflictClient(), "BTCUSDT", page_size=10, max_pages=1
    )
    assert duplicates == 1
    assert len(rows) == 3
    assert {row["rateType"] for row in rows} == {"Regular", "Special"}
    assert {row["markPrice"] for row in rows} == {"100", "101"}
