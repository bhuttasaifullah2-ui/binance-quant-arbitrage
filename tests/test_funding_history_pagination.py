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
