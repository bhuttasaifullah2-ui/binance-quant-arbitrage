import httpx
import pytest
from app.api.main import app

class FakeStore:
    def latest(self, limit=100, offset=0):
        return [{"symbol": "BTCUSDT"}]
    def funding_history(self, symbol, limit=100, offset=0):
        return [{"symbol": symbol, "funding_rate": "0.0001"}]

class FakeScanner:
    opportunities = []
    fresh_opportunities = []
    def status(self):
        return {"running": False, "last_scan_ms": None, "last_scan_age_ms": None,
                "matched_pair_count": 0, "valid_opportunity_count": 0, "last_error": None,
                "database_observations": 0, "live_trading_enabled": False, "execution_enabled": False}

class FakeClient:
    async def spot_exchange_info(self):
        return {"symbols": [{"symbol": "BTCUSDT", "status": "TRADING", "quoteAsset": "USDT", "isSpotTradingAllowed": True}]}
    async def spot_ticker_24h(self):
        return [{"symbol": "BTCUSDT", "lastPrice": "60000", "quoteVolume": "1000000"}]
    async def spot_book_ticker(self):
        return [{"symbol": "BTCUSDT", "bidPrice": "59999", "askPrice": "60001"}]
    async def futures_exchange_info(self):
        return {"symbols": [{"symbol": "BTCUSDT", "status": "TRADING", "quoteAsset": "USDT", "contractType": "PERPETUAL"}]}
    async def futures_ticker_24h(self):
        return [{"symbol": "BTCUSDT", "quoteVolume": "2000000"}]
    async def futures_book_ticker(self):
        return [{"symbol": "BTCUSDT", "bidPrice": "60001", "askPrice": "60002"}]
    async def futures_premium_index(self):
        return [{"symbol": "BTCUSDT", "markPrice": "60001", "indexPrice": "60000", "lastFundingRate": "0.0001", "nextFundingTime": 1900000000000}]

@pytest.mark.asyncio
async def test_read_only_api_routes():
    app.state.market_store = FakeStore()
    app.state.market_scanner = FakeScanner()
    app.state.binance_client = FakeClient()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        health = await client.get("/api/v1/health")
        spot = await client.get("/api/v1/market/spot")
        futures = await client.get("/api/v1/market/futures")
        funding = await client.get("/api/v1/funding/current")
        status = await client.get("/api/v1/scanner/status")
    assert health.status_code == 200
    assert health.json()["live_trading_enabled"] is False
    assert spot.json()["items"][0]["symbol"] == "BTCUSDT"
    assert futures.json()["items"][0]["funding_rate"] == "0.0001"
    assert funding.json()["items"][0]["symbol"] == "BTCUSDT"
    assert status.json()["execution_enabled"] is False
