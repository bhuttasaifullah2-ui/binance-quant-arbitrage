import httpx
import pytest
from app.api.main import app


class FakeStore:
    def latest(self, limit=100, offset=0):
        return [{"symbol": "BTCUSDT"}]
    def funding_history(self, symbol, limit=100, offset=0):
        return [{"symbol": symbol, "funding_rate": "0.0001"}]
    def rejections(self, limit=100):
        return [{"symbol": "ETHUSDT", "reason": "stale:spot_bid"}]
    def observation_summary(self):
        return {"opportunity_snapshots": 2, "lifecycle_events": 1, "symbols_observed": 1}
    def profitability_history(self, symbol, limit=100):
        return [{"symbol": symbol, "expected_net_usdt": "0.2"}]
    def websocket_health(self):
        return [{"market": "spot", "connected": 1}]
    def exchange_filter(self, market, symbol):
        if market != "spot":
            return None
        return {"symbol": symbol, "status": "TRADING", "baseAsset": "BTC", "quoteAsset": "USDT",
                "filters": [{"filterType": "PRICE_FILTER", "tickSize": "0.01", "minPrice": "0", "maxPrice": "0"},
                            {"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001", "maxQty": "10"}]}


class FakeScanner:
    opportunities = []
    fresh_opportunities = []
    rejections = [{"symbol": "ETHUSDT", "reason": "stale:spot_bid"}]
    def status(self):
        return {"running": False, "last_scan_ms": None, "last_scan_age_ms": None,
                "matched_pair_count": 0, "valid_opportunity_count": 0, "last_error": None,
                "database_observations": 0, "live_trading_enabled": False, "execution_enabled": False,
                "websocket_enabled": False}


class FakeHub:
    def stream_health(self):
        return {"streams": {}, "symbol_count": 0, "live_trading_enabled": False}
    def data_quality(self, max_age_ms=15000):
        return {"symbols": {}, "quality_events": [], "symbol_count": 0}


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
    app.state.market_hub = FakeHub()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        health = await client.get("/api/v1/health")
        spot = await client.get("/api/v1/market/spot")
        futures = await client.get("/api/v1/market/futures")
        funding = await client.get("/api/v1/funding/current")
        status = await client.get("/api/v1/scanner/status")
        stream_health = await client.get("/api/v1/market/stream-health")
        data_quality = await client.get("/api/v1/market/data-quality")
        rejections = await client.get("/api/v1/scanner/rejections")
        filters = await client.get("/api/v1/exchange/filters/BTCUSDT")
        profitability = await client.get("/api/v1/analytics/profitability/BTCUSDT")
        observation = await client.get("/api/v1/observation/summary")
    assert health.status_code == 200
    assert health.json()["live_trading_enabled"] is False
    assert spot.json()["items"][0]["symbol"] == "BTCUSDT"
    assert futures.json()["items"][0]["funding_rate"] == "0.0001"
    assert funding.json()["items"][0]["symbol"] == "BTCUSDT"
    assert status.json()["execution_enabled"] is False
    assert stream_health.status_code == 200
    assert data_quality.status_code == 200
    assert rejections.json()["items"][0]["symbol"] == "ETHUSDT"
    assert filters.json()["filters"]["spot"]["step_size"] == "0.001"
    assert profitability.json()["items"][0]["expected_net_usdt"] == "0.2"
    assert observation.json()["enabled"] is False
