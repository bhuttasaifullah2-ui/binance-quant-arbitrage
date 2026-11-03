import httpx
import pytest
from app.api.main import app

class FakeStore:
    def latest(self, limit=100):
        return [{'symbol':'BTCUSDT'}]
    def funding_history(self, symbol, limit=100):
        return [{'symbol':symbol,'funding_rate':'0.0001'}]

class FakeScanner:
    opportunities = []
    fresh_opportunities = []
    def status(self):
        return {'running':False,'last_scan_ms':None,'last_scan_age_ms':None,
                'matched_pair_count':0,'valid_opportunity_count':0,'last_error':None,
                'database_observations':0,'live_trading_enabled':False,'execution_enabled':False}

@pytest.mark.asyncio
async def test_read_only_api_routes():
    app.state.market_store = FakeStore()
    app.state.market_scanner = FakeScanner()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url='http://test') as client:
        health = await client.get('/api/v1/health')
        spot = await client.get('/api/v1/market/spot')
        status = await client.get('/api/v1/scanner/status')
    assert health.status_code == 200
    assert health.json()['live_trading_enabled'] is False
    assert spot.json()['items'][0]['symbol'] == 'BTCUSDT'
    assert status.json()['execution_enabled'] is False
