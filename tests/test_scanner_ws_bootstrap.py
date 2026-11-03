from types import SimpleNamespace
import pytest
from app.services.market_data_hub import MarketDataHub
from app.services.market_scanner import MarketScanner


class FakeClient:
    def __init__(self):
        self.bulk_spot_calls = 0
    async def spot_exchange_info(self):
        return {"symbols": []}
    async def futures_exchange_info(self):
        return {"symbols": []}
    async def spot_ticker_24h(self):
        self.bulk_spot_calls += 1
        return []
    async def spot_book_ticker(self):
        return []
    async def futures_ticker_24h(self):
        return []
    async def futures_book_ticker(self):
        return []
    async def futures_premium_index(self):
        return []
    async def close(self):
        pass


class FakeStore:
    def save_exchange_metadata(self, *args):
        pass
    def save_scanner_run(self, *args):
        pass
    def save_quality_event(self, *args):
        pass
    def prune(self, *args):
        pass
    def count(self):
        return 0


class FakeWebSocketManager:
    def set_symbols(self, symbols):
        self.symbols = symbols


@pytest.mark.asyncio
async def test_websocket_mode_uses_one_rest_bootstrap_then_hub_state():
    client = FakeClient()
    config = SimpleNamespace(websocket_enabled=True, max_data_age_seconds=75,
                             observation_mode=False)
    scanner = MarketScanner(client, FakeStore(), config, MarketDataHub(), FakeWebSocketManager())
    await scanner.scan_once()
    assert scanner._ws_bootstrap_done
    assert client.bulk_spot_calls == 1
    await scanner.scan_once()
    assert client.bulk_spot_calls == 1
