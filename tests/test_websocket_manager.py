from app.services.market_data_hub import MarketDataHub
from app.services.websocket_manager import BinanceWebSocketManager


def test_dynamic_symbol_subscription_builds_lowercase_spot_and_futures_shards():
    hub = MarketDataHub()
    manager = BinanceWebSocketManager(hub, "wss://spot/stream", "wss://futures/stream",
                                      streams_per_connection=20)
    manager.set_symbols({"BTCUSDT", "ETHUSDT"})
    assert manager._changed.is_set()
    shards = manager._shards()
    spot = [streams for market, _, streams in shards if market == "spot"]
    futures = [streams for market, _, streams in shards if market == "futures"]
    assert spot and futures
    assert "btcusdt@bookTicker" in spot[0]
    assert "ethusdt@ticker" in spot[0]
    assert "btcusdt@markPrice@1s" in futures[0]
    assert len(spot[0]) == 4
    assert len(futures[0]) == 6


def test_symbol_universe_change_replaces_subscription_set():
    hub = MarketDataHub()
    manager = BinanceWebSocketManager(hub, "wss://spot/stream", "wss://futures/stream")
    manager.set_symbols({"BTCUSDT"})
    manager._changed.clear()
    manager.set_symbols({"ETHUSDT"})
    assert hub.symbols == {"ETHUSDT"}
    assert manager._changed.is_set()
