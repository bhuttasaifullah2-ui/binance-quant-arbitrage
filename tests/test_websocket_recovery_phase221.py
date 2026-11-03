import asyncio
import json
import time

from app.services.market_data_hub import MarketDataHub
from app.services.websocket_manager import BinanceWebSocketManager


def test_websocket_reconnect_success_records_recovery(monkeypatch):
    hub = MarketDataHub()
    events = []
    manager = BinanceWebSocketManager(
        hub, "wss://spot/stream", "wss://futures/stream",
        event_sink=lambda at, kind, symbol, status, payload: events.append((kind, payload)))
    attempts = {"count": 0}

    class Socket:
        async def __aenter__(self):
            return self
        async def __aexit__(self, *args):
            return None
        async def __aiter__(self):
            now = int(time.time() * 1000)
            yield json.dumps({"stream": "btcusdt@ticker", "data": {
                "s": "BTCUSDT", "E": now, "c": "100", "q": "1000"}})
            manager._stop.set()

    class Connect:
        def __init__(self, fail):
            self.fail = fail
        async def __aenter__(self):
            if self.fail:
                raise OSError("simulated disconnect")
            return await Socket().__aenter__()
        async def __aexit__(self, *args):
            return None

    def connect(*args, **kwargs):
        attempts["count"] += 1
        return Connect(attempts["count"] == 1)

    async def no_delay(awaitable, timeout):
        if manager._stop.is_set():
            return await awaitable
        awaitable.close()
        raise asyncio.TimeoutError

    monkeypatch.setattr("app.services.websocket_manager.websockets.connect", connect)
    monkeypatch.setattr("app.services.websocket_manager.asyncio.wait_for", no_delay)
    asyncio.run(manager._consume("spot", "wss://spot/stream", ["btcusdt@ticker"]))
    metric = manager.metrics["spot"]
    assert metric["disconnect_count"] == 1
    assert metric["reconnect_attempts"] == 1
    assert metric["reconnect_count"] == 1
    assert metric["reconnection_success_rate"] == 1
    assert any(kind == "WEBSOCKET_DISCONNECTED" for kind, _ in events)
    assert any(kind == "WEBSOCKET_RECOVERED" for kind, _ in events)


def test_websocket_reconnect_failure_is_visible(monkeypatch):
    hub = MarketDataHub()
    events = []
    manager = BinanceWebSocketManager(
        hub, "wss://spot/stream", "wss://futures/stream",
        event_sink=lambda at, kind, symbol, status, payload: (
            events.append(kind),
            manager._stop.set() if len(events) >= 2 else None))
    async def fail_connect(*args, **kwargs):
        raise OSError("simulated connection failure")
    class FailedContext:
        async def __aenter__(self):
            raise OSError("simulated connection failure")
        async def __aexit__(self, *args):
            return None
    monkeypatch.setattr("app.services.websocket_manager.websockets.connect",
                        lambda *args, **kwargs: FailedContext())

    async def no_delay(awaitable, timeout):
        if manager._stop.is_set():
            return await awaitable
        awaitable.close()
        raise asyncio.TimeoutError
    monkeypatch.setattr("app.services.websocket_manager.asyncio.wait_for", no_delay)
    asyncio.run(manager._consume("spot", "wss://spot/stream", ["btcusdt@ticker"]))
    metric = manager.metrics["spot"]
    assert metric["disconnect_count"] == 2
    assert metric["reconnect_attempts"] == 1
    assert metric["reconnect_count"] == 0
    assert metric["consecutive_failures"] == 2
    assert events.count("WEBSOCKET_DISCONNECTED") == 2
