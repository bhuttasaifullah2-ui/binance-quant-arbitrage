"""Sharded public Binance combined-stream supervisor with reliability telemetry."""
import asyncio
import json
import logging
import time
from collections.abc import Awaitable, Callable
from typing import Any
import websockets
from app.services.market_data_hub import MarketDataHub

logger = logging.getLogger(__name__)


class BinanceWebSocketManager:
    def __init__(self, hub: MarketDataHub, spot_url: str, futures_url: str,
                 recovery: Callable[[list[str]], Awaitable[None]] | None = None,
                 streams_per_connection: int = 400,
                 health_sink: Callable[[str, dict[str, Any], int], None] | None = None,
                 event_sink: Callable[[int, str, str | None, str | None, dict[str, Any]], None] | None = None):
        self.hub, self.spot_url, self.futures_url = hub, spot_url, futures_url
        self.recovery, self.health_sink, self.event_sink = recovery, health_sink, event_sink
        self.streams_per_connection = max(20, streams_per_connection)
        self._stop = asyncio.Event()
        self._changed = asyncio.Event()
        self._tasks: list[asyncio.Task[None]] = []
        self._last_health_persist_ms: dict[str, int] = {}
        self.metrics: dict[str, dict[str, Any]] = {}
        self._connected_since: dict[str, int] = {}
        self._recovery_started: dict[str, int] = {}
        self._connected_shards: dict[str, set[str]] = {"spot": set(), "futures": set()}
        self._attempted_shards: dict[str, set[str]] = {"spot": set(), "futures": set()}
        self._failed_shards: dict[str, set[str]] = {"spot": set(), "futures": set()}

    def _metric(self, market: str) -> dict[str, Any]:
        return self.metrics.setdefault(market, {"connected": False, "disconnect_count": 0,
            "reconnect_attempts": 0, "reconnect_count": 0, "messages": 0,
            "consecutive_failures": 0, "last_message_ms": None, "last_update_id": None,
            "total_processing_latency_ms": 0.0, "max_processing_latency_ms": 0.0,
            "subscription_status": "PENDING", "recovery_durations_ms": [], "uptime_ms": 0,
            "initial_connection_attempted": False})

    def _set_health(self, market: str, value: dict[str, Any]) -> None:
        metric = self._metric(market)
        now = int(time.time() * 1000)
        was_connected = bool(metric.get("connected"))
        if was_connected and value.get("connected") is False:
            metric["uptime_ms"] += max(0, now - self._connected_since.get(market, now))
        if value.get("connected") is True and not was_connected:
            self._connected_since[market] = now
        metric.update(value)
        metric["checked_at_ms"] = now
        metric["average_processing_latency_ms"] = (
            metric["total_processing_latency_ms"] / metric["messages"] if metric["messages"] else None)
        metric["reconnection_success_rate"] = (
            metric["reconnect_count"] / metric["reconnect_attempts"] if metric["reconnect_attempts"] else None)
        self.hub.health[market] = dict(metric)
        if self.health_sink:
            self.health_sink(market, dict(metric), now)
            self._last_health_persist_ms[market] = now

    def set_symbols(self, symbols: set[str]) -> None:
        normalized = {s.lower() for s in symbols}
        if normalized != {s.lower() for s in self.hub.symbols}:
            self.hub.set_symbols({s.upper() for s in normalized})
            self._changed.set()

    def _shards(self) -> list[tuple[str, str, list[str]]]:
        symbols = [s.lower() for s in sorted(self.hub.symbols)]
        spot = [f"{s}@bookTicker" for s in symbols] + [f"{s}@ticker" for s in symbols]
        futures = ([f"{s}@bookTicker" for s in symbols] + [f"{s}@ticker" for s in symbols]
                   + [f"{s}@markPrice@1s" for s in symbols])
        output = []
        for market, url, streams in (("spot", self.spot_url, spot), ("futures", self.futures_url, futures)):
            for i in range(0, len(streams), self.streams_per_connection):
                output.append((market, url, streams[i:i + self.streams_per_connection]))
        return output

    async def _consume(self, market: str, url: str, streams: list[str]) -> None:
        endpoint = url.rstrip("/") + "?streams=" + "/".join(streams)
        delay = 1.0
        shard_key = streams[0] if streams else "empty"
        while not self._stop.is_set():
            metric = self._metric(market)
            attempted = self._attempted_shards.setdefault(market, set())
            if shard_key in attempted:
                metric["reconnect_attempts"] += 1
            else:
                attempted.add(shard_key)
                metric["initial_connection_attempted"] = True
            self._recovery_started.setdefault(market, int(time.time() * 1000))
            try:
                async with websockets.connect(endpoint, ping_interval=20, ping_timeout=20,
                                               close_timeout=5, max_size=2**20) as ws:
                    now = int(time.time() * 1000)
                    connected_shards = self._connected_shards.setdefault(market, set())
                    market_was_down = not connected_shards
                    failed_shards = self._failed_shards.setdefault(market, set())
                    if shard_key in failed_shards:
                        metric["reconnect_count"] += 1
                        failed_shards.discard(shard_key)
                    metric["consecutive_failures"] = 0
                    connected_shards.add(shard_key)
                    if market_was_down and metric["disconnect_count"]:
                        duration = max(0, now-self._recovery_started.get(market, now))
                        metric["recovery_durations_ms"].append(duration)
                        metric["recovery_durations_ms"] = metric["recovery_durations_ms"][-100:]
                        if self.event_sink:
                            self.event_sink(now, "WEBSOCKET_RECOVERED", None, "HEALTHY",
                                {"market": market, "recovery_duration_ms": duration})
                    elif market_was_down and self.event_sink:
                        self.event_sink(now, "WEBSOCKET_CONNECTED", None, "HEALTHY", {"market": market})
                    self._connected_since[market] = now
                    self._set_health(market, {"connected": True, "streams": len(streams),
                        "subscription_status": "SUBSCRIBED", "last_connected_ms": now})
                    if self.recovery:
                        await self.recovery(sorted({x.split("@", 1)[0].upper() for x in streams}))
                    delay = 1.0
                    async for raw in ws:
                        if self._stop.is_set():
                            break
                        received = int(time.time() * 1000)
                        started = time.perf_counter()
                        try:
                            message = json.loads(raw)
                            if isinstance(message, dict):
                                message["_market"] = market
                            accepted = self.hub.apply_message(message, received)
                            elapsed = (time.perf_counter() - started) * 1000
                            metric["messages"] += 1
                            metric["total_processing_latency_ms"] += elapsed
                            metric["max_processing_latency_ms"] = max(metric["max_processing_latency_ms"], elapsed)
                            metric["last_message_ms"] = received
                            payload = message.get("data", {}) if isinstance(message, dict) else {}
                            if isinstance(payload, dict) and payload.get("u") is not None:
                                metric["last_update_id"] = payload["u"]
                            if accepted:
                                metric["last_event_ms"] = received
                            if self.health_sink and received-self._last_health_persist_ms.get(market, 0) >= 10000:
                                self._set_health(market, {"connected": True})
                        except (ValueError, TypeError):
                            logger.exception("Invalid %s websocket event", market)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                now = int(time.time() * 1000)
                metric["disconnect_count"] += 1
                metric["consecutive_failures"] += 1
                failed_shards = self._failed_shards.setdefault(market, set())
                failed_shards.add(shard_key)
                connected_shards = self._connected_shards.setdefault(market, set())
                connected_shards.discard(shard_key)
                market_disconnected = not connected_shards
                metric["subscription_status"] = "DISCONNECTED" if market_disconnected else "SUBSCRIBED"
                if market_disconnected:
                    self._recovery_started[market] = now
                    if self.event_sink:
                        self.event_sink(now, "WEBSOCKET_DISCONNECTED", None, "UNHEALTHY",
                                        {"market": market, "error": type(exc).__name__})
                self._set_health(market, {"connected": not market_disconnected, "error": type(exc).__name__})
                logger.warning("%s websocket reconnecting after %s", market, type(exc).__name__)
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=delay)
                except asyncio.TimeoutError:
                    pass
                delay = min(delay * 2, 30.0)

    async def run(self) -> None:
        while not self._stop.is_set():
            self._changed.clear()
            self._tasks = [asyncio.create_task(self._consume(m, u, s))
                           for m, u, s in self._shards() if s]
            changed = asyncio.create_task(self._changed.wait())
            stop = asyncio.create_task(self._stop.wait())
            done, pending = await asyncio.wait([changed, stop], return_when=asyncio.FIRST_COMPLETED)
            for task in self._tasks:
                task.cancel()
            await asyncio.gather(*self._tasks, return_exceptions=True)
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            self._tasks = []
            if stop in done:
                break

    async def close(self) -> None:
        self._stop.set()
        self._changed.set()
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
