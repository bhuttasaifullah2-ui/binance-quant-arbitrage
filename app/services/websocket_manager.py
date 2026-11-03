"""Sharded public Binance combined-stream supervisor with dynamic symbol refresh."""
import asyncio
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
                 streams_per_connection: int = 400):
        self.hub, self.spot_url, self.futures_url = hub, spot_url, futures_url
        self.recovery = recovery
        self.streams_per_connection = max(20, streams_per_connection)
        self._stop = asyncio.Event()
        self._changed = asyncio.Event()
        self._tasks: list[asyncio.Task[None]] = []

    def set_symbols(self, symbols: set[str]) -> None:
        normalized = {s.lower() for s in symbols}
        if normalized != {s.lower() for s in self.hub.symbols}:
            self.hub.set_symbols({s.upper() for s in normalized})
            self._changed.set()

    def _shards(self) -> list[tuple[str, list[str]]]:
        spot = [f"{s}@bookTicker" for s in sorted(self.hub.symbols)]
        spot += [f"{s}@ticker" for s in sorted(self.hub.symbols)]
        futures = [f"{s}@bookTicker" for s in sorted(self.hub.symbols)]
        futures += [f"{s}@ticker" for s in sorted(self.hub.symbols)]
        futures += [f"{s}@markPrice@1s" for s in sorted(self.hub.symbols)]
        output: list[tuple[str, list[str]]] = []
        for market, url, streams in (("spot", self.spot_url, spot), ("futures", self.futures_url, futures)):
            for i in range(0, len(streams), self.streams_per_connection):
                output.append((market, streams[i:i+self.streams_per_connection]))
        return output

    async def _consume(self, market: str, url: str, streams: list[str]) -> None:
        endpoint = url.rstrip("/") + "?streams=" + "/".join(streams)
        delay = 1.0
        while not self._stop.is_set():
            try:
                async with websockets.connect(endpoint, ping_interval=20, ping_timeout=20,
                                               close_timeout=5, max_size=2**20) as ws:
                    self.hub.health[market] = {"connected": True, "streams": len(streams)}
                    if self.recovery:
                        await self.recovery(sorted({x.split("@", 1)[0].upper() for x in streams}))
                    delay = 1.0
                    async for raw in ws:
                        if self._stop.is_set():
                            break
                        try:
                            import json
                            message = json.loads(raw)
                            if isinstance(message, dict):
                                message["_market"] = market
                            accepted = self.hub.apply_message(message)
                            if accepted:
                                self.hub.health[market]["last_event_ms"] = int(time.time()*1000)
                        except (ValueError, TypeError):
                            logger.exception("Invalid %s websocket event", market)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.hub.health[market] = {"connected": False, "error": type(exc).__name__}
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
                           for m, s in self._shards() if s]
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
