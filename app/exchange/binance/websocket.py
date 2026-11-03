import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from typing import Any

import websockets

logger = logging.getLogger(__name__)

class BinanceCombinedStream:
    """Optional public combined-stream consumer with reconnect/backoff."""
    def __init__(self, url: str, streams: list[str], on_message: Callable[[dict[str, Any]], Awaitable[None]]):
        self.url = url
        self.streams = streams
        self.on_message = on_message
        self._stop = asyncio.Event()

    async def run(self) -> None:
        if not self.streams:
            return
        query = "/".join(self.streams)
        endpoint = self.url.rstrip("/") + "?streams=" + query
        delay = 1.0
        while not self._stop.is_set():
            try:
                async with websockets.connect(endpoint, ping_interval=20, ping_timeout=20,
                                               close_timeout=5, max_size=2**20) as ws:
                    delay = 1.0
                    async for raw in ws:
                        if self._stop.is_set():
                            break
                        try:
                            message = json.loads(raw)
                            await self.on_message(message)
                        except (json.JSONDecodeError, TypeError, ValueError):
                            logger.exception("Invalid websocket message")
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Binance websocket disconnected; reconnecting")
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=delay)
                except asyncio.TimeoutError:
                    pass
                delay = min(delay * 2, 30.0)

    def stop(self) -> None:
        self._stop.set()
