import asyncio
import logging
from typing import Any

import httpx

logger = logging.getLogger(__name__)

class BinanceAPIError(RuntimeError):
    """Raised when Binance returns an unusable response after retries."""

class BinancePublicClient:
    """Unauthenticated REST client for public Spot and USDⓈ-M Futures data only."""

    def __init__(
        self,
        spot_base_url: str,
        futures_base_url: str,
        timeout: float = 10.0,
        max_retries: int = 4,
        retry_base_seconds: float = 0.4,
        max_concurrent_requests: int = 8,
    ) -> None:
        self.spot_base_url = spot_base_url.rstrip("/")
        self.futures_base_url = futures_base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_base_seconds = retry_base_seconds
        self._semaphore = asyncio.Semaphore(max_concurrent_requests)
        self._client: httpx.AsyncClient | None = None

    async def __aenter__(self) -> "BinancePublicClient":
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(self.timeout),
            headers={"User-Agent": "binance-quant-arbitrage/0.2 read-only"},
        )
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def _get(self, market: str, path: str, params: dict[str, Any] | None = None) -> Any:
        base = self.spot_base_url if market == "spot" else self.futures_base_url
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(self.timeout),
                headers={"User-Agent": "binance-quant-arbitrage/0.2 read-only"},
            )
        url = f"{base}{path}"
        async with self._semaphore:
            for attempt in range(self.max_retries + 1):
                try:
                    response = await self._client.get(url, params=params)
                    if response.status_code in (418, 429):
                        retry_after = float(response.headers.get("Retry-After", "1"))
                        if attempt >= self.max_retries:
                            raise BinanceAPIError(
                                f"Binance rate limit response {response.status_code} for {path}"
                            )
                        await asyncio.sleep(max(retry_after, self.retry_base_seconds * (2 ** attempt)))
                        continue
                    if response.status_code >= 500 and attempt < self.max_retries:
                        await asyncio.sleep(self.retry_base_seconds * (2 ** attempt))
                        continue
                    response.raise_for_status()
                    try:
                        return response.json()
                    except ValueError as exc:
                        raise BinanceAPIError(f"Malformed JSON response from {path}") from exc
                except (httpx.TimeoutException, httpx.NetworkError) as exc:
                    if attempt >= self.max_retries:
                        raise BinanceAPIError(f"Binance request failed for {path}: {type(exc).__name__}") from exc
                    await asyncio.sleep(self.retry_base_seconds * (2 ** attempt))
                except httpx.HTTPStatusError as exc:
                    raise BinanceAPIError(
                        f"Binance HTTP {exc.response.status_code} for {path}"
                    ) from exc
        raise BinanceAPIError(f"Binance request exhausted retries for {path}")

    async def spot_exchange_info(self) -> dict[str, Any]:
        return await self._get("spot", "/api/v3/exchangeInfo")

    async def futures_exchange_info(self) -> dict[str, Any]:
        return await self._get("futures", "/fapi/v1/exchangeInfo")

    async def spot_ticker_24h(self) -> list[dict[str, Any]]:
        return await self._get("spot", "/api/v3/ticker/24hr")

    async def spot_ticker_price(self) -> list[dict[str, Any]]:
        return await self._get("spot", "/api/v3/ticker/price")

    async def spot_book_ticker(self) -> list[dict[str, Any]]:
        return await self._get("spot", "/api/v3/ticker/bookTicker")

    async def spot_depth(self, symbol: str, limit: int = 20) -> dict[str, Any]:
        return await self._get("spot", "/api/v3/depth", {"symbol": symbol, "limit": limit})

    async def futures_ticker_24h(self) -> list[dict[str, Any]]:
        return await self._get("futures", "/fapi/v1/ticker/24hr")

    async def futures_book_ticker(self) -> list[dict[str, Any]]:
        return await self._get("futures", "/fapi/v1/ticker/bookTicker")

    async def futures_premium_index(self) -> list[dict[str, Any]]:
        return await self._get("futures", "/fapi/v1/premiumIndex")

    async def futures_funding_info(self) -> list[dict[str, Any]]:
        return await self._get("futures", "/fapi/v1/fundingInfo")

    async def futures_funding_history(self, symbol: str, limit: int = 100,
                                      start_time_ms: int | None = None,
                                      end_time_ms: int | None = None) -> list[dict[str, Any]]:
        if not 1 <= limit <= 1000:
            raise ValueError("limit must be between 1 and 1000")
        params: dict[str, Any] = {"symbol": symbol, "limit": limit}
        if start_time_ms is not None:
            params["startTime"] = start_time_ms
        if end_time_ms is not None:
            params["endTime"] = end_time_ms
        return await self._get("futures", "/fapi/v1/fundingRate", params)

    async def futures_depth(self, symbol: str, limit: int = 20) -> dict[str, Any]:
        return await self._get("futures", "/fapi/v1/depth", {"symbol": symbol, "limit": limit})

    async def futures_open_interest(self, symbol: str) -> dict[str, Any]:
        return await self._get("futures", "/fapi/v1/openInterest", {"symbol": symbol})

    async def futures_server_time(self) -> dict[str, Any]:
        return await self._get("futures", "/fapi/v1/time")

    async def spot_server_time(self) -> dict[str, Any]:
        return await self._get("spot", "/api/v3/time")
