import httpx

class BinancePublicClient:
    """Public REST market-data client. No credentials and no order methods."""
    def __init__(self, spot_base_url: str, futures_base_url: str, timeout: float = 10.0):
        self.spot_base_url = spot_base_url.rstrip("/")
        self.futures_base_url = futures_base_url.rstrip("/")
        self.timeout = timeout

    async def _get(self, base: str, path: str, params: dict | None = None):
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.get(f"{base}{path}", params=params)
            response.raise_for_status()
            return response.json()

    async def spot_exchange_info(self):
        return await self._get(self.spot_base_url, "/api/v3/exchangeInfo")

    async def futures_exchange_info(self):
        return await self._get(self.futures_base_url, "/fapi/v1/exchangeInfo")

    async def spot_ticker_24h(self):
        return await self._get(self.spot_base_url, "/api/v3/ticker/24hr")

    async def spot_book_ticker(self):
        return await self._get(self.spot_base_url, "/api/v3/ticker/bookTicker")

    async def futures_premium_index(self):
        return await self._get(self.futures_base_url, "/fapi/v1/premiumIndex")

    async def funding_history(self, symbol: str, limit: int = 100):
        if not 1 <= limit <= 1000:
            raise ValueError("limit must be between 1 and 1000")
        return await self._get(self.futures_base_url, "/fapi/v1/fundingRate",
                               {"symbol": symbol, "limit": limit})

    async def server_time(self):
        return await self._get(self.futures_base_url, "/fapi/v1/time")
