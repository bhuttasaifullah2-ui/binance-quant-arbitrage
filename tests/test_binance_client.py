import httpx
import pytest
from app.exchange.binance.public_client import BinancePublicClient, BinanceAPIError

@pytest.mark.asyncio
async def test_spot_and_futures_endpoint_paths_are_public():
    seen = []
    def handler(request):
        seen.append(request.url.path)
        return httpx.Response(200, json={'symbols': []})
    client = BinancePublicClient('https://spot.test', 'https://futures.test')
    client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    try:
        await client.spot_exchange_info()
        await client.futures_exchange_info()
        assert seen == ['/api/v3/exchangeInfo', '/fapi/v1/exchangeInfo']
    finally:
        await client.close()

@pytest.mark.asyncio
async def test_malformed_json_is_reported():
    def handler(request):
        return httpx.Response(200, content=b'not-json')
    client = BinancePublicClient('https://spot.test', 'https://futures.test', max_retries=0)
    client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    try:
        with pytest.raises(BinanceAPIError, match='Malformed JSON'):
            await client.spot_exchange_info()
    finally:
        await client.close()
