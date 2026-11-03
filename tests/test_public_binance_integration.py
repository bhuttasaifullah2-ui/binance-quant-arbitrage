"""Opt-in public-only Binance smoke test. No credentials or trading routes are used."""
import os
import pytest
from app.exchange.binance.public_client import BinancePublicClient, BinanceAPIError


@pytest.mark.asyncio
@pytest.mark.skipif(os.getenv("BINANCE_PUBLIC_INTEGRATION") != "1",
                    reason="Set BINANCE_PUBLIC_INTEGRATION=1 to enable public network smoke tests")
async def test_public_spot_and_usdm_exchange_info():
    client = BinancePublicClient("https://api.binance.com", "https://fapi.binance.com",
                                 timeout=5, max_retries=0)
    try:
        spot, futures = await client.spot_exchange_info(), await client.futures_exchange_info()
    except BinanceAPIError as exc:
        pytest.skip(f"Binance public API unavailable from this environment: {exc}")
    finally:
        await client.close()
    assert isinstance(spot.get("symbols"), list)
    assert isinstance(futures.get("symbols"), list)
