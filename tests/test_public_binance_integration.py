"""Opt-in public-only Binance smoke test. No credentials or trading routes are used."""
import os
import pytest
from app.exchange.binance.public_client import BinancePublicClient, BinanceAPIError
from app.services.live_validation import LiveValidationService
from app.services.market_data_hub import MarketDataHub
from app.services.websocket_manager import BinanceWebSocketManager
from app.infrastructure.validation_store import ValidationStore


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


@pytest.mark.asyncio
@pytest.mark.skipif(os.getenv("BINANCE_PUBLIC_INTEGRATION") != "1",
                    reason="Set BINANCE_PUBLIC_INTEGRATION=1 to enable public network smoke tests")
async def test_full_public_read_only_connectivity(tmp_path):
    client = BinancePublicClient("https://api.binance.com", "https://fapi.binance.com",
                                 timeout=5, max_retries=0)
    hub = MarketDataHub()
    manager = BinanceWebSocketManager(hub, "wss://stream.binance.com:9443/stream",
                                      "wss://fstream.binance.com/stream")
    store = ValidationStore(str(tmp_path / "validation.sqlite3"))
    validator = LiveValidationService(client, hub, manager, store,
                                      websocket_probe_timeout=8)
    try:
        result = await validator.validate()
    except Exception as exc:
        pytest.fail(f"Opt-in public Binance connectivity validation failed: {type(exc).__name__}: {exc}")
    finally:
        await client.close()
    assert result["authentication"] == "PUBLIC_UNAUTHENTICATED"
    assert {row["name"] for row in result["checks"]} >= {
        "spot_rest", "futures_rest", "spot_exchange_metadata", "futures_exchange_metadata",
        "futures_funding", "spot_orderbook", "futures_orderbook", "spot_websocket", "futures_websocket"}
    assert all(row["status"] == "HEALTHY" for row in result["checks"])
