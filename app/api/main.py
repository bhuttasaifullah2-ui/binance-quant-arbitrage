import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, ConfigDict

from app.core.config import settings
from app.exchange.binance.public_client import BinancePublicClient, BinanceAPIError
from app.infrastructure.sqlite_store import SQLiteMarketStore
from app.services.market_scanner import MarketScanner

logger = logging.getLogger(__name__)

class OpportunityResponse(BaseModel):
    model_config = ConfigDict(extra="allow")
    symbol: str
    score: str
    funding_rate: str
    estimated_net_usdt: str
    status: str
    data_freshness: str

class StatusResponse(BaseModel):
    running: bool
    last_scan_ms: int | None
    last_scan_age_ms: int | None
    matched_pair_count: int
    valid_opportunity_count: int
    last_error: str | None
    database_observations: int
    live_trading_enabled: bool
    execution_enabled: bool

@asynccontextmanager
async def lifespan(app: FastAPI):
    client = BinancePublicClient(
        settings.binance_spot_base_url, settings.binance_futures_base_url,
        settings.request_timeout_seconds, settings.request_max_retries,
        settings.retry_base_seconds, settings.max_concurrent_requests)
    store = SQLiteMarketStore(settings.database_path, settings.retention_days)
    scanner = MarketScanner(client, store, settings)
    app.state.binance_client = client
    app.state.market_store = store
    app.state.market_scanner = scanner
    task = asyncio.create_task(scanner.run_forever(), name="market-scanner")
    try:
        yield
    finally:
        scanner.stop()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        await client.close()

app = FastAPI(title="Binance Quant Arbitrage", version="0.2.0",
              description="Read-only Binance Spot and USD-M funding intelligence API.",
              lifespan=lifespan)

def _scanner(request) -> MarketScanner:
    if not hasattr(request.app.state, "market_scanner"):
        raise HTTPException(status_code=503, detail="Scanner is not initialized")
    return request.app.state.market_scanner

@app.get("/api/v1/health")
@app.get("/health")
async def health(request):
    scanner = _scanner(request)
    status = scanner.status()
    return {"status": "ok" if status["last_error"] is None else "degraded",
            "mode": settings.app_mode, "live_trading_enabled": False,
            "execution_enabled": False, "scanner": status}

@app.get("/api/v1/market/overview")
@app.get("/market/overview")
async def market_overview(request, limit: int = Query(100, ge=1, le=500)):
    scanner = _scanner(request)
    return {"scanner": scanner.status(), "opportunities": scanner.opportunities[:limit]}

@app.get("/api/v1/market/spot")
async def spot_market(request, limit: int = Query(100, ge=1, le=500)):
    store = request.app.state.market_store
    return {"items": store.latest(limit), "limit": limit}

@app.get("/api/v1/market/futures")
async def futures_market(request, limit: int = Query(100, ge=1, le=500)):
    store = request.app.state.market_store
    return {"items": store.latest(limit), "limit": limit}

@app.get("/api/v1/funding/current")
async def current_funding(request, limit: int = Query(100, ge=1, le=500)):
    return {"items": request.app.state.market_store.latest(limit), "limit": limit}

@app.get("/api/v1/funding/history/{symbol}")
async def funding_history(request, symbol: str, limit: int = Query(100, ge=1, le=1000)):
    rows = request.app.state.market_store.funding_history(symbol, limit)
    if not rows:
        raise HTTPException(status_code=404, detail="No stored funding history for symbol")
    return {"symbol": symbol.upper(), "items": rows, "limit": limit}

@app.get("/api/v1/scanner/opportunities", response_model=list[OpportunityResponse])
async def opportunities(request, limit: int = Query(50, ge=1, le=200)):
    return _scanner(request).opportunities[:limit]

@app.get("/api/v1/scanner/status", response_model=StatusResponse)
async def scanner_status(request):
    return _scanner(request).status()

@app.get("/api/v1/market/orderbook/{symbol}")
async def orderbook(request, symbol: str, limit: int = Query(20, ge=5, le=100)):
    client: BinancePublicClient = request.app.state.binance_client
    try:
        spot, futures = await asyncio.gather(
            client.spot_depth(symbol.upper(), limit),
            client.futures_depth(symbol.upper(), limit))
        return {"symbol": symbol.upper(), "spot": spot, "futures": futures}
    except BinanceAPIError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

@app.get("/api/v1/market/symbols")
async def symbols(request):
    scanner = _scanner(request)
    return {"matched_pair_count": scanner.last_pair_count,
            "last_scan_ms": scanner.last_scan_ms}
