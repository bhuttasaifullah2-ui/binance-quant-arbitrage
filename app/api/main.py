import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Query, Request
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

def _scanner(request: Request) -> MarketScanner:
    if not hasattr(request.app.state, "market_scanner"):
        raise HTTPException(status_code=503, detail="Scanner is not initialized")
    return request.app.state.market_scanner

@app.get("/api/v1/health")
@app.get("/health")
async def health(request: Request):
    scanner = _scanner(request)
    status = scanner.status()
    return {"status": "ok" if status["last_error"] is None else "degraded",
            "mode": settings.app_mode, "live_trading_enabled": False,
            "execution_enabled": False, "scanner": status}

@app.get("/api/v1/market/overview")
@app.get("/market/overview")
async def market_overview(request: Request, limit: int = Query(100, ge=1, le=500), offset: int = Query(0, ge=0)):
    scanner = _scanner(request)
    return {"scanner": scanner.status(), "opportunities": scanner.fresh_opportunities[offset:offset+limit]}

@app.get("/api/v1/market/spot")
async def spot_market(request: Request, limit: int = Query(100, ge=1, le=500), offset: int = Query(0, ge=0)):
    client: BinancePublicClient = request.app.state.binance_client
    try:
        info, tickers, books = await asyncio.gather(
            client.spot_exchange_info(), client.spot_ticker_24h(), client.spot_book_ticker())
        ticker_map = {x["symbol"]: x for x in tickers if "symbol" in x}
        book_map = {x["symbol"]: x for x in books if "symbol" in x}
        items = []
        for symbol in info.get("symbols", []):
            name = symbol.get("symbol")
            if symbol.get("status") != "TRADING" or symbol.get("quoteAsset") != "USDT" or not symbol.get("isSpotTradingAllowed", True):
                continue
            ticker, book = ticker_map.get(name), book_map.get(name)
            if not ticker or not book or not all(k in ticker for k in ("lastPrice", "quoteVolume")) or not all(k in book for k in ("bidPrice", "askPrice")):
                continue
            items.append({"symbol": name, "status": symbol.get("status"), "last_price": ticker["lastPrice"],
                "quote_volume_24h_usdt": ticker["quoteVolume"], "bid": book["bidPrice"], "ask": book["askPrice"],
                "source": "binance_spot_rest"})
        return {"items": items[offset:offset+limit], "total": len(items), "limit": limit, "offset": offset}
    except BinanceAPIError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

@app.get("/api/v1/market/futures")
async def futures_market(request: Request, limit: int = Query(100, ge=1, le=500), offset: int = Query(0, ge=0)):
    client: BinancePublicClient = request.app.state.binance_client
    try:
        info, tickers, books, premium = await asyncio.gather(
            client.futures_exchange_info(), client.futures_ticker_24h(),
            client.futures_book_ticker(), client.futures_premium_index())
        ticker_map = {x["symbol"]: x for x in tickers if "symbol" in x}
        book_map = {x["symbol"]: x for x in books if "symbol" in x}
        premium_map = {x["symbol"]: x for x in premium if "symbol" in x}
        items = []
        for symbol in info.get("symbols", []):
            name = symbol.get("symbol")
            if symbol.get("status") != "TRADING" or symbol.get("quoteAsset") != "USDT" or symbol.get("contractType") != "PERPETUAL":
                continue
            ticker, book, prem = ticker_map.get(name), book_map.get(name), premium_map.get(name)
            if not ticker or not book or not prem or not all(k in ticker for k in ("quoteVolume",)) or not all(k in book for k in ("bidPrice", "askPrice")) or not all(k in prem for k in ("markPrice", "indexPrice", "lastFundingRate", "nextFundingTime")):
                continue
            items.append({"symbol": name, "status": symbol.get("status"), "quote_volume_24h_usdt": ticker["quoteVolume"],
                "bid": book["bidPrice"], "ask": book["askPrice"], "mark_price": prem["markPrice"],
                "index_price": prem["indexPrice"], "funding_rate": prem["lastFundingRate"],
                "next_funding_time_ms": prem["nextFundingTime"], "source": "binance_usdm_rest"})
        return {"items": items[offset:offset+limit], "total": len(items), "limit": limit, "offset": offset}
    except BinanceAPIError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

@app.get("/api/v1/funding/current")
async def current_funding(request: Request, limit: int = Query(100, ge=1, le=500), offset: int = Query(0, ge=0)):
    client: BinancePublicClient = request.app.state.binance_client
    try:
        info, premium = await asyncio.gather(client.futures_exchange_info(), client.futures_premium_index())
        active = {s["symbol"] for s in info.get("symbols", [])
                  if s.get("status") == "TRADING" and s.get("quoteAsset") == "USDT" and s.get("contractType") == "PERPETUAL"}
        items = [{"symbol": x["symbol"], "mark_price": x["markPrice"], "index_price": x["indexPrice"],
                  "funding_rate": x["lastFundingRate"], "next_funding_time_ms": x["nextFundingTime"],
                  "source": "binance_usdm_rest"}
                 for x in premium if x.get("symbol") in active and
                 all(k in x for k in ("markPrice", "indexPrice", "lastFundingRate", "nextFundingTime"))]
        return {"items": items[offset:offset+limit], "total": len(items), "limit": limit, "offset": offset}
    except BinanceAPIError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

@app.get("/api/v1/funding/history/{symbol}")
async def funding_history(request: Request, symbol: str, limit: int = Query(100, ge=1, le=1000), offset: int = Query(0, ge=0)):
    rows = request.app.state.market_store.funding_history(symbol, limit, offset)
    if not rows:
        raise HTTPException(status_code=404, detail="No stored funding history for symbol")
    return {"symbol": symbol.upper(), "items": rows, "limit": limit, "offset": offset}

@app.get("/api/v1/scanner/opportunities", response_model=list[OpportunityResponse])
async def opportunities(request: Request, limit: int = Query(50, ge=1, le=200), offset: int = Query(0, ge=0)):
    return _scanner(request).fresh_opportunities[offset:offset+limit]

@app.get("/api/v1/scanner/status", response_model=StatusResponse)
async def scanner_status(request: Request):
    return _scanner(request).status()

@app.get("/api/v1/market/orderbook/{symbol}")
async def orderbook(request: Request, symbol: str, limit: int = Query(20, ge=5, le=100)):
    client: BinancePublicClient = request.app.state.binance_client
    try:
        spot, futures = await asyncio.gather(
            client.spot_depth(symbol.upper(), limit),
            client.futures_depth(symbol.upper(), limit))
        return {"symbol": symbol.upper(), "spot": spot, "futures": futures}
    except BinanceAPIError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

@app.get("/api/v1/market/symbols")
async def symbols(request: Request):
    scanner = _scanner(request)
    return {"matched_pair_count": scanner.last_pair_count,
            "last_scan_ms": scanner.last_scan_ms}
