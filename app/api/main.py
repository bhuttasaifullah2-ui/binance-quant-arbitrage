import asyncio
import logging
import time
from contextlib import asynccontextmanager
from decimal import Decimal
from typing import Any

from fastapi import FastAPI, HTTPException, Query, Request
from pydantic import BaseModel, ConfigDict

from app.core.config import settings
from app.domain.exchange_filters import SymbolFilters
from app.exchange.binance.public_client import BinancePublicClient, BinanceAPIError
from app.infrastructure.sqlite_store import SQLiteMarketStore
from app.services.market_data_hub import MarketDataHub
from app.services.market_scanner import MarketScanner
from app.services.websocket_manager import BinanceWebSocketManager
from app.services.live_validation import LiveValidationService
from app.infrastructure.validation_store import ValidationStore
from app.infrastructure.session_evidence_store import SessionEvidenceStore
from app.api.validation import router as validation_router

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
    websocket_enabled: bool = False


@asynccontextmanager
async def lifespan(app: FastAPI):
    client = BinancePublicClient(
        settings.binance_spot_base_url, settings.binance_futures_base_url,
        settings.request_timeout_seconds, settings.request_max_retries,
        settings.retry_base_seconds, settings.max_concurrent_requests)
    store = SQLiteMarketStore(settings.database_path, settings.retention_days)
    hub = MarketDataHub(settings.max_clock_skew_ms, settings.max_data_age_seconds*1000)
    try:
        local_before = int(time.time()*1000)
        spot_time, futures_time = await asyncio.wait_for(
            asyncio.gather(client.spot_server_time(), client.futures_server_time()), timeout=5)
        local_now = (local_before + int(time.time()*1000)) // 2
        hub.set_clock_offset("spot", int(spot_time["serverTime"])-local_now, local_now)
        hub.set_clock_offset("futures", int(futures_time["serverTime"])-local_now, local_now)
    except (BinanceAPIError, KeyError, ValueError, TypeError, TimeoutError):
        logger.warning("Could not synchronize Binance exchange clocks; using local receive timestamps")
    recovery_lock = asyncio.Lock()
    last_recovery_ms = 0

    async def recover_snapshots(symbols: list[str]) -> None:
        nonlocal last_recovery_ms
        async with recovery_lock:
            symbols = sorted(hub.symbols)
            if not symbols:
                return
            now = int(time.time()*1000)
            if now - last_recovery_ms < 5000:
                return
            last_recovery_ms = now
            spot_books, spot_tickers, fut_books, fut_tickers, premium = await asyncio.gather(
                client.spot_book_ticker(), client.spot_ticker_24h(),
                client.futures_book_ticker(), client.futures_ticker_24h(),
                client.futures_premium_index())
            sb = {x["symbol"]: x for x in spot_books if x.get("symbol") in symbols}
            st = {x["symbol"]: x for x in spot_tickers if x.get("symbol") in symbols}
            fb = {x["symbol"]: x for x in fut_books if x.get("symbol") in symbols}
            ft = {x["symbol"]: x for x in fut_tickers if x.get("symbol") in symbols}
            pi = {x["symbol"]: x for x in premium if x.get("symbol") in symbols}
            for symbol in symbols:
                if symbol in sb and symbol in st:
                    hub.apply_rest(symbol, "spot:rest-recovery", {
                        "spot_bid": sb[symbol].get("bidPrice"), "spot_ask": sb[symbol].get("askPrice"),
                        "spot_last": st[symbol].get("lastPrice"), "spot_quote_volume": st[symbol].get("quoteVolume")}, now, now)
                if symbol in fb and symbol in ft and symbol in pi:
                    hub.apply_rest(symbol, "futures:rest-recovery", {
                        "futures_bid": fb[symbol].get("bidPrice"), "futures_ask": fb[symbol].get("askPrice"),
                        "futures_last": ft[symbol].get("lastPrice"), "futures_quote_volume": ft[symbol].get("quoteVolume"),
                        "futures_mark": pi[symbol].get("markPrice"), "futures_index": pi[symbol].get("indexPrice"),
                        "funding_rate": pi[symbol].get("lastFundingRate"),
                        "next_funding_time_ms": (int(pi[symbol]["nextFundingTime"]) - hub.clock_offsets_ms.get("futures", 0)
                                                 if pi[symbol].get("nextFundingTime") is not None else None)}, now, now)
            last_recovery_ms = now

    hub.event_sink = lambda market, symbol, stream, event_ms, received_ms, payload: store.save_market_event(
        market, symbol, stream, event_ms, received_ms, payload)
    hub.quality_sink = lambda event, event_ms: store.save_quality_event(event, event_ms)
    websocket_manager = BinanceWebSocketManager(
        hub, settings.binance_spot_ws_url, settings.binance_futures_ws_url,
        recovery=recover_snapshots, streams_per_connection=settings.websocket_streams_per_connection,
        health_sink=lambda market, health, now_ms: store.save_ws_health(market, health, now_ms),
        event_sink=lambda at_ms, kind, symbol, status, payload: validation_store.event(at_ms, kind, symbol, status, payload))
    scanner = MarketScanner(client, store, settings, hub, websocket_manager)
    validation_store = SessionEvidenceStore(settings.database_path, ValidationStore(settings.database_path, settings.retention_days))
    scanner.observation_store = validation_store
    validator = LiveValidationService(client, hub, websocket_manager, validation_store,
        settings.max_clock_drift_ms, settings.max_validation_message_age_ms,
        settings.websocket_probe_timeout_seconds, settings.max_reconnect_attempts,
        settings.max_recovery_duration_ms, settings.max_message_processing_latency_ms,
        settings.max_stale_symbols, market_store=store,
        websocket_probes_enabled=settings.websocket_enabled)
    app.state.binance_client = client
    app.state.market_store = store
    app.state.market_scanner = scanner
    app.state.market_hub = hub
    app.state.websocket_manager = websocket_manager
    app.state.validation_store = validation_store
    app.state.live_validator = validator
    try:
        await validator.check_clock()
    except Exception:
        logger.exception("Initial public exchange clock synchronization failed; scanner eligibility will remain blocked until recovered")
    scanner_task = asyncio.create_task(scanner.run_forever(), name="market-scanner")
    ws_task = asyncio.create_task(websocket_manager.run(), name="binance-public-websockets") if settings.websocket_enabled else None
    async def validation_loop() -> None:
        while True:
            try:
                await validator.validate()
            except Exception:
                logger.exception("Periodic public validation failed")
            await asyncio.sleep(max(30, settings.validation_interval_seconds))
    validation_task = asyncio.create_task(validation_loop(), name="binance-read-only-validation") if settings.live_validation_enabled else None
    async def clock_sync_loop() -> None:
        while True:
            await asyncio.sleep(max(30, settings.max_clock_sync_age_seconds // 2))
            try:
                await validator.check_clock()
            except Exception:
                logger.exception("Periodic public exchange clock synchronization failed")
    clock_task = asyncio.create_task(clock_sync_loop(), name="binance-clock-sync") if not settings.live_validation_enabled else None
    session_status = "COMPLETED"
    try:
        yield
    except BaseException:
        session_status = "FAILED"
        raise
    finally:
        scanner.stop()
        if validation_task:
            validation_task.cancel()
            try:
                await validation_task
            except asyncio.CancelledError:
                pass
        if clock_task:
            clock_task.cancel()
            try:
                await clock_task
            except asyncio.CancelledError:
                pass
        scanner_task.cancel()
        if ws_task:
            await websocket_manager.close()
            ws_task.cancel()
        try:
            await scanner_task
        except asyncio.CancelledError:
            pass
        if ws_task:
            try:
                await ws_task
            except asyncio.CancelledError:
                pass
        validation_store.close_session(session_status)
        await client.close()


app = FastAPI(title="Binance Quant Arbitrage", version="0.4.0",
              description="Read-only Binance Spot and USD-M funding intelligence API.", lifespan=lifespan)
app.include_router(validation_router)


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
        info, tickers, books = await asyncio.gather(client.spot_exchange_info(), client.spot_ticker_24h(), client.spot_book_ticker())
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
            if not ticker or not book or not prem or "quoteVolume" not in ticker or not all(k in book for k in ("bidPrice", "askPrice")) or not all(k in prem for k in ("markPrice", "indexPrice", "lastFundingRate", "nextFundingTime")):
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


@app.get("/api/v1/scanner/rejections")
async def scanner_rejections(request: Request, limit: int = Query(100, ge=1, le=500)):
    scanner = _scanner(request)
    return {"items": scanner.rejections[-limit:], "persisted": request.app.state.market_store.rejections(limit)}


@app.get("/api/v1/market/stream-health")
async def stream_health(request: Request):
    hub: MarketDataHub = request.app.state.market_hub
    return {"enabled": settings.websocket_enabled, **hub.stream_health(),
            "persisted": request.app.state.market_store.websocket_health()}


@app.get("/api/v1/market/data-quality")
async def data_quality(request: Request):
    hub: MarketDataHub = request.app.state.market_hub
    return {**hub.data_quality(max_age_ms=settings.max_data_age_seconds*1000),
            "persisted_events": request.app.state.market_store.rejections(100)}


@app.get("/api/v1/exchange/filters/{symbol}")
async def exchange_filters(request: Request, symbol: str):
    store: SQLiteMarketStore = request.app.state.market_store
    output = {}
    for market in ("spot", "futures"):
        raw = store.exchange_filter(market, symbol)
        if raw is not None:
            parsed = SymbolFilters.from_exchange_info(raw, market)
            output[market] = {k: str(v) if isinstance(v, Decimal) else v
                              for k, v in parsed.__dict__.items()}
    if not output:
        raise HTTPException(status_code=404, detail="Exchange metadata unavailable for symbol")
    return {"symbol": symbol.upper(), "filters": output}


@app.get("/api/v1/analytics/profitability/{symbol}")
async def profitability_history(request: Request, symbol: str, limit: int = Query(100, ge=1, le=500)):
    rows = request.app.state.market_store.profitability_history(symbol, limit)
    return {"symbol": symbol.upper(), "items": rows, "count": len(rows),
            "disclaimer": "Historical estimates are observations, not realized trading PnL."}


@app.get("/api/v1/observation/summary")
async def observation_summary(request: Request):
    return {**request.app.state.market_store.observation_summary(),
            "enabled": settings.observation_mode,
            "disclaimer": "Observations are market estimates only; no orders or realized PnL are simulated."}


@app.get("/api/v1/market/orderbook/{symbol}")
async def orderbook(request: Request, symbol: str, limit: int = Query(20, ge=5, le=100)):
    client: BinancePublicClient = request.app.state.binance_client
    try:
        spot, futures = await asyncio.gather(
            client.spot_depth(symbol.upper(), limit), client.futures_depth(symbol.upper(), limit))
        return {"symbol": symbol.upper(), "spot": spot, "futures": futures}
    except BinanceAPIError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@app.get("/api/v1/market/symbols")
async def symbols(request: Request):
    scanner = _scanner(request)
    return {"matched_pair_count": scanner.last_pair_count, "last_scan_ms": scanner.last_scan_ms}
