from decimal import Decimal
from fastapi import FastAPI, HTTPException
from app.core.config import settings
from app.exchange.binance.public_client import BinancePublicClient

app = FastAPI(title="Binance Quant Arbitrage", version="0.1.0",
              description="Research-only foundation. No order execution endpoints.")
client = BinancePublicClient(settings.binance_spot_base_url,
                              settings.binance_futures_base_url,
                              settings.request_timeout_seconds)

@app.get("/health")
async def health():
    return {"status": "ok", "mode": settings.app_mode,
            "live_trading_enabled": False, "order_execution": "not_implemented"}

@app.get("/market/funding")
async def funding(symbol: str = "BTCUSDT", limit: int = 30):
    try:
        rows = await client.funding_history(symbol.upper(), limit)
        return [{"symbol": row["symbol"], "fundingRate": row["fundingRate"],
                 "fundingTime": row["fundingTime"]} for row in rows]
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Binance market-data request failed: {type(exc).__name__}") from exc

@app.get("/market/overview")
async def market_overview():
    try:
        premium, book = await __import__("asyncio").gather(
            client.futures_premium_index(), client.spot_book_ticker())
        book_by_symbol = {x["symbol"]: x for x in book}
        output = []
        for item in premium:
            symbol = item.get("symbol", "")
            spot = book_by_symbol.get(symbol)
            if not spot:
                continue
            output.append({
                "symbol": symbol,
                "markPrice": item.get("markPrice"),
                "indexPrice": item.get("indexPrice"),
                "lastFundingRate": item.get("lastFundingRate"),
                "nextFundingTime": item.get("nextFundingTime"),
                "spotBid": spot.get("bidPrice"),
                "spotAsk": spot.get("askPrice"),
            })
        return output
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Binance market-data request failed: {type(exc).__name__}") from exc
