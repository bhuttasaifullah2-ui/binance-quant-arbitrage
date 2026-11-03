"""Bounded, deduplicated pagination for public USD-M funding settlements."""
from typing import Any
from app.exchange.binance.public_client import BinancePublicClient


async def fetch_funding_history(client: BinancePublicClient, symbol: str, *,
                                start_time_ms: int | None = None,
                                end_time_ms: int | None = None,
                                page_size: int = 1000, max_pages: int = 20) -> list[dict[str, Any]]:
    if not 1 <= page_size <= 1000 or max_pages < 1:
        raise ValueError("page_size must be 1..1000 and max_pages positive")
    cursor = start_time_ms
    seen: dict[int, dict[str, Any]] = {}
    for _ in range(max_pages):
        page = await client.futures_funding_history(symbol, page_size, start_time_ms=cursor,
                                                    end_time_ms=end_time_ms)
        if not page:
            break
        valid = [x for x in page if "fundingTime" in x and "fundingRate" in x]
        if not valid:
            break
        for row in valid:
            seen[int(row["fundingTime"])] = row
        latest = max(int(x["fundingTime"]) for x in valid)
        if len(page) < page_size or (end_time_ms is not None and latest >= end_time_ms):
            break
        next_cursor = latest + 1
        if cursor is not None and next_cursor <= cursor:
            break
        cursor = next_cursor
    return [seen[t] for t in sorted(seen)]
