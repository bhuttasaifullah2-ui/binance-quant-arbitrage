"""Bounded, deduplicated pagination for public USD-M funding settlements."""
from typing import Any
from app.exchange.binance.public_client import BinancePublicClient


async def fetch_funding_history_with_stats(client: BinancePublicClient, symbol: str, *,
                                          start_time_ms: int | None = None,
                                          end_time_ms: int | None = None,
                                          page_size: int = 1000,
                                          max_pages: int = 20) -> tuple[list[dict[str, Any]], int]:
    if not 1 <= page_size <= 1000 or max_pages < 1:
        raise ValueError("page_size must be 1..1000 and max_pages positive")
    cursor = start_time_ms
    seen: dict[tuple[Any, ...], dict[str, Any]] = {}
    duplicates = 0
    for _ in range(max_pages):
        page = await client.futures_funding_history(symbol, page_size, start_time_ms=cursor,
                                                    end_time_ms=end_time_ms)
        if not page:
            break
        valid = [x for x in page if "fundingTime" in x and "fundingRate" in x]
        if not valid:
            break
        for row in valid:
            # A timestamp is not a sufficient event key: Binance can return
            # distinct rateType or markPrice payloads at the same settlement time.
            identity = (
                int(row["fundingTime"]), str(row["fundingRate"]),
                str(row.get("markPrice") or ""), str(row.get("rateType") or ""),
            )
            if identity in seen:
                duplicates += 1
            else:
                seen[identity] = row
        latest = max(int(x["fundingTime"]) for x in valid)
        if len(page) < page_size or (end_time_ms is not None and latest >= end_time_ms):
            break
        next_cursor = latest + 1
        if cursor is not None and next_cursor <= cursor:
            break
        cursor = next_cursor
    return [seen[key] for key in sorted(seen, key=lambda item: (
        item[0], item[3], item[2], item[1]))], duplicates


async def fetch_funding_history(client: BinancePublicClient, symbol: str, *,
                                start_time_ms: int | None = None,
                                end_time_ms: int | None = None,
                                page_size: int = 1000, max_pages: int = 20) -> list[dict[str, Any]]:
    rows, _ = await fetch_funding_history_with_stats(
        client, symbol, start_time_ms=start_time_ms, end_time_ms=end_time_ms,
        page_size=page_size, max_pages=max_pages)
    return rows
