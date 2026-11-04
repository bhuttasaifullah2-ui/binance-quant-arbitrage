"""Bounded, overlap-safe pagination for public USD-M funding settlements."""
from typing import Any
import inspect
from app.exchange.binance.public_client import BinancePublicClient


class FundingHistoryPaginationError(RuntimeError):
    """Raised when a full page cannot be advanced without risking skipped events."""


async def fetch_funding_history_with_stats(client: BinancePublicClient, symbol: str, *,
                                          start_time_ms: int | None = None,
                                          end_time_ms: int | None = None,
                                          page_size: int = 1000,
                                          max_pages: int = 20,
                                          priority: str = "current") -> tuple[list[dict[str, Any]], int]:
    if not 1 <= page_size <= 1000 or max_pages < 1:
        raise ValueError("page_size must be 1..1000 and max_pages positive")
    if priority not in {"current", "backfill"}:
        raise ValueError("priority must be current or backfill")
    cursor = start_time_ms
    seen: dict[tuple[Any, ...], dict[str, Any]] = {}
    duplicates = 0
    previous_latest: int | None = None
    for _ in range(max_pages):
        request = client.futures_funding_history
        if "priority" in inspect.signature(request).parameters:
            page = await request(symbol, page_size, start_time_ms=cursor,
                                 end_time_ms=end_time_ms, priority=priority)
        else:
            acquire = getattr(client, "acquire_funding_request", None)
            if acquire is not None:
                await acquire(priority)
            page = await request(symbol, page_size, start_time_ms=cursor,
                                 end_time_ms=end_time_ms)
        if not page:
            break
        valid = [x for x in page if "fundingTime" in x and "fundingRate" in x]
        if not valid:
            break
        new_rows = 0
        for row in valid:
            identity = (
                int(row["fundingTime"]), str(row["fundingRate"]),
                str(row.get("markPrice") or ""), str(row.get("rateType") or "Regular"),
            )
            if identity in seen:
                duplicates += 1
            else:
                seen[identity] = row
                new_rows += 1
        latest = max(int(x["fundingTime"]) for x in valid)
        if len(page) < page_size or (end_time_ms is not None and latest >= end_time_ms):
            break
        if previous_latest is not None and latest <= previous_latest and new_rows == 0:
            raise FundingHistoryPaginationError(
                f"full funding page for {symbol} did not advance; same-timestamp events may be truncated"
            )
        # Inclusive overlap is deliberate: startTime=latest+1 can skip a
        # Regular/Special row sharing the last timestamp on the previous page.
        cursor = latest
        previous_latest = latest
    return [seen[key] for key in sorted(seen, key=lambda item: (
        item[0], item[3], item[2], item[1]))], duplicates


async def fetch_funding_history(client: BinancePublicClient, symbol: str, *,
                                start_time_ms: int | None = None,
                                end_time_ms: int | None = None,
                                page_size: int = 1000, max_pages: int = 20,
                                priority: str = "current") -> list[dict[str, Any]]:
    rows, _ = await fetch_funding_history_with_stats(
        client, symbol, start_time_ms=start_time_ms, end_time_ms=end_time_ms,
        page_size=page_size, max_pages=max_pages, priority=priority)
    return rows
