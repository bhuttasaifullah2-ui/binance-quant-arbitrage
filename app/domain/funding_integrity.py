"""Explicit settlement-history states and conservative interval resolution."""
from decimal import Decimal, InvalidOperation
from typing import Any

HISTORY_MISSING = "HISTORY_MISSING"
HISTORY_INCOMPLETE = "HISTORY_INCOMPLETE"
HISTORY_STALE = "HISTORY_STALE"
HISTORY_RECONCILIATION_FAILED = "HISTORY_RECONCILIATION_FAILED"
FUNDING_RATE_RECONCILED = "FUNDING_RATE_RECONCILED"
_SUPPORTED_INTERVALS = (1, 4, 8)


def resolve_funding_interval(
    funding_info: dict[str, Any] | None,
    funding_times_ms: list[int],
    tolerance_ms: int = 60_000,
) -> tuple[int | None, str]:
    """Resolve interval from authoritative fundingInfo or sufficiently consistent history.

    Binance fundingInfo only returns symbols with adjusted parameters. An absent
    symbol is not treated as proof that an 8-hour interval currently applies.
    """
    if funding_info is not None:
        try:
            interval = int(funding_info["fundingIntervalHours"])
        except (KeyError, TypeError, ValueError):
            return None, "FUNDING_INFO_INVALID"
        if interval not in _SUPPORTED_INTERVALS:
            return None, "FUNDING_INFO_UNSUPPORTED_INTERVAL"
        return interval, "BINANCE_FUNDING_INFO"

    ordered = sorted(set(int(value) for value in funding_times_ms))
    if len(ordered) < 3:
        return None, "INTERVAL_UNCONFIRMED_NO_FUNDING_INFO"
    gaps = [right - left for left, right in zip(ordered, ordered[1:])]
    for candidate in reversed(_SUPPORTED_INTERVALS):
        step = candidate * 3_600_000
        exact = 0
        consistent = True
        for gap in gaps:
            multiple = max(1, round(gap / step))
            if abs(gap - multiple * step) > tolerance_ms:
                consistent = False
                break
            if abs(gap - step) <= tolerance_ms:
                exact += 1
        if consistent and exact >= 1:
            return candidate, "SETTLEMENT_HISTORY_INFERRED_NOT_EXCHANGE_AUTHORITY"
    return None, "INTERVAL_HISTORY_INCONSISTENT"


def assess_funding_history(
    rows: list[dict[str, Any]],
    *,
    interval_hours: int | None,
    next_funding_time_ms: int,
    current_funding_rate: Decimal | str,
    rate_tolerance: Decimal = Decimal("0.00000001"),
    tolerance_ms: int = 60_000,
    coverage_start_ms: int | None = None,
) -> dict[str, Any]:
    """Reconcile settlement timestamps and rates without synthesizing missing events."""
    if not rows:
        return {"state": HISTORY_MISSING, "missing_intervals_are_not_zero": True}
    if interval_hours not in _SUPPORTED_INTERVALS:
        return {"state": HISTORY_INCOMPLETE, "reason": "funding interval is not authoritative or inferred",
                "missing_intervals_are_not_zero": True}

    normalized: list[tuple[int, Decimal, dict[str, Any]]] = []
    for row in rows:
        try:
            stamp = int(row.get("fundingTime", row.get("funding_time_ms")))
            rate = Decimal(str(row.get("fundingRate", row.get("funding_rate"))))
        except (TypeError, ValueError, InvalidOperation):
            return {"state": HISTORY_RECONCILIATION_FAILED, "reason": "malformed settlement record"}
        normalized.append((stamp, rate, row))

    by_time: dict[int, set[tuple[str, str, str]]] = {}
    for stamp, rate, row in normalized:
        by_time.setdefault(stamp, set()).add((
            str(rate), str(row.get("markPrice", row.get("settlement_mark_price")) or ""),
            str(row.get("rateType", row.get("rate_type")) or ""),
        ))
    conflicts = sorted(stamp for stamp, variants in by_time.items() if len(variants) > 1)
    if conflicts:
        return {"state": HISTORY_RECONCILIATION_FAILED, "conflicting_settlement_timestamps": conflicts,
                "missing_intervals_are_not_zero": True}

    times = sorted(by_time)
    expected_previous = int(next_funding_time_ms) - interval_hours * 3_600_000
    latest = times[-1]
    if latest < expected_previous - tolerance_ms:
        return {"state": HISTORY_STALE, "latest_settlement_ms": latest,
                "expected_previous_settlement_ms": expected_previous,
                "missing_intervals_are_not_zero": True}
    if latest > expected_previous + tolerance_ms:
        return {"state": HISTORY_RECONCILIATION_FAILED, "latest_settlement_ms": latest,
                "expected_previous_settlement_ms": expected_previous,
                "missing_intervals_are_not_zero": True}

    step = interval_hours * 3_600_000
    gaps = [(left, right) for left, right in zip(times, times[1:])
            if right - left > step + tolerance_ms]
    if gaps:
        return {"state": HISTORY_INCOMPLETE, "missing_settlement_intervals": gaps,
                "missing_intervals_are_not_zero": True}
    if coverage_start_ms is not None and times[0] > coverage_start_ms + tolerance_ms:
        return {"state": HISTORY_INCOMPLETE, "coverage_start_ms": int(coverage_start_ms),
                "earliest_settlement_ms": times[0], "missing_intervals_are_not_zero": True}

    try:
        current_rate = Decimal(str(current_funding_rate))
    except (TypeError, ValueError, InvalidOperation):
        return {"state": HISTORY_RECONCILIATION_FAILED, "reason": "current funding rate is invalid"}
    latest_rate = next(rate for stamp, rate, _ in normalized if stamp == latest)
    if abs(current_rate - latest_rate) > rate_tolerance:
        return {"state": HISTORY_RECONCILIATION_FAILED, "latest_settlement_ms": latest,
                "latest_settlement_rate": str(latest_rate), "current_funding_rate": str(current_rate),
                "missing_intervals_are_not_zero": True}

    latest_row = next(row for stamp, _, row in normalized if stamp == latest)
    settlement_mark = latest_row.get("markPrice", latest_row.get("settlement_mark_price"))
    return {
        "state": FUNDING_RATE_RECONCILED,
        "latest_settlement_ms": latest,
        "latest_settlement_rate": str(latest_rate),
        "settlement_mark_price": None if settlement_mark is None else str(settlement_mark),
        "cashflow_valuation": "SETTLEMENT_MARK_PRICE" if settlement_mark is not None else "ESTIMATE_PROXY_REQUIRED",
        "missing_intervals_are_not_zero": True,
    }


def adjusted_rate_within_limits(rate: Decimal | str, funding_info: dict[str, Any] | None) -> bool | None:
    """Check the current rate against adjusted cap/floor only when Binance supplies them."""
    if funding_info is None:
        return None
    try:
        value = Decimal(str(rate))
        floor = Decimal(str(funding_info["adjustedFundingRateFloor"]))
        cap = Decimal(str(funding_info["adjustedFundingRateCap"]))
    except (KeyError, TypeError, ValueError, InvalidOperation):
        return None
    return floor <= value <= cap


def compatible_spot_futures(spot: dict[str, Any], futures: dict[str, Any]) -> bool:
    """Require matching explicit base and quote assets, not just a matching ticker."""
    return bool(
        spot.get("symbol") == futures.get("symbol")
        and spot.get("baseAsset")
        and spot.get("baseAsset") == futures.get("baseAsset")
        and spot.get("quoteAsset")
        and spot.get("quoteAsset") == futures.get("quoteAsset")
        and Decimal(str(futures.get("contractSize", "1"))) > 0
    )


def snapshot_skew_is_acceptable(spot_snapshot_ms: int, futures_snapshot_ms: int,
                                max_skew_ms: int) -> bool:
    """Return whether separately timestamped market snapshots satisfy the skew bound."""
    if max_skew_ms < 0:
        raise ValueError("max_skew_ms cannot be negative")
    return abs(int(spot_snapshot_ms) - int(futures_snapshot_ms)) <= max_skew_ms
