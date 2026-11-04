"""Explicit settlement-history states and conservative interval resolution."""
from decimal import Decimal, InvalidOperation
from typing import Any

HISTORY_MISSING = "HISTORY_MISSING"
HISTORY_INCOMPLETE = "HISTORY_INCOMPLETE"
HISTORY_STALE = "HISTORY_STALE"
HISTORY_RECONCILIATION_FAILED = "HISTORY_RECONCILIATION_FAILED"
FUNDING_RATE_RECONCILED = "FUNDING_RATE_RECONCILED"
_SUPPORTED_INTERVALS = (1, 4, 8)
_REQUIRED_SNAPSHOT_FIELDS = (
    "spot_bid", "spot_ask", "spot_last", "spot_quote_volume",
    "futures_bid", "futures_ask", "futures_last", "futures_quote_volume",
    "futures_mark", "futures_index", "funding_rate", "next_funding_time_ms",
)


def _event_type(row: dict[str, Any]) -> str:
    value = str(row.get("rateType", row.get("rate_type")) or "Regular").strip().title()
    return value


def resolve_funding_interval(
    funding_info: dict[str, Any] | None,
    funding_times_ms: list[int],
    tolerance_ms: int = 60_000,
) -> tuple[int | None, str]:
    """Resolve an interval only from exchange metadata or three exact Regular events.

    With no fundingInfo entry, two adjacent Regular intervals must both equal
    the candidate cadence. Multi-interval gaps and Special events are not used
    to infer the normal Regular settlement schedule.
    """
    if funding_info is not None:
        try:
            raw = funding_info["fundingIntervalHours"]
            interval = int(raw)
            if str(raw).strip() not in {str(interval), f"{interval}.0"}:
                return None, "FUNDING_INFO_INVALID"
        except (KeyError, TypeError, ValueError):
            return None, "FUNDING_INFO_INVALID"
        if interval not in _SUPPORTED_INTERVALS:
            return None, "FUNDING_INFO_UNSUPPORTED_INTERVAL"
        return interval, "BINANCE_FUNDING_INFO"

    ordered = sorted(set(int(value) for value in funding_times_ms))
    if len(ordered) < 3:
        return None, "INTERVAL_UNCONFIRMED_NO_FUNDING_INFO"
    gaps = [right - left for left, right in zip(ordered, ordered[1:])]
    for candidate in _SUPPORTED_INTERVALS:
        step = candidate * 3_600_000
        if all(abs(gap - step) <= tolerance_ms for gap in gaps):
            return candidate, "SETTLEMENT_HISTORY_INFERRED_NOT_EXCHANGE_AUTHORITY"
    return None, "INTERVAL_HISTORY_INCONSISTENT"


def deduplicate_economic_settlements(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Collapse source-only repeats, retaining provenance and all economic variants.

    Different rate, mark or rateType values are not collapsed. They remain
    visible to assess_funding_history(), which blocks ambiguous event identities.
    """
    grouped: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in rows:
        stamp = row.get("fundingTime", row.get("funding_time_ms"))
        rate = row.get("fundingRate", row.get("funding_rate"))
        if stamp is None or rate is None:
            continue
        mark = row.get("markPrice", row.get("settlement_mark_price"))
        rate_type = _event_type(row)
        key = (int(stamp), str(rate), "" if mark is None else str(mark), rate_type)
        if key not in grouped:
            grouped[key] = dict(row)
            grouped[key]["sources"] = []
        sources = grouped[key]["sources"]
        source_values = row.get("sources") or [row.get("source")]
        for source_value in source_values:
            source = str(source_value or "binance_fapi_fundingRate")
            if source not in sources:
                sources.append(source)
    return [grouped[key] for key in sorted(grouped, key=lambda x: (x[0], x[3], x[1], x[2]))]


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
    """Reconcile Regular cadence and separate Special events without zero filling."""
    if not rows:
        return {"state": HISTORY_MISSING, "missing_intervals_are_not_zero": True}
    if interval_hours not in _SUPPORTED_INTERVALS:
        return {"state": HISTORY_INCOMPLETE, "reason": "funding interval is not authoritative or inferred",
                "missing_intervals_are_not_zero": True}

    normalized: list[tuple[int, Decimal, str, dict[str, Any]]] = []
    for row in rows:
        try:
            stamp = int(row.get("fundingTime", row.get("funding_time_ms")))
            rate = Decimal(str(row.get("fundingRate", row.get("funding_rate"))))
            event_type = _event_type(row)
            if event_type not in {"Regular", "Special"}:
                raise ValueError("unsupported rateType")
        except (TypeError, ValueError, InvalidOperation):
            return {"state": HISTORY_RECONCILIATION_FAILED, "reason": "malformed settlement record"}
        normalized.append((stamp, rate, event_type, row))

    # A same-time Regular and Special are distinct events. Disagreement between
    # payloads for the same (timestamp, rateType) is ambiguous and fails closed.
    variants: dict[tuple[int, str], set[tuple[str, str]]] = {}
    for stamp, rate, event_type, row in normalized:
        mark = row.get("markPrice", row.get("settlement_mark_price"))
        variants.setdefault((stamp, event_type), set()).add(
            (str(rate), "" if mark is None else str(mark))
        )
    conflicts = sorted(stamp for (stamp, _), values in variants.items() if len(values) > 1)
    if conflicts:
        return {"state": HISTORY_RECONCILIATION_FAILED,
                "conflicting_settlement_timestamps": sorted(set(conflicts)),
                "missing_intervals_are_not_zero": True}

    regular_times = sorted({stamp for stamp, _, event_type, _ in normalized
                            if event_type == "Regular"})
    special_times = sorted({stamp for stamp, _, event_type, _ in normalized
                            if event_type == "Special"})
    if not regular_times:
        return {"state": HISTORY_INCOMPLETE, "reason": "no Regular settlement available",
                "missing_intervals_are_not_zero": True}

    expected_previous = int(next_funding_time_ms) - interval_hours * 3_600_000
    latest_regular = regular_times[-1]
    if latest_regular < expected_previous - tolerance_ms:
        return {"state": HISTORY_STALE, "latest_settlement_ms": latest_regular,
                "expected_previous_settlement_ms": expected_previous,
                "missing_intervals_are_not_zero": True}
    if latest_regular > expected_previous + tolerance_ms:
        return {"state": HISTORY_RECONCILIATION_FAILED, "latest_settlement_ms": latest_regular,
                "expected_previous_settlement_ms": expected_previous,
                "missing_intervals_are_not_zero": True}

    step = interval_hours * 3_600_000
    short_gaps = [(left, right) for left, right in zip(regular_times, regular_times[1:])
                  if right - left < step - tolerance_ms]
    if short_gaps:
        return {"state": HISTORY_INCOMPLETE, "reason_code": "REGULAR_SETTLEMENT_CADENCE_INVALID",
                "invalid_regular_settlement_gaps": short_gaps,
                "missing_intervals_are_not_zero": True}
    missing_gaps = [(left, right) for left, right in zip(regular_times, regular_times[1:])
                    if right - left > step + tolerance_ms]
    if missing_gaps:
        return {"state": HISTORY_INCOMPLETE, "missing_settlement_intervals": missing_gaps,
                "missing_intervals_are_not_zero": True}
    if len(regular_times) < 3:
        return {"state": HISTORY_INCOMPLETE,
                "reason": "fewer than three distinct Regular settlements; history coverage is insufficient",
                "settlement_count": len(regular_times), "missing_intervals_are_not_zero": True}
    if coverage_start_ms is not None and regular_times[0] > coverage_start_ms + tolerance_ms:
        return {"state": HISTORY_INCOMPLETE, "coverage_start_ms": int(coverage_start_ms),
                "earliest_settlement_ms": regular_times[0], "missing_intervals_are_not_zero": True}

    try:
        current_rate = Decimal(str(current_funding_rate))
    except (TypeError, ValueError, InvalidOperation):
        return {"state": HISTORY_RECONCILIATION_FAILED, "reason": "current funding rate is invalid"}
    latest_rate = next(rate for stamp, rate, event_type, _ in normalized
                       if stamp == latest_regular and event_type == "Regular")
    if abs(current_rate - latest_rate) > rate_tolerance:
        return {"state": HISTORY_RECONCILIATION_FAILED, "latest_settlement_ms": latest_regular,
                "latest_settlement_rate": str(latest_rate), "current_funding_rate": str(current_rate),
                "missing_intervals_are_not_zero": True}

    latest_row = next(row for stamp, _, event_type, row in normalized
                      if stamp == latest_regular and event_type == "Regular")
    settlement_mark = latest_row.get("markPrice", latest_row.get("settlement_mark_price"))
    return {
        "state": FUNDING_RATE_RECONCILED,
        "latest_settlement_ms": latest_regular,
        "latest_settlement_rate": str(latest_rate),
        "coverage_start_ms": regular_times[0],
        "coverage_end_ms": regular_times[-1],
        "settlement_count": len(regular_times),
        "regular_settlement_count": len(regular_times),
        "special_event_count": len(special_times),
        "special_settlement_timestamps_ms": special_times,
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
    try:
        contract_size = Decimal(str(futures.get("contractSize", "1")))
    except (TypeError, ValueError, InvalidOperation):
        return False
    return bool(
        spot.get("symbol") == futures.get("symbol")
        and spot.get("baseAsset")
        and spot.get("baseAsset") == futures.get("baseAsset")
        and spot.get("quoteAsset")
        and spot.get("quoteAsset") == futures.get("quoteAsset")
        and contract_size > 0
    )


def snapshot_fields_are_acceptable(
    timestamps: dict[str, int], *, now_ms: int, max_age_ms: int, max_skew_ms: int,
) -> bool:
    """Require every required market field to be fresh and the entire snapshot aligned."""
    if max_age_ms < 0 or max_skew_ms < 0:
        raise ValueError("snapshot age and skew bounds cannot be negative")
    if any(name not in timestamps for name in _REQUIRED_SNAPSHOT_FIELDS):
        return False
    values = [int(timestamps[name]) for name in _REQUIRED_SNAPSHOT_FIELDS]
    if any(value > now_ms + max_skew_ms or now_ms - value > max_age_ms for value in values):
        return False
    return max(values) - min(values) <= max_skew_ms


def snapshot_skew_is_acceptable(spot_snapshot_ms: int, futures_snapshot_ms: int,
                                max_skew_ms: int) -> bool:
    """Return whether separately timestamped market snapshots satisfy the skew bound."""
    if max_skew_ms < 0:
        raise ValueError("max_skew_ms cannot be negative")
    return abs(int(spot_snapshot_ms) - int(futures_snapshot_ms)) <= max_skew_ms
