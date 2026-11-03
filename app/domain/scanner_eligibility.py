"""Single final eligibility gate for all scanner-produced funding opportunities.

This module is deliberately pure: it makes no network calls and never submits orders.
"""
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any


@dataclass(frozen=True)
class Rejection:
    code: str
    explanation: str


@dataclass(frozen=True)
class EligibilityDecision:
    eligible: bool
    symbol: str
    checked_at_ms: int
    rejections: tuple[Rejection, ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "eligible": self.eligible,
            "status": "ELIGIBLE" if self.eligible else "REJECTED",
            "checked_at_ms": self.checked_at_ms,
            "rejection_codes": [item.code for item in self.rejections],
            "rejections": [
                {"code": item.code, "explanation": item.explanation}
                for item in self.rejections
            ],
            "execution": "THEORETICAL_ONLY",
        }


def _decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None
    return result if result.is_finite() else None


def validate_scanner_eligibility(
    symbol: str,
    checked_at_ms: int,
    context: dict[str, Any],
) -> EligibilityDecision:
    """Return the single authoritative scanner eligibility decision.

    Context keys are intentionally explicit so callers cannot silently replace
    absent measurements with zero.
    """
    rejected: list[Rejection] = []

    def reject(code: str, explanation: str) -> None:
        rejected.append(Rejection(code, explanation))

    fields = context.get("market_fields") or {}
    required = (
        "spot_bid", "spot_ask", "spot_last", "spot_quote_volume",
        "futures_bid", "futures_ask", "futures_last", "futures_quote_volume",
        "futures_mark", "futures_index", "funding_rate", "next_funding_time_ms",
    )
    missing = [name for name in required if fields.get(name) is None]
    if missing:
        reject("MARKET_FIELDS_MISSING", "Missing required market fields: " + ", ".join(missing))

    numeric = [name for name in required if name != "next_funding_time_ms"]
    invalid = []
    for name in numeric:
        value = _decimal(fields.get(name))
        if value is None or (name != "funding_rate" and value <= 0):
            invalid.append(name)
    if invalid:
        reject("MARKET_VALUES_INVALID", "Invalid, non-finite, or non-positive market values: " + ", ".join(invalid))

    max_age = int(context.get("max_data_age_ms", 0))
    ages = context.get("field_ages_ms") or {}
    stale = [name for name in required if name in ages and ages[name] is not None and int(ages[name]) > max_age]
    if context.get("market_data_stale") or stale:
        reject("MARKET_DATA_STALE", "Spot or Futures market data exceeded the configured freshness limit" +
               (": " + ", ".join(stale) if stale else ""))

    if context.get("websocket_enabled"):
        feeds = context.get("websocket_feeds") or {}
        unhealthy = [name for name in ("spot", "futures")
                     if not feeds.get(name, {}).get("healthy", False)]
        if unhealthy:
            reject("WEBSOCKET_UNHEALTHY", "Required WebSocket feed is unhealthy: " + ", ".join(unhealthy))

    offsets = context.get("clock_offsets_ms") or {}
    max_drift = int(context.get("max_clock_drift_ms", 5000))
    bad_clock = [name for name in ("spot", "futures")
                 if offsets.get(name) is not None and abs(int(offsets[name])) > max_drift]
    if bad_clock:
        reject("CLOCK_DRIFT_EXCEEDED", "Exchange clock offset exceeds configured limit: " + ", ".join(bad_clock))
    if context.get("clock_sync_required") and any(offsets.get(name) is None for name in ("spot", "futures")):
        reject("CLOCK_SYNC_UNAVAILABLE", "Spot and Futures clock synchronization is required but unavailable")

    if not context.get("spot_filters_valid", False) or not context.get("futures_filters_valid", False):
        reject("EXCHANGE_FILTERS_INVALID", "Spot or Futures exchange filters are unavailable or invalid")

    funding_age = context.get("funding_age_ms")
    funding_max_age = int(context.get("max_funding_age_ms", max_age))
    if funding_age is None or int(funding_age) > funding_max_age:
        reject("FUNDING_DATA_STALE", "Current funding observation is missing or stale")
    if context.get("funding_inconsistent") or context.get("funding_gaps"):
        reject("FUNDING_HISTORY_INCONSISTENT", "Funding history contains unresolved or inconsistent settlement intervals")
    next_funding = _decimal(fields.get("next_funding_time_ms"))
    if next_funding is None or next_funding <= checked_at_ms:
        reject("FUNDING_TIMESTAMP_INVALID", "Next funding timestamp is missing, invalid, or already expired")

    if not context.get("hedge_valid", False):
        reject("HEDGE_FILTER_VALIDATION_FAILED", "Theoretical Spot/Futures hedge quantities fail exchange-filter validation")
    residual = _decimal(context.get("residual_exposure_usdt"))
    residual_limit = _decimal(context.get("max_residual_exposure_usdt"))
    if residual is None or residual_limit is None or abs(residual) > residual_limit:
        reject("RESIDUAL_EXPOSURE_EXCEEDED", "Theoretical hedge residual exposure exceeds the configured limit or is unavailable")

    min_depth = _decimal(context.get("min_orderbook_depth_usdt"))
    spot_depth = _decimal(context.get("spot_orderbook_depth_usdt"))
    futures_depth = _decimal(context.get("futures_orderbook_depth_usdt"))
    if min_depth is None or spot_depth is None or futures_depth is None or min(spot_depth, futures_depth) < min_depth:
        reject("ORDERBOOK_LIQUIDITY_INSUFFICIENT", "Spot or Futures visible order-book depth is below the configured minimum")

    return EligibilityDecision(not rejected, symbol.upper(), checked_at_ms, tuple(rejected))
