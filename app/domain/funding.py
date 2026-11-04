from decimal import Decimal, ROUND_HALF_UP
from .models import FundingObservation


def funding_payment(notional: Decimal, funding_rate: Decimal, short_receives_positive: bool = True) -> Decimal:
    """Signed cash flow for the short leg; positive means receipt by short."""
    if notional < 0:
        raise ValueError("notional cannot be negative")
    sign = Decimal("1") if short_receives_positive else Decimal("-1")
    return notional * funding_rate * sign


def deduplicate_funding_observations(observations: list[FundingObservation]) -> list[FundingObservation]:
    """Remove source-only repeats while preserving Regular/Special event identity."""
    unique: dict[tuple, FundingObservation] = {}
    for item in observations:
        key = (item.funding_time_ms, item.funding_rate, item.settlement_mark_price, item.rate_type)
        unique.setdefault(key, item)
    return sorted(unique.values(), key=lambda x: (x.funding_time_ms, x.rate_type or "Regular"))


def summarize_funding(observations: list[FundingObservation]) -> dict[str, Decimal | int]:
    observations = [item for item in deduplicate_funding_observations(observations)
                    if (item.rate_type or "Regular") == "Regular"]
    if not observations:
        return {"count": 0, "mean": Decimal("0"), "median": Decimal("0"),
                "min": Decimal("0"), "max": Decimal("0"), "negative_fraction": Decimal("0"),
                "positive_fraction": Decimal("0"), "volatility": Decimal("0"),
                "stddev": Decimal("0"), "positive_percent": Decimal("0"),
                "negative_percent": Decimal("0"), "consecutive_positive": 0,
                "consecutive_negative": 0, "recent_trend": Decimal("0"),
                "consistency": Decimal("0")}
    ordered = sorted(observations, key=lambda x: (x.funding_time_ms, x.rate_type or "Regular"))
    rates = [x.funding_rate for x in ordered]
    n = len(rates)
    mean = sum(rates, Decimal("0")) / n
    variance = sum(((x - mean) ** 2 for x in rates), Decimal("0")) / n
    positive = sum(1 for x in rates if x > 0)
    negative = sum(1 for x in rates if x < 0)
    cp = cn = 0
    for rate in reversed(rates):
        if rate > 0:
            cp += 1
        else:
            break
    for rate in reversed(rates):
        if rate < 0:
            cn += 1
        else:
            break
    sorted_rates = sorted(rates)
    mid_index = n // 2
    median_rate = sorted_rates[mid_index] if n % 2 else (sorted_rates[mid_index-1] + sorted_rates[mid_index]) / 2
    window = max(1, min(3, n // 2))
    if n >= 2:
        recent_trend = (sum(rates[-window:], Decimal("0")) / window
                        - sum(rates[-2*window:-window], Decimal("0")) / window)
    else:
        recent_trend = Decimal("0")
    positive_fraction = Decimal(positive) / n
    negative_fraction = Decimal(negative) / n
    return {
        "count": n, "mean": mean, "median": median_rate,
        "min": min(rates), "max": max(rates),
        "negative_fraction": negative_fraction,
        "positive_fraction": positive_fraction,
        "positive_percent": positive_fraction * 100,
        "negative_percent": negative_fraction * 100,
        "volatility": variance.sqrt(), "stddev": variance.sqrt(),
        "consecutive_positive": cp, "consecutive_negative": cn,
        "recent_trend": recent_trend, "consistency": positive_fraction,
    }


def detect_funding_interval_hours(funding_times_ms: list[int], default_hours: int = 8) -> int:
    """Infer interval from settlement timestamps without binary floating point."""
    ordered = sorted(set(funding_times_ms))
    if len(ordered) < 2:
        return default_hours
    gaps = [b - a for a, b in zip(ordered, ordered[1:]) if b > a]
    if not gaps:
        return default_hours
    ordered_gaps = sorted(gaps)
    mid = len(ordered_gaps) // 2
    median_gap = (Decimal(ordered_gaps[mid]) if len(ordered_gaps) % 2
                  else Decimal(ordered_gaps[mid-1] + ordered_gaps[mid]) / 2)
    hours = (median_gap / Decimal("3600000")).to_integral_value(rounding=ROUND_HALF_UP)
    result = int(hours)
    return result if result in {1, 4, 8} else default_hours


def funding_history_gaps(observations: list[FundingObservation], interval_hours: int,
                         tolerance_ms: int = 60000) -> list[tuple[int, int]]:
    """Return adjacent settlement gaps that exceed the expected interval plus tolerance."""
    if interval_hours <= 0:
        raise ValueError("interval_hours must be positive")
    ordered = sorted({x.funding_time_ms for x in observations if (x.rate_type or "Regular") == "Regular"})
    expected = interval_hours * 3600000
    return [(a, b) for a, b in zip(ordered, ordered[1:]) if b-a > expected+tolerance_ms]


def linear_futures_funding_notional(quantity: Decimal, contract_multiplier: Decimal,
                                    mark_price: Decimal) -> Decimal:
    """Return linear USDⓈ-M position notional at the supplied mark price."""
    if quantity <= 0 or contract_multiplier <= 0 or mark_price <= 0:
        raise ValueError("quantity, contract multiplier and mark price must be positive")
    return quantity * contract_multiplier * mark_price
