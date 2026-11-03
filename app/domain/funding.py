from decimal import Decimal
from statistics import median
from .models import FundingObservation

def funding_payment(notional: Decimal, funding_rate: Decimal, short_receives_positive: bool = True) -> Decimal:
    """Signed cash flow for the short leg; positive means receipt by short."""
    if notional < 0:
        raise ValueError("notional cannot be negative")
    sign = Decimal("1") if short_receives_positive else Decimal("-1")
    return notional * funding_rate * sign

def summarize_funding(observations: list[FundingObservation]) -> dict[str, Decimal | int]:
    if not observations:
        return {"count": 0, "mean": Decimal("0"), "median": Decimal("0"),
                "min": Decimal("0"), "max": Decimal("0"), "negative_fraction": Decimal("0"),
                "positive_fraction": Decimal("0"), "volatility": Decimal("0")}
    rates = sorted(x.funding_rate for x in observations)
    n = len(rates)
    mean = sum(rates, Decimal("0")) / n
    variance = sum(((x - mean) ** 2 for x in rates), Decimal("0")) / n
    return {
        "count": n,
        "mean": mean,
        "median": Decimal(str(median(rates))),
        "min": rates[0],
        "max": rates[-1],
        "negative_fraction": Decimal(sum(1 for x in rates if x < 0)) / n,
        "positive_fraction": Decimal(sum(1 for x in rates if x > 0)) / n,
        "volatility": variance.sqrt(),
    }

def detect_funding_interval_hours(funding_times_ms: list[int], default_hours: int = 8) -> int:
    """Infer interval from observed settlement timestamps; fall back to Binance default."""
    ordered = sorted(set(funding_times_ms))
    if len(ordered) < 2:
        return default_hours
    gaps = [b - a for a, b in zip(ordered, ordered[1:]) if b > a]
    if not gaps:
        return default_hours
    hours = int(round(float(Decimal(str(median(gaps))) / Decimal("3600000"))))
    return hours if 1 <= hours <= 24 else default_hours
