from decimal import Decimal
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
                "min": Decimal("0"), "max": Decimal("0"), "negative_fraction": Decimal("0")}
    rates = sorted(x.funding_rate for x in observations)
    n = len(rates)
    median = rates[n // 2] if n % 2 else (rates[n // 2 - 1] + rates[n // 2]) / 2
    return {
        "count": n,
        "mean": sum(rates, Decimal("0")) / n,
        "median": median,
        "min": rates[0],
        "max": rates[-1],
        "negative_fraction": Decimal(sum(1 for x in rates if x < 0)) / n,
    }
