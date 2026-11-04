from decimal import Decimal as D
from app.domain.funding import funding_payment, summarize_funding
from app.domain.models import FundingObservation

def test_positive_funding_short_receives():
    assert funding_payment(D("10000"), D("0.0001")) == D("1.0000")

def test_negative_funding_is_payment_by_short():
    assert funding_payment(D("10000"), D("-0.0001")) == D("-1.0000")

def test_summary_median_and_negative_fraction():
    rows = [FundingObservation("X", D("0.1"), 1),
            FundingObservation("X", D("-0.1"), 2),
            FundingObservation("X", D("0.3"), 3)]
    result = summarize_funding(rows)
    assert result["median"] == D("0.1")
    assert result["negative_fraction"] == D(1) / D(3)


def test_linear_futures_funding_notional_uses_mark_price():
    from app.domain.funding import linear_futures_funding_notional

    assert linear_futures_funding_notional(D("0.2"), D("1"), D("50000")) == D("10000.0")


def test_linear_futures_funding_notional_rejects_invalid_inputs():
    import pytest
    from app.domain.funding import linear_futures_funding_notional

    with pytest.raises(ValueError):
        linear_futures_funding_notional(D("0"), D("1"), D("50000"))
