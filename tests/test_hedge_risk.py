from decimal import Decimal as D
from app.domain.hedge import matched_quantities
from app.domain.risk import approve_trade

def test_matched_quantities_respect_steps():
    spot, futures = matched_quantities(D("1000"), D("100"), D("101"),
                                       D(".001"), D(".001"))
    assert spot == D("10")
    assert futures == D("9.900")

def test_risk_blocks_mismatch():
    decision = approve_trade(D("10000"), D(".05"), D(".20"),
                             D("1000"), D("900"), D("0"), D("0"))
    assert not decision.approved
    assert "Hedge mismatch limit exceeded" in decision.reasons
