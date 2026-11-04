from decimal import Decimal as D
from app.domain.profitability import CostModel, estimate_net_funding

def test_all_four_legs_fees_and_slippage_are_counted():
    costs = CostModel(D(".001"), D(".0005"), D(".001"), D(".0005"),
                      D(".0002"), D(".0002"))
    result = estimate_net_funding(D("10000"), D(".003"), costs)
    assert result.gross_funding == D("30")
    assert result.entry_fees == D("15.0000")
    assert result.exit_fees == D("15.0000")
    assert result.slippage == D("8.0000")
    assert result.expected_net == D("-8.0000")


def test_round_trip_spread_cost_charges_all_four_legs():
    from app.domain.profitability import estimate_round_trip_spread_cost

    result = estimate_round_trip_spread_cost(
        D("10"), D("10"), D("1000"), D("1000"), D("1000"), D("1000")
    )
    assert result == D("2.0000")


def test_round_trip_spread_cost_rejects_negative_inputs():
    import pytest
    from app.domain.profitability import estimate_round_trip_spread_cost

    with pytest.raises(ValueError):
        estimate_round_trip_spread_cost(D("-1"), D("10"), D("1000"), D("1000"),
                                        D("1000"), D("1000"))
