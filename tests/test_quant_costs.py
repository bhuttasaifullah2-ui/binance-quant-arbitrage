from decimal import Decimal as D

from app.domain.funding import detect_funding_interval_hours, funding_payment
from app.domain.orderbook import BookExecution
from app.domain.profitability import four_leg_execution_shortfall_usdt


def execution(vwap: str, quantity: str = "1") -> BookExecution:
    qty = D(quantity)
    price = D(vwap)
    return BookExecution(price, qty, price * qty, D("0"), D("0"), True)


def test_four_leg_execution_shortfall_includes_spread_crossing_and_book_impact():
    result = four_leg_execution_shortfall_usdt(
        spot_buy=execution("101"),
        spot_sell=execution("99"),
        futures_sell=execution("99"),
        futures_buy=execution("101"),
        spot_mid=D("100"),
        futures_mid=D("100"),
    )
    assert result == D("4")


def test_four_leg_execution_shortfall_does_not_credit_favorable_execution():
    result = four_leg_execution_shortfall_usdt(
        spot_buy=execution("99"),
        spot_sell=execution("101"),
        futures_sell=execution("101"),
        futures_buy=execution("99"),
        spot_mid=D("100"),
        futures_mid=D("100"),
    )
    assert result == D("0")


def test_funding_payment_uses_mark_based_futures_notional():
    futures_quantity = D("2")
    mark_price = D("101")
    contract_multiplier = D("0.1")
    funding_notional = futures_quantity * mark_price * contract_multiplier
    assert funding_notional == D("20.2")
    assert funding_payment(funding_notional, D("0.001")) == D("0.0202")


def test_unsupported_sixteen_hour_observed_gap_does_not_become_interval():
    hour = 60 * 60 * 1000
    assert detect_funding_interval_hours([0, 16 * hour]) == 8


def test_supported_four_hour_interval_is_inferred():
    hour = 60 * 60 * 1000
    assert detect_funding_interval_hours([0, 4 * hour, 8 * hour]) == 4
