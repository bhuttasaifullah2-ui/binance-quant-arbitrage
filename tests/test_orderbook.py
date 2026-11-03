from decimal import Decimal as D
from app.domain.orderbook import estimate_vwap


def test_buy_vwap_uses_quote_notional_and_reports_slippage():
    result = estimate_vwap([["100", "1"], ["101", "2"]], D("150"), "buy")
    assert result is not None and result.complete
    assert result.quote_notional == D("150")
    assert result.executed_quantity == D("1.495049504950495049504950495")
    assert result.slippage_bps > 0


def test_insufficient_depth_is_not_marked_complete():
    result = estimate_vwap([["100", "1"]], D("500"), "sell")
    assert result is not None
    assert not result.complete
