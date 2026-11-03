from decimal import Decimal as D
from app.domain.market_quality import spread_bps, estimate_market_slippage, basis_bps

def test_spread_is_decimal_bps():
    assert spread_bps(D('99'), D('101')) == D('200')

def test_market_slippage_buy_and_insufficient_depth():
    levels = [['100', '2'], ['101', '2']]
    slip = estimate_market_slippage(levels, D('300'), 'buy')
    assert slip is not None and slip > 0
    assert estimate_market_slippage(levels, D('1000'), 'buy') is None

def test_basis_sign():
    assert basis_bps(D('100'), D('101')) == D('100')
