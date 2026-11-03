from decimal import Decimal as D
from app.domain.ranking import score_opportunity

def _score(age=D('1'), net=D('5')):
    return score_opportunity(net_edge_usdt=net, funding_positive_fraction=D('.9'),
        funding_volatility=D('.00001'), spot_depth_usdt=D('100000'),
        futures_depth_usdt=D('100000'), spot_spread_bps=D('1'),
        futures_spread_bps=D('1'), estimated_slippage_bps=D('2'),
        basis_bps_value=D('5'), data_age_seconds=age, max_data_age_seconds=D('15'),
        min_depth_usdt=D('1000'), max_spread_bps=D('10'), max_slippage_bps=D('30'),
        max_basis_bps=D('100'))

def test_stale_data_never_scores():
    assert _score(age=D('16')) == 0

def test_better_net_edge_has_nonlower_score():
    assert _score(net=D('10')) >= _score(net=D('1'))
