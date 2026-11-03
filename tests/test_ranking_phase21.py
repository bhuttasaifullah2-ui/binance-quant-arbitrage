from decimal import Decimal as D
from app.domain.ranking import score_opportunity_breakdown


def test_ranking_explains_hedge_precision_and_residual_exposure():
    result = score_opportunity_breakdown(
        net_edge_usdt=D("5"), funding_positive_fraction=D(".9"), funding_volatility=D(".00001"),
        spot_depth_usdt=D("100000"), futures_depth_usdt=D("100000"),
        spot_spread_bps=D("1"), futures_spread_bps=D("1"), estimated_slippage_bps=D("2"),
        basis_bps_value=D("5"), data_age_seconds=D("1"), max_data_age_seconds=D("15"),
        min_depth_usdt=D("1000"), max_spread_bps=D("10"), max_slippage_bps=D("30"),
        max_basis_bps=D("100"), hedge_mismatch_fraction=D(".001"),
        max_hedge_mismatch=D(".005"), residual_exposure_usdt=D("1"),
        max_residual_exposure_usdt=D("5"))
    assert result["score"] > 0
    assert result["hedge_precision"] == D(".8")
    assert result["residual_exposure"] == D(".8")


def test_ranking_rejects_critical_hedge_mismatch():
    result = score_opportunity_breakdown(
        net_edge_usdt=D("100"), funding_positive_fraction=D("1"), funding_volatility=D("0"),
        spot_depth_usdt=D("100000"), futures_depth_usdt=D("100000"),
        spot_spread_bps=D("0"), futures_spread_bps=D("0"), estimated_slippage_bps=D("0"),
        basis_bps_value=D("0"), data_age_seconds=D("0"), max_data_age_seconds=D("15"),
        min_depth_usdt=D("1000"), max_spread_bps=D("10"), max_slippage_bps=D("30"),
        max_basis_bps=D("100"), hedge_mismatch_fraction=D(".02"), max_hedge_mismatch=D(".005"))
    assert result["score"] == D("0")
