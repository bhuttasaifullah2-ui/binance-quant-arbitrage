from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class RankingWeights:
    net_edge: Decimal = Decimal("0.25")
    funding_consistency: Decimal = Decimal("0.15")
    liquidity: Decimal = Decimal("0.15")
    spread: Decimal = Decimal("0.10")
    slippage: Decimal = Decimal("0.10")
    basis: Decimal = Decimal("0.10")
    freshness: Decimal = Decimal("0.10")
    funding_volatility: Decimal = Decimal("0.05")
    hedge_precision: Decimal = Decimal("0.05")
    residual_exposure: Decimal = Decimal("0.05")


def _clamp(x: Decimal) -> Decimal:
    return max(Decimal("0"), min(Decimal("1"), x))


def score_opportunity_breakdown(*, net_edge_usdt: Decimal, funding_positive_fraction: Decimal,
                      funding_volatility: Decimal, spot_depth_usdt: Decimal,
                      futures_depth_usdt: Decimal, spot_spread_bps: Decimal,
                      futures_spread_bps: Decimal, estimated_slippage_bps: Decimal,
                      basis_bps_value: Decimal, data_age_seconds: Decimal,
                      max_data_age_seconds: Decimal, min_depth_usdt: Decimal,
                      max_spread_bps: Decimal, max_slippage_bps: Decimal,
                      max_basis_bps: Decimal, weights: RankingWeights = RankingWeights(),
                      hedge_mismatch_fraction: Decimal = Decimal("0"),
                      max_hedge_mismatch: Decimal = Decimal("0.005"),
                      residual_exposure_usdt: Decimal = Decimal("0"),
                      max_residual_exposure_usdt: Decimal = Decimal("100")) -> dict[str, Decimal]:
    zero = Decimal("0")
    if (data_age_seconds > max_data_age_seconds or min(spot_depth_usdt, futures_depth_usdt) < min_depth_usdt
            or hedge_mismatch_fraction > max_hedge_mismatch
            or abs(residual_exposure_usdt) > max_residual_exposure_usdt):
        return {"score": zero, "net_edge": zero, "funding_consistency": zero,
                "liquidity": zero, "spread": zero, "slippage": zero, "basis": zero,
                "freshness": zero, "funding_volatility": zero,
                "hedge_precision": zero, "residual_exposure": zero}
    factors = {
        "net_edge": _clamp(net_edge_usdt / max(min_depth_usdt * Decimal("0.001"), Decimal("1"))),
        "funding_consistency": _clamp(funding_positive_fraction),
        "liquidity": _clamp(min(spot_depth_usdt, futures_depth_usdt) / max(min_depth_usdt * 5, Decimal("1"))),
        "spread": _clamp(Decimal("1") - max(spot_spread_bps, futures_spread_bps) / max(max_spread_bps, Decimal("0.01"))),
        "slippage": _clamp(Decimal("1") - estimated_slippage_bps / max(max_slippage_bps, Decimal("0.01"))),
        "basis": _clamp(Decimal("1") - abs(basis_bps_value) / max(max_basis_bps, Decimal("0.01"))),
        "freshness": _clamp(Decimal("1") - data_age_seconds / max(max_data_age_seconds, Decimal("1"))),
        "funding_volatility": _clamp(Decimal("1") - funding_volatility / Decimal("0.001")),
        "hedge_precision": _clamp(Decimal("1") - hedge_mismatch_fraction / max(max_hedge_mismatch, Decimal("0.00000001"))),
        "residual_exposure": _clamp(Decimal("1") - abs(residual_exposure_usdt) / max(max_residual_exposure_usdt, Decimal("0.01"))),
    }
    configured = {
        "net_edge": weights.net_edge, "funding_consistency": weights.funding_consistency,
        "liquidity": weights.liquidity, "spread": weights.spread, "slippage": weights.slippage,
        "basis": weights.basis, "freshness": weights.freshness,
        "funding_volatility": weights.funding_volatility,
        "hedge_precision": weights.hedge_precision, "residual_exposure": weights.residual_exposure,
    }
    total_weight = sum(configured.values(), zero)
    score = zero if total_weight <= zero else sum((factors[k] * configured[k] for k in factors), zero) / total_weight * 100
    return {"score": score, **factors}


def score_opportunity(**kwargs: Decimal) -> Decimal:
    return score_opportunity_breakdown(**kwargs)["score"]
