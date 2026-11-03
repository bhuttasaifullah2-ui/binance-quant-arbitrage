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

def _clamp(x: Decimal) -> Decimal:
    return max(Decimal("0"), min(Decimal("1"), x))

def score_opportunity(*, net_edge_usdt: Decimal, funding_positive_fraction: Decimal,
                      funding_volatility: Decimal, spot_depth_usdt: Decimal,
                      futures_depth_usdt: Decimal, spot_spread_bps: Decimal,
                      futures_spread_bps: Decimal, estimated_slippage_bps: Decimal,
                      basis_bps_value: Decimal, data_age_seconds: Decimal,
                      max_data_age_seconds: Decimal, min_depth_usdt: Decimal,
                      max_spread_bps: Decimal, max_slippage_bps: Decimal,
                      max_basis_bps: Decimal, weights: RankingWeights = RankingWeights()) -> Decimal:
    if data_age_seconds > max_data_age_seconds or min(spot_depth_usdt, futures_depth_usdt) < min_depth_usdt:
        return Decimal("0")
    edge = _clamp(net_edge_usdt / max(min_depth_usdt * Decimal("0.001"), Decimal("1")))
    liquidity = _clamp(min(spot_depth_usdt, futures_depth_usdt) / max(min_depth_usdt * 5, Decimal("1")))
    spread = _clamp(Decimal("1") - max(spot_spread_bps, futures_spread_bps) / max(max_spread_bps, Decimal("0.01")))
    slip = _clamp(Decimal("1") - estimated_slippage_bps / max(max_slippage_bps, Decimal("0.01")))
    basis = _clamp(Decimal("1") - abs(basis_bps_value) / max(max_basis_bps, Decimal("0.01")))
    fresh = _clamp(Decimal("1") - data_age_seconds / max(max_data_age_seconds, Decimal("1")))
    vol = _clamp(Decimal("1") - funding_volatility / Decimal("0.001"))
    components = [
        (edge, weights.net_edge), (_clamp(funding_positive_fraction), weights.funding_consistency),
        (liquidity, weights.liquidity), (spread, weights.spread), (slip, weights.slippage),
        (basis, weights.basis), (fresh, weights.freshness), (vol, weights.funding_volatility),
    ]
    total_weight = sum((w for _, w in components), Decimal("0"))
    if total_weight <= 0:
        return Decimal("0")
    return sum((v * w for v, w in components), Decimal("0")) / total_weight * Decimal("100")
