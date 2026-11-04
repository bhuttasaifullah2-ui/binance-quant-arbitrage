from dataclasses import dataclass
from decimal import Decimal

@dataclass(frozen=True)
class CostModel:
    spot_entry_fee_rate: Decimal
    futures_entry_fee_rate: Decimal
    spot_exit_fee_rate: Decimal
    futures_exit_fee_rate: Decimal
    entry_slippage_rate: Decimal = Decimal("0")
    exit_slippage_rate: Decimal = Decimal("0")

@dataclass(frozen=True)
class ProfitEstimate:
    gross_funding: Decimal
    entry_fees: Decimal
    exit_fees: Decimal
    slippage: Decimal
    expected_net: Decimal

def estimate_net_funding(notional: Decimal, funding_rate: Decimal, cost: CostModel) -> ProfitEstimate:
    if notional <= 0:
        raise ValueError("notional must be positive")
    gross = notional * funding_rate
    entry_fees = notional * (cost.spot_entry_fee_rate + cost.futures_entry_fee_rate)
    exit_fees = notional * (cost.spot_exit_fee_rate + cost.futures_exit_fee_rate)
    slippage = notional * 2 * (cost.entry_slippage_rate + cost.exit_slippage_rate)
    return ProfitEstimate(gross, entry_fees, exit_fees, slippage,
                          gross - entry_fees - exit_fees - slippage)


def estimate_round_trip_spread_cost(
    spot_spread_bps: Decimal,
    futures_spread_bps: Decimal,
    spot_entry_notional: Decimal,
    spot_exit_notional: Decimal,
    futures_entry_notional: Decimal,
    futures_exit_notional: Decimal,
) -> Decimal:
    """Estimate four-leg spread crossing cost relative to each market midpoint.

    Spread inputs are full bid/ask widths in basis points. Each buy or sell
    pays approximately half the spread; the four legs are summed separately.
    Additional depth impact is intentionally calculated elsewhere.
    """
    values = (
        spot_spread_bps, futures_spread_bps, spot_entry_notional,
        spot_exit_notional, futures_entry_notional, futures_exit_notional,
    )
    if any(value < 0 for value in values):
        raise ValueError("spreads and notionals cannot be negative")
    half_bps_to_rate = Decimal("0.5") / Decimal("10000")
    return (
        spot_spread_bps * half_bps_to_rate * (spot_entry_notional + spot_exit_notional)
        + futures_spread_bps * half_bps_to_rate * (futures_entry_notional + futures_exit_notional)
    )
