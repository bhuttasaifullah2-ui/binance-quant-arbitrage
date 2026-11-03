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
