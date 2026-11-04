from dataclasses import dataclass
from decimal import Decimal
from app.domain.orderbook import BookExecution

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


def four_leg_execution_shortfall_usdt(
    spot_buy: BookExecution,
    spot_sell: BookExecution,
    futures_sell: BookExecution,
    futures_buy: BookExecution,
    spot_mid: Decimal,
    futures_mid: Decimal,
) -> Decimal:
    """Conservative four-leg execution shortfall versus contemporaneous mid prices.

    This includes both spread crossing and visible-book impact. It deliberately
    floors each leg's shortfall at zero rather than crediting favorable execution.
    """
    if spot_mid <= 0 or futures_mid <= 0:
        raise ValueError("mid prices must be positive")

    legs = (
        (spot_buy, spot_mid, "buy"),
        (spot_sell, spot_mid, "sell"),
        (futures_sell, futures_mid, "sell"),
        (futures_buy, futures_mid, "buy"),
    )
    total = Decimal("0")
    for execution, mid, side in legs:
        if execution.executed_quantity < 0 or execution.vwap <= 0:
            raise ValueError("execution quantity and VWAP must be valid")
        per_unit = execution.vwap - mid if side == "buy" else mid - execution.vwap
        total += max(Decimal("0"), per_unit) * execution.executed_quantity
    return total
