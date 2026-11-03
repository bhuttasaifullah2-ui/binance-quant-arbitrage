"""Decimal-only hypothetical funding sensitivity; never realized PnL."""
from decimal import Decimal
from typing import Any
from app.domain.profitability import CostModel, estimate_net_funding


def profitability_scenarios(notional: Decimal, funding_rate: Decimal, cost: CostModel,
                            basis_cost_rate: Decimal = Decimal("0"),
                            scenarios: dict[str, dict[str, Decimal]] | None = None) -> list[dict[str, Any]]:
    if notional <= 0:
        raise ValueError("notional must be positive")
    cases = scenarios or {
        "normal": {"funding_multiplier": Decimal("1"), "basis_multiplier": Decimal("1"),
                   "slippage_multiplier": Decimal("1")},
        "funding_minus_25pct": {"funding_multiplier": Decimal("0.75"), "basis_multiplier": Decimal("1"),
                   "slippage_multiplier": Decimal("1")},
        "funding_minus_50pct": {"funding_multiplier": Decimal("0.5"), "basis_multiplier": Decimal("1"),
                   "slippage_multiplier": Decimal("1")},
        "funding_zero": {"funding_multiplier": Decimal("0"), "basis_multiplier": Decimal("1"),
                   "slippage_multiplier": Decimal("1")},
        "funding_negative": {"funding_multiplier": Decimal("-1"), "basis_multiplier": Decimal("1"),
                   "slippage_multiplier": Decimal("1")},
        "basis_adverse": {"funding_multiplier": Decimal("1"), "basis_multiplier": Decimal("2"),
                   "slippage_multiplier": Decimal("1")},
        "slippage_double": {"funding_multiplier": Decimal("1"), "basis_multiplier": Decimal("1"),
                   "slippage_multiplier": Decimal("2")},
        "liquidity_spread_stress": {"funding_multiplier": Decimal("0.75"), "basis_multiplier": Decimal("2"),
                   "slippage_multiplier": Decimal("3")},
    }
    results=[]
    for name, factors in cases.items():
        slip=factors["slippage_multiplier"]
        adjusted=CostModel(cost.spot_entry_fee_rate,cost.futures_entry_fee_rate,
            cost.spot_exit_fee_rate,cost.futures_exit_fee_rate,
            cost.entry_slippage_rate*slip,cost.exit_slippage_rate*slip)
        estimate=estimate_net_funding(notional,funding_rate*factors["funding_multiplier"],adjusted)
        basis=notional*basis_cost_rate*factors["basis_multiplier"]
        net=estimate.expected_net-basis
        fixed=estimate.entry_fees+estimate.exit_fees+estimate.slippage+basis
        results.append({"scenario":name,"gross_funding_estimate":str(estimate.gross_funding),
            "estimated_total_costs":str(fixed),"estimated_net_result":str(net),
            "break_even_funding_rate":str(fixed/notional),
            "risk_warnings":["Hypothetical sensitivity only; not realized PnL.",
                "Funding and basis can change before settlement.",
                "Visible liquidity and fee assumptions are not execution guarantees."]})
    return results
