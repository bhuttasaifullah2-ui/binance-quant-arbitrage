from dataclasses import dataclass
from decimal import Decimal
from .hedge import hedge_mismatch_fraction

@dataclass(frozen=True)
class RiskLimits:
    max_symbol_allocation: Decimal = Decimal("0.10")
    max_total_deployed: Decimal = Decimal("0.30")
    max_hedge_mismatch: Decimal = Decimal("0.005")
    max_daily_loss: Decimal = Decimal("0.01")
    max_drawdown: Decimal = Decimal("0.02")

@dataclass(frozen=True)
class RiskDecision:
    approved: bool
    reasons: tuple[str, ...]

def approve_trade(equity: Decimal, symbol_allocation: Decimal, total_deployed: Decimal,
                  spot_notional: Decimal, futures_notional: Decimal,
                  daily_loss_fraction: Decimal, drawdown_fraction: Decimal,
                  limits: RiskLimits = RiskLimits()) -> RiskDecision:
    reasons: list[str] = []
    if equity <= 0:
        reasons.append("Equity must be positive")
    if symbol_allocation > limits.max_symbol_allocation:
        reasons.append("Per-symbol allocation limit exceeded")
    if total_deployed > limits.max_total_deployed:
        reasons.append("Total deployed capital limit exceeded")
    if hedge_mismatch_fraction(spot_notional, futures_notional) > limits.max_hedge_mismatch:
        reasons.append("Hedge mismatch limit exceeded")
    if daily_loss_fraction >= limits.max_daily_loss:
        reasons.append("Daily loss limit reached")
    if drawdown_fraction >= limits.max_drawdown:
        reasons.append("Portfolio drawdown emergency threshold reached")
    return RiskDecision(not reasons, tuple(reasons))
