import time
from decimal import Decimal
from typing import Any
from app.domain.models import MarketQuote, Opportunity
from app.domain.profitability import CostModel, estimate_net_funding
from app.domain.scanner_eligibility import validate_scanner_eligibility


def build_opportunity(symbol: str, funding_rate: Decimal, notional: Decimal,
                      cost: CostModel, quote: MarketQuote,
                      min_quote_volume: Decimal, max_spread_bps: Decimal,
                      min_net: Decimal, eligibility_context: dict[str, Any] | None = None) -> Opportunity:
    """Legacy candidate builder; refuses eligibility unless the shared gate context is supplied."""
    estimate = estimate_net_funding(notional, funding_rate, cost)
    reasons: list[str] = []
    if quote.quote_volume_24h < min_quote_volume:
        reasons.append("24h quote volume below threshold")
    if quote.spread_bps > max_spread_bps:
        reasons.append("Spread above threshold")
    if funding_rate <= 0:
        reasons.append("Funding is not positive for the short-receives-positive setup")
    if estimate.expected_net <= min_net:
        reasons.append("Expected net funding does not exceed configured minimum")
    if eligibility_context is None:
        reasons.append("ELIGIBILITY_GATE_REQUIRED: full scanner eligibility evidence was not supplied")
    else:
        decision = validate_scanner_eligibility(
            symbol, int(time.time() * 1000), eligibility_context)
        reasons.extend(f"{item.code}: {item.explanation}" for item in decision.rejections)
        if not decision.eligible and not reasons:
            reasons.append("SHARED_ELIGIBILITY_GATE_REJECTED")
    status = "REJECTED" if reasons else "WAITING"
    return Opportunity(symbol, funding_rate, notional, estimate.gross_funding,
                       estimate.entry_fees + estimate.exit_fees, estimate.slippage,
                       estimate.expected_net, status, "; ".join(reasons) or
                       "Passed shared eligibility gate; theoretical only, no execution");
