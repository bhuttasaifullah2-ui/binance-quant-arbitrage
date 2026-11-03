from decimal import Decimal
from app.domain.models import MarketQuote, Opportunity
from app.domain.profitability import CostModel, estimate_net_funding

def build_opportunity(symbol: str, funding_rate: Decimal, notional: Decimal,
                      cost: CostModel, quote: MarketQuote,
                      min_quote_volume: Decimal, max_spread_bps: Decimal,
                      min_net: Decimal) -> Opportunity:
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
    status = "REJECTED" if reasons else "WAITING"
    return Opportunity(symbol, funding_rate, notional, estimate.gross_funding,
                       estimate.entry_fees + estimate.exit_fees, estimate.slippage,
                       estimate.expected_net, status, "; ".join(reasons) or
                       "Passed initial filters; requires full account and depth checks")
