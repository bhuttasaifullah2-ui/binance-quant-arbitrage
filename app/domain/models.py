from dataclasses import dataclass
from decimal import Decimal
from datetime import datetime

@dataclass(frozen=True)
class FundingObservation:
    symbol: str
    funding_rate: Decimal
    funding_time_ms: int
    settlement_mark_price: Decimal | None = None
    rate_type: str = "Regular"
    source: str | None = None

@dataclass(frozen=True)
class MarketQuote:
    symbol: str
    bid: Decimal
    ask: Decimal
    last: Decimal
    quote_volume_24h: Decimal
    observed_at: datetime

    @property
    def spread_bps(self) -> Decimal:
        mid = (self.bid + self.ask) / Decimal("2")
        if mid <= 0:
            return Decimal("0")
        return ((self.ask - self.bid) / mid) * Decimal("10000")

@dataclass(frozen=True)
class Opportunity:
    symbol: str
    funding_rate: Decimal
    funding_notional: Decimal
    gross_funding: Decimal
    estimated_round_trip_fees: Decimal
    estimated_slippage: Decimal
    expected_net: Decimal
    status: str
    reason: str
