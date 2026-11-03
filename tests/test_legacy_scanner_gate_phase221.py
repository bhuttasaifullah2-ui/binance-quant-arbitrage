from datetime import datetime, timezone
from decimal import Decimal

from app.domain.models import MarketQuote
from app.domain.profitability import CostModel
from app.services.scanner import build_opportunity


def test_legacy_builder_cannot_mark_candidate_eligible_without_shared_gate_context():
    quote = MarketQuote("BTCUSDT", Decimal("99"), Decimal("100"),
                        Decimal("99.5"), Decimal("1000000"),
                        datetime.now(timezone.utc))
    costs = CostModel(*(Decimal("0.0001") for _ in range(4)))
    result = build_opportunity(
        "BTCUSDT", Decimal("0.01"), Decimal("1000"), costs, quote,
        Decimal("10000"), Decimal("10"), Decimal("0"))
    assert result.status == "REJECTED"
    assert "ELIGIBILITY_GATE_REQUIRED" in result.reason
