from decimal import Decimal as D
from app.domain.funding import summarize_funding, funding_history_gaps
from app.domain.models import FundingObservation


def test_funding_summary_reports_distribution_and_streaks():
    rows = [FundingObservation("X", D("0.001"), 1),
            FundingObservation("X", D("-0.001"), 2),
            FundingObservation("X", D("0.002"), 3),
            FundingObservation("X", D("0.003"), 4)]
    stats = summarize_funding(rows)
    assert stats["median"] == D("0.0015")
    assert stats["positive_percent"] == D("75")
    assert stats["negative_percent"] == D("25")
    assert stats["consecutive_positive"] == 2
    assert stats["stddev"] == stats["volatility"]


def test_funding_gap_detection_uses_expected_interval():
    interval = 8
    rows = [FundingObservation("X", D("0.001"), 0),
            FundingObservation("X", D("0.001"), 8*3600000),
            FundingObservation("X", D("0.001"), 24*3600000)]
    assert funding_history_gaps(rows, interval) == [(8*3600000, 24*3600000)]
