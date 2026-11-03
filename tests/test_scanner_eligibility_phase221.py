from decimal import Decimal
import pytest

from app.domain.scanner_eligibility import validate_scanner_eligibility


def healthy_context():
    return {
        "market_fields": {
            "spot_bid": "99", "spot_ask": "100", "spot_last": "99.5",
            "spot_quote_volume": "1000000", "futures_bid": "100",
            "futures_ask": "101", "futures_last": "100.5",
            "futures_quote_volume": "1000000", "futures_mark": "100.5",
            "futures_index": "100", "funding_rate": "0.0001",
            "next_funding_time_ms": "200000",
        },
        "field_ages_ms": {key: 100 for key in (
            "spot_bid", "spot_ask", "spot_last", "spot_quote_volume",
            "futures_bid", "futures_ask", "futures_last", "futures_quote_volume",
            "futures_mark", "futures_index", "funding_rate", "next_funding_time_ms")},
        "max_data_age_ms": 1000,
        "websocket_enabled": True,
        "websocket_feeds": {
            "spot": {"healthy": True}, "futures": {"healthy": True}},
        "clock_offsets_ms": {"spot": 10, "futures": -10},
        "max_clock_drift_ms": 1000,
        "clock_sync_required": True,
        "spot_filters_valid": True, "futures_filters_valid": True,
        "funding_age_ms": 100, "max_funding_age_ms": 1000,
        "funding_gaps": [], "funding_inconsistent": False,
        "hedge_valid": True, "residual_exposure_usdt": "1",
        "max_residual_exposure_usdt": "5",
        "min_orderbook_depth_usdt": "100",
        "spot_orderbook_depth_usdt": "1000",
        "futures_orderbook_depth_usdt": "1000",
    }


def test_shared_eligibility_gate_accepts_complete_healthy_candidate():
    result = validate_scanner_eligibility("btcusdt", 100000, healthy_context())
    assert result.eligible
    assert result.as_dict()["rejection_codes"] == []


@pytest.mark.parametrize(("mutate", "code"), [
    (lambda x: x.update(market_data_stale=True), "MARKET_DATA_STALE"),
    (lambda x: x["market_fields"].update(spot_bid=None), "MARKET_FIELDS_MISSING"),
    (lambda x: x["market_fields"].update(futures_mark="0"), "MARKET_VALUES_INVALID"),
    (lambda x: x["websocket_feeds"]["spot"].update(healthy=False), "WEBSOCKET_UNHEALTHY"),
    (lambda x: x["clock_offsets_ms"].update(futures=9000), "CLOCK_DRIFT_EXCEEDED"),
    (lambda x: x.update(spot_filters_valid=False), "EXCHANGE_FILTERS_INVALID"),
    (lambda x: x.update(funding_age_ms=5000), "FUNDING_DATA_STALE"),
    (lambda x: x.update(funding_gaps=[(1, 2)]), "FUNDING_HISTORY_INCONSISTENT"),
    (lambda x: x.update(hedge_valid=False), "HEDGE_FILTER_VALIDATION_FAILED"),
    (lambda x: x.update(residual_exposure_usdt="9"), "RESIDUAL_EXPOSURE_EXCEEDED"),
    (lambda x: x.update(spot_orderbook_depth_usdt="1"), "ORDERBOOK_LIQUIDITY_INSUFFICIENT"),
    (lambda x: x["market_fields"].update(next_funding_time_ms="1"), "FUNDING_TIMESTAMP_INVALID"),
])
def test_shared_eligibility_gate_rejects_each_failure(mutate, code):
    context = healthy_context()
    mutate(context)
    result = validate_scanner_eligibility("BTCUSDT", 100000, context)
    assert not result.eligible
    assert code in result.as_dict()["rejection_codes"]
    assert all(item["explanation"] for item in result.as_dict()["rejections"])
