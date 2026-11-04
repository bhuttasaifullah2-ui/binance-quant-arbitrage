from app.domain.funding_integrity import (
    FUNDING_RATE_RECONCILED,
    HISTORY_INCOMPLETE,
    HISTORY_MISSING,
    HISTORY_RECONCILIATION_FAILED,
    HISTORY_STALE,
    adjusted_rate_within_limits,
    assess_funding_history,
    compatible_spot_futures,
    resolve_funding_interval,
    snapshot_skew_is_acceptable,
)


def _rows(step=8 * 3_600_000):
    first = 1_700_000_000_000
    return [
        {"fundingTime": first, "fundingRate": "0.0001", "markPrice": "50000", "rateType": "Regular"},
        {"fundingTime": first + step, "fundingRate": "0.0002", "markPrice": "50100", "rateType": "Regular"},
        {"fundingTime": first + 2 * step, "fundingRate": "0.0003", "markPrice": "50200", "rateType": "Regular"},
    ]


def test_interval_resolution_authoritative_adjusted_and_missing_info():
    times = [row["fundingTime"] for row in _rows(4 * 3_600_000)]
    assert resolve_funding_interval({"fundingIntervalHours": 4}, times)[0] == 4
    inferred, source = resolve_funding_interval(None, times)
    assert inferred == 4
    assert "NOT_EXCHANGE_AUTHORITY" in source
    assert resolve_funding_interval(None, times[:2])[0] is None


def test_normal_interval_and_invalid_funding_info():
    times = [row["fundingTime"] for row in _rows()]
    assert resolve_funding_interval({"fundingIntervalHours": 8}, times) == (8, "BINANCE_FUNDING_INFO")
    assert resolve_funding_interval({"fundingIntervalHours": 3}, times)[0] is None


def test_history_missing_stale_and_missing_intervals_are_distinct():
    now = 1_700_000_000_000 + 3 * 8 * 3_600_000
    assert assess_funding_history([], interval_hours=8, next_funding_time_ms=now,
                                  current_funding_rate="0.0003")["state"] == HISTORY_MISSING
    stale = _rows()[:2]
    assert assess_funding_history(stale, interval_hours=8, next_funding_time_ms=now,
                                  current_funding_rate="0.0002")["state"] == HISTORY_STALE
    gap = [_rows()[0], _rows()[2]]
    assert assess_funding_history(gap, interval_hours=8,
                                  next_funding_time_ms=gap[-1]["fundingTime"] + 8 * 3_600_000,
                                  current_funding_rate="0.0003")["state"] == HISTORY_INCOMPLETE


def test_history_reconciles_timestamp_rate_and_settlement_mark():
    rows = _rows()
    next_time = rows[-1]["fundingTime"] + 8 * 3_600_000
    result = assess_funding_history(rows, interval_hours=8, next_funding_time_ms=next_time,
                                    current_funding_rate="0.0003")
    assert result["state"] == FUNDING_RATE_RECONCILED
    assert result["settlement_mark_price"] == "50200"
    assert result["coverage_start_ms"] == rows[0]["fundingTime"]
    assert result["coverage_end_ms"] == rows[-1]["fundingTime"]
    assert result["settlement_count"] == 3
    assert result["cashflow_valuation"] == "SETTLEMENT_MARK_PRICE"
    no_mark = [{k: v for k, v in row.items() if k != "markPrice"} for row in rows]
    no_mark_result = assess_funding_history(
        no_mark, interval_hours=8, next_funding_time_ms=next_time,
        current_funding_rate="0.0003"
    )
    assert no_mark_result["cashflow_valuation"] == "ESTIMATE_PROXY_REQUIRED"


def test_conflicting_same_time_payloads_and_rate_mismatch_fail_closed():
    rows = _rows()
    next_time = rows[-1]["fundingTime"] + 8 * 3_600_000
    conflict = rows + [{**rows[-1], "rateType": "Special"}]
    assert assess_funding_history(conflict, interval_hours=8, next_funding_time_ms=next_time,
                                  current_funding_rate="0.0003")["state"] == HISTORY_RECONCILIATION_FAILED
    assert assess_funding_history(rows, interval_hours=8, next_funding_time_ms=next_time,
                                  current_funding_rate="0.9")["state"] == HISTORY_RECONCILIATION_FAILED


def test_history_coverage_boundary_is_checked_without_zero_filling():
    rows = _rows()
    next_time = rows[-1]["fundingTime"] + 8 * 3_600_000
    result = assess_funding_history(rows, interval_hours=8, next_funding_time_ms=next_time,
                                    current_funding_rate="0.0003",
                                    coverage_start_ms=rows[0]["fundingTime"] - 3 * 8 * 3_600_000)
    assert result["state"] == HISTORY_INCOMPLETE
    assert result["missing_intervals_are_not_zero"] is True


def test_adjusted_rate_caps_and_base_quote_and_snapshot_skew():
    assert adjusted_rate_within_limits("0.01", {
        "adjustedFundingRateFloor": "-0.02", "adjustedFundingRateCap": "0.02"
    }) is True
    assert adjusted_rate_within_limits("0.03", {
        "adjustedFundingRateFloor": "-0.02", "adjustedFundingRateCap": "0.02"
    }) is False
    spot = {"symbol": "BTCUSDT", "baseAsset": "BTC", "quoteAsset": "USDT"}
    futures = {"symbol": "BTCUSDT", "baseAsset": "BTC", "quoteAsset": "USDT", "contractSize": "1"}
    assert compatible_spot_futures(spot, futures)
    assert not compatible_spot_futures(spot, {**futures, "baseAsset": "WBTC"})
    assert snapshot_skew_is_acceptable(1000, 1500, 500)
    assert not snapshot_skew_is_acceptable(1000, 1501, 500)


def test_public_funding_info_failure_can_recover_without_credentials():
    import asyncio
    from app.exchange.binance.public_client import BinanceAPIError, BinancePublicClient

    class FlakyClient(BinancePublicClient):
        def __init__(self):
            super().__init__("https://api.binance.com", "https://fapi.binance.com")
            self.calls = 0
            self.paths = []

        async def _get(self, market, path, params=None):
            self.calls += 1
            self.paths.append((market, path))
            if self.calls == 1:
                raise BinanceAPIError("temporary public endpoint failure")
            return [{"symbol": "ABCUSDT", "fundingIntervalHours": 1,
                     "adjustedFundingRateCap": "0.02", "adjustedFundingRateFloor": "-0.02"}]

    async def scenario():
        client = FlakyClient()
        try:
            try:
                await client.futures_funding_info()
            except BinanceAPIError:
                pass
            rows = await client.futures_funding_info()
            return rows, client.calls, client.paths
        finally:
            await client.close()

    rows, calls, paths = asyncio.run(scenario())
    assert calls == 2
    assert paths == [("futures", "/fapi/v1/fundingInfo")] * 2
    assert rows[0]["fundingIntervalHours"] == 1
