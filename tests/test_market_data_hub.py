from app.services.market_data_hub import MarketDataHub


def test_spot_futures_streams_normalize_and_reject_duplicate_update_ids():
    hub = MarketDataHub()
    now = 1_800_000_000_000
    spot = {"stream": "btcusdt@bookTicker", "_market": "spot",
            "data": {"s": "BTCUSDT", "u": 10, "b": "99", "B": "2", "a": "101", "A": "3"}}
    assert hub.apply_message(spot, now)
    assert not hub.apply_message(spot, now+1)
    futures = {"stream": "btcusdt@markPrice@1s", "_market": "futures",
               "data": {"s": "BTCUSDT", "E": now, "p": "100", "i": "99.9", "r": "0.0001", "T": now+1000}}
    assert hub.apply_message(futures, now)
    snap = hub.pair_snapshot("BTCUSDT")
    assert snap["spot_bid"] == "99"
    assert snap["futures_mark"] == "100"


def test_market_state_rejects_out_of_order_and_stale_fields():
    hub = MarketDataHub()
    now = 1_800_000_000_000
    state = hub._state("ETHUSDT")
    assert state.apply("spot:ticker", now, now, {"spot_last": "10"})
    assert not state.apply("spot:ticker", now-1, now, {"spot_last": "9"})
    quality = state.freshness(now+10000, 5000, ("spot_last", "spot_bid"))
    assert not quality["valid"]
    assert "stale:spot_last" in quality["reasons"]
    assert "missing:spot_bid" in quality["reasons"]
