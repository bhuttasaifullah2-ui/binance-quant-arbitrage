from app.services.market_data_hub import MarketDataHub


def test_hub_rejects_delayed_and_future_dated_events():
    hub = MarketDataHub(max_clock_skew_ms=50, max_event_delay_ms=1000)
    now = 1_800_000_000_000
    delayed = {"stream": "btcusdt@ticker", "_market": "spot",
               "data": {"s": "BTCUSDT", "E": now-1001, "c": "100", "q": "1000"}}
    future = {"stream": "ethusdt@ticker", "_market": "spot",
              "data": {"s": "ETHUSDT", "E": now+51, "c": "100", "q": "1000"}}
    assert not hub.apply_message(delayed, now)
    assert not hub.apply_message(future, now)
    assert {x["reason"] for x in hub.quality_events} == {"delayed_event", "future_event_timestamp"}


def test_clock_offset_normalizes_exchange_event_time():
    hub = MarketDataHub(max_clock_skew_ms=50, max_event_delay_ms=1000)
    now = 1_800_000_000_000
    hub.set_clock_offset("futures", 100, now)
    message = {"stream": "btcusdt@markPrice@1s", "_market": "futures",
               "data": {"s": "BTCUSDT", "E": now+100, "p": "100", "i": "99", "r": "0.001", "T": now+1000}}
    assert hub.apply_message(message, now)
    assert hub.pair_snapshot("BTCUSDT")["futures_mark_event_time_ms"] == now
