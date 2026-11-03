"""In-memory normalized feed state shared by scanner and monitoring APIs."""
import time
from typing import Any
from app.domain.market_data import SymbolMarketState


class MarketDataHub:
    def __init__(self, max_clock_skew_ms: int = 5000):
        self.states: dict[str, SymbolMarketState] = {}
        self.symbols: set[str] = set()
        self.max_clock_skew_ms = max_clock_skew_ms
        self.health: dict[str, dict[str, Any]] = {}
        self.quality_events: list[dict[str, Any]] = []
        self._last_update_ids: dict[str, int] = {}

    def set_symbols(self, symbols: set[str]) -> None:
        self.symbols = set(symbols)

    def _state(self, symbol: str) -> SymbolMarketState:
        return self.states.setdefault(symbol.upper(), SymbolMarketState(symbol.upper()))

    def apply_message(self, message: dict[str, Any], received_at_ms: int | None = None) -> bool:
        stream = str(message.get("stream", ""))
        data = message.get("data", message)
        if not isinstance(data, dict):
            return False
        symbol = str(data.get("s", "")).upper()
        if not symbol:
            return False
        now = received_at_ms or int(time.time() * 1000)
        event_ms = int(data.get("E", data.get("T", 0)) or 0)
        if event_ms <= 0 and "bookticker" in stream.lower():
            event_ms = now
            update_id = int(data.get("u", 0) or 0)
            update_key = f"{message.get('_market', 'unknown')}:{symbol}:bookticker"
            previous_id = self._last_update_ids.get(update_key)
            if update_id and previous_id is not None and update_id <= previous_id:
                self.quality_events.append({"symbol": symbol, "reason": "duplicate_or_out_of_order_update_id"})
                return False
            if update_id:
                self._last_update_ids[update_key] = update_id
        if event_ms <= 0:
            self.quality_events.append({"symbol": symbol, "reason": "missing_event_timestamp"})
            return False
        if event_ms > now + self.max_clock_skew_ms:
            self.quality_events.append({"symbol": symbol, "reason": "future_event_timestamp", "event_time_ms": event_ms})
            return False
        values: dict[str, Any] = {}
        stream_name = stream.lower()
        market_hint = str(message.get("_market", data.get("_market", ""))).lower()
        if "bookticker" in stream_name or ("b" in data and "a" in data and "B" in data and "A" in data):
            market = "spot" if market_hint == "spot" else "futures"
            stream_name = f"{market}:bookticker"
            values.update({f"{market}_bid": data.get("b"), f"{market}_ask": data.get("a"),
                           f"{market}_bid_qty": data.get("B"), f"{market}_ask_qty": data.get("A")})
        elif "markprice" in stream_name or all(k in data for k in ("p", "i", "r", "T")):
            values.update({"futures_mark": data.get("p"), "futures_index": data.get("i"),
                           "funding_rate": data.get("r"), "next_funding_time_ms": data.get("T")})
            stream_name = "futures:markprice"
        elif "ticker" in stream_name or "24hr" in stream_name or "mini" in stream_name:
            market = "spot" if market_hint == "spot" else "futures"
            prefix = "spot" if market == "spot" else "futures"
            values.update({f"{prefix}_last": data.get("c"), f"{prefix}_quote_volume": data.get("q")})
            stream_name = f"{market}:ticker"
        else:
            return False
        market = "spot" if stream_name.startswith("spot:") else "futures"
        key = stream_name
        accepted = self._state(symbol).apply(key, event_ms, now, values)
        if not accepted:
            self.quality_events.append({"symbol": symbol, "reason": "duplicate_or_out_of_order", "stream": key})
        return accepted

    def apply_rest(self, symbol: str, stream: str, values: dict[str, Any],
                   event_time_ms: int, received_at_ms: int | None = None) -> bool:
        return self._state(symbol).apply(stream, event_time_ms, received_at_ms or int(time.time()*1000),
                                         values, source="rest_snapshot")

    def pair_snapshot(self, symbol: str, now_ms: int | None = None) -> dict[str, Any] | None:
        state = self.states.get(symbol.upper())
        if state is None:
            return None
        result: dict[str, Any] = {}
        for key, item in state.fields.items():
            result[key] = item.value
            result[f"{key}_event_time_ms"] = item.event_time_ms
            result[f"{key}_received_at_ms"] = item.received_at_ms
        return result

    def data_quality(self, now_ms: int | None = None, max_age_ms: int = 15000) -> dict[str, Any]:
        now = now_ms or int(time.time()*1000)
        fields = ("spot_bid", "spot_ask", "spot_last", "spot_quote_volume",
                  "futures_bid", "futures_ask", "futures_mark", "futures_index", "funding_rate")
        items = {}
        for symbol, state in self.states.items():
            items[symbol] = state.freshness(now, max_age_ms, fields)
        return {"symbols": items, "quality_events": self.quality_events[-100:],
                "symbol_count": len(items)}

    def stream_health(self) -> dict[str, Any]:
        return {"streams": self.health, "symbol_count": len(self.symbols),
                "live_trading_enabled": False}
