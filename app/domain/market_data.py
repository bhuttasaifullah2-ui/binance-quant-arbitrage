"""Normalized market event validation and per-field freshness tracking."""
from dataclasses import dataclass, field
from typing import Any


@dataclass
class FieldValue:
    value: Any
    event_time_ms: int
    received_at_ms: int
    source: str


@dataclass
class SymbolMarketState:
    symbol: str
    fields: dict[str, FieldValue] = field(default_factory=dict)
    last_event_by_stream: dict[str, int] = field(default_factory=dict)
    duplicate_events: int = 0
    out_of_order_events: int = 0

    def apply(self, stream: str, event_time_ms: int, received_at_ms: int,
              values: dict[str, Any], source: str = "websocket") -> bool:
        if event_time_ms <= 0 or received_at_ms <= 0 or not values:
            return False
        previous = self.last_event_by_stream.get(stream)
        if previous is not None and event_time_ms == previous:
            self.duplicate_events += 1
            return False
        if previous is not None and event_time_ms < previous:
            self.out_of_order_events += 1
            return False
        self.last_event_by_stream[stream] = event_time_ms
        accepted = 0
        for name, value in values.items():
            if value is None:
                continue
            previous_field = self.fields.get(name)
            if previous_field is not None:
                if received_at_ms < previous_field.received_at_ms:
                    self.out_of_order_events += 1
                    continue
                if source == "websocket" and previous_field.source == "websocket" and event_time_ms < previous_field.event_time_ms:
                    self.out_of_order_events += 1
                    continue
            self.fields[name] = FieldValue(value, event_time_ms, received_at_ms, source)
            accepted += 1
        if accepted == 0:
            return False
        return True

    def freshness(self, now_ms: int, max_age_ms: int, required: tuple[str, ...]) -> dict[str, Any]:
        ages: dict[str, int | None] = {}
        reasons: list[str] = []
        for name in required:
            item = self.fields.get(name)
            age = None if item is None else max(0, now_ms - item.received_at_ms)
            ages[name] = age
            if item is None:
                reasons.append(f"missing:{name}")
            elif age is not None and age > max_age_ms:
                reasons.append(f"stale:{name}")
        return {"valid": not reasons, "ages_ms": ages, "reasons": reasons}
