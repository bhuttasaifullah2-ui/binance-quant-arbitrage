"""Order-book execution estimates using Decimal quote-notional targets."""
from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class BookExecution:
    vwap: Decimal
    executed_quantity: Decimal
    quote_notional: Decimal
    slippage_bps: Decimal
    market_impact_bps: Decimal
    complete: bool


def estimate_vwap(levels: list[list[str]], quote_notional: Decimal, side: str) -> BookExecution | None:
    if side not in {"buy", "sell"} or quote_notional <= 0 or not levels:
        return None
    parsed = [(Decimal(str(p)), Decimal(str(q))) for p, q in levels]
    parsed = [(p, q) for p, q in parsed if p > 0 and q > 0]
    if not parsed:
        return None
    best = parsed[0][0]
    remaining = quote_notional
    qty_total = Decimal("0")
    quote_total = Decimal("0")
    for price, quantity in parsed:
        take_qty = min(quantity, remaining / price)
        used = take_qty * price
        qty_total += take_qty
        quote_total += used
        remaining -= used
        if remaining <= 0:
            break
    if qty_total <= 0:
        return None
    vwap = quote_total / qty_total
    raw = ((vwap - best) / best if side == "buy" else (best - vwap) / best) * Decimal("10000")
    slip = max(Decimal("0"), raw)
    return BookExecution(vwap, qty_total, quote_total, slip, slip, remaining <= 0)



def estimate_vwap_quantity(levels: list[list[str]], target_quantity: Decimal, side: str) -> BookExecution | None:
    """Estimate execution for an exact base-asset quantity; incomplete depth is explicit."""
    if side not in {"buy", "sell"} or target_quantity <= 0 or not levels:
        return None
    parsed = [(Decimal(str(p)), Decimal(str(q))) for p, q in levels]
    parsed = [(p, q) for p, q in parsed if p > 0 and q > 0]
    if not parsed:
        return None
    best = parsed[0][0]
    remaining = target_quantity
    qty_total = Decimal("0")
    quote_total = Decimal("0")
    for price, available in parsed:
        take_qty = min(available, remaining)
        qty_total += take_qty
        quote_total += take_qty * price
        remaining -= take_qty
        if remaining <= 0:
            break
    if qty_total <= 0:
        return None
    vwap = quote_total / qty_total
    raw = ((vwap - best) / best if side == "buy" else (best - vwap) / best) * Decimal("10000")
    slip = max(Decimal("0"), raw)
    return BookExecution(vwap, qty_total, quote_total, slip, slip, remaining <= 0)
