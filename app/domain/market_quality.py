from decimal import Decimal

def spread_bps(bid: Decimal, ask: Decimal) -> Decimal:
    if bid <= 0 or ask <= 0 or ask < bid:
        raise ValueError("invalid bid/ask")
    mid = (bid + ask) / Decimal("2")
    return (ask - bid) / mid * Decimal("10000")

def depth_notional(levels: list[list[str]], limit_price: Decimal, side: str) -> Decimal:
    """Quote-currency notional executable up to limit_price for asks or bids."""
    total = Decimal("0")
    for price_raw, qty_raw in levels:
        price, qty = Decimal(price_raw), Decimal(qty_raw)
        if price <= 0 or qty <= 0:
            continue
        if side == "buy" and price > limit_price:
            continue
        if side == "sell" and price < limit_price:
            continue
        total += price * qty
    return total

def estimate_market_slippage(levels: list[list[str]], target_notional: Decimal, side: str) -> Decimal | None:
    """Return weighted-average execution slippage in bps; None if depth is insufficient."""
    if target_notional <= 0 or side not in {"buy", "sell"} or not levels:
        return None
    best = Decimal(levels[0][0])
    remaining = target_notional
    executed_qty = Decimal("0")
    quote_spent = Decimal("0")
    for price_raw, qty_raw in levels:
        price, qty = Decimal(price_raw), Decimal(qty_raw)
        if price <= 0 or qty <= 0:
            continue
        available_notional = price * qty
        take_notional = min(remaining, available_notional)
        take_qty = take_notional / price
        quote_spent += take_notional
        executed_qty += take_qty
        remaining -= take_notional
        if remaining <= 0:
            break
    if remaining > 0 or executed_qty <= 0:
        return None
    average = quote_spent / executed_qty
    if side == "buy":
        return (average - best) / best * Decimal("10000")
    return (best - average) / best * Decimal("10000")

def basis_bps(spot_price: Decimal, futures_price: Decimal) -> Decimal:
    if spot_price <= 0:
        raise ValueError("spot price must be positive")
    return (futures_price - spot_price) / spot_price * Decimal("10000")
