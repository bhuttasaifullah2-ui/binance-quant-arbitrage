from decimal import Decimal
from app.core.decimal_utils import quantize_step

def matched_quantities(notional: Decimal, spot_price: Decimal, futures_price: Decimal,
                       spot_step: Decimal, futures_step: Decimal) -> tuple[Decimal, Decimal]:
    if min(notional, spot_price, futures_price) <= 0:
        raise ValueError("notional and prices must be positive")
    spot_qty = quantize_step(notional / spot_price, spot_step)
    futures_qty = quantize_step(notional / futures_price, futures_step)
    return spot_qty, futures_qty

def hedge_mismatch_fraction(spot_notional: Decimal, futures_notional: Decimal) -> Decimal:
    denominator = max(spot_notional, futures_notional)
    if denominator <= 0:
        return Decimal("0")
    return abs(spot_notional - futures_notional) / denominator
