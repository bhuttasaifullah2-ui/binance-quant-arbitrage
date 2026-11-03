from decimal import Decimal, ROUND_DOWN

ZERO = Decimal("0")
ONE = Decimal("1")
BPS = Decimal("10000")

def D(value: str | int | Decimal) -> Decimal:
    # Never pass binary floats into money calculations.
    if isinstance(value, float):
        raise TypeError("float is prohibited for financial calculations")
    return value if isinstance(value, Decimal) else Decimal(str(value))

def quantize_step(quantity: Decimal, step: Decimal) -> Decimal:
    if step <= ZERO:
        raise ValueError("step must be positive")
    return (quantity / step).to_integral_value(rounding=ROUND_DOWN) * step
