"""Decimal-only parsing and validation for Binance Spot/USD-M symbol filters."""
from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from typing import Any

ZERO = Decimal("0")


@dataclass(frozen=True)
class SymbolFilters:
    symbol: str
    market: str
    status: str
    base_asset: str
    quote_asset: str
    step_size: Decimal
    min_qty: Decimal
    max_qty: Decimal
    market_step_size: Decimal
    market_min_qty: Decimal
    market_max_qty: Decimal
    tick_size: Decimal
    min_price: Decimal
    max_price: Decimal
    min_notional: Decimal
    max_notional: Decimal
    apply_min_notional_to_market: bool
    contract_size: Decimal = Decimal("1")

    @classmethod
    def from_exchange_info(cls, symbol_info: dict[str, Any], market: str) -> "SymbolFilters":
        if market not in {"spot", "futures"}:
            raise ValueError("market must be spot or futures")
        filters = {x.get("filterType"): x for x in symbol_info.get("filters", [])}
        lot = filters.get("LOT_SIZE", {})
        market_lot = filters.get("MARKET_LOT_SIZE", lot)
        price = filters.get("PRICE_FILTER", {})
        notional = filters.get("NOTIONAL") or filters.get("MIN_NOTIONAL") or {}
        min_notional = notional.get("minNotional", notional.get("notional", "0"))
        max_notional = notional.get("maxNotional", "0")
        apply_min = notional.get("applyToMarket", notional.get("applyMinToMarket", True))
        return cls(
            symbol=str(symbol_info["symbol"]), market=market,
            status=str(symbol_info.get("status", "UNKNOWN")),
            base_asset=str(symbol_info.get("baseAsset", "")),
            quote_asset=str(symbol_info.get("quoteAsset", "")),
            step_size=Decimal(str(lot.get("stepSize", "0"))),
            min_qty=Decimal(str(lot.get("minQty", "0"))),
            max_qty=Decimal(str(lot.get("maxQty", "0"))),
            market_step_size=Decimal(str(market_lot.get("stepSize", lot.get("stepSize", "0")))),
            market_min_qty=Decimal(str(market_lot.get("minQty", lot.get("minQty", "0")))),
            market_max_qty=Decimal(str(market_lot.get("maxQty", lot.get("maxQty", "0")))),
            tick_size=Decimal(str(price.get("tickSize", "0"))),
            min_price=Decimal(str(price.get("minPrice", "0"))),
            max_price=Decimal(str(price.get("maxPrice", "0"))),
            min_notional=Decimal(str(min_notional)),
            max_notional=Decimal(str(max_notional)),
            apply_min_notional_to_market=bool(apply_min),
            contract_size=Decimal(str(symbol_info.get("contractSize", "1"))),
        )

    def round_quantity(self, quantity: Decimal, market_order: bool = True) -> Decimal:
        step = self.market_step_size if market_order else self.step_size
        if quantity < ZERO or step <= ZERO:
            raise ValueError("quantity must be non-negative and exchange step size positive")
        return (quantity / step).to_integral_value(rounding=ROUND_DOWN) * step

    def round_price(self, price: Decimal, rounding: str = "down") -> Decimal:
        if price <= ZERO or self.tick_size <= ZERO:
            raise ValueError("price and tick size must be positive")
        mode = ROUND_DOWN if rounding == "down" else ROUND_UP if rounding == "up" else None
        if mode is None:
            raise ValueError("rounding must be 'down' or 'up'")
        return (price / self.tick_size).to_integral_value(rounding=mode) * self.tick_size

    def validate(self, quantity: Decimal, price: Decimal, market_order: bool = True) -> tuple[str, ...]:
        reasons: list[str] = []
        step = self.market_step_size if market_order else self.step_size
        min_qty = self.market_min_qty if market_order else self.min_qty
        max_qty = self.market_max_qty if market_order else self.max_qty
        if self.status != "TRADING":
            reasons.append("symbol is not TRADING")
        if quantity <= ZERO:
            reasons.append("quantity must be positive")
        if step <= ZERO or quantity % step != ZERO:
            reasons.append("quantity does not conform to step size")
        if quantity < min_qty:
            reasons.append("quantity below minimum quantity")
        if max_qty > ZERO and quantity > max_qty:
            reasons.append("quantity above maximum quantity")
        if price <= ZERO:
            reasons.append("price must be positive")
        if self.tick_size > ZERO and price % self.tick_size != ZERO:
            reasons.append("price does not conform to tick size")
        if self.min_price > ZERO and price < self.min_price:
            reasons.append("price below minimum price")
        if self.max_price > ZERO and price > self.max_price:
            reasons.append("price above maximum price")
        notional = quantity * price * self.contract_size
        if self.min_notional > ZERO and (not market_order or self.apply_min_notional_to_market) and notional < self.min_notional:
            reasons.append("notional below minimum notional")
        if self.max_notional > ZERO and notional > self.max_notional:
            reasons.append("notional above maximum notional")
        return tuple(reasons)


def matched_valid_hedge(notional: Decimal, spot_price: Decimal, futures_price: Decimal,
                        spot: SymbolFilters, futures: SymbolFilters,
                        max_mismatch: Decimal = Decimal("0.005")) -> dict[str, Any]:
    if notional <= ZERO or spot_price <= ZERO or futures_price <= ZERO:
        raise ValueError("notional and prices must be positive")
    spot_qty = spot.round_quantity(notional / spot_price)
    futures_qty = futures.round_quantity(notional / (futures_price * futures.contract_size))
    spot_reasons = spot.validate(spot_qty, spot_price)
    futures_reasons = futures.validate(futures_qty, futures_price)
    spot_notional = spot_qty * spot_price * spot.contract_size
    futures_notional = futures_qty * futures_price * futures.contract_size
    denominator = max(spot_notional, futures_notional)
    mismatch = abs(spot_notional - futures_notional) / denominator if denominator else Decimal("1")
    reasons = [f"Spot: {x}" for x in spot_reasons] + [f"Futures: {x}" for x in futures_reasons]
    if mismatch > max_mismatch:
        reasons.append("hedge mismatch exceeds configured maximum")
    return {
        "spot_quantity": spot_qty, "futures_quantity": futures_qty,
        "spot_notional": spot_notional, "futures_notional": futures_notional,
        "hedge_ratio": futures_notional / spot_notional if spot_notional else ZERO,
        "residual_directional_exposure": spot_notional - futures_notional,
        "mismatch_fraction": mismatch, "executable": not reasons, "reasons": tuple(reasons),
    }
