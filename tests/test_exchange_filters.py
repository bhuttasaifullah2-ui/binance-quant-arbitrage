from decimal import Decimal as D
from app.domain.exchange_filters import SymbolFilters, matched_valid_hedge


def _meta(symbol="BTCUSDT", market=True):
    return {"symbol": symbol, "status": "TRADING", "baseAsset": "BTC", "quoteAsset": "USDT",
            "marginAsset": "USDT", "contractType": "PERPETUAL",
            "filters": [
                {"filterType": "PRICE_FILTER", "minPrice": "0.01", "maxPrice": "1000000", "tickSize": "0.01"},
                {"filterType": "LOT_SIZE", "minQty": "0.001", "maxQty": "100", "stepSize": "0.001"},
                {"filterType": "MARKET_LOT_SIZE", "minQty": "0.001", "maxQty": "100", "stepSize": "0.001"},
                {"filterType": "MIN_NOTIONAL", "minNotional": "5", "applyToMarket": market}
            ]}


def test_quantity_and_price_rounding_are_decimal_and_valid():
    f = SymbolFilters.from_exchange_info(_meta(), "spot")
    assert f.round_quantity(D("0.12399")) == D("0.123")
    assert f.round_price(D("100.019"), "down") == D("100.01")
    assert f.round_price(D("100.011"), "up") == D("100.02")
    assert f.validate(D("0.123"), D("100.01")) == ()


def test_min_notional_and_step_mismatch_are_rejected():
    f = SymbolFilters.from_exchange_info(_meta(), "spot")
    reasons = f.validate(D("0.0015"), D("100"))
    assert "quantity does not conform to step size" in reasons
    assert "notional below minimum notional" in reasons


def test_hedge_returns_actual_quantities_and_reasons():
    spot = SymbolFilters.from_exchange_info(_meta(), "spot")
    futures = SymbolFilters.from_exchange_info(_meta(), "futures")
    result = matched_valid_hedge(D("1000"), D("60000"), D("60010"), spot, futures, D("0.005"))
    assert result["spot_quantity"] == D("0.016")
    assert result["futures_quantity"] == D("0.016")
    assert result["executable"]


def test_hedge_rejects_same_ticker_with_incompatible_base_asset():
    spot = SymbolFilters.from_exchange_info(_meta(), "spot")
    futures_meta = _meta()
    futures_meta["baseAsset"] = "WBTC"
    futures = SymbolFilters.from_exchange_info(futures_meta, "futures")
    result = matched_valid_hedge(D("1000"), D("60000"), D("60010"), spot, futures)
    assert not result["executable"]
    assert "Spot and Futures base assets do not match" in result["reasons"]


def test_hedge_rejects_missing_usdm_margin_asset_metadata():
    spot = SymbolFilters.from_exchange_info(_meta(), "spot")
    futures_meta = _meta()
    futures_meta.pop("marginAsset")
    futures = SymbolFilters.from_exchange_info(futures_meta, "futures")
    result = matched_valid_hedge(D("1000"), D("60000"), D("60010"), spot, futures)
    assert not result["executable"]
    assert "USD-M Futures margin asset does not match the hedge quote asset" in result["reasons"]


def test_contract_size_normalizes_futures_quantity():
    spot_meta = _meta()
    futures_meta = _meta()
    futures_meta["contractSize"] = "10"
    spot = SymbolFilters.from_exchange_info(spot_meta, "spot")
    futures = SymbolFilters.from_exchange_info(futures_meta, "futures")
    result = matched_valid_hedge(D("1000"), D("100"), D("100"), spot, futures, D("0.005"))
    assert result["spot_quantity"] == D("10")
    assert result["futures_quantity"] == D("1")
    assert result["spot_notional"] == D("1000")
    assert result["futures_notional"] == D("1000")
    assert result["executable"]
