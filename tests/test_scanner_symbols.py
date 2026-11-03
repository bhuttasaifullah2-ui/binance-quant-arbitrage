from app.services.market_scanner import _eligible_spot, _eligible_futures

def test_matching_requires_trading_usdt_spot_and_perpetual():
    spot = {'symbols':[
        {'symbol':'BTCUSDT','status':'TRADING','quoteAsset':'USDT','isSpotTradingAllowed':True},
        {'symbol':'OLDUSDT','status':'BREAK','quoteAsset':'USDT','isSpotTradingAllowed':True},
        {'symbol':'ETHBTC','status':'TRADING','quoteAsset':'BTC','isSpotTradingAllowed':True}]}
    futures = {'symbols':[
        {'symbol':'BTCUSDT','status':'TRADING','quoteAsset':'USDT','contractType':'PERPETUAL'},
        {'symbol':'BTCUSDT_240628','status':'TRADING','quoteAsset':'USDT','contractType':'CURRENT_QUARTER'}]}
    assert _eligible_spot(spot) & _eligible_futures(futures) == {'BTCUSDT'}
