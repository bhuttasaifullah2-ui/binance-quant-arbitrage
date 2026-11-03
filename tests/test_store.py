from app.infrastructure.sqlite_store import SQLiteMarketStore

def test_store_deduplicates_market_and_funding_rows(tmp_path):
    store = SQLiteMarketStore(str(tmp_path / 'market.sqlite'), retention_days=90)
    row = {'symbol':'BTCUSDT','observed_at_ms':1000,'funding_rate':'0.0001','source':'test'}
    assert store.save_observation(row)
    assert not store.save_observation(row)
    funding = [{'fundingTime':800,'fundingRate':'0.0002'}]
    assert store.save_funding_observations('BTCUSDT', funding, 1000) == 1
    assert store.save_funding_observations('BTCUSDT', funding, 1000) == 0
    assert store.funding_history('BTCUSDT')[0]['funding_rate'] == '0.0002'
