from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    app_env: str = "development"
    app_mode: str = "RESEARCH"
    binance_spot_base_url: str = "https://api.binance.com"
    binance_futures_base_url: str = "https://fapi.binance.com"
    binance_spot_ws_url: str = "wss://stream.binance.com:9443/stream"
    binance_futures_ws_url: str = "wss://fstream.binance.com/stream"
    request_timeout_seconds: float = 10.0
    request_max_retries: int = 4
    retry_base_seconds: float = 0.4
    scan_interval_seconds: int = 60
    max_data_age_seconds: int = 15
    min_spot_quote_volume_usdt: str = "10000000"
    min_futures_quote_volume_usdt: str = "10000000"
    max_spot_spread_bps: str = "8"
    max_futures_spread_bps: str = "8"
    trade_notional_usdt: str = "1000"
    spot_taker_fee_rate: str = "0.001"
    futures_taker_fee_rate: str = "0.0005"
    max_depth_symbols_per_cycle: int = 30
    max_history_symbols_per_cycle: int = 30
    orderbook_limit: int = 20
    depth_levels: int = 20
    history_limit: int = 100
    max_concurrent_requests: int = 8
    min_net_edge_usdt: str = "0"
    basis_risk_haircut: str = "0.25"
    database_path: str = "data/market_data.sqlite3"
    retention_days: int = 90
    websocket_enabled: bool = False
    live_trading_enabled: bool = False
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

settings = Settings()
