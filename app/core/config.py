from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    app_env: str = "development"
    app_mode: str = "RESEARCH"
    binance_spot_base_url: str = "https://api.binance.com"
    binance_futures_base_url: str = "https://fapi.binance.com"
    request_timeout_seconds: float = 10.0
    max_data_age_seconds: int = 15
    min_24h_quote_volume_usdt: str = "10000000"
    max_spread_bps: str = "5"
    min_net_edge_usdt: str = "0"
    live_trading_enabled: bool = False
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

settings = Settings()
