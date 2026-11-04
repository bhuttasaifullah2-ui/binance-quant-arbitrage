from app.core.config import Settings


def test_all_sensitive_modes_default_disabled(monkeypatch):
    for name in ("WEBSOCKET_ENABLED", "LIVE_VALIDATION_ENABLED",
                 "OBSERVATION_MODE", "LIVE_TRADING_ENABLED"):
        monkeypatch.delenv(name, raising=False)
    settings = Settings(_env_file=None)
    assert settings.websocket_enabled is False
    assert settings.live_validation_enabled is False
    assert settings.observation_mode is False
    assert settings.live_trading_enabled is False
