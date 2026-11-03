import time
from decimal import Decimal
from app.domain.sensitivity import profitability_scenarios
from app.domain.profitability import CostModel
from app.infrastructure.validation_store import ValidationStore
from app.services.live_validation import LiveValidationService
from app.services.market_data_hub import MarketDataHub


def test_sensitivity_scenarios_are_decimal_estimates():
    model=CostModel(*(Decimal("0.001") for _ in range(4)),
                    entry_slippage_rate=Decimal("0.0001"),exit_slippage_rate=Decimal("0.0001"))
    rows=profitability_scenarios(Decimal("1000"),Decimal("0.001"),model)
    assert len(rows)==8
    assert rows[0]["scenario"]=="normal"
    assert Decimal(rows[3]["gross_funding_estimate"])==0
    assert Decimal(rows[4]["gross_funding_estimate"])<0
    assert all("not realized PnL" in row["risk_warnings"][0] for row in rows)


def test_validation_store_survives_reopen_and_reports_events(tmp_path):
    path=str(tmp_path/"validation.sqlite3")
    store=ValidationStore(path,retention_days=30)
    store.event(1000,"WEBSOCKET_DISCONNECTED","BTCUSDT","UNHEALTHY",{"market":"spot"})
    store.record("clock","spot",1100,"WARNING",{"offset_ms":7000})
    reopened=ValidationStore(path,retention_days=30)
    assert reopened.events()[0]["event_type"]=="WEBSOCKET_DISCONNECTED"
    assert reopened.latest("clock")[0]["payload"]["offset_ms"]==7000
    assert reopened.report()["event_counts"]["WEBSOCKET_DISCONNECTED"]==1


def test_data_quality_distinguishes_missing_invalid_and_stale():
    hub=MarketDataHub()
    now=int(time.time()*1000)
    hub.apply_rest("BTCUSDT","spot:test",{"spot_bid":"0","spot_ask":"10","spot_last":"9"},now,now)
    class Store:
        def record(self,*args): pass
    class Manager:
        metrics={}
    class Client: pass
    service=LiveValidationService(Client(),hub,Manager(),Store(),max_message_age_ms=1000)
    result=service.data_quality()
    assert result["symbols"]["BTCUSDT"]["status"]=="INVALID"
    assert "futures_bid" in result["symbols"]["BTCUSDT"]["missing_fields"]
