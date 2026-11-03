"""Public-only Binance connectivity, clock, feed and market-data validation."""
import asyncio
import time
from decimal import Decimal, InvalidOperation
from typing import Any
import websockets
from app.exchange.binance.public_client import BinanceAPIError


class LiveValidationService:
    def __init__(self, client: Any, hub: Any, websocket_manager: Any, store: Any,
                 max_clock_drift_ms: int = 5000, max_message_age_ms: int = 15000,
                 websocket_probe_timeout: float = 5.0) -> None:
        self.client,self.hub,self.websocket_manager,self.store=client,hub,websocket_manager,store
        self.max_clock_drift_ms=max_clock_drift_ms
        self.max_message_age_ms=max_message_age_ms
        self.websocket_probe_timeout=websocket_probe_timeout
        self.last_clock: dict[str,Any]={}
        self.connectivity: dict[str,Any]={}
        self.funding: dict[str,Any]={}
        self.hedge: dict[str,Any]={"status":"NOT_RUN","reason":"No theoretical size supplied"}
        self.profitability: dict[str,Any]={"status":"NOT_RUN","reason":"No candidate supplied"}
    async def _check(self,name: str,call: Any) -> dict[str,Any]:
        start=time.perf_counter(); now=int(time.time()*1000)
        try:
            result=await call()
            row={"name":name,"status":"HEALTHY","checked_at_ms":now,
                 "latency_ms":round((time.perf_counter()-start)*1000,3),
                 "last_success_at_ms":int(time.time()*1000),"last_error":None,
                 "response_available":result is not None}
        except Exception as exc:
            row={"name":name,"status":"UNHEALTHY","checked_at_ms":now,
                 "latency_ms":round((time.perf_counter()-start)*1000,3),
                 "last_success_at_ms":None,"last_error":f"{type(exc).__name__}: {exc}",
                 "response_available":False}
        self.store.record("connectivity",name,now,row["status"],row)
        return row
    async def check_clock(self) -> dict[str,Any]:
        output={}
        for market,method in (("spot",self.client.spot_server_time),("futures",self.client.futures_server_time)):
            start=time.perf_counter(); before=int(time.time()*1000)
            try:
                response=await method(); after=int(time.time()*1000)
                server=int(response["serverTime"]); midpoint=(before+after)//2
                offset=server-midpoint; latency=(time.perf_counter()-start)*1000
                status="HEALTHY" if abs(offset)<=self.max_clock_drift_ms else "WARNING"
                self.hub.set_clock_offset(market,offset,after)
                output[market]={"status":status,"server_time_ms":server,"local_midpoint_ms":midpoint,
                    "offset_ms":offset,"request_latency_ms":round(latency,3),"checked_at_ms":after}
            except Exception as exc:
                output[market]={"status":"UNHEALTHY","checked_at_ms":int(time.time()*1000),
                    "error":f"{type(exc).__name__}: {exc}"}
        self.last_clock={"status":"HEALTHY" if all(x["status"]=="HEALTHY" for x in output.values()) else "WARNING",
                         "markets":output,"checked_at_ms":int(time.time()*1000)}
        self.store.record("clock","binance",self.last_clock["checked_at_ms"],self.last_clock["status"],self.last_clock)
        return self.last_clock
    async def _ws_probe(self,market: str,url: str,stream: str) -> dict[str,Any]:
        started=int(time.time()*1000); tick=time.perf_counter()
        endpoint=url.rstrip("/")+"/"+stream
        try:
            async with asyncio.timeout(self.websocket_probe_timeout):
                async with websockets.connect(endpoint,ping_interval=20,ping_timeout=20,close_timeout=2) as ws:
                    raw=await ws.recv()
            import json
            data=json.loads(raw)
            return {"name":market,"status":"HEALTHY","started_at_ms":started,
                    "checked_at_ms":int(time.time()*1000),"latency_ms":round((time.perf_counter()-tick)*1000,3),
                    "message_received":isinstance(data,dict),"error":None}
        except Exception as exc:
            return {"name":market,"status":"UNHEALTHY","started_at_ms":started,
                    "checked_at_ms":int(time.time()*1000),"latency_ms":round((time.perf_counter()-tick)*1000,3),
                    "message_received":False,"error":f"{type(exc).__name__}: {exc}"}
    async def validate(self) -> dict[str,Any]:
        checks=[
          ("spot_rest",self.client.spot_server_time),("futures_rest",self.client.futures_server_time),
          ("spot_exchange_metadata",self.client.spot_exchange_info),("futures_exchange_metadata",self.client.futures_exchange_info),
          ("futures_funding",self.client.futures_premium_index),
          ("spot_orderbook",lambda:self.client.spot_depth("BTCUSDT",5)),
          ("futures_orderbook",lambda:self.client.futures_depth("BTCUSDT",5))]
        rows=await asyncio.gather(*(self._check(n,c) for n,c in checks))
        spot_ws,futures_ws=await asyncio.gather(
            self._ws_probe("spot_websocket","wss://stream.binance.com:9443/ws","btcusdt@bookTicker"),
            self._ws_probe("futures_websocket","wss://fstream.binance.com/ws","btcusdt@bookTicker"))
        for row in (spot_ws,futures_ws):
            self.store.record("connectivity",row["name"],row["checked_at_ms"],row["status"],row)
        self.connectivity={"checked_at_ms":int(time.time()*1000),"checks":rows+[spot_ws,futures_ws],
            "status":"HEALTHY" if all(x["status"]=="HEALTHY" for x in rows+[spot_ws,futures_ws]) else "DEGRADED",
            "authentication":"PUBLIC_UNAUTHENTICATED"}
        await self.check_clock()
        return self.connectivity
    def websocket_health(self) -> dict[str,Any]:
        streams=self.hub.stream_health()
        if getattr(self.websocket_manager, 'metrics', None):
            streams['streams'] = {k: dict(value) for k, value in self.websocket_manager.metrics.items()}
        now=int(time.time()*1000)
        for market,item in streams.get("streams",{}).items():
            last_message=item.get("last_message_ms",item.get("last_event_ms"))
            age=None if last_message is None else max(0,now-int(last_message))
            item["last_message_age_ms"]=age
            item["current_uptime_ms"]=item.get("uptime_ms",0)+(max(0,now-self.websocket_manager._connected_since.get(market,now)) if item.get("connected") else 0)
            item["status"]="HEALTHY" if item.get("connected") and age is not None and age<=self.max_message_age_ms else "STALE"
            item["checked_at_ms"]=now
        return {"checked_at_ms":now,"status":"HEALTHY" if all(x.get("status")=="HEALTHY" for x in streams.get("streams",{}).values()) and streams.get("streams") else "WARNING",
                "streams":streams.get("streams",{}),"live_trading_enabled":False}
    def data_quality(self) -> dict[str,Any]:
        now=int(time.time()*1000)
        required=("spot_bid","spot_ask","spot_last","futures_bid","futures_ask",
                  "futures_mark","futures_index","funding_rate")
        symbols={}
        for symbol,state in self.hub.states.items():
            missing=[]; invalid=[]; stale=[]; ages={}
            for field in required:
                item=state.fields.get(field)
                if item is None:
                    missing.append(field); ages[field]=None; continue
                age=max(0,now-item.received_at_ms); ages[field]=age
                try:
                    value=Decimal(str(item.value))
                    if field!="funding_rate" and value<=0: invalid.append(field)
                except (InvalidOperation,ValueError,TypeError):
                    invalid.append(field)
                if age>self.max_message_age_ms: stale.append(field)
            status="INVALID" if invalid else "MISSING" if missing else "STALE" if stale else "HEALTHY"
            symbols[symbol]={"status":status,"missing_fields":missing,"invalid_fields":invalid,
                "stale_fields":stale,"field_age_ms":ages,"eligible":status=="HEALTHY",
                "checked_at_ms":now}
        return {"checked_at_ms":now,"symbols":symbols,"symbol_count":len(symbols),
            "eligible_symbol_count":sum(1 for x in symbols.values() if x["eligible"]),
            "status":"HEALTHY" if symbols and all(x["eligible"] for x in symbols.values()) else "WARNING"}
    async def validate_funding(self,symbol: str="BTCUSDT") -> dict[str,Any]:
        current=await self.client.futures_premium_index()
        item=next((x for x in current if x.get("symbol")==symbol.upper()),None)
        history=await self.client.futures_funding_history(symbol.upper(),limit=100)
        times=[int(x["fundingTime"]) for x in history]
        unique=sorted(set(times)); gaps=[]
        if len(unique)>1:
            gaps=[b-a for a,b in zip(unique,unique[1:]) if b-a>9*3600000]
        current_rate=None if item is None else str(item.get("lastFundingRate"))
        self.funding={"symbol":symbol.upper(),"status":"HEALTHY" if item and history and not gaps else "WARNING",
          "checked_at_ms":int(time.time()*1000),"current_observed_rate":current_rate,
          "next_funding_time_ms":None if item is None else item.get("nextFundingTime"),
          "historical_realized_count":len(history),"unique_settlement_count":len(unique),
          "duplicate_interval_count":len(times)-len(unique),"possible_gap_count":len(gaps),
          "gap_durations_ms":gaps,"estimated_next_rate":"NOT_PREDICTED",
          "actual_future_funding":"UNKNOWN_UNTIL_REALIZED"}
        self.store.record("funding",symbol.upper(),self.funding["checked_at_ms"],self.funding["status"],self.funding)
        return self.funding

    async def validate_hedge(self, symbol: str = "BTCUSDT") -> dict[str,Any]:
        from app.domain.exchange_filters import SymbolFilters, matched_valid_hedge
        spot_info, futures_info = await asyncio.gather(
            self.client.spot_exchange_info(), self.client.futures_exchange_info())
        spot_raw = next((x for x in spot_info.get("symbols", []) if x.get("symbol") == symbol.upper()), None)
        futures_raw = next((x for x in futures_info.get("symbols", []) if x.get("symbol") == symbol.upper()), None)
        if not spot_raw or not futures_raw:
            self.hedge = {"symbol":symbol.upper(),"status":"INVALID","reason":"Exchange metadata missing"}
            return self.hedge
        spot_book, futures_book = await asyncio.gather(
            self.client.spot_depth(symbol.upper(), 5), self.client.futures_depth(symbol.upper(), 5))
        spot_price = Decimal(str(spot_book["asks"][0][0]))
        futures_price = Decimal(str(futures_book["bids"][0][0]))
        spot_filter = SymbolFilters.from_exchange_info(spot_raw,"spot")
        futures_filter = SymbolFilters.from_exchange_info(futures_raw,"futures")
        scenarios={}
        for label,notional in (("small",Decimal("100")),("medium",Decimal("1000")),("large",Decimal("10000"))):
            result=matched_valid_hedge(notional,spot_price,futures_price,spot_filter,futures_filter)
            scenarios[label]={k:(str(val) if isinstance(val,Decimal) else list(val) if isinstance(val,tuple) else val)
                for k,val in result.items()}
        self.hedge={"symbol":symbol.upper(),"status":"COMPLETED","checked_at_ms":int(time.time()*1000),
            "spot_status":spot_filter.status,"futures_status":futures_filter.status,
            "scenarios":scenarios,"execution":"THEORETICAL_ONLY — no orders placed"}
        self.store.record("hedge",symbol.upper(),self.hedge["checked_at_ms"],"COMPLETED",self.hedge)
        return self.hedge
    def validate_profitability(self, notional: str = "1000") -> dict[str,Any]:
        from app.domain.profitability import CostModel
        from app.domain.sensitivity import profitability_scenarios
        amount=Decimal(notional)
        rate=Decimal(str(self.funding.get("current_observed_rate") or "0"))
        fees=CostModel(Decimal("0.001"),Decimal("0.0005"),Decimal("0.001"),Decimal("0.0005"),
                       Decimal("0.0002"),Decimal("0.0002"))
        results=profitability_scenarios(amount,rate,fees,Decimal("0.0001"))
        self.profitability={"status":"ESTIMATES_ONLY","checked_at_ms":int(time.time()*1000),
            "symbol":self.funding.get("symbol"),"notional_usdt":str(amount),
            "observed_funding_rate":str(rate),"scenarios":results,
            "disclaimer":"Hypothetical estimates, not realized profit or guaranteed funding."}
        self.store.record("profitability","sensitivity",self.profitability["checked_at_ms"],
                          "ESTIMATES_ONLY",self.profitability)
        return self.profitability

    def summary(self) -> dict[str,Any]:
        quality=self.data_quality(); ws=self.websocket_health()
        return {"checked_at_ms":int(time.time()*1000),
          "status":"HEALTHY" if self.connectivity.get("status")=="HEALTHY" and quality["status"]=="HEALTHY" and ws["status"]=="HEALTHY" else "WARNING",
          "connectivity":self.connectivity.get("status","NOT_RUN"),"websocket":ws["status"],
          "clock":self.last_clock.get("status","NOT_RUN"),"data_quality":quality["status"],
          "funding":self.funding.get("status","NOT_RUN"),"read_only":True,
          "actual_trading_performance":"NONE — no orders or account endpoints"}
