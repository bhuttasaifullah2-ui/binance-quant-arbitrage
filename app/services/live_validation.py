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
        now=int(time.time()*1000)
        for market,item in streams.get("streams",{}).items():
            age=item.get("last_event_age_ms")
            item["status"]="HEALTHY" if item.get("connected") and age is not None and age<=self.max_message_age_ms else "STALE"
            item["checked_at_ms"]=now
        return {"checked_at_ms":now,"status":"HEALTHY" if all(x.get("status")=="HEALTHY" for x in streams.get("streams",{}).values()) and streams.get("streams") else "WARNING",
                "streams":streams.get("streams",{}),"live_trading_enabled":False}
    def data_quality(self) -> dict[str,Any]:
        raw=self.hub.data_quality(max_age_ms=self.max_message_age_ms)
        now=int(time.time()*1000); fields=("spot_bid","spot_ask","spot_last","futures_bid","futures_ask","futures_mark","futures_index","funding_rate")
        symbols={}
        for symbol,values in raw.get("symbols",{}).items():
            missing=[]; invalid=[]; stale=[]
            for field in fields:
                item=values.get(field,{})
                if not item or item.get("value") is None: missing.append(field); continue
                try:
                    if field.endswith("time_ms"): continue
                    if Decimal(str(item["value"]))<=0 and field!="funding_rate": invalid.append(field)
                except (InvalidOperation,ValueError,TypeError): invalid.append(field)
                age=item.get("age_ms")
                if age is not None and age>self.max_message_age_ms: stale.append(field)
            status="INVALID" if invalid else "MISSING" if missing else "STALE" if stale else "HEALTHY"
            symbols[symbol]={"status":status,"missing_fields":missing,"invalid_fields":invalid,
                             "stale_fields":stale,"checked_at_ms":now,"eligible":status=="HEALTHY"}
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
    def summary(self) -> dict[str,Any]:
        quality=self.data_quality(); ws=self.websocket_health()
        return {"checked_at_ms":int(time.time()*1000),
          "status":"HEALTHY" if self.connectivity.get("status")=="HEALTHY" and quality["status"]=="HEALTHY" and ws["status"]=="HEALTHY" else "WARNING",
          "connectivity":self.connectivity.get("status","NOT_RUN"),"websocket":ws["status"],
          "clock":self.last_clock.get("status","NOT_RUN"),"data_quality":quality["status"],
          "funding":self.funding.get("status","NOT_RUN"),"read_only":True,
          "actual_trading_performance":"NONE — no orders or account endpoints"}
