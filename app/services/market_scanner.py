import asyncio
import logging
import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from app.domain.funding import summarize_funding, detect_funding_interval_hours
from app.domain.market_quality import estimate_market_slippage, basis_bps
from app.domain.models import FundingObservation
from app.domain.profitability import CostModel, estimate_net_funding
from app.domain.ranking import score_opportunity, RankingWeights
from app.exchange.binance.public_client import BinancePublicClient
from app.infrastructure.sqlite_store import SQLiteMarketStore

logger = logging.getLogger(__name__)

def _d(value: Any) -> Decimal:
    if value is None:
        raise ValueError("missing numeric market field")
    return Decimal(str(value))

def _eligible_spot(info: dict[str, Any]) -> set[str]:
    return {s["symbol"] for s in info.get("symbols", [])
            if s.get("status") == "TRADING" and s.get("quoteAsset") == "USDT"
            and s.get("isSpotTradingAllowed", True)}

def _eligible_futures(info: dict[str, Any]) -> set[str]:
    return {s["symbol"] for s in info.get("symbols", [])
            if s.get("status") == "TRADING" and s.get("quoteAsset") == "USDT"
            and s.get("contractType") == "PERPETUAL"}

class MarketScanner:
    def __init__(self, client: BinancePublicClient, store: SQLiteMarketStore, config: Any):
        self.client, self.store, self.config = client, store, config
        self._lock = asyncio.Lock()
        self.last_scan_ms: int | None = None
        self.last_error: str | None = None
        self.last_pair_count = 0
        self.last_valid_count = 0
        self._opportunities: list[dict[str, Any]] = []
        self._running = False

    @property
    def opportunities(self) -> list[dict[str, Any]]:
        return list(self._opportunities)

    @property
    def fresh_opportunities(self) -> list[dict[str, Any]]:
        if self.last_scan_ms is None:
            return []
        age_ms = int(time.time() * 1000) - self.last_scan_ms
        if age_ms > self.config.max_data_age_seconds * 1000:
            return []
        return list(self._opportunities)

    async def scan_once(self) -> list[dict[str, Any]]:
        async with self._lock:
            started = int(time.time() * 1000)
            try:
                spot_info, fut_info, spot_tickers, spot_books, fut_tickers, fut_books, premium = await asyncio.gather(
                    self.client.spot_exchange_info(), self.client.futures_exchange_info(),
                    self.client.spot_ticker_24h(), self.client.spot_book_ticker(),
                    self.client.futures_ticker_24h(), self.client.futures_book_ticker(),
                    self.client.futures_premium_index())
                spot_symbols, fut_symbols = _eligible_spot(spot_info), _eligible_futures(fut_info)
                common = sorted(spot_symbols & fut_symbols)
                self.last_pair_count = len(common)
                spot_t = {x["symbol"]: x for x in spot_tickers if x.get("symbol") in common}
                spot_b = {x["symbol"]: x for x in spot_books if x.get("symbol") in common}
                fut_t = {x["symbol"]: x for x in fut_tickers if x.get("symbol") in common}
                fut_b = {x["symbol"]: x for x in fut_books if x.get("symbol") in common}
                prem = {x["symbol"]: x for x in premium if x.get("symbol") in common}
                # Fetch detailed depth/history only for a bounded shortlist, avoiding a request storm.
                prelim = []
                for symbol in common:
                    try:
                        st, sb, ft, fb, pi = spot_t[symbol], spot_b[symbol], fut_t[symbol], fut_b[symbol], prem[symbol]
                        if any(k not in sb for k in ("bidPrice","askPrice")) or any(k not in fb for k in ("bidPrice","askPrice")):
                            continue
                        funding = _d(pi.get("lastFundingRate"))
                        sv, fv = _d(st.get("quoteVolume")), _d(ft.get("quoteVolume"))
                        if sv < _d(self.config.min_spot_quote_volume_usdt) or fv < _d(self.config.min_futures_quote_volume_usdt):
                            continue
                        spread_s = ( _d(sb["askPrice"]) - _d(sb["bidPrice"]) ) / ((_d(sb["askPrice"])+_d(sb["bidPrice"]))/2) * Decimal(10000)
                        spread_f = ( _d(fb["askPrice"]) - _d(fb["bidPrice"]) ) / ((_d(fb["askPrice"])+_d(fb["bidPrice"]))/2) * Decimal(10000)
                        if spread_s > _d(self.config.max_spot_spread_bps) or spread_f > _d(self.config.max_futures_spread_bps):
                            continue
                        prelim.append((symbol, funding, st, sb, ft, fb, pi, sv, fv))
                    except (KeyError, ValueError, ArithmeticError):
                        logger.warning("Skipping malformed preliminary market data for %s", symbol)
                prelim.sort(key=lambda x: x[1], reverse=True)
                detailed = prelim[:self.config.max_depth_symbols_per_cycle]
                semaphore = asyncio.Semaphore(self.config.max_concurrent_requests)
                async def enrich(item):
                    symbol=item[0]
                    async with semaphore:
                        return await asyncio.gather(
                            self.client.spot_depth(symbol, self.config.orderbook_limit),
                            self.client.futures_depth(symbol, self.config.orderbook_limit),
                            self.client.futures_open_interest(symbol),
                            self.client.futures_funding_history(symbol, self.config.history_limit))
                results = await asyncio.gather(*(enrich(x) for x in detailed), return_exceptions=True)
                out=[]
                for item, result in zip(detailed, results):
                    symbol,funding,st,sb,ft,fb,pi,sv,fv=item
                    if isinstance(result, BaseException):
                        logger.warning("Detailed data unavailable for %s: %s",symbol,type(result).__name__)
                        continue
                    sd,fd,oi,hist=result
                    now=int(time.time()*1000)
                    try:
                        if not sd.get("bids") or not sd.get("asks") or not fd.get("bids") or not fd.get("asks"):
                            continue
                        spot_ask=_d(sb["askPrice"]); spot_bid=_d(sb["bidPrice"])
                        fut_ask=_d(fb["askPrice"]); fut_bid=_d(fb["bidPrice"])
                        mark=_d(pi["markPrice"]); index=_d(pi["indexPrice"])
                        next_funding=int(pi["nextFundingTime"])
                        notional=_d(self.config.trade_notional_usdt)
                        spot_buy_slip=estimate_market_slippage(sd["asks"],notional,"buy")
                        spot_sell_slip=estimate_market_slippage(sd["bids"],notional,"sell")
                        fut_sell_slip=estimate_market_slippage(fd["bids"],notional,"sell")
                        fut_buy_slip=estimate_market_slippage(fd["asks"],notional,"buy")
                        slips=[x for x in [spot_buy_slip,spot_sell_slip,fut_sell_slip,fut_buy_slip] if x is not None]
                        if len(slips)!=4:
                            continue
                        slip_rate=sum(slips,Decimal("0"))/Decimal("10000")
                        costs=CostModel(_d(self.config.spot_taker_fee_rate),_d(self.config.futures_taker_fee_rate),
                                        _d(self.config.spot_taker_fee_rate),_d(self.config.futures_taker_fee_rate),
                                        slip_rate/4,slip_rate/4)
                        profit=estimate_net_funding(notional,funding,costs)
                        funding_obs=[FundingObservation(symbol,_d(x["fundingRate"]),int(x["fundingTime"]))
                                     for x in hist if "fundingRate" in x and "fundingTime" in x]
                        stats=summarize_funding(funding_obs)
                        ordered_funding = sorted(funding_obs, key=lambda x: x.funding_time_ms)
                        funding_trend_delta = (ordered_funding[-1].funding_rate - ordered_funding[0].funding_rate) if len(ordered_funding) >= 2 else None
                        self.store.save_funding_observations(symbol, hist, now)
                        funding_interval = detect_funding_interval_hours([x.funding_time_ms for x in funding_obs])
                        spot_depth=sum((_d(p)*_d(q) for p,q in sd["bids"][:self.config.depth_levels]),Decimal("0")) + sum((_d(p)*_d(q) for p,q in sd["asks"][:self.config.depth_levels]),Decimal("0"))
                        fut_depth=sum((_d(p)*_d(q) for p,q in fd["bids"][:self.config.depth_levels]),Decimal("0")) + sum((_d(p)*_d(q) for p,q in fd["asks"][:self.config.depth_levels]),Decimal("0"))
                        bps=basis_bps(spot_ask,mark)
                        data_age=Decimal("0")
                        weight=Decimal("1")-_d(self.config.basis_risk_haircut)
                        # Conservative basis haircut is a ranking penalty, not an assumed cash gain.
                        net=profit.expected_net - abs(bps)/Decimal("10000")*notional*_d(self.config.basis_risk_haircut)
                        weights=RankingWeights(
                            net_edge=_d(self.config.score_weight_net_edge),
                            funding_consistency=_d(self.config.score_weight_funding_consistency),
                            liquidity=_d(self.config.score_weight_liquidity),
                            spread=_d(self.config.score_weight_spread),
                            slippage=_d(self.config.score_weight_slippage),
                            basis=_d(self.config.score_weight_basis),
                            freshness=_d(self.config.score_weight_freshness),
                            funding_volatility=_d(self.config.score_weight_funding_volatility))
                        score=score_opportunity(net_edge_usdt=net,
                            funding_positive_fraction=stats["positive_fraction"],funding_volatility=stats["volatility"],
                            spot_depth_usdt=spot_depth,futures_depth_usdt=fut_depth,
                            spot_spread_bps=(spot_ask-spot_bid)/((spot_ask+spot_bid)/2)*Decimal("10000"),
                            futures_spread_bps=(fut_ask-fut_bid)/((fut_ask+fut_bid)/2)*Decimal("10000"),
                            estimated_slippage_bps=sum(slips,Decimal("0")),basis_bps_value=bps,
                            data_age_seconds=data_age,max_data_age_seconds=Decimal(self.config.max_data_age_seconds),
                            min_depth_usdt=notional,max_spread_bps=_d(self.config.ranking_max_spread_bps),
                            max_slippage_bps=_d(self.config.ranking_max_slippage_bps),
                            max_basis_bps=_d(self.config.ranking_max_basis_bps),weights=weights)
                        if next_funding <= now or stats["count"] == 0 or net <= _d(self.config.min_net_edge_usdt):
                            continue
                        row={"symbol":symbol,"spot_bid":spot_bid,"spot_ask":spot_ask,"spot_last":_d(st["lastPrice"]),
                            "spot_quote_volume":sv,"futures_bid":fut_bid,"futures_ask":fut_ask,
                            "futures_mark":mark,"futures_index":index,"futures_quote_volume":fv,
                            "funding_rate":funding,"next_funding_time_ms":next_funding,
                            "open_interest":_d(oi["openInterest"]),"spot_depth_usdt":spot_depth,
                            "futures_depth_usdt":fut_depth,"spot_slippage_bps":spot_buy_slip+spot_sell_slip,
                            "futures_slippage_bps":fut_sell_slip+fut_buy_slip,"basis_bps":bps,
                            "observed_at_ms":now,"source":"binance_rest"}
                        self.store.save_observation(row)
                        spot_qty_theoretical = notional / spot_ask
                        futures_qty_theoretical = notional / mark
                        spot_exposure = spot_qty_theoretical * spot_ask
                        futures_exposure = futures_qty_theoretical * mark
                        hedge_ratio = futures_exposure / spot_exposure if spot_exposure else Decimal("0")
                        break_even_rate = (profit.entry_fees + profit.exit_fees + profit.slippage) / notional
                        out.append({"symbol":symbol,"score":str(score),"funding_rate":str(funding),
                            "funding_history":{k:str(v) for k,v in stats.items()},
                            "funding_trend_delta":None if funding_trend_delta is None else str(funding_trend_delta),
                            "next_funding_time_ms":next_funding,"funding_interval_hours":funding_interval,
                            "spot_price":str(spot_ask),"futures_mark":str(mark),
                            "spot_volume_24h_usdt":str(sv),"futures_volume_24h_usdt":str(fv),
                            "spot_spread_bps":str((spot_ask-spot_bid)/((spot_ask+spot_bid)/2)*Decimal("10000")),
                            "futures_spread_bps":str((fut_ask-fut_bid)/((fut_ask+fut_bid)/2)*Decimal("10000")),
                            "spot_depth_usdt":str(spot_depth),"futures_depth_usdt":str(fut_depth),
                            "estimated_slippage_bps":str(sum(slips,Decimal("0"))),
                            "basis_bps":str(bps),"entry_basis_bps":str(bps),
                            "estimated_exit_basis_bps_assumption":str(bps),
                            "spot_quantity_theoretical":str(spot_qty_theoretical),
                            "futures_quantity_theoretical":str(futures_qty_theoretical),
                            "hedge_ratio":str(hedge_ratio),
                            "break_even_funding_rate":str(break_even_rate),
                            "expected_holding_period_return_pct":str(net/notional*Decimal("100")),
                            "holding_period_assumption":"one next funding interval",
                            "gross_funding_usdt":str(profit.gross_funding),
                            "estimated_fees_usdt":str(profit.entry_fees+profit.exit_fees),
                            "estimated_slippage_cost_usdt":str(profit.slippage),
                            "estimated_net_usdt":str(net),"status":"ESTIMATED_NOT_REALIZED",
                            "data_freshness":"FRESH","reason":"Passed initial market, depth, funding and cost filters"})
                    except (KeyError, ValueError, ArithmeticError, TypeError):
                        logger.exception("Failed to enrich %s",symbol)
                self._opportunities=sorted(out,key=lambda x:Decimal(x["score"]),reverse=True)
                self.last_scan_ms=int(time.time()*1000)
                self.last_error=None
                self.last_valid_count=len(self._opportunities)
                self.store.prune(self.last_scan_ms)
                return self._opportunities
            except Exception as exc:
                self.last_error=f"{type(exc).__name__}: {exc}"
                self._opportunities = []
                self.last_valid_count = 0
                logger.exception("Market scan failed")
                return []

    async def run_forever(self) -> None:
        self._running=True
        while self._running:
            started=time.monotonic()
            await self.scan_once()
            elapsed=time.monotonic()-started
            await asyncio.sleep(max(1,self.config.scan_interval_seconds-elapsed))

    def stop(self) -> None:
        self._running=False

    def status(self) -> dict[str, Any]:
        age=None if self.last_scan_ms is None else max(0,int(time.time()*1000)-self.last_scan_ms)
        return {"running":self._running,"last_scan_ms":self.last_scan_ms,"last_scan_age_ms":age,
                "matched_pair_count":self.last_pair_count,"valid_opportunity_count":self.last_valid_count,
                "last_error":self.last_error,"database_observations":self.store.count(),
                "live_trading_enabled":False,"execution_enabled":False}
