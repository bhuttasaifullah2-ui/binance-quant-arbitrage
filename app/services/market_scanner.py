import asyncio
import logging
import time
from decimal import Decimal
from typing import Any

from app.domain.exchange_filters import SymbolFilters, matched_valid_hedge
from app.domain.funding import summarize_funding, detect_funding_interval_hours, funding_history_gaps
from app.domain.market_quality import basis_bps, spread_bps
from app.domain.models import FundingObservation
from app.domain.scanner_eligibility import validate_scanner_eligibility
from app.domain.orderbook import estimate_vwap, estimate_vwap_quantity
from app.domain.ranking import score_opportunity_breakdown, RankingWeights
from app.exchange.binance.funding_history import fetch_funding_history
from app.exchange.binance.public_client import BinancePublicClient
from app.infrastructure.sqlite_store import SQLiteMarketStore
from app.services.market_data_hub import MarketDataHub

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
    def __init__(self, client: BinancePublicClient, store: SQLiteMarketStore, config: Any,
                 market_hub: MarketDataHub | None = None, websocket_manager: Any | None = None):
        self.client, self.store, self.config = client, store, config
        self.market_hub, self.websocket_manager = market_hub, websocket_manager
        self._lock = asyncio.Lock()
        self.last_scan_ms: int | None = None
        self.last_error: str | None = None
        self.last_pair_count = 0
        self.last_valid_count = 0
        self._opportunities: list[dict[str, Any]] = []
        self._rejections: list[dict[str, Any]] = []
        self._running = False
        self._metadata_at_ms = 0
        self._spot_info: dict[str, Any] | None = None
        self._futures_info: dict[str, Any] | None = None
        self.clock_offset_ms = 0
        self._observed_symbols: set[str] = set()
        self._eligibility_decisions: dict[str, dict[str, Any]] = {}
        self.observation_store: Any | None = None
        self._ws_bootstrap_done = False

    @property
    def opportunities(self) -> list[dict[str, Any]]:
        return list(self._opportunities)

    @property
    def fresh_opportunities(self) -> list[dict[str, Any]]:
        if self.last_scan_ms is None:
            return []
        if int(time.time() * 1000) - self.last_scan_ms > self.config.max_data_age_seconds * 1000:
            return []
        return list(self._opportunities)

    @property
    def rejections(self) -> list[dict[str, Any]]:
        return list(self._rejections)

    def _reject(self, symbol: str, reason: str, now_ms: int, code: str = "SCANNER_FILTER_REJECTED") -> None:
        if code == "SCANNER_FILTER_REJECTED":
            text = reason.lower()
            if "missing" in text or "malformed" in text:
                code = "MARKET_FIELDS_MISSING"
            elif "stale" in text:
                code = "MARKET_DATA_STALE"
            elif "depth" in text or "volume" in text or "liquidity" in text:
                code = "ORDERBOOK_LIQUIDITY_INSUFFICIENT"
            elif "funding" in text:
                code = "FUNDING_HISTORY_INCONSISTENT"
            elif "hedge" in text or "filter" in text:
                code = "HEDGE_FILTER_VALIDATION_FAILED"
        item = {"symbol": symbol, "reason": reason, "explanation": reason,
                "rejection_code": code, "rejection_codes": [code], "event_time_ms": now_ms,
                "eligible": False, "status": "REJECTED"}
        self._eligibility_decisions[symbol.upper()] = item
        self._rejections.append(item)
        self._rejections = self._rejections[-500:]
        self.store.save_quality_event(item, now_ms)

    async def _metadata(self, now_ms: int) -> tuple[dict[str, Any], dict[str, Any]]:
        if self._spot_info is None or self._futures_info is None or now_ms - self._metadata_at_ms > 3600000:
            self._spot_info, self._futures_info = await asyncio.gather(
                self.client.spot_exchange_info(), self.client.futures_exchange_info())
            self._metadata_at_ms = now_ms
            self.store.save_exchange_metadata("spot", self._spot_info.get("symbols", []), now_ms)
            self.store.save_exchange_metadata("futures", self._futures_info.get("symbols", []), now_ms)
        return self._spot_info, self._futures_info

    def _rest_seed(self, common: list[str], spot_t: dict[str, Any], spot_b: dict[str, Any],
                   fut_t: dict[str, Any], fut_b: dict[str, Any], prem: dict[str, Any], now: int) -> None:
        if self.market_hub is None:
            return
        for symbol in common:
            st, sb, ft, fb, pi = (spot_t.get(symbol), spot_b.get(symbol), fut_t.get(symbol),
                                  fut_b.get(symbol), prem.get(symbol))
            if st and sb:
                self.market_hub.apply_rest(symbol, "spot:rest", {
                    "spot_bid": sb.get("bidPrice"), "spot_ask": sb.get("askPrice"),
                    "spot_last": st.get("lastPrice"), "spot_quote_volume": st.get("quoteVolume")},
                    now, now)
            if ft and fb and pi:
                self.market_hub.apply_rest(symbol, "futures:rest", {
                    "futures_bid": fb.get("bidPrice"), "futures_ask": fb.get("askPrice"),
                    "futures_last": ft.get("lastPrice"), "futures_quote_volume": ft.get("quoteVolume"),
                    "futures_mark": pi.get("markPrice"), "futures_index": pi.get("indexPrice"),
                    "funding_rate": pi.get("lastFundingRate"),
                    "next_funding_time_ms": (int(pi["nextFundingTime"]) - self.market_hub.clock_offsets_ms.get("futures", 0)
                                             if pi.get("nextFundingTime") is not None else None)}, now, now)

    def _hub_maps(self, common: list[str], now: int) -> tuple[dict, dict, dict, dict, dict]:
        spot_t: dict[str, Any] = {}; spot_b: dict[str, Any] = {}
        fut_t: dict[str, Any] = {}; fut_b: dict[str, Any] = {}; prem: dict[str, Any] = {}
        max_age = self.config.max_data_age_seconds * 1000
        for symbol in common:
            state = self.market_hub.states.get(symbol) if self.market_hub else None
            if state is None:
                self._reject(symbol, "missing normalized market state", now)
                continue
            snap = self.market_hub.pair_snapshot(symbol) or {}
            freshness = state.freshness(now, max_age, (
                "spot_bid", "spot_ask", "spot_last", "spot_quote_volume",
                "futures_bid", "futures_ask", "futures_last", "futures_quote_volume",
                "futures_mark", "futures_index", "funding_rate", "next_funding_time_ms"))
            if not freshness["valid"]:
                self._reject(symbol, ";".join(freshness["reasons"]), now)
                continue
            spot_t[symbol] = {"lastPrice": snap["spot_last"], "quoteVolume": snap["spot_quote_volume"]}
            spot_b[symbol] = {"bidPrice": snap["spot_bid"], "askPrice": snap["spot_ask"]}
            fut_t[symbol] = {"lastPrice": snap["futures_last"], "quoteVolume": snap["futures_quote_volume"]}
            fut_b[symbol] = {"bidPrice": snap["futures_bid"], "askPrice": snap["futures_ask"]}
            prem[symbol] = {"markPrice": snap["futures_mark"], "indexPrice": snap["futures_index"],
                            "lastFundingRate": snap["funding_rate"],
                            "nextFundingTime": snap["next_funding_time_ms"]}
        return spot_t, spot_b, fut_t, fut_b, prem

    async def scan_once(self) -> list[dict[str, Any]]:
        async with self._lock:
            started = int(time.time() * 1000)
            self._rejections = []
            self._eligibility_decisions = {}
            try:
                spot_info, fut_info = await self._metadata(started)
                spot_symbols, fut_symbols = _eligible_spot(spot_info), _eligible_futures(fut_info)
                common = sorted(spot_symbols & fut_symbols)
                self.last_pair_count = len(common)
                if self.websocket_manager is not None and self.config.websocket_enabled:
                    self.websocket_manager.set_symbols(set(common))
                ws_ready = bool(self.config.websocket_enabled and self.market_hub and self._ws_bootstrap_done)
                rest_snapshot_ms: int | None = None
                if ws_ready:
                    spot_t, spot_b, fut_t, fut_b, prem = self._hub_maps(common, int(time.time()*1000))
                else:
                    rest_started = int(time.time() * 1000)
                    rest_snapshot_ms = rest_started
                    spot_tickers, spot_books, fut_tickers, fut_books, premium = await asyncio.gather(
                        self.client.spot_ticker_24h(), self.client.spot_book_ticker(),
                        self.client.futures_ticker_24h(), self.client.futures_book_ticker(),
                        self.client.futures_premium_index())
                    spot_t = {x["symbol"]: x for x in spot_tickers if x.get("symbol") in common}
                    spot_b = {x["symbol"]: x for x in spot_books if x.get("symbol") in common}
                    fut_t = {x["symbol"]: x for x in fut_tickers if x.get("symbol") in common}
                    fut_b = {x["symbol"]: x for x in fut_books if x.get("symbol") in common}
                    prem = {x["symbol"]: x for x in premium if x.get("symbol") in common}
                    self._rest_seed(common, spot_t, spot_b, fut_t, fut_b, prem, rest_started)
                    if self.config.websocket_enabled and self.market_hub:
                        self._ws_bootstrap_done = True
                spot_meta = {x["symbol"]: x for x in spot_info.get("symbols", [])}
                futures_meta = {x["symbol"]: x for x in fut_info.get("symbols", [])}
                prelim = []
                for symbol in common:
                    try:
                        st, sb, ft, fb, pi = (spot_t[symbol], spot_b[symbol], fut_t[symbol],
                                              fut_b[symbol], prem[symbol])
                        funding = _d(pi.get("lastFundingRate"))
                        sv, fv = _d(st.get("quoteVolume")), _d(ft.get("quoteVolume"))
                        if sv < _d(self.config.min_spot_quote_volume_usdt) or fv < _d(self.config.min_futures_quote_volume_usdt):
                            self._reject(symbol, "24h quote volume below configured threshold", int(time.time()*1000)); continue
                        ss, fs = spread_bps(_d(sb["bidPrice"]), _d(sb["askPrice"])), spread_bps(_d(fb["bidPrice"]), _d(fb["askPrice"]))
                        if ss > _d(self.config.max_spot_spread_bps) or fs > _d(self.config.max_futures_spread_bps):
                            self._reject(symbol, "bid/ask spread above configured threshold", int(time.time()*1000)); continue
                        prelim.append((symbol, funding, st, sb, ft, fb, pi, sv, fv, ss, fs))
                    except (KeyError, ValueError, ArithmeticError, TypeError):
                        self._reject(symbol, "missing or malformed bulk market data", int(time.time()*1000))
                # Liquidity and spread take priority over headline funding in the depth shortlist.
                prelim.sort(key=lambda x: (min(x[7], x[8]), -max(x[9], x[10])), reverse=True)
                detailed = prelim[:self.config.max_depth_symbols_per_cycle]
                semaphore = asyncio.Semaphore(self.config.max_concurrent_requests)

                async def enrich(item: tuple):
                    symbol = item[0]
                    async with semaphore:
                        depth_tasks = [self.client.spot_depth(symbol, self.config.orderbook_limit),
                                       self.client.futures_depth(symbol, self.config.orderbook_limit),
                                       self.client.futures_open_interest(symbol)]
                        async def history_fetch():
                            if symbol not in history_symbols:
                                return []
                            latest = self.store.latest_funding_time(symbol)
                            rows = await fetch_funding_history(
                                self.client, symbol, start_time_ms=None if latest is None else latest + 1,
                                page_size=min(self.config.history_limit, 1000),
                                max_pages=self.config.funding_history_max_pages)
                            existing = self.store.funding_history(symbol, self.config.history_limit)
                            existing_obs = [FundingObservation(symbol, _d(x["funding_rate"]), int(x["funding_time_ms"]))
                                            for x in existing]
                            if existing_obs:
                                interval = detect_funding_interval_hours([x.funding_time_ms for x in existing_obs])
                                gaps = funding_history_gaps(existing_obs, interval)
                                if gaps:
                                    gap_start, gap_end = gaps[0]
                                    backfill = await fetch_funding_history(
                                        self.client, symbol,
                                        start_time_ms=gap_start + interval*3600000,
                                        end_time_ms=gap_end-1,
                                        page_size=min(self.config.history_limit, 1000),
                                        max_pages=self.config.funding_history_max_pages)
                                    rows.extend(backfill)
                            return rows
                        history_tasks = [history_fetch()]
                        return await asyncio.gather(*depth_tasks, *history_tasks)

                history_symbols = {x[0] for x in detailed[:self.config.max_history_symbols_per_cycle]}
                results = await asyncio.gather(*(enrich(x) for x in detailed), return_exceptions=True)
                out: list[dict[str, Any]] = []
                now = int(time.time() * 1000)
                for item, result in zip(detailed, results):
                    symbol, funding, st, sb, ft, fb, pi, sv, fv, ss, fs = item
                    if isinstance(result, BaseException):
                        self._reject(symbol, f"detailed market request failed:{type(result).__name__}", now); continue
                    sd, fd, oi, new_history = result
                    hist = list(new_history) + self.store.funding_history(symbol, self.config.history_limit)
                    try:
                        if not sd.get("bids") or not sd.get("asks") or not fd.get("bids") or not fd.get("asks"):
                            self._reject(symbol, "missing order-book side", now); continue
                        spot_ask, spot_bid = _d(sb["askPrice"]), _d(sb["bidPrice"])
                        fut_ask, fut_bid = _d(fb["askPrice"]), _d(fb["bidPrice"])
                        mark, index = _d(pi["markPrice"]), _d(pi["indexPrice"])
                        next_funding = (int(pi["nextFundingTime"]) - (self.market_hub.clock_offsets_ms.get("futures", 0)
                                          if self.market_hub else 0))
                        notional = _d(self.config.trade_notional_usdt)
                        spot_filter = SymbolFilters.from_exchange_info(spot_meta[symbol], "spot")
                        futures_filter = SymbolFilters.from_exchange_info(futures_meta[symbol], "futures")
                        spot_entry_est = estimate_vwap(sd["asks"], notional, "buy")
                        futures_entry_est = estimate_vwap(fd["bids"], notional, "sell")
                        if (spot_entry_est is None or futures_entry_est is None or
                                not spot_entry_est.complete or not futures_entry_est.complete):
                            self._reject(symbol, "insufficient entry-side order-book depth", now); continue
                        hedge = matched_valid_hedge(notional, spot_entry_est.vwap, futures_entry_est.vwap,
                                                    spot_filter, futures_filter,
                                                    _d(self.config.max_hedge_mismatch))
                        if not hedge["executable"]:
                            self._reject(symbol, ";".join(hedge["reasons"]), now); continue
                        spot_buy = estimate_vwap_quantity(sd["asks"], hedge["spot_quantity"], "buy")
                        spot_sell = estimate_vwap_quantity(sd["bids"], hedge["spot_quantity"], "sell")
                        fut_sell = estimate_vwap_quantity(fd["bids"], hedge["futures_quantity"], "sell")
                        fut_buy = estimate_vwap_quantity(fd["asks"], hedge["futures_quantity"], "buy")
                        executions = [spot_buy, spot_sell, fut_sell, fut_buy]
                        if any(x is None or not x.complete for x in executions):
                            self._reject(symbol, "insufficient order-book depth for configured notional", now); continue
                        spot_entry_notional, futures_entry_notional = spot_buy.quote_notional, fut_sell.quote_notional
                        spot_exit_notional, futures_exit_notional = spot_sell.quote_notional, fut_buy.quote_notional
                        actual_denominator = max(spot_entry_notional, futures_entry_notional)
                        actual_mismatch = (abs(spot_entry_notional-futures_entry_notional)/actual_denominator
                                           if actual_denominator else Decimal("1"))
                        actual_validation = (list(spot_filter.validate(hedge["spot_quantity"], spot_buy.vwap))
                                             + list(futures_filter.validate(hedge["futures_quantity"], fut_sell.vwap)))
                        if actual_mismatch > _d(self.config.max_hedge_mismatch) or actual_validation:
                            self._reject(symbol, ";".join(actual_validation) or "actual VWAP hedge mismatch exceeds limit", now); continue
                        hedge["spot_notional"] = spot_entry_notional
                        hedge["futures_notional"] = futures_entry_notional
                        hedge["hedge_ratio"] = futures_entry_notional / spot_entry_notional
                        hedge["residual_directional_exposure"] = spot_entry_notional-futures_entry_notional
                        hedge["mismatch_fraction"] = actual_mismatch
                        slips = [x.slippage_bps for x in executions if x is not None]
                        matched_notional = min(spot_entry_notional, futures_entry_notional)
                        gross_funding = matched_notional * funding
                        entry_fees = (spot_entry_notional * _d(self.config.spot_taker_fee_rate)
                                      + futures_entry_notional * _d(self.config.futures_taker_fee_rate))
                        exit_fees = (spot_exit_notional * _d(self.config.spot_taker_fee_rate)
                                     + futures_exit_notional * _d(self.config.futures_taker_fee_rate))
                        slippage_cost = sum((x.quote_notional * x.slippage_bps / Decimal("10000")
                                             for x in executions if x is not None), Decimal("0"))
                        if new_history:
                            self.store.save_funding_observations(symbol, new_history, now)
                        funding_obs = []
                        for x in hist:
                            rate = x.get("fundingRate", x.get("funding_rate"))
                            stamp = x.get("fundingTime", x.get("funding_time_ms"))
                            if rate is not None and stamp is not None:
                                funding_obs.append(FundingObservation(symbol, _d(rate), int(stamp)))
                        stats = summarize_funding(funding_obs)
                        funding_interval = detect_funding_interval_hours([x.funding_time_ms for x in funding_obs])
                        history_gaps = funding_history_gaps(funding_obs, funding_interval)
                        if history_gaps:
                            self._reject(symbol, f"historical funding gaps detected:{len(history_gaps)}", now)
                        bps = basis_bps(spot_buy.vwap, fut_sell.vwap)
                        basis_penalty = abs(bps)/Decimal("10000")*matched_notional*_d(self.config.basis_risk_haircut)
                        net = gross_funding - entry_fees - exit_fees - slippage_cost - basis_penalty
                        age_ms = 0
                        data_source = "binance_rest"
                        if self.config.websocket_enabled and self.market_hub:
                            state = self.market_hub.states.get(symbol)
                            required = ("spot_bid", "spot_ask", "spot_last", "spot_quote_volume",
                                        "futures_bid", "futures_ask", "futures_last", "futures_quote_volume",
                                        "futures_mark", "futures_index", "funding_rate", "next_funding_time_ms")
                            quality = state.freshness(now, self.config.max_data_age_seconds*1000, required) if state else {"valid": False, "ages_ms": {}, "reasons": ["missing state"]}
                            if not quality["valid"]:
                                self._reject(symbol, ";".join(quality["reasons"]), now); continue
                            age_ms = max((x for x in quality["ages_ms"].values() if x is not None), default=0)
                            sources = {state.fields[k].source for k in required if k in state.fields}
                            data_source = "binance_ws" if sources == {"websocket"} else "binance_rest_recovery"
                        else:
                            age_ms = max(0, now - (rest_snapshot_ms or now))
                            if age_ms > self.config.max_data_age_seconds*1000:
                                self._reject(symbol, "REST snapshot stale", now); continue
                        spot_visible_depth = sum((_d(p)*_d(q) for p,q in sd["bids"][:self.config.depth_levels]), Decimal("0")) + sum((_d(p)*_d(q) for p,q in sd["asks"][:self.config.depth_levels]), Decimal("0"))
                        futures_visible_depth = sum((_d(p)*_d(q) for p,q in fd["bids"][:self.config.depth_levels]), Decimal("0")) + sum((_d(p)*_d(q) for p,q in fd["asks"][:self.config.depth_levels]), Decimal("0"))
                        weights = RankingWeights(
                            net_edge=_d(self.config.score_weight_net_edge),
                            funding_consistency=_d(self.config.score_weight_funding_consistency),
                            liquidity=_d(self.config.score_weight_liquidity), spread=_d(self.config.score_weight_spread),
                            slippage=_d(self.config.score_weight_slippage), basis=_d(self.config.score_weight_basis),
                            freshness=_d(self.config.score_weight_freshness),
                            funding_volatility=_d(self.config.score_weight_funding_volatility),
                            hedge_precision=_d(self.config.score_weight_hedge_precision),
                            residual_exposure=_d(self.config.score_weight_residual_exposure))
                        score_factors = score_opportunity_breakdown(
                            net_edge_usdt=net, funding_positive_fraction=stats["positive_fraction"],
                            funding_volatility=stats["volatility"],
                            spot_depth_usdt=spot_visible_depth,
                            futures_depth_usdt=futures_visible_depth,
                            spot_spread_bps=ss, futures_spread_bps=fs,
                            estimated_slippage_bps=sum(slips, Decimal("0")), basis_bps_value=bps,
                            data_age_seconds=Decimal(age_ms)/1000,
                            max_data_age_seconds=Decimal(self.config.max_data_age_seconds),
                            min_depth_usdt=notional, max_spread_bps=_d(self.config.ranking_max_spread_bps),
                            max_slippage_bps=_d(self.config.ranking_max_slippage_bps),
                            max_basis_bps=_d(self.config.ranking_max_basis_bps), weights=weights,
                            hedge_mismatch_fraction=hedge["mismatch_fraction"],
                            max_hedge_mismatch=_d(self.config.max_hedge_mismatch),
                            residual_exposure_usdt=hedge["residual_directional_exposure"],
                            max_residual_exposure_usdt=matched_notional*_d(self.config.max_hedge_mismatch))
                        score = score_factors["score"]
                        if next_funding <= now or stats["count"] == 0 or net <= _d(self.config.min_net_edge_usdt):
                            self._reject(symbol, "funding history absent/expired or estimated net edge below threshold", now); continue
                        # All candidate opportunities pass this one final, shared gate.
                        state = self.market_hub.states.get(symbol) if self.market_hub else None
                        required_fields = ("spot_bid", "spot_ask", "spot_last", "spot_quote_volume",
                            "futures_bid", "futures_ask", "futures_last", "futures_quote_volume",
                            "futures_mark", "futures_index", "funding_rate", "next_funding_time_ms")
                        if state is not None:
                            quality = state.freshness(now, self.config.max_data_age_seconds*1000, required_fields)
                            field_ages = quality["ages_ms"]
                            funding_field = state.fields.get("funding_rate")
                            funding_age = None if funding_field is None else max(0, now-funding_field.received_at_ms)
                        else:
                            field_ages = {name: max(0, now-(rest_snapshot_ms or now)) for name in required_fields}
                            funding_age = max(0, now-(rest_snapshot_ms or now))
                            quality = {"valid": bool(rest_snapshot_ms), "reasons": [] if rest_snapshot_ms else ["missing REST snapshot"]}
                        ws_feeds = {}
                        if self.config.websocket_enabled:
                            metrics = getattr(self.websocket_manager, "metrics", {}) if self.websocket_manager else {}
                            for market in ("spot", "futures"):
                                metric = metrics.get(market, {})
                                msg_at = metric.get("last_message_ms")
                                msg_age = None if msg_at is None else max(0, now-int(msg_at))
                                ws_feeds[market] = {"healthy": bool(metric.get("connected")) and
                                    msg_age is not None and msg_age <= self.config.max_data_age_seconds*1000 and
                                    metric.get("subscription_status") == "SUBSCRIBED"}
                        else:
                            ws_feeds = {"spot": {"healthy": True}, "futures": {"healthy": True}}
                        spot_filter_ok = spot_filter.status == "TRADING" and not spot_filter.validate(hedge["spot_quantity"], spot_buy.vwap)
                        futures_filter_ok = futures_filter.status == "TRADING" and not futures_filter.validate(hedge["futures_quantity"], fut_sell.vwap)
                        gate = validate_scanner_eligibility(symbol, now, {
                            "market_fields": {"spot_bid": spot_bid, "spot_ask": spot_ask,
                                "spot_last": st.get("lastPrice"), "spot_quote_volume": sv,
                                "futures_bid": fut_bid, "futures_ask": fut_ask,
                                "futures_last": ft.get("lastPrice"), "futures_quote_volume": fv,
                                "futures_mark": mark, "futures_index": index,
                                "funding_rate": funding, "next_funding_time_ms": next_funding},
                            "field_ages_ms": field_ages, "max_data_age_ms": self.config.max_data_age_seconds*1000,
                            "market_data_stale": not quality["valid"],
                            "websocket_enabled": bool(self.config.websocket_enabled), "websocket_feeds": ws_feeds,
                            "clock_offsets_ms": self.market_hub.clock_offsets_ms if self.market_hub else {},
                            "max_clock_drift_ms": getattr(self.config, "max_clock_drift_ms", self.config.max_clock_skew_ms),
                            "clock_sync_required": bool(self.config.websocket_enabled or self.config.live_validation_enabled),
                            "spot_filters_valid": spot_filter_ok, "futures_filters_valid": futures_filter_ok,
                            "funding_age_ms": funding_age, "max_funding_age_ms": self.config.max_data_age_seconds*1000,
                            "funding_gaps": history_gaps, "funding_inconsistent": False,
                            "hedge_valid": bool(hedge.get("executable")) and not actual_validation,
                            "residual_exposure_usdt": hedge.get("residual_directional_exposure"),
                            "max_residual_exposure_usdt": matched_notional*_d(self.config.max_hedge_mismatch),
                            "min_orderbook_depth_usdt": _d(self.config.trade_notional_usdt),
                            "spot_orderbook_depth_usdt": spot_visible_depth,
                            "futures_orderbook_depth_usdt": futures_visible_depth})
                        self._eligibility_decisions[symbol] = gate.as_dict()
                        if not gate.eligible:
                            for rejection in gate.rejections:
                                self._reject(symbol, rejection.explanation, now, rejection.code)
                            self._eligibility_decisions[symbol] = gate.as_dict()
                            continue
                        self._eligibility_decisions[symbol] = gate.as_dict()
                        row = {"symbol": symbol, "spot_bid": spot_bid, "spot_ask": spot_ask,
                               "spot_last": _d(st["lastPrice"]), "spot_quote_volume": sv,
                               "futures_bid": fut_bid, "futures_ask": fut_ask,
                               "futures_mark": mark, "futures_index": index, "futures_quote_volume": fv,
                               "funding_rate": funding, "next_funding_time_ms": next_funding,
                               "open_interest": _d(oi["openInterest"]),
                               "spot_depth_usdt": spot_visible_depth,
                               "futures_depth_usdt": futures_visible_depth,
                               "spot_slippage_bps": spot_buy.slippage_bps + spot_sell.slippage_bps,
                               "futures_slippage_bps": fut_sell.slippage_bps + fut_buy.slippage_bps,
                               "basis_bps": bps, "observed_at_ms": now, "source": data_source}
                        self.store.save_observation(row)
                        break_even = (entry_fees + exit_fees + slippage_cost + basis_penalty) / matched_notional
                        out.append({
                            "symbol": symbol, "score": str(score), "funding_rate": str(funding),
                            "funding_history": {k: str(v) for k, v in stats.items()},
                            "next_funding_time_ms": next_funding, "funding_interval_hours": funding_interval,
                            "spot_price": str(spot_buy.vwap), "futures_mark": str(mark),
                            "entry_spot_vwap": str(spot_buy.vwap), "entry_futures_vwap": str(fut_sell.vwap),
                            "estimated_exit_spot_vwap": str(spot_sell.vwap),
                            "estimated_exit_futures_vwap": str(fut_buy.vwap),
                            "spot_volume_24h_usdt": str(sv), "futures_volume_24h_usdt": str(fv),
                            "spot_spread_bps": str(ss), "futures_spread_bps": str(fs),
                            "spot_depth_usdt": str(row["spot_depth_usdt"]),
                            "futures_depth_usdt": str(row["futures_depth_usdt"]),
                            "estimated_slippage_bps": str(sum(slips, Decimal("0"))),
                            "basis_bps": str(bps), "entry_basis_bps": str(bps),
                            "estimated_exit_basis_bps_assumption": str(bps),
                            "basis_sensitivity_per_10bps_usdt": str(matched_notional*Decimal("10")/Decimal("10000")),
                            "spot_quantity": str(hedge["spot_quantity"]),
                            "futures_quantity": str(hedge["futures_quantity"]),
                            "spot_notional": str(hedge["spot_notional"]),
                            "futures_notional": str(hedge["futures_notional"]),
                            "hedge_ratio": str(hedge["hedge_ratio"]),
                            "residual_directional_exposure": str(hedge["residual_directional_exposure"]),
                            "hedge_executable": True, "hedge_validation_reasons": [],
                            "break_even_funding_rate": str(break_even),
                            "expected_holding_period_return_pct": str(net/matched_notional*100),
                            "holding_period_assumption": "one next funding interval",
                            "gross_funding_usdt": str(gross_funding),
                            "estimated_fees_usdt": str(entry_fees + exit_fees),
                            "estimated_slippage_cost_usdt": str(slippage_cost),
                            "estimated_net_usdt": str(net), "status": "ESTIMATED_NOT_REALIZED",
                            "data_freshness": "FRESH", "max_field_age_ms": age_ms,
                            "field_timestamps_ms": ({} if not self.market_hub or not ws_ready else {
                                k: v.received_at_ms for k, v in self.market_hub.states[symbol].fields.items()}),
                            "source": data_source,
                            "rejection_reasons": [], "risk_warnings": (["Funding and exit basis are estimates; no execution guarantee."] +
                                              (["Historical funding data has gaps."] if history_gaps else [])),
                            "funding_history_gap_count": len(history_gaps),
                            "scoring_factors": {k: str(v) for k, v in score_factors.items()},
                            "funding_metrics": {
                                "funding_positive_fraction": str(stats["positive_fraction"]),
                                "funding_volatility": str(stats["volatility"]),
                                "spot_depth_usdt": str(row["spot_depth_usdt"]),
                                "futures_depth_usdt": str(row["futures_depth_usdt"]),
                                "spot_spread_bps": str(ss), "futures_spread_bps": str(fs),
                                "slippage_bps": str(sum(slips, Decimal("0"))),
                                "basis_bps": str(bps), "hedge_mismatch_fraction": str(hedge["mismatch_fraction"]),
                                "max_field_age_ms": str(age_ms)
                            }
                        })
                    except (KeyError, ValueError, ArithmeticError, TypeError) as exc:
                        self._reject(symbol, f"validation or enrichment error:{type(exc).__name__}", now)
                        logger.exception("Failed to enrich %s", symbol)
                self._opportunities = sorted(out, key=lambda x: Decimal(x["score"]), reverse=True)
                self.last_scan_ms = int(time.time()*1000)
                self.last_error = None
                self.last_valid_count = len(self._opportunities)
                if self.config.observation_mode:
                    current_symbols = {x["symbol"] for x in self._opportunities}
                    self.store.save_opportunities(self._opportunities, self.last_scan_ms)
                    if self.observation_store is not None:
                        ws_status = {}
                        if self.websocket_manager is not None:
                            ws_status = {k: bool(v.get("connected")) for k, v in
                                         getattr(self.websocket_manager, "metrics", {}).items()}
                        self.observation_store.record_snapshot(
                            self._opportunities, self._eligibility_decisions, self.last_scan_ms, ws_status)
                    else:
                        for symbol in sorted(current_symbols - self._observed_symbols):
                            row = next(x for x in self._opportunities if x["symbol"] == symbol)
                            self.store.save_opportunity_lifecycle(symbol, "APPEARED", self.last_scan_ms,
                                                                   row.get("score"), row.get("estimated_net_usdt"))
                        for symbol in sorted(self._observed_symbols - current_symbols):
                            self.store.save_opportunity_lifecycle(symbol, "DISAPPEARED", self.last_scan_ms)
                    self._observed_symbols = current_symbols
                self.store.save_scanner_run(started, self.last_scan_ms, self.last_pair_count,
                                            len(self._opportunities), len(self._rejections), None)
                self.store.prune(self.last_scan_ms)
                return self._opportunities
            except Exception as exc:
                self.last_error = f"{type(exc).__name__}: {exc}"
                self._opportunities = []
                self.last_valid_count = 0
                self.store.save_scanner_run(started, int(time.time()*1000), self.last_pair_count,
                                            0, len(self._rejections), self.last_error)
                logger.exception("Market scan failed")
                return []

    async def run_forever(self) -> None:
        self._running = True
        while self._running:
            started = time.monotonic()
            await self.scan_once()
            await asyncio.sleep(max(1, self.config.scan_interval_seconds-(time.monotonic()-started)))

    def stop(self) -> None:
        self._running = False

    def status(self) -> dict[str, Any]:
        age = None if self.last_scan_ms is None else max(0, int(time.time()*1000)-self.last_scan_ms)
        return {"running": self._running, "last_scan_ms": self.last_scan_ms, "last_scan_age_ms": age,
                "matched_pair_count": self.last_pair_count, "valid_opportunity_count": self.last_valid_count,
                "last_error": self.last_error, "database_observations": self.store.count(),
                "live_trading_enabled": False, "execution_enabled": False,
                "websocket_enabled": bool(self.config.websocket_enabled)}
