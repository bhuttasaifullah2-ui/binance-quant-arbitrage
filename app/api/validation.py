"""Read-only Phase 2.2 validation and observation API routes."""
from datetime import date, datetime, time, timezone
from fastapi import APIRouter, HTTPException, Query, Request
router=APIRouter()
def _service(request: Request):
    service=getattr(request.app.state,"live_validator",None)
    if service is None: raise HTTPException(status_code=503,detail="Validation service is not initialized")
    return service
@router.get("/api/v1/validation/summary")
async def summary(request: Request): return _service(request).summary()
@router.get("/api/v1/validation/connectivity")
async def connectivity(request: Request):
    service=_service(request)
    if not service.connectivity: return {"status":"NOT_RUN","checked_at_ms":None,"checks":[]}
    return service.connectivity
@router.post("/api/v1/validation/connectivity/check")
async def run_connectivity_check(request: Request): return await _service(request).validate()
@router.get("/api/v1/validation/websocket")
async def websocket_health(request: Request): return _service(request).websocket_health()
@router.get("/api/v1/validation/clock")
async def clock(request: Request): return _service(request).last_clock or {"status":"NOT_RUN","markets":{}}
@router.post("/api/v1/validation/clock/check")
async def run_clock_check(request: Request): return await _service(request).check_clock()
@router.get("/api/v1/validation/data-quality")
async def quality(request: Request): return _service(request).data_quality()
@router.get("/api/v1/validation/funding")
async def funding(request: Request): return _service(request).funding or {"status":"NOT_RUN"}
@router.post("/api/v1/validation/funding/{symbol}")
async def run_funding(request: Request,symbol: str): return await _service(request).validate_funding(symbol)
@router.get("/api/v1/validation/hedge")
async def hedge(request: Request): return _service(request).hedge
@router.post("/api/v1/validation/hedge/{symbol}")
async def run_hedge(request: Request,symbol: str): return await _service(request).validate_hedge(symbol)
@router.get("/api/v1/validation/profitability")
async def profitability(request: Request): return _service(request).profitability
@router.post("/api/v1/validation/profitability/run")
async def run_profitability(request: Request,notional: str="1000"): return _service(request).validate_profitability(notional)
@router.get("/api/v1/observation/events")
async def observation_events(request: Request,limit: int=Query(200,ge=1,le=2000)):
    items = request.app.state.validation_store.events(limit)
    return {"items": items, "count": len(items)}
@router.get("/api/v1/observation/report")
async def observation_report(request: Request):
    return request.app.state.validation_store.statistical_report()


@router.get("/api/v1/scanner/eligibility")
async def scanner_eligibility(request: Request):
    scanner = request.app.state.market_scanner
    return {
        "checked_at_ms": scanner.last_scan_ms,
        "decisions": scanner._eligibility_decisions,
        "eligible_count": sum(1 for x in scanner._eligibility_decisions.values() if x.get("eligible")),
        "rejected_count": sum(1 for x in scanner._eligibility_decisions.values() if not x.get("eligible")),
        "source": "same final eligibility gate used by MarketScanner",
    }


@router.get("/api/v1/observation/report/cumulative")
async def cumulative_observation_report(request: Request):
    return request.app.state.validation_store.statistical_report()


@router.get("/api/v1/observation/report/daily")
async def daily_observation_report(request: Request, day: str | None = None):
    selected = date.fromisoformat(day) if day else datetime.now(timezone.utc).date()
    start = datetime.combine(selected, time.min, tzinfo=timezone.utc)
    end = datetime.combine(selected, time.max, tzinfo=timezone.utc)
    return request.app.state.validation_store.statistical_report(
        int(start.timestamp() * 1000), int(end.timestamp() * 1000) + 1)



@router.get("/api/v1/observation/sessions")
async def list_observation_sessions(request: Request, limit: int = Query(100, ge=1, le=500)):
    return {"items": request.app.state.validation_store.list_sessions(limit)}


def _session_export(request: Request, session_id: str):
    try:
        return request.app.state.validation_store.export_session(session_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Observation session not found") from exc


@router.get("/api/v1/observation/sessions/{session_id}")
async def observation_session_summary(request: Request, session_id: str):
    try:
        return request.app.state.validation_store.session_summary(session_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Observation session not found") from exc


@router.get("/api/v1/observation/sessions/{session_id}/report.json")
async def export_observation_session(request: Request, session_id: str):
    return _session_export(request, session_id)


@router.get("/api/v1/observation/sessions/{session_id}/rest")
async def session_rest_statistics(request: Request, session_id: str):
    return _session_export(request, session_id)["summary"]["rest_checks"]


@router.get("/api/v1/observation/sessions/{session_id}/websocket")
async def session_websocket_statistics(request: Request, session_id: str):
    return _session_export(request, session_id)["summary"]["websocket"]


@router.get("/api/v1/observation/sessions/{session_id}/clock")
async def session_clock_statistics(request: Request, session_id: str):
    return _session_export(request, session_id)["summary"]["clock"]


@router.get("/api/v1/observation/sessions/{session_id}/data-quality")
async def session_data_quality_statistics(request: Request, session_id: str):
    return _session_export(request, session_id)["summary"]["data_quality"]


@router.get("/api/v1/observation/sessions/{session_id}/eligibility")
async def session_eligibility_statistics(request: Request, session_id: str):
    return _session_export(request, session_id)["summary"]["scanner_eligibility"]


@router.get("/api/v1/observation/sessions/{session_id}/funding")
async def session_funding_statistics(request: Request, session_id: str):
    return _session_export(request, session_id)["summary"]["funding"]
