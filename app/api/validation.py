"""Read-only Phase 2.2 validation and observation API routes."""
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
@router.get("/api/v1/validation/profitability")
async def profitability(request: Request): return _service(request).profitability
@router.get("/api/v1/observation/events")
async def observation_events(request: Request,limit: int=Query(200,ge=1,le=2000)):
    return {"items":request.app.state.validation_store.events(limit),"count":len(request.app.state.validation_store.events(limit))}
@router.get("/api/v1/observation/report")
async def observation_report(request: Request):
    return request.app.state.validation_store.report()
