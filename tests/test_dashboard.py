from pathlib import Path

import httpx
import pytest

from app.api.main import app


@pytest.mark.asyncio
async def test_dashboard_route_serves_self_contained_read_only_html():
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/dashboard")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/html")
    assert response.headers["cache-control"] == "no-store"
    assert "QUANT RESEARCH" in response.text
    assert "Estimated net USDT" in response.text
    assert "No execution UI" in response.text
    assert "setInterval(load,REFRESH)" in response.text


def test_dashboard_asset_uses_existing_read_only_api_endpoints():
    dashboard = Path(__file__).resolve().parents[1] / "app" / "static" / "dashboard.html"
    content = dashboard.read_text(encoding="utf-8")

    assert 'method:"GET"' in content
    assert "/api/v1/scanner/opportunities" in content
    assert "/api/v1/scanner/eligibility" in content
    assert "/api/v1/validation/data-quality" in content
    assert "/api/v1/observation/sessions/" in content
    assert "LIVE_TRADING_ENABLED" not in content
    assert 'method:"POST"' not in content
