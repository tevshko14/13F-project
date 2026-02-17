"""Tests for the 13F Filing Viewer web application."""

import pytest
from httpx import ASGITransport, AsyncClient

from filings.web import app


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
async def client():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


@pytest.mark.anyio
async def test_homepage_returns_200(client):
    resp = await client.get("/")
    assert resp.status_code == 200
    assert "13F" in resp.text


@pytest.mark.anyio
async def test_health_endpoint(client):
    resp = await client.get("/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "ok"
    assert "uptime_seconds" in data
    assert "cache_entries" in data


@pytest.mark.anyio
async def test_robots_txt(client):
    resp = await client.get("/robots.txt")
    assert resp.status_code == 200
    assert "User-agent" in resp.text
    assert "Sitemap" in resp.text


@pytest.mark.anyio
async def test_sitemap_xml(client):
    resp = await client.get("/sitemap.xml")
    assert resp.status_code == 200
    assert "urlset" in resp.text
    assert "<loc>" in resp.text


@pytest.mark.anyio
async def test_404_error_page(client):
    resp = await client.get("/nonexistent-page-xyz")
    assert resp.status_code == 404
    assert "doesn't exist" in resp.text.lower() or "something went wrong" in resp.text.lower()


@pytest.mark.anyio
async def test_security_headers(client):
    resp = await client.get("/")
    assert resp.headers.get("x-frame-options") == "DENY"
    assert resp.headers.get("x-content-type-options") == "nosniff"
    assert "strict-origin" in resp.headers.get("referrer-policy", "")


@pytest.mark.anyio
async def test_search_empty(client):
    resp = await client.get("/search")
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_activity_page(client):
    resp = await client.get("/activity")
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_grand_portfolio_page(client):
    resp = await client.get("/grand-portfolio")
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_footer_disclaimer(client):
    resp = await client.get("/")
    assert "Not financial advice" in resp.text
    assert "SEC EDGAR" in resp.text
