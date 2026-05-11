"""Tests for the 13F Filing Viewer web application.

Most tests run against the v1 router surface.  When the redesign
preview is enabled (``PP_REDESIGN_PREVIEW=1``) the v2 templates
serve ``/`` and the 404 page with different copy -- assertions
that touch user-facing strings are kept mode-tolerant.
"""

import re

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
    """404 returns 404 status + a recognisable 404 page.

    The v1 template ("Error - PaperPanda" / "Something went wrong") and
    the v2 template ("Page not found · PaperPanda") use different copy;
    we accept either by checking the <title>.
    """
    resp = await client.get("/nonexistent-page-xyz")
    assert resp.status_code == 404
    title_match = re.search(r"<title>(.+?)</title>", resp.text, re.IGNORECASE)
    assert title_match is not None, "404 page has no <title>"
    title = title_match.group(1).lower()
    assert ("error" in title) or ("not found" in title), (
        f"404 title doesn't look like a 404 page: {title!r}"
    )


@pytest.mark.anyio
async def test_security_headers(client):
    resp = await client.get("/")
    assert resp.headers.get("x-frame-options") == "DENY"
    assert resp.headers.get("x-content-type-options") == "nosniff"
    assert "strict-origin" in resp.headers.get("referrer-policy", "")
    assert "Content-Security-Policy" in resp.headers


@pytest.mark.anyio
async def test_activity_page_redirects(client):
    """/activity → /funds?view=Activity (permanent backward-compat redirect)."""
    resp = await client.get("/activity", follow_redirects=False)
    assert resp.status_code == 301
    assert resp.headers.get("location") == "/funds?view=Activity"


@pytest.mark.anyio
async def test_grand_portfolio_redirects(client):
    """/grand-portfolio → /funds (permanent backward-compat redirect)."""
    resp = await client.get("/grand-portfolio", follow_redirects=False)
    assert resp.status_code == 301
    # Location preserves the legacy ?view= query when present; with no
    # view it defaults to "Funds".
    assert (resp.headers.get("location") or "").startswith("/funds?view=")


@pytest.mark.anyio
async def test_footer_disclaimer(client):
    """Homepage carries the SEC EDGAR source attribution.

    The v1 template additionally carries a "Not financial advice"
    disclaimer; v2's disclaimer lives in a separate modal / page.
    Only SEC EDGAR is shared by both templates, so that's the stable
    contract we assert on.
    """
    resp = await client.get("/")
    assert resp.status_code == 200
    assert "SEC EDGAR" in resp.text


@pytest.mark.anyio
async def test_health_detail_requires_secret(client):
    """Health detail endpoint returns 404 without correct secret."""
    resp = await client.get("/health/detail")
    assert resp.status_code == 404


@pytest.mark.anyio
async def test_cik_validation(client):
    """Endpoints reject invalid CIK values."""
    resp = await client.get("/api/fund-row/DROP TABLE")
    assert resp.status_code == 400


@pytest.mark.anyio
async def test_ticker_validation(client):
    """Endpoints reject invalid ticker values."""
    resp = await client.get("/api/analysts/DROP;TABLE")
    assert resp.status_code == 400
