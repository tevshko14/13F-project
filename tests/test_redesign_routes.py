"""Refactor-verification tests for the redesign router split.

These tests don't go through the FastAPI app's lifespan (which would
trigger slow external calls -- yfinance, Supabase warmups, fund_cache
load).  They verify the static surface area of the refactor:

  * Every sub-router module imports cleanly
  * The composite ``redesign_preview.router`` registers exactly the
    expected URL paths
  * The public API web.py reads from ``redesign_preview`` (is_enabled,
    warm_homepage_caches, build_stock_data_bundle, warm_l2_caches)
    is still importable
  * Each sub-router contributes the routes it should

Run with::

    PP_REDESIGN_PREVIEW=1 uv run pytest tests/test_redesign_routes.py -v
"""

from __future__ import annotations

import os

import pytest

# Enable the redesign router so its routes register at import time.
# Set BEFORE any filings.* import below.
os.environ.setdefault("PP_REDESIGN_PREVIEW", "1")
os.environ.setdefault("PP_PROFILE_PREVIEW", "1")
os.environ.setdefault("PP_PLACEHOLDERS", "1")


# ── Expected route inventory ─────────────────────────────────────────
#
# Every (method, path) pair the redesign router must expose.  This is
# the contract the refactor must preserve.  If you intentionally
# add/remove a route, update this list.

EXPECTED_ROUTES: set[tuple[str, str]] = {
    # Pages index (dev-only)
    ("GET", "/_pages"),
    # Home + lazy partials
    ("GET", "/"),
    ("GET", "/api/home/heatmap"),
    ("GET", "/api/home/activity"),
    ("GET", "/api/home/calendar"),
    # Funds
    ("GET", "/funds"),
    ("GET", "/api/funds-index/holdings"),
    ("GET", "/api/funds-index/activity"),
    ("GET", "/funds/detail"),
    ("GET", "/funds/{cik}"),
    # Stock
    ("GET", "/stock/{ticker}"),
    ("GET", "/stock/{ticker}/chart/{period}"),
    # Signal pages
    ("GET", "/congress"),
    ("GET", "/insiders"),
    # User pages
    ("GET", "/watchlist"),
    ("GET", "/profile"),
    ("GET", "/notifications"),
    # Other feature pages
    ("GET", "/retail"),
    ("GET", "/macro"),
    ("GET", "/api/macro/volatility"),  # Phase 6 lazy partial
    ("GET", "/api/macro/heatmap"),     # Phase 6 lazy partial
    # Support / payments
    ("GET", "/support"),
    ("GET", "/support/thank-you"),
    # Placeholder pages -- only registered when PP_PLACEHOLDERS=1
    # (the test conftest sets that env var; production unsets it).
    ("GET", "/options"),
    ("GET", "/screener"),
}


# ── Tests ────────────────────────────────────────────────────────────


def test_redesign_preview_imports():
    """The orchestrator module imports without errors."""
    from filings.routers import redesign_preview  # noqa: F401


def test_sub_router_modules_import():
    """Every sub-router module imports without errors."""
    from filings.routers._redesign import (  # noqa: F401
        congress,
        helpers,
        insiders,
        notifications,
        profile_watchlist,
        support,
    )


def test_public_api_for_web_py():
    """web.py imports these symbols from redesign_preview — they must exist.

    ``build_stock_data_bundle`` lives in ``_redesign.stock`` post-extraction
    but is re-exported from ``redesign_preview`` so web.py's existing import
    keeps working without changes.
    """
    from filings.routers import redesign_preview as rp

    # Functions web.py calls.
    assert callable(rp.is_enabled), "is_enabled must be callable"
    assert callable(rp.warm_homepage_caches), "warm_homepage_caches must be callable"
    assert callable(rp.warm_l2_caches), "warm_l2_caches must be callable"
    assert callable(rp.build_stock_data_bundle), "build_stock_data_bundle must be callable"

    # The composite router must exist and have routes.
    assert hasattr(rp, "router"), "redesign_preview must expose `router`"
    assert len(rp.router.routes) > 0, "router must have routes registered"


def test_all_expected_routes_registered():
    """The composite router must expose every expected URL.

    This is the headline regression check: if a sub-router import is
    missing from the orchestrator or a route was accidentally dropped
    during the split, this test fails immediately.
    """
    from filings.routers import redesign_preview as rp

    actual: set[tuple[str, str]] = set()
    for r in rp.router.routes:
        if not hasattr(r, "path") or not hasattr(r, "methods"):
            continue
        for method in r.methods:
            actual.add((method, r.path))

    missing = EXPECTED_ROUTES - actual
    extra = actual - EXPECTED_ROUTES

    assert not missing, f"Missing routes: {sorted(missing)}"
    assert not extra, f"Unexpected routes: {sorted(extra)}"


@pytest.mark.parametrize("module,path,expected_route", [
    ("support",           "/support",          ("GET", "/support")),
    ("support",           "/support/thank-you", ("GET", "/support/thank-you")),
    ("profile_watchlist", "/profile",          ("GET", "/profile")),
    ("profile_watchlist", "/watchlist",        ("GET", "/watchlist")),
    ("insiders",          "/insiders",         ("GET", "/insiders")),
    ("notifications",     "/notifications",    ("GET", "/notifications")),
    ("congress",          "/congress",         ("GET", "/congress")),
    ("macro",             "/macro",            ("GET", "/macro")),
    ("retail",            "/retail",           ("GET", "/retail")),
    ("funds",             "/funds",            ("GET", "/funds")),
    ("funds",             "/funds/{cik}",      ("GET", "/funds/{cik}")),
    ("stock",             "/stock/{ticker}",   ("GET", "/stock/{ticker}")),
    ("stock",             "/stock/{ticker}/chart/{period}",
                                                ("GET", "/stock/{ticker}/chart/{period}")),
    ("home",              "/",                 ("GET", "/")),
    ("home",              "/_pages",           ("GET", "/_pages")),
    ("home",              "/api/home/heatmap", ("GET", "/api/home/heatmap")),
])
def test_sub_router_owns_its_route(module, path, expected_route):
    """Each sub-router module exposes its own ``router`` carrying its routes.

    Catches the failure mode where a sub-router file is created but
    the orchestrator forgot to ``include_router(...)`` it.
    """
    import importlib
    mod = importlib.import_module(f"filings.routers._redesign.{module}")
    assert hasattr(mod, "router"), f"{module} must expose `router`"
    paths_in_module = {
        (m, r.path)
        for r in mod.router.routes
        if hasattr(r, "path") and hasattr(r, "methods")
        for m in r.methods
    }
    assert expected_route in paths_in_module, (
        f"{module}.router does not register {expected_route}; "
        f"has {sorted(paths_in_module)}"
    )


def test_helpers_public_surface():
    """helpers.py must expose the shared utilities sub-routers depend on."""
    from filings.routers._redesign import helpers as h

    # Feature flags (also re-exported via redesign_preview for web.py).
    assert callable(h.is_enabled)
    assert callable(h.is_placeholders_enabled)
    assert callable(h.is_profile_preview_enabled)

    # Bounding utilities used everywhere.
    assert callable(h._bounded)
    assert callable(h._bounded_call)

    # Shell context (every redesign route calls this).
    assert callable(h._shell_context)

    # Shared formatters that were moved out of redesign_preview.py.
    assert callable(h._format_compact_dollars)
    assert callable(h._format_dollars_compact)
    assert callable(h._nice_axis_step)

    # Shared trade-type classifiers (home fetchers call these).
    assert callable(h._insiders_action)
    assert callable(h._insiders_format_title)
    assert callable(h._congress_action)

    # Shell-cookie constants notifications.py reads.
    assert isinstance(h._SHELL_NOTIF_COOKIE, str)
    assert isinstance(h._SHELL_NOTIF_COOKIE_MAX_AGE, int)


def test_home_module_can_import_shared_helpers():
    """The home module imports the helpers it needs from _redesign.helpers.

    Catches the failure mode where a helper got renamed or moved without
    updating home's import block -- which would crash at home import time.
    """
    from filings.routers._redesign.home import (  # noqa: F401
        _bounded_call,
        _build_cusip_ticker_map,
        _compact_amount_str,
        _compact_range_str,
        _congress_action,
        _insiders_action,
        _insiders_format_title,
        _shell_context,
        _short_date,
    )


def test_no_duplicate_route_paths():
    """Catch the failure mode where two sub-routers both register the
    same URL (would silently shadow at FastAPI level)."""
    from filings.routers import redesign_preview as rp

    seen: dict[tuple[str, str], int] = {}
    for r in rp.router.routes:
        if not hasattr(r, "path") or not hasattr(r, "methods"):
            continue
        for method in r.methods:
            key = (method, r.path)
            seen[key] = seen.get(key, 0) + 1

    duplicates = {k: v for k, v in seen.items() if v > 1}
    assert not duplicates, f"Duplicate routes registered: {duplicates}"


# ── Live render smoke tests ──────────────────────────────────────────
#
# Hit each extracted route through FastAPI TestClient and assert it
# either renders (200) or redirects (302).  Catches the failure mode
# static checks can't: "module imports fine, but the route's template
# render path raises because a context var is missing or a helper
# returns the wrong shape."
#
# These are the routes we've extracted to sub-routers so far.  Routes
# still living in redesign_preview.py (home, stock, funds, retail,
# macro) are out of scope -- they each have heavier upstream deps and
# deserve their own test class with proper mocking.


SMOKE_ROUTES: list[tuple[str, set[int]]] = [
    # (url, set of acceptable status codes)
    ("/support",           {200}),
    ("/support/thank-you", {200}),
    ("/profile",           {200, 302}),    # 302 to /login when unauth'd
    ("/watchlist",         {200, 302}),    # 302 to /login when unauth'd
    ("/insiders",          {200}),
    ("/notifications",     {200}),
    ("/congress",          {200}),
    ("/retail",            {200}),
    ("/funds",             {200}),
    # /macro is intentionally NOT in the smoke list -- it does ~12
    # parallel upstream fetches that all hit their (bounded) timeouts
    # without API keys configured locally, which makes the test take
    # 5+ minutes.  Static registration is verified above and the live
    # post-deploy curl in prod (which has data caches) validates it
    # actually renders correctly.
]


@pytest.fixture(scope="module")
def test_client():
    """FastAPI TestClient shared across smoke tests.

    Module-scoped so we pay the import cost once.  Lifespan starts but
    most of its async tasks fire-and-forget; they don't block the client.
    """
    from fastapi.testclient import TestClient
    from filings.web import app
    return TestClient(app)


@pytest.mark.parametrize("path,ok_statuses", SMOKE_ROUTES)
def test_extracted_route_renders(test_client, path, ok_statuses):
    """Each extracted route returns a successful or expected-redirect status.

    A 500 here means the refactor broke the route -- usually a missing
    import, a helper that returns the wrong shape, or a context var the
    template requires but the new module doesn't pass.
    """
    resp = test_client.get(path, follow_redirects=False)
    assert resp.status_code in ok_statuses, (
        f"{path}: got {resp.status_code}, expected one of {ok_statuses}. "
        f"Response head: {resp.text[:200]}"
    )


# ── Degraded-upstream tests ──────────────────────────────────────────
#
# Monkeypatch every blocking-upstream wrapper to raise TimeoutError,
# then hit each route and assert it doesn't crash (renders normally
# OR returns the GracefulRoute 503 fallback page).  Catches:
#   * bounded() fallback shape drift from template expectations
#   * ungated `await to_X(...)` inside a route handler
#   * any unhandled exception that escapes the route handler


# 503 = GracefulRoute fallback; 200 = bounded() fallback render;
# 301/302 = auth-gate redirect.
DEGRADED_OK_STATUSES = {200, 301, 302, 503}


@pytest.fixture
def mock_all_upstreams_timeout(monkeypatch):
    """Replace ``to_heavy`` / ``to_light`` / ``to_supabase`` with stubs
    that immediately raise TimeoutError.  Simulates what happens during
    a real upstream degradation event (yfinance rate limit, Supabase
    slowdown, etc.) without waiting for actual timeouts.
    """
    import asyncio

    async def _raise_timeout(*args, **kwargs):
        raise asyncio.TimeoutError("simulated upstream timeout")

    # Patch the source module so every importer sees the stub.
    monkeypatch.setattr("filings.concurrency.to_heavy",    _raise_timeout)
    monkeypatch.setattr("filings.concurrency.to_light",    _raise_timeout)
    monkeypatch.setattr("filings.concurrency.to_supabase", _raise_timeout)
    monkeypatch.setattr("filings.concurrency.to_upstream", _raise_timeout)
    yield


# Routes to verify under degraded conditions.  Mirrors SMOKE_ROUTES
# but accepts the GracefulRoute 503 fallback as "passed" -- a 503 with
# the error.html shell is the correct behaviour when something inside
# the route truly can't render.  A 500 (or hung connection) means
# either the route's bounded fallbacks are wrong shape OR an ungated
# upstream call escaped the safety net.
DEGRADED_ROUTES = [
    "/support",
    "/support/thank-you",
    "/profile",
    "/watchlist",
    "/insiders",
    "/notifications",
    "/congress",
    "/retail",
    "/funds",
    "/api/home/heatmap",
    "/api/home/activity",
    "/api/home/calendar",
]


@pytest.mark.parametrize("path", DEGRADED_ROUTES)
def test_route_survives_upstream_degradation(
    test_client, mock_all_upstreams_timeout, path,
):
    """Each route returns a non-500 even when every upstream times out.

    Today's prod crashes (Phase 1 fixed) were exactly this: yfinance
    got rate-limited, bounds fired, fallback dicts were missing keys
    the templates referenced, Jinja UndefinedError → 500.  With
    GracefulRoute in place AND the fallback shapes fixed in Phase 1,
    the same scenario now degrades to either a successful render
    (em-dashed widgets) or a 503 error page (full shell intact).
    """
    resp = test_client.get(path, follow_redirects=False)
    assert resp.status_code in DEGRADED_OK_STATUSES, (
        f"{path} crashed under simulated upstream timeout: "
        f"status={resp.status_code}, body head: {resp.text[:300]}"
    )


def test_graceful_route_catches_unhandled_exception(test_client):
    """A route that raises an arbitrary exception should return a 503
    via GracefulRoute, NOT propagate the exception up.

    Asserts that GracefulRoute is wired correctly: explicitly raising
    inside a fetcher should NOT crash the page.  Validates the safety
    net for crash classes we haven't predicted.
    """
    from unittest.mock import patch

    async def _boom(*args, **kwargs):
        raise RuntimeError("simulated crash in shell_context")

    # Patch where the support route's handler LOOKS UP _shell_context
    # (it imported it directly via `from ... import _shell_context`,
    # so the binding to patch is on the support module).
    with patch("filings.routers._redesign.support._shell_context", _boom):
        resp = test_client.get("/support", follow_redirects=False)

    # Should be the templated 503 or the static-HTML last-resort, never
    # a 500 or hung connection.
    assert resp.status_code == 503, (
        f"GracefulRoute didn't catch the exception: status={resp.status_code}"
    )
    # The error page mentions either "temporarily unavailable" (templated
    # path) or "Refresh" (last-resort HTML).
    body_lc = resp.text.lower()
    assert "temporarily unavailable" in body_lc or "refresh" in body_lc, (
        f"503 body doesn't look like the error page: {resp.text[:300]}"
    )
