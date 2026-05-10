"""Shared module-level helpers and state used by both ``web.py`` and
``routers/*.py``.  Kept small and dependency-light (no heavy imports like
``client`` or ``market_data``) so it can be imported early without
triggering a startup cascade.

web.py keeps the FastAPI ``app`` instance and all module-level caches
(``_logo_cache``, ``_admin_cache``, …) — routers reach state that lives
on the app via ``request.app.state`` or direct import from ``filings.web``
(safe because routers are loaded *after* web.py is fully initialized).
"""

from __future__ import annotations

import re
from pathlib import Path

from fastapi import Request
from fastapi.templating import Jinja2Templates

# ── Templates ─────────────────────────────────────────────────────────
# Single shared Jinja2Templates instance.  web.py still registers
# ``templates.env.globals`` and ``templates.env.filters`` after auth /
# market data / etc. have loaded — routers should only *render* templates,
# never modify env state.
templates = Jinja2Templates(directory=Path(__file__).parent / "templates")

# Register the canonical date filters here (rather than in web.py) so
# they're available to every template — including routers that render
# before web.py's late-init filters land.  Filters: dt_long / dt_short /
# dt_dow → "MMM DD YYYY" / "MMM DD" / "Wed, MMM DD YYYY".
from filings.dates_format import register_jinja_filters as _register_date_filters  # noqa: E402
_register_date_filters(templates.env)


# ── Input validation regexes ──────────────────────────────────────────
# Same patterns historically defined in web.py; kept here so routers can
# validate path parameters without importing web.py (which would be a
# circular import).
_TICKER_RE = re.compile(r"^[A-Za-z][A-Za-z0-9.]{0,11}$")
_CIK_RE = re.compile(r"^[0-9]{1,10}$")
_CUSIP_RE = re.compile(r"^[A-Za-z0-9]{6,9}$")
_MEMBER_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,40}$")


def valid_ticker(ticker: str) -> bool:
    """Return True if *ticker* looks like a valid stock symbol."""
    return bool(_TICKER_RE.match(ticker))


def valid_cik(cik: str) -> bool:
    """Return True if *cik* looks like a valid SEC CIK (1-10 digits)."""
    return bool(_CIK_RE.match(cik))


def valid_cusip(cusip: str) -> bool:
    """Return True if *cusip* looks like a valid CUSIP (6-9 alphanumeric)."""
    return bool(_CUSIP_RE.match(cusip))


def valid_member_id(member_id: str) -> bool:
    """Return True if *member_id* looks like a valid Congress member ID."""
    return bool(_MEMBER_ID_RE.match(member_id))


def real_ip(request: Request) -> str:
    """Return the real client IP, trusting Railway's X-Forwarded-For header.

    Railway (and most reverse proxies) append the true client IP as the
    first value in X-Forwarded-For.  Falling back to request.client.host
    would give the proxy's internal IP, causing all users to share one
    rate-limit bucket.
    """
    forwarded_for = request.headers.get("x-forwarded-for")
    if forwarded_for:
        return forwarded_for.split(",")[0].strip()
    if request.client:
        return request.client.host
    return "unknown"


# ── Rate limiter (slowapi) ────────────────────────────────────────────
# Lives here (rather than in web.py) so any router can attach
# ``@limiter.limit(...)`` decorators at function-definition time
# without a circular import back through web.py.  Falls back to
# ``None`` when slowapi isn't installed -- callers must guard with
# ``has_limiter`` (or simply check ``limiter is not None``).
try:
    from slowapi import Limiter
    from slowapi.errors import RateLimitExceeded  # noqa: F401  (re-exported)

    limiter: Limiter | None = Limiter(key_func=real_ip, default_limits=["60/minute"])
    has_limiter: bool = True
except ImportError:
    limiter = None
    has_limiter = False
