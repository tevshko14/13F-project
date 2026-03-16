"""Shared FMP earnings-calendar cache for PaperPanda.

Single fetch, shared by all consumers.  FMP free tier allows 250 API
calls/day and 500 MB bandwidth/30 days, so we fetch the bulk
``/stable/earnings-calendar`` endpoint at most once every 6 hours and
serve all consumers from the in-memory + Supabase cache.

Consumers
---------
- ``earnings.py``           — per-ticker revenue enrichment
- ``earnings_scorecard.py`` — scorecard fallback, trend chart fallback
- ``earnings_calendar.py``  — calendar grid fallback

Cache tiers
-----------
L1  In-memory list  (6 h TTL, thread-safe)
L2  Supabase ``api_cache`` table  (24 h TTL, survives restarts)
L3  FMP ``/stable/earnings-calendar`` API call
"""

from __future__ import annotations

import logging
import os
import threading
import time
from datetime import datetime, timedelta

import httpx

from filings import supabase_cache

logger = logging.getLogger(__name__)

# ── Config ───────────────────────────────────────────────────────
_FMP_STABLE = "https://financialmodelingprep.com/stable"
_TIMEOUT = 15
_L1_TTL = 21_600       # 6 hours — in-memory
_L2_TTL = 86_400       # 24 hours — Supabase
_CACHE_KEY = "fmp_earnings_bulk"
_CACHE_CATEGORY = "fmp_earnings"
_LOOKBACK_DAYS = 21    # free tier caps total range at ~52 days
_LOOKAHEAD_DAYS = 30   # how far forward

# ── L1 in-memory cache ──────────────────────────────────────────
_l1_data: list[dict] | None = None
_l1_ts: float = 0.0
_lock = threading.Lock()


# ── Internal helpers ─────────────────────────────────────────────

def _fmp_key() -> str:
    return os.environ.get("FMP_API_KEY", "").strip()


def _fetch_from_api() -> list[dict] | None:
    """Single HTTP call to FMP /stable/earnings-calendar.

    Returns the raw JSON list on success, None on error.
    """
    key = _fmp_key()
    if not key:
        return None

    today = datetime.now()
    from_date = (today - timedelta(days=_LOOKBACK_DAYS)).strftime("%Y-%m-%d")
    to_date = (today + timedelta(days=_LOOKAHEAD_DAYS)).strftime("%Y-%m-%d")

    try:
        r = httpx.get(
            f"{_FMP_STABLE}/earnings-calendar",
            params={"from": from_date, "to": to_date, "apikey": key},
            timeout=_TIMEOUT,
        )
        r.raise_for_status()
        data = r.json()

        # FMP returns dict with error messages on plan issues
        if isinstance(data, dict):
            err = (
                data.get("Error Message")
                or data.get("message")
                or data.get("error")
            )
            if err:
                logger.warning("FMP earnings-calendar plan error: %s", err)
                return None

        if not isinstance(data, list):
            logger.warning("FMP earnings-calendar unexpected type: %s", type(data))
            return None

        logger.info(
            "FMP earnings-calendar fetched %d entries (%s to %s)",
            len(data), from_date, to_date,
        )
        return data

    except Exception:
        logger.warning("FMP earnings-calendar fetch failed", exc_info=True)
        return None


def _persist_to_l2(data: list[dict]) -> None:
    """Write bulk data to Supabase api_cache (fire-and-forget)."""
    try:
        supabase_cache.set_cached(
            _CACHE_KEY,
            _CACHE_CATEGORY,
            {"entries": data},
            ttl_seconds=_L2_TTL,
        )
    except Exception:
        logger.debug("Failed to persist FMP cache to Supabase", exc_info=True)


def _load_from_l2() -> tuple[list[dict] | None, bool]:
    """Load bulk data from Supabase.

    Returns (data, is_fresh).  Stale data is still usable as fallback.
    """
    try:
        raw, is_fresh = supabase_cache.get_cached_with_stale(_CACHE_KEY)
        if raw and isinstance(raw, dict):
            entries = raw.get("entries")
            if isinstance(entries, list):
                return entries, is_fresh
    except Exception:
        logger.debug("Failed to load FMP cache from Supabase", exc_info=True)
    return None, False


# ── Field-name helpers (stable API uses different keys than v3) ──

def actual_eps(item: dict) -> float | None:
    """Extract actual EPS from an FMP record (handles both field names)."""
    v = item.get("epsActual")
    return v if v is not None else item.get("eps")


def actual_rev(item: dict) -> float | None:
    """Extract actual revenue from an FMP record (handles both field names)."""
    v = item.get("revenueActual")
    return v if v is not None else item.get("revenue")


# ── Public API ───────────────────────────────────────────────────

def get_bulk_earnings(force_refresh: bool = False) -> list[dict]:
    """Return the full cached bulk earnings list.

    Flow: L1 (6 h) → L2 (24 h) → API → stale L2 fallback → [].
    Thread-safe; concurrent callers block while one fetch runs.
    """
    global _l1_data, _l1_ts

    now = time.time()

    # ── L1 hit ───────────────────────────────────────────────────
    if not force_refresh and _l1_data is not None and (now - _l1_ts) < _L1_TTL:
        return _l1_data

    to_persist: list[dict] | None = None

    with _lock:
        now = time.time()  # recapture after acquiring lock

        # Double-check after acquiring lock
        if not force_refresh and _l1_data is not None and (now - _l1_ts) < _L1_TTL:
            return _l1_data

        # ── L2 check ────────────────────────────────────────────
        l2_data, l2_fresh = _load_from_l2()
        if l2_fresh and l2_data and not force_refresh:
            _l1_data = l2_data
            _l1_ts = now
            logger.debug("FMP cache populated from L2 (%d entries)", len(l2_data))
            return _l1_data

        # ── L3: API fetch ───────────────────────────────────────
        api_data = _fetch_from_api()
        if api_data is not None:
            _l1_data = api_data
            _l1_ts = now
            to_persist = api_data  # persist outside the lock
            result = _l1_data
        elif l2_data:
            # ── Stale L2 fallback ───────────────────────────────
            logger.info("FMP API failed, using stale L2 cache (%d entries)", len(l2_data))
            _l1_data = l2_data
            _l1_ts = now  # prevent hammering API on every call
            result = _l1_data
        else:
            # ── Nothing available ───────────────────────────────
            logger.warning("FMP cache: no data available (no key, API down, or empty)")
            result = []

    # Persist to L2 outside the lock (fire-and-forget)
    if to_persist is not None:
        _persist_to_l2(to_persist)

    return result


def get_earnings_for_ticker(ticker: str) -> list[dict]:
    """Filter cached bulk data for a single ticker symbol."""
    upper = ticker.upper()
    return [e for e in get_bulk_earnings() if e.get("symbol", "").upper() == upper]


def get_earnings_in_range(start: str, end: str) -> list[dict]:
    """Filter cached bulk data to a date range (inclusive).

    Args:
        start: YYYY-MM-DD
        end:   YYYY-MM-DD
    """
    return [
        e for e in get_bulk_earnings()
        if start <= e.get("date", "") <= end
    ]


def get_revenue_for_ticker(ticker: str) -> dict[str, dict] | None:
    """Revenue enrichment lookup for a single ticker.

    Returns ``{date_str: {"revenue": ..., "revenueEstimated": ...}}``
    or ``None`` if no data.  Drop-in replacement for the old
    ``earnings.py:_fetch_fmp_revenue_raw()``.
    """
    entries = get_earnings_for_ticker(ticker)
    if not entries:
        return None

    lookup: dict[str, dict] = {}
    for item in entries:
        d = item.get("date")
        rev = actual_rev(item)
        rev_est = item.get("revenueEstimated")
        if d and (rev is not None or rev_est is not None):
            lookup[d] = {"revenue": rev, "revenueEstimated": rev_est}

    return lookup if lookup else None
