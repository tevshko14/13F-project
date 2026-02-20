"""Persistent caching layer for 13F filing data.

Uses a Stale-While-Revalidate strategy with three tiers:
  - L1: ``app.state.fund_cache`` (in-memory dict, process lifetime)
  - L2: Supabase ``api_cache`` table, category ``"13f"`` (survives deploys)
  - L3: Disk JSON at ``~/.13f-cache/fund_data.json`` (local fallback)

On startup the cache is hydrated from Supabase first
(``load_cache_from_supabase``), falling back to disk
(``load_cache``).  Every successful SEC EDGAR fetch writes through
to both Supabase and disk so subsequent deploys are instant.

Per-fund TTL: Each fund entry tracks its own ``_last_refreshed``
timestamp, allowing selective refresh of only stale funds.
"""

import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

from filings import supabase_cache

logger = logging.getLogger(__name__)

CACHE_DIR = Path(os.environ.get("CACHE_DIR", Path.home() / ".13f-cache"))
CACHE_FILE = CACHE_DIR / "fund_data.json"

# ── TTL Configuration ────────────────────────────────────────────────
# 13F data only changes quarterly. A 7-day TTL ensures we catch new filings
# within a week while avoiding unnecessary API calls.
REFRESH_INTERVAL = timedelta(days=7)

# During filing season (±15 days of deadline), use a shorter TTL
# so new filings appear faster.
FILING_SEASON_REFRESH_INTERVAL = timedelta(hours=12)

# On startup (deploy), use a much longer TTL so routine deploys
# never trigger a mass SEC refresh.  The periodic _poll_loop still
# uses the normal shorter TTL to catch new filings.
STARTUP_REFRESH_INTERVAL = timedelta(hours=48)


def _get_effective_ttl() -> timedelta:
    """Return the appropriate TTL based on filing season."""
    from filings.notifications import is_filing_season
    try:
        if is_filing_season():
            return FILING_SEASON_REFRESH_INTERVAL
        return REFRESH_INTERVAL
    except Exception:
        return REFRESH_INTERVAL


def _get_effective_ttl_seconds() -> int:
    """Return the effective TTL in seconds (for Supabase ``ttl_seconds``)."""
    return int(_get_effective_ttl().total_seconds())


# ── Core Cache Operations ────────────────────────────────────────────

def load_cache() -> dict:
    """Load all cached fund data from disk. Returns empty dict if no cache.

    The cache is a JSON dict keyed by CIK. Each value contains the fund's
    data plus a `_last_refreshed` ISO timestamp for per-fund staleness.
    """
    if not CACHE_FILE.exists():
        return {}
    try:
        data = json.loads(CACHE_FILE.read_text())
        logger.info("Loaded cache with %d funds from %s", len(data), CACHE_FILE)
        return data
    except (json.JSONDecodeError, OSError) as e:
        logger.warning("Cache load failed: %s — starting fresh", e)
        return {}


def load_cache_from_supabase() -> dict:
    """Load all 13F fund data from Supabase L2 cache.

    Returns a dict keyed by CIK (same structure as ``load_cache()``),
    or ``{}`` if Supabase is unavailable / has no 13F data.

    Called on startup as the primary cache source — the disk cache
    (``load_cache()``) is the fallback.
    """
    rows = supabase_cache.get_all_by_category("13f")
    if not rows:
        return {}

    result: dict = {}
    for row in rows:
        cache_key = row.get("cache_key", "")      # e.g. "13f:1067983"
        data = row.get("response_data", {})
        cik = cache_key.replace("13f:", "", 1) if cache_key.startswith("13f:") else None
        if cik and isinstance(data, dict):
            result[cik] = data

    if result:
        logger.info("Loaded %d funds from Supabase L2 cache", len(result))
    return result


def save_cache(data: dict) -> None:
    """Write all cached fund data to disk (atomic write via temp file)."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    tmp_file = CACHE_FILE.with_suffix(".tmp")
    try:
        tmp_file.write_text(json.dumps(data, indent=2))
        tmp_file.replace(CACHE_FILE)
    except OSError as e:
        logger.error("Cache save failed: %s", e)


# ── Staleness Checks ────────────────────────────────────────────────

def is_cache_stale(cache_data: dict | None = None) -> bool:
    """Check if the cache needs a background refresh.

    If *cache_data* is provided (e.g. loaded from Supabase), checks whether
    ANY fund in it is stale via per-fund ``_last_refreshed`` timestamps.
    Otherwise falls back to disk-file mtime (local dev).
    """
    if cache_data:
        # In-memory data from Supabase — check per-fund timestamps
        all_ciks = list(cache_data.keys())
        stale = get_stale_ciks(cache_data, all_ciks)
        return len(stale) > 0

    # Fallback: disk file mtime (local dev)
    if not CACHE_FILE.exists():
        return True
    try:
        mtime = datetime.fromtimestamp(CACHE_FILE.stat().st_mtime)
        return datetime.now() - mtime > _get_effective_ttl()
    except OSError:
        return True


def is_cache_stale_for_startup(cache_data: dict) -> bool:
    """Startup-specific staleness check with a longer TTL (48 h).

    Used in the ``lifespan()`` handler so that routine Railway deploys
    never trigger a mass SEC refresh.  The periodic ``_poll_loop`` still
    uses ``is_cache_stale()`` / ``get_stale_ciks()`` with the normal
    12-hour filing-season TTL.
    """
    if not cache_data:
        return True
    for fund in cache_data.values():
        if not isinstance(fund, dict):
            continue
        lr = fund.get("_last_refreshed")
        if not lr:
            return True  # No timestamp → treat as stale
        try:
            age = datetime.now() - datetime.fromisoformat(lr)
            if age > STARTUP_REFRESH_INTERVAL:
                return True
        except (ValueError, TypeError):
            return True
    return False


def is_fund_stale(fund_data: dict) -> bool:
    """Check if a single fund's data is older than the effective TTL.

    Uses the per-fund `_last_refreshed` timestamp. Falls back to True
    (stale) if no timestamp exists (backward compatibility).
    """
    last_refreshed = fund_data.get("_last_refreshed")
    if not last_refreshed:
        return True  # No timestamp → treat as stale
    try:
        refreshed_dt = datetime.fromisoformat(last_refreshed)
        return datetime.now() - refreshed_dt > _get_effective_ttl()
    except (ValueError, TypeError):
        return True


def get_stale_ciks(cache_data: dict, cik_list: list[str]) -> list[str]:
    """Return list of CIKs from cik_list whose cached data is stale.

    Allows selective refresh: only re-fetch funds that actually need it,
    avoiding unnecessary API calls for recently-refreshed funds.
    """
    stale = []
    for cik in cik_list:
        fund_data = cache_data.get(cik)
        if fund_data is None or is_fund_stale(fund_data):
            stale.append(cik)
    return stale


# ── Cache Metadata ───────────────────────────────────────────────────

def stamp_fund_data(data: dict) -> dict:
    """Add a `_last_refreshed` timestamp to fund data.

    Called after successfully fetching fresh data from SEC EDGAR.
    The timestamp is used by is_fund_stale() for per-fund TTL checks.
    """
    data["_last_refreshed"] = datetime.now().isoformat(timespec="seconds")
    return data


def _format_age(age: timedelta) -> str:
    """Format a timedelta into a human-readable age string."""
    secs = age.total_seconds()
    if secs < 60:
        return "Just now"
    elif secs < 3600:
        return f"{int(secs / 60)} min ago"
    elif secs < 86400:
        return f"{int(secs / 3600)} hours ago"
    else:
        return f"{int(secs / 86400)} days ago"


def get_cache_age_str(cache_data: dict | None = None) -> str:
    """Return human-readable cache age string.

    If *cache_data* is provided, uses the oldest ``_last_refreshed``
    timestamp across all funds.  Otherwise falls back to disk-file mtime.
    """
    # ── Try in-memory data first (works on Railway where no disk file exists)
    if cache_data:
        oldest: datetime | None = None
        for fund in cache_data.values():
            lr = fund.get("_last_refreshed") if isinstance(fund, dict) else None
            if lr:
                try:
                    dt = datetime.fromisoformat(lr)
                    if oldest is None or dt < oldest:
                        oldest = dt
                except (ValueError, TypeError):
                    pass
        if oldest:
            return _format_age(datetime.now() - oldest)

    # ── Fallback: disk file mtime (local dev) ──
    if not CACHE_FILE.exists():
        return "No cache"
    try:
        mtime = datetime.fromtimestamp(CACHE_FILE.stat().st_mtime)
        return _format_age(datetime.now() - mtime)
    except OSError:
        return "Unknown"


def get_fund_age_str(fund_data: dict) -> str:
    """Return human-readable age of a single fund's cached data."""
    last_refreshed = fund_data.get("_last_refreshed")
    if not last_refreshed:
        return "Unknown"
    try:
        refreshed_dt = datetime.fromisoformat(last_refreshed)
        age = datetime.now() - refreshed_dt
        if age.total_seconds() < 60:
            return "Just now"
        elif age.total_seconds() < 3600:
            return f"{int(age.total_seconds() / 60)} min ago"
        elif age.total_seconds() < 86400:
            return f"{int(age.total_seconds() / 3600)} hours ago"
        else:
            return f"{int(age.total_seconds() / 86400)} days ago"
    except (ValueError, TypeError):
        return "Unknown"


# ── SEC Rate Limiting ────────────────────────────────────────────────
# Guard against excessive SEC API calls during deploys / background refreshes.
_sec_calls_this_session = 0
_SEC_MAX_CALLS_PER_SESSION = int(os.environ.get("SEC_MAX_CALLS", "200"))
_SEC_BATCH_SIZE = 10               # Pause after every N funds
_SEC_BATCH_PAUSE = 5               # Seconds to pause between batches


def _check_sec_rate_limit() -> bool:
    """Return True if we're OK to make another SEC API call."""
    if _sec_calls_this_session >= _SEC_MAX_CALLS_PER_SESSION:
        logger.warning(
            "SEC session call limit reached (%d) — stopping further requests",
            _SEC_MAX_CALLS_PER_SESSION,
        )
        return False
    return True


def _record_sec_call() -> None:
    """Record that we made a SEC API call."""
    global _sec_calls_this_session
    _sec_calls_this_session += 1


# ── Refresh Operations ───────────────────────────────────────────────

def refresh_single_fund(cik: str) -> dict | None:
    """Fetch fresh data for a single fund from SEC EDGAR. Synchronous.

    Returns stamped data dict on success, None on failure.
    On failure, existing cached data is preserved (stale-while-revalidate).

    Write-through: on success the data is also persisted to Supabase L2
    so that subsequent deploys can hydrate from there instantly.

    Respects the per-session SEC call cap (see ``_check_sec_rate_limit``).
    """
    if not _check_sec_rate_limit():
        return None

    from filings.client import get_fund_summary
    try:
        _record_sec_call()
        data = get_fund_summary(cik)
        stamped = stamp_fund_data(data)

        # ── Persist to Supabase L2 (non-fatal) ──
        try:
            supabase_cache.set_cached(
                cache_key=f"13f:{cik}",
                category="13f",
                data=stamped,
                ttl_seconds=_get_effective_ttl_seconds(),
            )
        except Exception as sb_exc:
            logger.debug("Supabase write-through failed for CIK %s: %s", cik, sb_exc)

        return stamped
    except Exception as e:
        logger.warning("Failed to refresh CIK %s: %s — keeping stale data", cik, e)
        return None
