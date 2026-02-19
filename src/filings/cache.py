"""Persistent caching layer for 13F filing data.

Uses a Stale-While-Revalidate strategy:
  1. Serve cached data immediately (never block on API calls)
  2. Refresh stale funds in the background
  3. Keep old data on API failure (never lose data)

Storage: JSON file on disk at ~/.13f-cache/fund_data.json
  - Configurable via CACHE_DIR environment variable
  - For Railway: set CACHE_DIR to a mounted volume path for persistence
    across deployments (e.g. /data/cache)

Per-fund TTL: Each fund entry tracks its own `_last_refreshed` timestamp,
allowing selective refresh of only stale funds instead of all-or-nothing.
"""

import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

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


def _get_effective_ttl() -> timedelta:
    """Return the appropriate TTL based on filing season."""
    from filings.notifications import is_filing_season
    try:
        if is_filing_season():
            return FILING_SEASON_REFRESH_INTERVAL
        return REFRESH_INTERVAL
    except Exception:
        return REFRESH_INTERVAL


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

def is_cache_stale() -> bool:
    """Check if the overall cache file is older than the effective TTL.

    Used on startup to decide whether to trigger a background refresh.
    """
    if not CACHE_FILE.exists():
        return True
    try:
        mtime = datetime.fromtimestamp(CACHE_FILE.stat().st_mtime)
        return datetime.now() - mtime > _get_effective_ttl()
    except OSError:
        return True


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


def get_cache_age_str() -> str:
    """Return human-readable cache age string."""
    if not CACHE_FILE.exists():
        return "No cache"
    try:
        mtime = datetime.fromtimestamp(CACHE_FILE.stat().st_mtime)
        age = datetime.now() - mtime
        if age.total_seconds() < 60:
            return "Just now"
        elif age.total_seconds() < 3600:
            return f"{int(age.total_seconds() / 60)} min ago"
        elif age.total_seconds() < 86400:
            return f"{int(age.total_seconds() / 3600)} hours ago"
        else:
            return f"{int(age.total_seconds() / 86400)} days ago"
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


# ── Refresh Operations ───────────────────────────────────────────────

def refresh_single_fund(cik: str) -> dict | None:
    """Fetch fresh data for a single fund from SEC EDGAR. Synchronous.

    Returns stamped data dict on success, None on failure.
    On failure, existing cached data is preserved (stale-while-revalidate).
    """
    from filings.client import get_fund_summary
    try:
        data = get_fund_summary(cik)
        return stamp_fund_data(data)
    except Exception as e:
        logger.warning("Failed to refresh CIK %s: %s — keeping stale data", cik, e)
        return None
