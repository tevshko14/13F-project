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
from datetime import datetime, timedelta, timezone
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


_HASH_FILE = CACHE_DIR / "fund_hashes.json"


def _load_local_hashes() -> dict[str, str]:
    """Load {cache_key: content_hash} from local disk.

    Used to detect which funds changed in Supabase since last startup,
    so we only fetch the ones that actually changed (saves egress).
    """
    if not _HASH_FILE.exists():
        return {}
    try:
        return json.loads(_HASH_FILE.read_text())
    except (json.JSONDecodeError, OSError):
        return {}


def _save_local_hashes(hashes: dict[str, str]) -> None:
    """Persist {cache_key: content_hash} to disk for next startup."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    try:
        _HASH_FILE.write_text(json.dumps(hashes))
    except OSError as e:
        logger.debug("Could not save hash file: %s", e)


def load_cache_from_supabase() -> dict:
    """Load 13F fund data from Supabase L2 cache with delta detection.

    On first run (no local hash file), fetches all funds as before.
    On subsequent runs, compares content_hash values and only fetches
    funds whose data actually changed — typically saving 90%+ egress
    since 13F data only changes quarterly.

    Returns a dict keyed by CIK (same structure as ``load_cache()``),
    or ``{}`` if Supabase is unavailable / has no 13F data.
    """
    # ── Step 1: Fetch remote hashes (lightweight: ~2 KB for 84 funds) ──
    remote_hashes = supabase_cache.get_all_content_hashes("13f")

    if not remote_hashes:
        # Fallback: column might not exist yet, do full load
        return _load_cache_from_supabase_full()

    # ── Step 2: Compare with local hashes from last startup ──
    local_hashes = _load_local_hashes()
    local_cache = load_cache()  # Load existing disk cache

    changed_keys: list[str] = []
    for key, remote_hash in remote_hashes.items():
        if not remote_hash:
            # Hash not populated yet — must fetch
            changed_keys.append(key)
        elif local_hashes.get(key) != remote_hash:
            changed_keys.append(key)

    # CIKs in local cache that aren't in remote anymore (removed funds)
    remote_ciks = {k.replace("13f:", "") for k in remote_hashes}

    total_funds = len(remote_hashes)
    unchanged = total_funds - len(changed_keys)

    if not changed_keys and local_cache:
        # Nothing changed — reuse local cache entirely (zero egress!)
        logger.info(
            "All %d funds unchanged (hash match) — zero Supabase egress",
            total_funds,
        )
        _save_local_hashes(remote_hashes)
        return local_cache

    logger.info(
        "Delta load: %d/%d funds changed, %d unchanged — fetching only changed",
        len(changed_keys), total_funds, unchanged,
    )

    # ── Step 3: Start with local cache, then overwrite changed funds ──
    result = {cik: data for cik, data in local_cache.items() if cik in remote_ciks}

    for key in changed_keys:
        cik = key.replace("13f:", "", 1) if key.startswith("13f:") else None
        if not cik:
            continue
        data, _is_fresh = supabase_cache.get_cached_with_stale(key)
        if isinstance(data, dict):
            result[cik] = data

    logger.info(
        "Loaded %d funds (%d from Supabase, %d from local cache)",
        len(result), len(changed_keys), unchanged,
    )

    # ── Step 4: Save hashes + full cache to disk for next startup ──
    _save_local_hashes(remote_hashes)
    save_cache(result)

    return result


def _load_cache_from_supabase_full() -> dict:
    """Full load from Supabase — used when content_hash column is empty.

    Uses get_all_by_category() to fetch all 13f rows in paginated
    batches (~5 round-trips for 84 funds) instead of one query per
    fund (~85 round-trips).
    """
    rows = supabase_cache.get_all_by_category("13f", page_size=20)
    if rows is None:
        return {}

    now = datetime.now(timezone.utc)
    ttl = _get_effective_ttl()
    result: dict = {}
    stale_count = 0

    for row in rows:
        key = row.get("cache_key", "")
        if not key.startswith("13f:"):
            continue
        cik = key[4:]  # strip "13f:" prefix
        data = row.get("response_data")
        if not isinstance(data, dict):
            continue

        result[cik] = data

        # Determine freshness from created_at (approximates expires_at)
        created_at = row.get("created_at")
        if created_at:
            try:
                ts = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
                if (now - ts) > ttl:
                    stale_count += 1
            except (ValueError, TypeError):
                stale_count += 1
        else:
            stale_count += 1

    if result:
        fresh_count = len(result) - stale_count
        logger.info(
            "Loaded %d funds from Supabase L2 cache (%d fresh, %d stale)",
            len(result), fresh_count, stale_count,
        )

    # Save hashes for next startup (populate from the data we just loaded)
    hashes: dict[str, str] = {}
    for cik, data in result.items():
        hashes[f"13f:{cik}"] = supabase_cache._compute_hash(data)
    _save_local_hashes(hashes)
    save_cache(result)

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

    Hoists TTL and datetime.now() out of the loop so they're computed once
    instead of 84× (is_filing_season() can't change mid-iteration).
    """
    ttl = _get_effective_ttl()
    now = datetime.now()
    stale = []
    for cik in cik_list:
        fund_data = cache_data.get(cik)
        if fund_data is None:
            stale.append(cik)
            continue
        last_refreshed = fund_data.get("_last_refreshed")
        if not last_refreshed:
            stale.append(cik)
            continue
        try:
            refreshed_dt = datetime.fromisoformat(last_refreshed)
            if (now - refreshed_dt) > ttl:
                stale.append(cik)
        except (ValueError, TypeError):
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
        return _format_age(age)
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

def _archive_old_quarters(cik: str, quarterly_changes: list[dict], keep: int = 2) -> bool:
    """Archive quarters beyond *keep* to cold storage.

    Returns True if all uploads succeeded (safe to trim), False otherwise.
    On failure, the caller should NOT trim -- hot data stays intact.

    Uses cold_storage.is_available() to short-circuit immediately when
    the bucket is known to be missing (avoids 84× failing HTTP calls).
    """
    if len(quarterly_changes) <= keep:
        return True  # Nothing to archive

    from filings import cold_storage

    # Fast check: skip if bucket is known to be unavailable
    if not cold_storage.is_available():
        return False

    to_archive = quarterly_changes[keep:]
    for q in to_archive:
        period = q.get("period", "").replace(" ", "_")
        if not period:
            continue
        if not cold_storage.upload_json(f"13f/{cik}/quarterly/{period}.json", q):
            logger.warning("Cold storage upload failed for CIK %s period %s", cik, period)
            return False
    return True


def load_historical_quarters(cik: str) -> list[dict]:
    """Load archived quarterly changes from Supabase Storage (cold tier).

    Returns a list of quarter dicts sorted newest-first,
    or empty list on failure / if no archived quarters exist.
    """
    from filings import cold_storage

    try:
        prefix = f"13f/{cik}/quarterly"
        files = cold_storage.list_files(prefix)
        if not files:
            return []

        quarters: list[dict] = []
        for filename in files:
            path = f"13f/{cik}/quarterly/{filename}"
            q = cold_storage.download_json(path)
            if q and isinstance(q, dict):
                quarters.append(q)

        # Sort newest-first: Q4 2025, Q3 2025, Q2 2025, ...
        def _sort_key(q: dict) -> tuple[int, int]:
            try:
                parts = q["period"].split()  # "Q3 2024" -> ["Q3", "2024"]
                q_num = int(parts[0][1])
                year = int(parts[1])
                return (-year, -q_num)
            except (IndexError, ValueError, KeyError):
                return (0, 0)

        quarters.sort(key=_sort_key)
        return quarters
    except Exception as exc:
        logger.warning("Failed to load historical quarters for CIK %s: %s", cik, exc)
        return []


def refresh_single_fund(cik: str) -> dict | None:
    """Fetch fresh data for a single fund from SEC EDGAR. Synchronous.

    Returns stamped data dict on success, None on failure.
    On failure, existing cached data is preserved (stale-while-revalidate).

    Write-through: on success the data is also persisted to Supabase L2
    so that subsequent deploys can hydrate from there instantly.

    Hot/cold: archives quarters 3+ to Supabase Storage before trimming
    the blob to 2 quarters for the hot Postgres cache.

    Respects the per-session SEC call cap (see ``_check_sec_rate_limit``).
    """
    if not _check_sec_rate_limit():
        return None

    from filings.client import get_fund_summary
    try:
        _record_sec_call()
        data = get_fund_summary(cik)
        stamped = stamp_fund_data(data)

        # ── Archive older quarters to cold storage, then trim ──
        quarterly = stamped.get("quarterly_changes", [])
        if len(quarterly) > 2:
            if _archive_old_quarters(cik, quarterly, keep=2):
                stamped["quarterly_changes"] = quarterly[:2]
                logger.debug("Archived %d quarters for CIK %s", len(quarterly) - 2, cik)
            else:
                # Archive failed — keep all quarters in hot (safe fallback)
                # but trim to 2 anyway to prevent OOM during bulk sync.
                # Data is NOT lost: SEC EDGAR always has the full history.
                stamped["quarterly_changes"] = quarterly[:2]
                logger.info(
                    "Cold storage unavailable for CIK %s — trimmed to 2 quarters (full history available via SEC EDGAR)",
                    cik,
                )

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
