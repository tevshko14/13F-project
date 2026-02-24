"""Alternative-data "Vitals" tab — employee, culture & product signals.

Combines three data sources into a single tab:
  1. Employee Pulse (People Data Labs) – headcount, size, industry
  2. Culture (Glassdoor via RapidAPI) – ratings, CEO approval, outlook
  3. Product Sentiment (Apple iTunes Search) – app rating, reviews

Glassdoor uses a **quota-first persistent caching** strategy:
  - L2 persistent cache in Supabase Postgres (survives Railway deploys)
  - L3 fallback: disk cache at ~/.13f-cache/glassdoor_cache.json
  - Monthly quota tracker (hard cap, auto-resets each month)
  - Stale-while-revalidate: returns stale data instantly, refreshes in background
  - Deployments NEVER trigger batch refreshes (lazy hydration only)
  - Supabase is optional: if SUPABASE_URL is not set, disk cache is used

PDL and App Store use in-memory caches with aggressive TTLs.
Each source degrades gracefully when its API key is missing.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
import urllib.parse
import urllib.request
from datetime import datetime

from filings import supabase_cache
from filings.cache import CACHE_DIR

logger = logging.getLogger(__name__)

# ── Thread lock for all cache reads/writes ────────────────────────────
_lock = threading.Lock()

# ── Cache TTLs ──────────────────────────────────────────────────────
_GLASSDOOR_TTL = 2_592_000  # 30 days — ratings change very slowly
_PDL_TTL = 604_800  # 7 days — headcount changes slowly
_APPSTORE_TTL = 604_800  # 7 days — app ratings change slowly

# ── Glassdoor quota configuration ────────────────────────────────────
MAX_MONTHLY_QUOTA = 90  # Hard cap on Glassdoor API calls/month
_GLASSDOOR_STALE_REFRESH_CAP = 80  # Only background-refresh stale entries below this
GLASSDOOR_CACHE_FILE = CACHE_DIR / "glassdoor_cache.json"

# ── LRU max entries for per-ticker caches ─────────────────────────────
_MAX_CACHE_ENTRIES = 2000

# ── Per-ticker caches: {TICKER: (timestamp, data | None)} ───────────
_glassdoor_cache: dict[str, tuple[float, dict | None]] = {}
_pdl_cache: dict[str, tuple[float, dict | None]] = {}
_appstore_cache: dict[str, tuple[float, dict | None]] = {}

# ── Persistent cache state (lazy-hydrated from Supabase on first use) ─
_glassdoor_hydrated = False
_pdl_hydrated = False
_appstore_hydrated = False
_pending_refreshes: set[str] = set()  # Prevent duplicate concurrent refreshes

# ── PDL quota configuration ──────────────────────────────────────────
MAX_MONTHLY_PDL_QUOTA = 100  # Free tier: 100 calls/month


def _evict_oldest(cache: dict, max_size: int = _MAX_CACHE_ENTRIES) -> None:
    """Evict oldest entries if cache exceeds max_size. Must hold _lock."""
    if len(cache) <= max_size:
        return
    sorted_keys = sorted(cache, key=lambda k: cache[k][0])
    for k in sorted_keys[: len(cache) - max_size]:
        del cache[k]


# ═══════════════════════════════════════════════════════════════════
# Glassdoor Persistent Cache (disk-backed, survives deploys)
# ═══════════════════════════════════════════════════════════════════


def _load_glassdoor_disk_cache() -> dict:
    """Load Glassdoor cache from disk.

    Returns dict with structure:
    {
        "quota": {"month": "2026-02", "count": 14},
        "entries": {"AAPL": {"ts": 1740000000.0, "data": {...} | null}, ...}
    }
    """
    if not GLASSDOOR_CACHE_FILE.exists():
        return {"quota": {"month": "", "count": 0}, "entries": {}}
    try:
        raw = json.loads(GLASSDOOR_CACHE_FILE.read_text())
        if "quota" not in raw:
            raw["quota"] = {"month": "", "count": 0}
        if "entries" not in raw:
            raw["entries"] = {}
        return raw
    except (json.JSONDecodeError, OSError) as e:
        logger.warning("Glassdoor cache load failed: %s — starting fresh", e)
        return {"quota": {"month": "", "count": 0}, "entries": {}}


def _save_glassdoor_disk_cache(disk_data: dict) -> None:
    """Atomic write of Glassdoor cache to disk (temp file + replace)."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    tmp = GLASSDOOR_CACHE_FILE.with_suffix(".tmp")
    try:
        tmp.write_text(json.dumps(disk_data, indent=2))
        tmp.replace(GLASSDOOR_CACHE_FILE)
    except OSError as e:
        logger.error("Glassdoor cache save failed: %s", e)


def _persist_glassdoor_entry(ticker_key: str, ts: float, data: dict | None) -> None:
    """Write a single Glassdoor cache entry.  Must hold _lock.

    Strategy: try Supabase first, then fall back to disk JSON.
    Both paths are attempted so that Supabase and disk stay in sync
    when both are available.
    """
    # ── Supabase L2 ──
    if data is not None:
        payload = {"ts": ts, "data": data}
        supabase_cache.set_cached(
            cache_key=f"glassdoor:{ticker_key}",
            category="glassdoor",
            data=payload,
            ttl_seconds=None,  # managed by our own TTL logic
        )

    # ── Disk fallback (always, keeps local cache warm) ──
    disk = _load_glassdoor_disk_cache()
    entries = disk.setdefault("entries", {})
    entries[ticker_key] = {"ts": ts, "data": data}
    # Evict oldest entries from disk if too large
    if len(entries) > _MAX_CACHE_ENTRIES:
        sorted_keys = sorted(entries, key=lambda k: entries[k].get("ts", 0))
        for k in sorted_keys[: len(entries) - _MAX_CACHE_ENTRIES]:
            del entries[k]
    _save_glassdoor_disk_cache(disk)


def _hydrate_glassdoor_cache() -> None:
    """One-time load into in-memory _glassdoor_cache.

    Strategy: try Supabase first (one query for all Glassdoor entries),
    fall back to local disk JSON if Supabase is unavailable.

    Called lazily on first Glassdoor data request.  Does NOT trigger
    any API calls (requirement: no batch refresh on deploy).
    """
    global _glassdoor_hydrated
    if _glassdoor_hydrated:
        return
    _glassdoor_hydrated = True

    # ── Try Supabase first ──
    rows = supabase_cache.get_all_by_category("glassdoor")
    if rows is not None and len(rows) > 0:
        loaded = 0
        for row in rows:
            cache_key = row.get("cache_key", "")  # e.g. "glassdoor:AAPL"
            payload = row.get("response_data", {})
            # Extract ticker from cache_key
            ticker_key = (
                cache_key.replace("glassdoor:", "", 1)
                if cache_key.startswith("glassdoor:")
                else cache_key
            )
            ts = payload.get("ts", 0.0)
            data = payload.get("data")
            if ticker_key:
                _glassdoor_cache[ticker_key] = (ts, data)
                loaded += 1
        if loaded:
            logger.info(
                "Hydrated Glassdoor in-memory cache with %d entries from Supabase",
                loaded,
            )
        return

    # ── Fallback: disk cache ──
    disk = _load_glassdoor_disk_cache()
    entries = disk.get("entries", {})
    loaded = 0
    for ticker_key, entry in entries.items():
        ts = entry.get("ts", 0.0)
        data = entry.get("data")  # may be None (cached miss)
        _glassdoor_cache[ticker_key] = (ts, data)
        loaded += 1
    if loaded:
        logger.info(
            "Hydrated Glassdoor in-memory cache with %d entries from disk", loaded
        )


# ═══════════════════════════════════════════════════════════════════
# Glassdoor Monthly Quota Tracker (persistent)
# ═══════════════════════════════════════════════════════════════════


def _get_current_month_str() -> str:
    """Return current month as 'YYYY-MM' string."""
    return datetime.now().strftime("%Y-%m")


def _check_glassdoor_quota(threshold: int = MAX_MONTHLY_QUOTA) -> bool:
    """Return True if we can still make Glassdoor API calls this month.

    Strategy: try Supabase first, fall back to disk.  If the stored
    month doesn't match the current month, the counter is effectively 0.

    Args:
        threshold: The quota limit to check against. Default is
            MAX_MONTHLY_QUOTA (90). Pass _GLASSDOOR_STALE_REFRESH_CAP (80)
            for stale-data background refresh checks.
    """
    current_month = _get_current_month_str()

    # ── Try Supabase first ──
    sb_quota = supabase_cache.get_quota("glassdoor", current_month)
    if sb_quota is not None:
        return sb_quota.get("count", 0) < threshold

    # ── Fallback: disk ──
    disk = _load_glassdoor_disk_cache()
    quota = disk.get("quota", {})

    if quota.get("month") != current_month:
        return True  # New month -- counter is effectively 0

    return quota.get("count", 0) < threshold


def _increment_glassdoor_quota() -> int:
    """Increment the monthly API call counter. Returns new count.

    Strategy: try Supabase first, then always update disk as fallback.
    Auto-resets if the stored month differs from the current month.
    Must hold _lock.
    """
    current_month = _get_current_month_str()

    # ── Supabase L2 ──
    sb_count = supabase_cache.increment_quota("glassdoor", current_month)
    if sb_count > 0:
        logger.info(
            "Glassdoor monthly quota (Supabase): %d/%d", sb_count, MAX_MONTHLY_QUOTA
        )

    # ── Disk (always update to keep in sync) ──
    disk = _load_glassdoor_disk_cache()
    quota = disk.get("quota", {})

    if quota.get("month") != current_month:
        quota = {"month": current_month, "count": 0}

    quota["count"] = quota.get("count", 0) + 1
    disk["quota"] = quota
    _save_glassdoor_disk_cache(disk)

    # Prefer Supabase count as source of truth when available
    final_count = sb_count if sb_count > 0 else quota["count"]
    logger.info("Glassdoor monthly quota: %d/%d", final_count, MAX_MONTHLY_QUOTA)
    return final_count


# ═══════════════════════════════════════════════════════════════════
# PDL Persistent Cache (Supabase-backed)
# ═══════════════════════════════════════════════════════════════════


def _hydrate_pdl_cache() -> None:
    """One-time load of PDL data from Supabase into in-memory cache.

    Called lazily on first PDL data request.  No API calls triggered.
    """
    global _pdl_hydrated
    if _pdl_hydrated:
        return
    _pdl_hydrated = True

    rows = supabase_cache.get_all_by_category("pdl")
    if rows is not None and len(rows) > 0:
        loaded = 0
        for row in rows:
            cache_key = row.get("cache_key", "")  # e.g. "pdl:AAPL"
            payload = row.get("response_data", {})
            ticker_key = (
                cache_key.replace("pdl:", "", 1)
                if cache_key.startswith("pdl:")
                else cache_key
            )
            ts = payload.get("ts", 0.0)
            data = payload.get("data")
            if ticker_key:
                _pdl_cache[ticker_key] = (ts, data)
                loaded += 1
        if loaded:
            logger.info(
                "Hydrated PDL in-memory cache with %d entries from Supabase", loaded
            )


def _persist_pdl_entry(ticker_key: str, ts: float, data: dict | None) -> None:
    """Write a single PDL cache entry to Supabase. Must hold _lock."""
    payload = {"ts": ts, "data": data}
    supabase_cache.set_cached(
        cache_key=f"pdl:{ticker_key}",
        category="pdl",
        data=payload,
        ttl_seconds=None,  # managed by our own TTL logic
    )


# ── PDL Monthly Quota Tracker ────────────────────────────────────────


def _check_pdl_quota(threshold: int = MAX_MONTHLY_PDL_QUOTA) -> bool:
    """Return True if we can still make PDL API calls this month."""
    current_month = _get_current_month_str()
    sb_quota = supabase_cache.get_quota("pdl", current_month)
    if sb_quota is not None:
        return sb_quota.get("count", 0) < threshold
    return True  # No quota data → assume fresh month


def _increment_pdl_quota() -> int:
    """Increment the PDL monthly API call counter. Returns new count."""
    current_month = _get_current_month_str()
    sb_count = supabase_cache.increment_quota("pdl", current_month)
    if sb_count > 0:
        logger.info(
            "PDL monthly quota (Supabase): %d/%d", sb_count, MAX_MONTHLY_PDL_QUOTA
        )
    return sb_count


def get_pdl_quota_info() -> dict:
    """Return current PDL quota status for diagnostics."""
    current_month = _get_current_month_str()
    sb_quota = supabase_cache.get_quota("pdl", current_month)
    count = (sb_quota or {}).get("count", 0)
    return {
        "month": current_month,
        "count": count,
        "max": MAX_MONTHLY_PDL_QUOTA,
        "remaining": max(0, MAX_MONTHLY_PDL_QUOTA - count),
        "exhausted": count >= MAX_MONTHLY_PDL_QUOTA,
    }


# ═══════════════════════════════════════════════════════════════════
# App Store Persistent Cache (Supabase-backed)
# ═══════════════════════════════════════════════════════════════════


def _hydrate_appstore_cache() -> None:
    """One-time load of App Store data from Supabase into in-memory cache.

    Called lazily on first App Store data request.  No API calls triggered.
    """
    global _appstore_hydrated
    if _appstore_hydrated:
        return
    _appstore_hydrated = True

    rows = supabase_cache.get_all_by_category("appstore")
    if rows is not None and len(rows) > 0:
        loaded = 0
        for row in rows:
            cache_key = row.get("cache_key", "")  # e.g. "appstore:AAPL"
            payload = row.get("response_data", {})
            ticker_key = (
                cache_key.replace("appstore:", "", 1)
                if cache_key.startswith("appstore:")
                else cache_key
            )
            ts = payload.get("ts", 0.0)
            data = payload.get("data")
            if ticker_key:
                _appstore_cache[ticker_key] = (ts, data)
                loaded += 1
        if loaded:
            logger.info(
                "Hydrated App Store in-memory cache with %d entries from Supabase",
                loaded,
            )


def _persist_appstore_entry(ticker_key: str, ts: float, data: dict | None) -> None:
    """Write a single App Store cache entry to Supabase. Must hold _lock."""
    payload = {"ts": ts, "data": data}
    supabase_cache.set_cached(
        cache_key=f"appstore:{ticker_key}",
        category="appstore",
        data=payload,
        ttl_seconds=None,
    )


# ── Shared HTTP helper ────────────────────────────────────────────────

_BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


def _http_get_json(
    url: str, headers: dict | None = None, timeout: int = 10
) -> dict | list | None:
    """Simple GET→JSON helper using stdlib urllib."""
    req = urllib.request.Request(url)
    req.add_header("User-Agent", _BROWSER_UA)
    if headers:
        for k, v in headers.items():
            req.add_header(k, v)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode())
    except Exception as exc:
        logger.debug("HTTP GET %s failed: %s", url, exc)
        return None


# ── Shared company-name resolver ──────────────────────────────────────


def _resolve_company_name(ticker: str) -> str | None:
    """Resolve ticker to company name via yfinance (reuses existing pattern)."""
    try:
        import yfinance as yf

        tk = yf.Ticker(ticker.upper())
        info = tk.info or {}
        return info.get("longName") or info.get("shortName")
    except Exception as exc:
        logger.debug("yfinance company name lookup failed for %s: %s", ticker, exc)
        return None


# ── Key checks ────────────────────────────────────────────────────────


def has_glassdoor_key() -> bool:
    return bool(os.environ.get("GLASSDOOR_RAPIDAPI_KEY"))


def has_pdl_key() -> bool:
    return bool(os.environ.get("PDL_API_KEY"))


# ═══════════════════════════════════════════════════════════════════
# 1. Employee Pulse — People Data Labs
# ═══════════════════════════════════════════════════════════════════


def _get_pdl_data(ticker: str) -> dict | None:
    """Fetch company employee data from People Data Labs.

    Uses the /v5/company/enrich endpoint with the `ticker` parameter.
    Returns dict with: employee_count, size, industry, founded, name,
    location, linkedin_url.

    Free tier: 100 calls/month, no credit card required.

    Caching strategy (mirrors Glassdoor):
      1. Cached AND fresh (< 7 days) → return immediately
      2. Cached AND stale → return stale data (conserve quota — no background refresh)
      3. Not cached → API call if monthly quota allows, else None
    """
    key = ticker.upper()

    # ── Lazy hydration from Supabase on first call ──
    _hydrate_pdl_cache()

    # ── Check in-memory cache ──
    with _lock:
        cached_entry = _pdl_cache.get(key)

    if cached_entry is not None:
        ts, data = cached_entry
        age = time.time() - ts

        if age < _PDL_TTL:
            # Case 1: Fresh cache — return immediately
            return data

        # Case 2: Stale cache — return stale data, conserve quota
        if data is not None:
            logger.debug(
                "PDL stale data for %s (age %.0fs) — returning cached", key, age
            )
        return data

    # ── Case 3: Not cached at all — must call API ──
    api_key = os.environ.get("PDL_API_KEY", "")
    if not api_key:
        return None

    if not _check_pdl_quota(MAX_MONTHLY_PDL_QUOTA):
        logger.warning("PDL quota exhausted — cannot fetch %s (no cached data)", key)
        return None

    return _fetch_pdl_from_api(key)


def _fetch_pdl_from_api(key: str) -> dict | None:
    """Make an actual PDL API call and persist the result.

    Increments the monthly quota counter. Persists to both in-memory
    cache and Supabase. Returns the parsed data dict or None.
    """
    api_key = os.environ.get("PDL_API_KEY", "")
    if not api_key:
        return None

    # Increment quota BEFORE the call (pessimistic — prevents overshoot)
    with _lock:
        _increment_pdl_quota()

    url = f"https://api.peopledatalabs.com/v5/company/enrich?ticker={key}"

    raw = _http_get_json(url, headers={"X-Api-Key": api_key}, timeout=15)

    now = time.time()

    if not raw or not isinstance(raw, dict):
        logger.info("PDL returned no data for %s", key)
        with _lock:
            _pdl_cache[key] = (now, None)
            _evict_oldest(_pdl_cache)
            _persist_pdl_entry(key, now, None)
        return None

    # Check for error status
    if raw.get("status") and raw["status"] != 200:
        logger.info(
            "PDL error for %s: %s", key, raw.get("error", {}).get("message", "unknown")
        )
        with _lock:
            _pdl_cache[key] = (now, None)
            _evict_oldest(_pdl_cache)
            _persist_pdl_entry(key, now, None)
        return None

    employee_count = raw.get("employee_count")
    if not employee_count:
        # No employee data — cache the miss
        with _lock:
            _pdl_cache[key] = (now, None)
            _evict_oldest(_pdl_cache)
            _persist_pdl_entry(key, now, None)
        return None

    # Extract location
    location = raw.get("location") or {}
    if isinstance(location, dict):
        loc_str = ", ".join(
            filter(
                None,
                [
                    location.get("locality"),
                    location.get("region"),
                    location.get("country"),
                ],
            )
        )
    else:
        loc_str = str(location) if location else ""

    result = {
        "employee_count": int(employee_count),
        "size": raw.get("size") or "",
        "industry": raw.get("industry") or "",
        "founded": raw.get("founded"),
        "name": raw.get("display_name") or raw.get("name") or "",
        "location": loc_str,
        "linkedin_url": raw.get("linkedin_url") or "",
        "website": raw.get("website") or "",
        "_fetched_at": datetime.now().isoformat(timespec="seconds"),
    }

    logger.info(
        "PDL data for %s: %d employees, industry=%s",
        key,
        result["employee_count"],
        result["industry"],
    )

    with _lock:
        _pdl_cache[key] = (now, result)
        _evict_oldest(_pdl_cache)
        _persist_pdl_entry(key, now, result)
    return result


# ═══════════════════════════════════════════════════════════════════
# 2. Culture — Glassdoor (via RapidAPI) — Quota-First Persistent Cache
# ═══════════════════════════════════════════════════════════════════


def _get_glassdoor_data(ticker: str) -> dict | None:
    """Fetch Glassdoor company ratings via RapidAPI.

    Uses a quota-first request guard:
      1. Cached AND fresh (< 30 days) → return immediately
      2. Cached AND stale (> 30 days) → return stale data + background
         refresh if monthly quota < 80
      3. Not cached → API call if monthly quota < 90, else return None

    Returns dict with: overall_rating, recommend_to_friend_pct,
    ceo_approval_pct, ceo_name, review_count, business_outlook_pct,
    company_name, sub-ratings, company details, _fetched_at.

    Set USE_MOCK_GLASSDOOR=1 env var to use mock data for UI development.
    """
    key = ticker.upper()

    # ── Mock data injection for UI development ──
    # TODO: Replace with API fetch once UI is finalized.
    if os.environ.get("USE_MOCK_GLASSDOOR") == "1":
        from filings.mocks.glassdoor_data import MOCK_GLASSDOOR_AAPL

        logger.info("Using MOCK Glassdoor data for %s", key)
        return dict(MOCK_GLASSDOOR_AAPL)  # Return a copy

    # ── Lazy hydration from disk on first call ──
    _hydrate_glassdoor_cache()

    # ── Check in-memory cache ──
    with _lock:
        cached_entry = _glassdoor_cache.get(key)

    if cached_entry is not None:
        ts, data = cached_entry
        age = time.time() - ts

        if age < _GLASSDOOR_TTL:
            # Case 1: Fresh cache — return immediately
            return data

        # Case 2: Stale cache — return stale data, maybe refresh in background
        if data is not None and _check_glassdoor_quota(_GLASSDOOR_STALE_REFRESH_CAP):
            _schedule_glassdoor_refresh(key)
        elif data is not None:
            logger.info(
                "Glassdoor stale data for %s — quota near limit (%d), skipping refresh",
                key,
                MAX_MONTHLY_QUOTA,
            )
        # Return stale data (or stale None)
        return data

    # ── Case 3: Not cached at all — must call API ──
    api_key = os.environ.get("GLASSDOOR_RAPIDAPI_KEY", "")
    if not api_key:
        return None

    if not _check_glassdoor_quota(MAX_MONTHLY_QUOTA):
        logger.warning(
            "Glassdoor quota exhausted — cannot fetch %s (no cached data)",
            key,
        )
        return None

    return _fetch_glassdoor_from_api(key)


def _fetch_glassdoor_from_api(key: str) -> dict | None:
    """Make an actual Glassdoor API call and persist the result.

    Increments the monthly quota counter. Persists to both in-memory
    cache and disk. Returns the parsed data dict or None.
    """
    api_key = os.environ.get("GLASSDOOR_RAPIDAPI_KEY", "")
    if not api_key:
        return None

    # Resolve ticker to company name
    company_name = _resolve_company_name(key)
    if not company_name:
        logger.info("Cannot resolve company name for %s — skipping Glassdoor", key)
        now = time.time()
        with _lock:
            _glassdoor_cache[key] = (now, None)
            _evict_oldest(_glassdoor_cache)
            _persist_glassdoor_entry(key, now, None)
        return None

    # Increment quota BEFORE the call (pessimistic — prevents overshoot)
    with _lock:
        _increment_glassdoor_quota()

    # Query RapidAPI Glassdoor company-search endpoint
    encoded_name = urllib.parse.quote(company_name)
    url = (
        f"https://real-time-glassdoor-data.p.rapidapi.com/"
        f"company-search?query={encoded_name}"
    )

    resp = _http_get_json(
        url,
        headers={
            "X-RapidAPI-Key": api_key,
            "X-RapidAPI-Host": "real-time-glassdoor-data.p.rapidapi.com",
        },
        timeout=15,
    )

    now = time.time()

    if not resp or not isinstance(resp, dict):
        logger.info("Glassdoor returned no data for %s (%s)", key, company_name)
        with _lock:
            _glassdoor_cache[key] = (now, None)
            _evict_oldest(_glassdoor_cache)
            _persist_glassdoor_entry(key, now, None)
        return None

    # Extract best match from search results — prefer stock ticker match
    results = resp.get("data") or []
    if not isinstance(results, list) or not results:
        logger.info(
            "Glassdoor search returned empty results for %s (%s)", key, company_name
        )
        with _lock:
            _glassdoor_cache[key] = (now, None)
            _evict_oldest(_glassdoor_cache)
            _persist_glassdoor_entry(key, now, None)
        return None

    # Try to match by stock ticker first, then fall back to first result
    raw = results[0]
    for r in results:
        if str(r.get("stock", "")).upper() == key:
            raw = r
            break

    # Parse response — field names may vary by provider
    overall = raw.get("overallRating") or raw.get("overall_rating") or raw.get("rating")
    if overall is None:
        ratings = raw.get("ratings") or raw.get("data") or {}
        if isinstance(ratings, dict):
            overall = (
                ratings.get("overallRating")
                or ratings.get("overall_rating")
                or ratings.get("rating")
            )

    if overall is None:
        logger.info(
            "Glassdoor response missing rating for %s: keys=%s",
            key,
            list(raw.keys())[:10],
        )
        with _lock:
            _glassdoor_cache[key] = (now, None)
            _evict_oldest(_glassdoor_cache)
            _persist_glassdoor_entry(key, now, None)
        return None

    try:
        overall_float = round(float(overall), 1)
    except (ValueError, TypeError):
        overall_float = 0.0

    def _pct(val: any) -> float | None:
        if val is None:
            return None
        try:
            v = float(str(val).replace("%", ""))
            if 0 < v <= 1:
                v = round(v * 100)
            return round(v)
        except (ValueError, TypeError):
            return None

    def _rating(val: any) -> float | None:
        """Parse a sub-rating (1.0-5.0 scale)."""
        if val is None:
            return None
        try:
            return round(float(val), 1)
        except (ValueError, TypeError):
            return None

    result = {
        # ── Primary metrics ──
        "overall_rating": overall_float,
        "recommend_to_friend_pct": _pct(
            raw.get("recommendToFriend")
            or raw.get("recommend_to_friend")
            or raw.get("recommend_to_friend_rating")
            or raw.get("recommendPercent")
        ),
        "ceo_approval_pct": _pct(
            raw.get("ceoApproval")
            or raw.get("ceo_approval")
            or raw.get("ceo_rating")
            or raw.get("ceoApprovalPercent")
        ),
        "ceo_name": (raw.get("ceoName") or raw.get("ceo_name") or raw.get("ceo") or ""),
        "review_count": int(
            raw.get("reviewCount")
            or raw.get("review_count")
            or raw.get("numberOfRatings")
            or 0
        ),
        "business_outlook_pct": _pct(
            raw.get("businessOutlook")
            or raw.get("business_outlook")
            or raw.get("business_outlook_rating")
            or raw.get("positiveBusinessOutlookPercent")
        ),
        "company_name": raw.get("companyName") or raw.get("name") or company_name,
        # ── Sub-ratings (1.0-5.0 scale) ──
        "culture_and_values_rating": _rating(
            raw.get("cultureAndValues") or raw.get("culture_and_values_rating")
        ),
        "work_life_balance_rating": _rating(
            raw.get("workLifeBalance") or raw.get("work_life_balance_rating")
        ),
        "senior_management_rating": _rating(
            raw.get("seniorManagement") or raw.get("senior_management_rating")
        ),
        "compensation_and_benefits_rating": _rating(
            raw.get("compensationAndBenefits")
            or raw.get("compensation_and_benefits_rating")
        ),
        "career_opportunities_rating": _rating(
            raw.get("careerOpportunities") or raw.get("career_opportunities_rating")
        ),
        "diversity_and_inclusion_rating": _rating(
            raw.get("diversityAndInclusion")
            or raw.get("diversity_and_inclusion_rating")
        ),
        # ── Company details ──
        "logo_url": raw.get("logo") or raw.get("squareLogo") or "",
        "headquarters": raw.get("headquarters_location")
        or raw.get("headquarters")
        or "",
        "website": raw.get("website") or "",
        "company_size": raw.get("company_size") or raw.get("size") or "",
        "industry": raw.get("industry") or "",
        "company_type": raw.get("company_type") or raw.get("type") or "",
        "revenue": raw.get("revenue") or "",
        "year_founded": raw.get("year_founded") or raw.get("yearFounded"),
        "stock_ticker": raw.get("stock") or raw.get("ticker") or key,
        "glassdoor_url": raw.get("company_link") or raw.get("glassdoorUrl") or "",
        "reviews_url": raw.get("reviews_link") or raw.get("reviewsUrl") or "",
        # ── Metadata ──
        "_fetched_at": datetime.now().isoformat(timespec="seconds"),
    }

    logger.info(
        "Glassdoor data for %s: rating=%.1f, reviews=%d",
        key,
        result["overall_rating"],
        result["review_count"],
    )

    with _lock:
        _glassdoor_cache[key] = (now, result)
        _evict_oldest(_glassdoor_cache)
        _persist_glassdoor_entry(key, now, result)
    return result


def _schedule_glassdoor_refresh(key: str) -> None:
    """Fire-and-forget background refresh of a stale Glassdoor entry.

    De-duplicates: won't schedule a refresh if one is already pending
    for the same ticker.
    """
    with _lock:
        if key in _pending_refreshes:
            return
        _pending_refreshes.add(key)

    def _do_refresh():
        try:
            logger.info("Background refreshing Glassdoor data for %s", key)
            _fetch_glassdoor_from_api(key)
        except Exception as exc:
            logger.warning("Background Glassdoor refresh failed for %s: %s", key, exc)
        finally:
            with _lock:
                _pending_refreshes.discard(key)

    t = threading.Thread(target=_do_refresh, daemon=True)
    t.start()


# ── Glassdoor public helpers ─────────────────────────────────────────


def get_glassdoor_age_str(ticker: str) -> str:
    """Return human-readable age of cached Glassdoor data for a ticker.

    Examples: "Updated 3 days ago", "Updated 12 days ago", "Updated just now"
    Returns empty string if no cached data.
    """
    key = ticker.upper()
    _hydrate_glassdoor_cache()

    with _lock:
        entry = _glassdoor_cache.get(key)

    if entry is None:
        return ""

    ts, data = entry
    if data is None:
        return ""

    age_seconds = time.time() - ts
    if age_seconds < 60:
        return "Updated just now"
    elif age_seconds < 3600:
        return f"Updated {int(age_seconds / 60)} min ago"
    elif age_seconds < 86400:
        return f"Updated {int(age_seconds / 3600)} hours ago"
    else:
        days = int(age_seconds / 86400)
        return f"Updated {days} day{'s' if days != 1 else ''} ago"


def get_glassdoor_quota_info() -> dict:
    """Return current Glassdoor quota status for diagnostics.

    Strategy: try Supabase first, fall back to disk.
    Returns dict with: month, count, max, remaining, exhausted.
    """
    current_month = _get_current_month_str()

    # ── Try Supabase first ──
    sb_quota = supabase_cache.get_quota("glassdoor", current_month)
    if sb_quota is not None:
        count = sb_quota.get("count", 0)
    else:
        # ── Fallback: disk ──
        disk = _load_glassdoor_disk_cache()
        quota = disk.get("quota", {})
        if quota.get("month") != current_month:
            count = 0
        else:
            count = quota.get("count", 0)

    return {
        "month": current_month,
        "count": count,
        "max": MAX_MONTHLY_QUOTA,
        "remaining": max(0, MAX_MONTHLY_QUOTA - count),
        "exhausted": count >= MAX_MONTHLY_QUOTA,
    }


# ═══════════════════════════════════════════════════════════════════
# 3. Product Sentiment — Apple iTunes Search API
# ═══════════════════════════════════════════════════════════════════

# Well-known ticker → app search overrides (where company name != product name)
_TICKER_APP_OVERRIDES: dict[str, str] = {
    "GOOG": "Google",
    "GOOGL": "Google",
    "META": "Instagram",
    "AMZN": "Amazon Shopping",
    "MSFT": "Microsoft Outlook",
    "CRM": "Salesforce",
    "ABNB": "Airbnb",
    "SQ": "Cash App",
    "PYPL": "PayPal",
    "SNAP": "Snapchat",
    "PINS": "Pinterest",
    "DASH": "DoorDash",
    "LYFT": "Lyft",
    "ZM": "Zoom Workplace",
    "SHOP": "Shopify",
    "RBLX": "Roblox",
    "U": "Unity",
    "ROKU": "The Roku App",
    "COIN": "Coinbase",
    "HOOD": "Robinhood",
    "DUOL": "Duolingo",
    "BMBL": "Bumble",
    "MTCH": "Tinder",
}


def _get_appstore_data(ticker: str) -> dict | None:
    """Search for a company's iOS app and return ratings.

    Uses the free Apple iTunes Search API (no auth required).
    Workflow: ticker → yfinance company name → iTunes search → top result.

    Returns dict with: app_name, rating, rating_count,
    current_version_rating, app_icon_url, app_url, developer_name.

    Caching strategy:
      1. Cached AND fresh (< 7 days) → return immediately
      2. Cached AND stale → return stale data (free API, but avoid redundant calls)
      3. Not cached → fetch from iTunes API
    """
    key = ticker.upper()

    # ── Lazy hydration from Supabase on first call ──
    _hydrate_appstore_cache()

    # ── Check in-memory cache ──
    with _lock:
        cached_entry = _appstore_cache.get(key)

    if cached_entry is not None:
        ts, data = cached_entry
        age = time.time() - ts

        if age < _APPSTORE_TTL:
            # Case 1: Fresh cache — return immediately
            return data

        # Case 2: Stale cache — return stale data
        if data is not None:
            logger.debug(
                "App Store stale data for %s (age %.0fs) — returning cached", key, age
            )
        return data

    # ── Case 3: Not cached at all — fetch from iTunes ──
    return _fetch_appstore_from_api(key)


def _fetch_appstore_from_api(key: str) -> dict | None:
    """Make an actual iTunes Search API call and persist the result.

    Persists to both in-memory cache and Supabase.
    Returns the parsed data dict or None.
    """
    # Check for hardcoded override first
    search_name = _TICKER_APP_OVERRIDES.get(key)

    if not search_name:
        # Resolve ticker to company name
        company_name = _resolve_company_name(key)
        if not company_name:
            logger.info("Cannot resolve company name for %s — skipping App Store", key)
            now = time.time()
            with _lock:
                _appstore_cache[key] = (now, None)
                _evict_oldest(_appstore_cache)
                _persist_appstore_entry(key, now, None)
            return None

        # Clean company name for search (remove common suffixes)
        search_name = company_name
        for suffix in [
            ", Inc.",
            " Inc.",
            " Inc",
            " Corp.",
            " Corp",
            " Corporation",
            " Ltd.",
            " Ltd",
            " Limited",
            " S.A.",
            " SE",
            " PLC",
            " plc",
            " N.V.",
            " Group",
            " Holdings",
            " Co.",
            " Technologies",
            " Technology",
            " Platforms",
        ]:
            search_name = search_name.replace(suffix, "")
        search_name = search_name.strip()

    # Search iTunes
    encoded_term = urllib.parse.quote(search_name)
    url = f"https://itunes.apple.com/search?term={encoded_term}&entity=software&country=us&limit=5"

    raw = _http_get_json(url, timeout=10)
    now = time.time()

    if not raw or not isinstance(raw, dict) or raw.get("resultCount", 0) == 0:
        logger.info("iTunes returned no apps for %s (searched: %s)", key, search_name)
        with _lock:
            _appstore_cache[key] = (now, None)
            _evict_oldest(_appstore_cache)
            _persist_appstore_entry(key, now, None)
        return None

    results = raw.get("results", [])
    if not results:
        with _lock:
            _appstore_cache[key] = (now, None)
            _evict_oldest(_appstore_cache)
            _persist_appstore_entry(key, now, None)
        return None

    # Pick the best match — prefer results whose artist name matches the company
    # or whose track name closely matches the search
    best = results[0]
    search_lower = search_name.lower()
    for app in results:
        artist = (app.get("artistName") or "").lower()
        track = (app.get("trackName") or "").lower()
        # Exact or partial match on developer name
        if search_lower in artist or artist in search_lower:
            best = app
            break
        # Match on app name containing the search term
        if search_lower in track:
            best = app
            break

    # For overrides, also prefer the result with the most ratings (flagship app)
    if key in _TICKER_APP_OVERRIDES and len(results) > 1:
        best = max(results, key=lambda a: int(a.get("userRatingCount", 0)))

    rating = best.get("averageUserRating")
    if rating is None:
        with _lock:
            _appstore_cache[key] = (now, None)
            _evict_oldest(_appstore_cache)
            _persist_appstore_entry(key, now, None)
        return None

    result = {
        "app_name": best.get("trackName", ""),
        "rating": round(float(rating), 1),
        "rating_count": int(best.get("userRatingCount", 0)),
        "current_version_rating": round(
            float(best.get("averageUserRatingForCurrentVersion", rating)), 1
        ),
        "current_version_rating_count": int(
            best.get("userRatingCountForCurrentVersion", 0)
        ),
        "app_icon_url": best.get("artworkUrl100", ""),
        "app_url": best.get("trackViewUrl", ""),
        "developer_name": best.get("artistName", ""),
        "bundle_id": best.get("bundleId", ""),
        "version": best.get("version", ""),
        "price": best.get("formattedPrice", "Free"),
        "_fetched_at": datetime.now().isoformat(timespec="seconds"),
    }

    logger.info(
        "App Store data for %s: %s (%.1f stars, %d reviews)",
        key,
        result["app_name"],
        result["rating"],
        result["rating_count"],
    )

    with _lock:
        _appstore_cache[key] = (now, result)
        _evict_oldest(_appstore_cache)
        _persist_appstore_entry(key, now, result)
    return result


# ═══════════════════════════════════════════════════════════════════
# Public API
# ═══════════════════════════════════════════════════════════════════


def _glassdoor_to_company_info(gd: dict) -> dict | None:
    """Extract company-info fields from Glassdoor data as PDL fallback.

    When no PDL API key is configured, we can still populate the
    Employee Pulse card using fields already present in the Glassdoor
    search response (company_size, industry, year_founded, headquarters).
    Returns a dict shaped like the PDL response, or None if insufficient data.
    """
    if not gd:
        return None

    company_size = gd.get("company_size", "")
    industry = gd.get("industry", "")
    headquarters = gd.get("headquarters", "")
    year_founded = gd.get("year_founded")

    # Only return if we have at least some useful data
    if not any([company_size, industry, headquarters, year_founded]):
        return None

    return {
        "employee_count": 0,  # Glassdoor gives size range, not exact count
        "size": company_size,
        "industry": industry,
        "founded": year_founded,
        "location": headquarters,
        "linkedin_url": "",
        "name": gd.get("company_name", ""),
        "_source": "glassdoor",  # Flag so template knows origin
    }


def get_vitals_data(ticker: str) -> dict:
    """Aggregate vitals from all sources for a ticker.

    Each source is fetched independently; failures in one do not
    affect the others. Returns dict with keys:
        glassdoor, pdl, appstore
    Each value is either a dict of data or None.
    """
    result: dict[str, dict | None] = {}

    try:
        result["glassdoor"] = _get_glassdoor_data(ticker)
    except Exception as exc:
        logger.warning("Glassdoor vitals failed for %s: %s", ticker, exc)
        result["glassdoor"] = None

    try:
        result["pdl"] = _get_pdl_data(ticker)
    except Exception as exc:
        logger.warning("PDL vitals failed for %s: %s", ticker, exc)
        result["pdl"] = None

    # Fallback: derive company info from Glassdoor when PDL unavailable
    if not result["pdl"] and not has_pdl_key() and result["glassdoor"]:
        result["pdl"] = _glassdoor_to_company_info(result["glassdoor"])

    try:
        result["appstore"] = _get_appstore_data(ticker)
    except Exception as exc:
        logger.warning("App Store vitals failed for %s: %s", ticker, exc)
        result["appstore"] = None

    return result


def get_vitals_cache_info() -> dict:
    """Return cache status for all vitals data sources.

    Useful for diagnostics and the /health endpoint.
    """
    # Count non-None cached entries for each source
    gd_count = sum(1 for _, (_, d) in _glassdoor_cache.items() if d is not None)
    pdl_count = sum(1 for _, (_, d) in _pdl_cache.items() if d is not None)
    app_count = sum(1 for _, (_, d) in _appstore_cache.items() if d is not None)

    return {
        "glassdoor": {
            "cached_tickers": gd_count,
            "quota": get_glassdoor_quota_info(),
        },
        "pdl": {
            "cached_tickers": pdl_count,
            "quota": get_pdl_quota_info(),
        },
        "appstore": {
            "cached_tickers": app_count,
        },
    }
