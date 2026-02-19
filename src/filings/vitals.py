"""Alternative-data "Vitals" tab — employee, culture & product signals.

Combines three data sources into a single tab:
  1. Employee Pulse (People Data Labs) – headcount, size, industry
  2. Culture (Glassdoor via RapidAPI) – ratings, CEO approval, outlook
  3. Product Sentiment (Apple iTunes Search) – app rating, reviews

All results are cached in-memory with aggressive TTLs (7-30 days) since
this data changes very slowly.  Each source degrades gracefully when
its API key is missing or the service is unavailable.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
import urllib.parse
import urllib.request

logger = logging.getLogger(__name__)

# ── Thread lock for all cache reads/writes ────────────────────────────
_lock = threading.Lock()

# ── Cache TTLs ──────────────────────────────────────────────────────
_GLASSDOOR_TTL = 2_592_000   # 30 days — ratings change very slowly
_PDL_TTL = 604_800           # 7 days — headcount changes slowly
_APPSTORE_TTL = 604_800      # 7 days — app ratings change slowly

# ── LRU max entries for per-ticker caches ─────────────────────────────
_MAX_CACHE_ENTRIES = 2000

# ── Per-ticker caches: {TICKER: (timestamp, data | None)} ───────────
_glassdoor_cache: dict[str, tuple[float, dict | None]] = {}
_pdl_cache: dict[str, tuple[float, dict | None]] = {}
_appstore_cache: dict[str, tuple[float, dict | None]] = {}


def _evict_oldest(cache: dict, max_size: int = _MAX_CACHE_ENTRIES) -> None:
    """Evict oldest entries if cache exceeds max_size. Must hold _lock."""
    if len(cache) <= max_size:
        return
    sorted_keys = sorted(cache, key=lambda k: cache[k][0])
    for k in sorted_keys[: len(cache) - max_size]:
        del cache[k]


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
    """
    key = ticker.upper()

    # Check cache
    with _lock:
        if key in _pdl_cache:
            ts, data = _pdl_cache[key]
            if time.time() - ts < _PDL_TTL:
                return data

    api_key = os.environ.get("PDL_API_KEY", "")
    if not api_key:
        return None

    url = f"https://api.peopledatalabs.com/v5/company/enrich?ticker={key}"

    raw = _http_get_json(url, headers={"X-Api-Key": api_key}, timeout=15)

    if not raw or not isinstance(raw, dict):
        logger.info("PDL returned no data for %s", key)
        with _lock:
            _pdl_cache[key] = (time.time(), None)
            _evict_oldest(_pdl_cache)
        return None

    # Check for error status
    if raw.get("status") and raw["status"] != 200:
        logger.info("PDL error for %s: %s", key, raw.get("error", {}).get("message", "unknown"))
        with _lock:
            _pdl_cache[key] = (time.time(), None)
            _evict_oldest(_pdl_cache)
        return None

    employee_count = raw.get("employee_count")
    if not employee_count:
        # No employee data — cache the miss
        with _lock:
            _pdl_cache[key] = (time.time(), None)
            _evict_oldest(_pdl_cache)
        return None

    # Extract location
    location = raw.get("location") or {}
    if isinstance(location, dict):
        loc_str = ", ".join(
            filter(None, [location.get("locality"), location.get("region"), location.get("country")])
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
    }

    logger.info("PDL data for %s: %d employees, industry=%s",
                key, result["employee_count"], result["industry"])

    with _lock:
        _pdl_cache[key] = (time.time(), result)
        _evict_oldest(_pdl_cache)
    return result


# ═══════════════════════════════════════════════════════════════════
# 2. Culture — Glassdoor (via RapidAPI)
# ═══════════════════════════════════════════════════════════════════

def _get_glassdoor_data(ticker: str) -> dict | None:
    """Fetch Glassdoor company ratings via RapidAPI.

    Returns dict with: overall_rating, recommend_to_friend_pct,
    ceo_approval_pct, ceo_name, review_count, business_outlook_pct,
    company_name.

    Uses 30-day cache (ratings change very slowly).
    """
    key = ticker.upper()

    # Check cache
    with _lock:
        if key in _glassdoor_cache:
            ts, data = _glassdoor_cache[key]
            if time.time() - ts < _GLASSDOOR_TTL:
                return data

    api_key = os.environ.get("GLASSDOOR_RAPIDAPI_KEY", "")
    if not api_key:
        return None

    # Resolve ticker to company name
    company_name = _resolve_company_name(key)
    if not company_name:
        logger.info("Cannot resolve company name for %s — skipping Glassdoor", key)
        with _lock:
            _glassdoor_cache[key] = (time.time(), None)
            _evict_oldest(_glassdoor_cache)
        return None

    # Query RapidAPI Glassdoor endpoint
    encoded_name = urllib.parse.quote(company_name)
    url = f"https://real-time-glassdoor-data.p.rapidapi.com/company-overview?companyName={encoded_name}"

    raw = _http_get_json(
        url,
        headers={
            "X-RapidAPI-Key": api_key,
            "X-RapidAPI-Host": "real-time-glassdoor-data.p.rapidapi.com",
        },
        timeout=15,
    )

    if not raw or not isinstance(raw, dict):
        logger.info("Glassdoor returned no data for %s (%s)", key, company_name)
        with _lock:
            _glassdoor_cache[key] = (time.time(), None)
            _evict_oldest(_glassdoor_cache)
        return None

    # Parse response — field names may vary by provider
    overall = raw.get("overallRating") or raw.get("overall_rating")
    if overall is None:
        ratings = raw.get("ratings") or raw.get("data") or {}
        if isinstance(ratings, dict):
            overall = ratings.get("overallRating") or ratings.get("overall_rating")

    if overall is None:
        logger.info(
            "Glassdoor response missing rating for %s: keys=%s",
            key,
            list(raw.keys())[:10],
        )
        with _lock:
            _glassdoor_cache[key] = (time.time(), None)
            _evict_oldest(_glassdoor_cache)
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

    result = {
        "overall_rating": overall_float,
        "recommend_to_friend_pct": _pct(
            raw.get("recommendToFriend")
            or raw.get("recommend_to_friend")
            or raw.get("recommendPercent")
        ),
        "ceo_approval_pct": _pct(
            raw.get("ceoApproval")
            or raw.get("ceo_approval")
            or raw.get("ceoApprovalPercent")
        ),
        "ceo_name": (
            raw.get("ceoName")
            or raw.get("ceo_name")
            or raw.get("ceo")
            or ""
        ),
        "review_count": int(
            raw.get("reviewCount")
            or raw.get("review_count")
            or raw.get("numberOfRatings")
            or 0
        ),
        "business_outlook_pct": _pct(
            raw.get("businessOutlook")
            or raw.get("business_outlook")
            or raw.get("positiveBusinessOutlookPercent")
        ),
        "company_name": raw.get("companyName") or raw.get("name") or company_name,
    }

    logger.info(
        "Glassdoor data for %s: rating=%.1f, reviews=%d",
        key,
        result["overall_rating"],
        result["review_count"],
    )

    with _lock:
        _glassdoor_cache[key] = (time.time(), result)
        _evict_oldest(_glassdoor_cache)
    return result


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

    7-day cache TTL.
    """
    key = ticker.upper()

    # Check cache
    with _lock:
        if key in _appstore_cache:
            ts, data = _appstore_cache[key]
            if time.time() - ts < _APPSTORE_TTL:
                return data

    # Check for hardcoded override first
    search_name = _TICKER_APP_OVERRIDES.get(key)

    if not search_name:
        # Resolve ticker to company name
        company_name = _resolve_company_name(key)
        if not company_name:
            logger.info("Cannot resolve company name for %s — skipping App Store", key)
            with _lock:
                _appstore_cache[key] = (time.time(), None)
                _evict_oldest(_appstore_cache)
            return None

        # Clean company name for search (remove common suffixes)
        search_name = company_name
        for suffix in [", Inc.", " Inc.", " Inc", " Corp.", " Corp", " Corporation",
                       " Ltd.", " Ltd", " Limited", " S.A.", " SE", " PLC",
                       " plc", " N.V.", " Group", " Holdings", " Co.",
                       " Technologies", " Technology", " Platforms"]:
            search_name = search_name.replace(suffix, "")
        search_name = search_name.strip()

    # Search iTunes
    encoded_term = urllib.parse.quote(search_name)
    url = f"https://itunes.apple.com/search?term={encoded_term}&entity=software&country=us&limit=5"

    raw = _http_get_json(url, timeout=10)

    if not raw or not isinstance(raw, dict) or raw.get("resultCount", 0) == 0:
        logger.info("iTunes returned no apps for %s (searched: %s)", key, search_name)
        with _lock:
            _appstore_cache[key] = (time.time(), None)
            _evict_oldest(_appstore_cache)
        return None

    results = raw.get("results", [])
    if not results:
        with _lock:
            _appstore_cache[key] = (time.time(), None)
            _evict_oldest(_appstore_cache)
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
            _appstore_cache[key] = (time.time(), None)
            _evict_oldest(_appstore_cache)
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
    }

    logger.info(
        "App Store data for %s: %s (%.1f stars, %d reviews)",
        key,
        result["app_name"],
        result["rating"],
        result["rating_count"],
    )

    with _lock:
        _appstore_cache[key] = (time.time(), result)
        _evict_oldest(_appstore_cache)
    return result


# ═══════════════════════════════════════════════════════════════════
# Public API
# ═══════════════════════════════════════════════════════════════════

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

    try:
        result["appstore"] = _get_appstore_data(ticker)
    except Exception as exc:
        logger.warning("App Store vitals failed for %s: %s", ticker, exc)
        result["appstore"] = None

    return result
