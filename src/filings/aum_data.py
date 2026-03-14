"""AUM & deployment ratio data for PaperPanda.

Fetches Regulatory AUM (RAUM) from SEC Form ADV bulk data and XBRL
cash-on-hand figures from public company 10-K/10-Q filings.  Combines
both with existing 13F portfolio values to compute a "deployment ratio"
(equity value / total AUM) for each superinvestor.

Data sources (both free, no API key):
  - SEC Investment Adviser Information Reports (bulk CSV zip)
    → Item 5F(2)(c) = total RAUM
  - SEC EDGAR XBRL companyfacts API
    → CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents
    → CashAndCashEquivalentsAtCarryingValue
    → ShortTermInvestments

Performance note:
  All upstream data changes infrequently (ADV annually, XBRL quarterly,
  13F quarterly).  Once written to Supabase the deployment metrics are
  essentially static for months.  Cache TTLs are set accordingly — the
  sync worker performs a cheap freshness check before touching the
  network.
"""

from __future__ import annotations

import csv
import io
import logging
import os
import time
import zipfile
from datetime import datetime, timezone

import httpx

from filings import supabase_cache

logger = logging.getLogger(__name__)


def _cache_upsert(cache_key: str, category: str, data: dict, ttl_seconds: int | None = None) -> bool:
    """Upsert a row into the Supabase api_cache table directly.

    Bypasses the content_hash optimisation in supabase_cache.set_cached()
    so that this module works even when the content_hash column hasn't
    been migrated onto the table yet.
    """
    from datetime import timedelta

    client = supabase_cache._get_client()
    if client is None:
        return False

    expires_at = None
    if ttl_seconds is not None:
        expires_at = (datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)).isoformat()

    row = {
        "cache_key": cache_key,
        "category": category,
        "response_data": data,
        "expires_at": expires_at,
    }

    try:
        client.table("api_cache").upsert(row, on_conflict="cache_key").execute()
        return True
    except Exception as exc:
        logger.debug("Cache upsert failed for %s: %s", cache_key, exc)
        return False


# ── SEC HTTP config ─────────────────────────────────────────────────
_IDENTITY = os.environ.get(
    "SEC_IDENTITY", "PaperPanda/1.0 (13f-tool contact@paperpanda.io)"
)
_HEADERS = {"User-Agent": _IDENTITY, "Accept": "application/json"}

# Conservative rate limiting for SEC APIs
_MIN_REQUEST_GAP = 0.5  # seconds between requests

# ── Cache TTLs (aligned to actual data cadence) ────────────────────
# ADV RAUM: firms file Form ADV annually; SEC publishes the bulk CSV
# monthly but the underlying data rarely changes between updates.
_TTL_ADV_BULK = 90 * 24 * 3600  # 90 days

# XBRL cash: public companies file 10-Q quarterly and 10-K annually.
# Re-checking every ~45 days is more than enough.
_TTL_XBRL = 45 * 24 * 3600  # 45 days

# Deployment metrics: derived from ADV + XBRL + 13F.  The slowest-
# moving input (ADV) changes annually, so 90 days is safe.  We use
# a shorter window so ratios update within a few days of a new 13F.
_TTL_DEPLOYMENT = 30 * 24 * 3600  # 30 days


# ═══════════════════════════════════════════════════════════════════════
# 1. Form ADV — Regulatory AUM (RAUM)
# ═══════════════════════════════════════════════════════════════════════

# URL for the latest SEC IA firm roster (updated monthly)
_ADV_BULK_URL = (
    "https://www.sec.gov/data-research/sec-markets-data/"
    "information-about-registered-investment-advisers-exempt-reporting-advisers"
)


_ADV_ZIP_BASE = (
    "https://www.sec.gov/files/investment/data/other/"
    "information-about-registered-investment-advisers-exempt-reporting-advisers"
)


def _build_adv_zip_url() -> str:
    """Build the URL for the latest ADV bulk CSV zip.

    The SEC publishes monthly files with a consistent naming pattern:
    ``ia{MMDDYY}.zip`` at a known base path.  We try the current month
    first, then fall back to the previous month.
    """
    from datetime import timedelta

    now = datetime.now()
    # Try current month (e.g., ia020226.zip for Feb 02 2026 → MMDDYY)
    for delta_months in range(3):  # Try current, previous, 2 months ago
        d = now.replace(day=2) - timedelta(days=30 * delta_months)
        # SEC uses MMDDYY format: ia020226.zip = Feb 02, 2026
        date_str = d.strftime("%m%d%y")
        filename = f"ia{date_str}.zip"
        url = f"{_ADV_ZIP_BASE}/{filename}"

        try:
            resp = httpx.head(url, headers=_HEADERS, timeout=10, follow_redirects=True)
            if resp.status_code == 200:
                return url
        except Exception:
            continue

    # Last resort fallback
    date_str = now.strftime("%m%d%y")
    return f"{_ADV_ZIP_BASE}/ia{date_str}.zip"


def _parse_money(val: str) -> int:
    """Parse '$1,234,567.00' or '1234567.00' → 1234567."""
    cleaned = val.strip().replace(",", "").replace("$", "")
    if not cleaned or cleaned == ".00":
        return 0
    try:
        return int(float(cleaned))
    except (ValueError, OverflowError):
        return 0


def _parse_raum_from_csv(csv_content: str) -> dict[str, dict]:
    """Parse the bulk ADV CSV and extract RAUM data.

    Returns dict keyed by CRD number (str): {
        "crd": str,
        "cik": str,
        "firm_name": str,
        "raum_discretionary": int,
        "raum_non_discretionary": int,
        "raum_total": int,
        "num_accounts": int,
    }
    """
    # Normalize line endings and use StringIO for proper CSV parsing
    csv_content = csv_content.replace("\r\n", "\n").replace("\r", "\n")
    sio = io.StringIO(csv_content, newline="")
    reader_iter = csv.reader(sio)
    try:
        headers = next(reader_iter)
    except StopIteration:
        return {}

    # Find column indices (csv.reader already strips outer quotes)
    col_map: dict[str, int] = {}
    for i, h in enumerate(headers):
        h_clean = h.strip()
        if h_clean == "Organization CRD#":
            col_map["crd"] = i
        elif h_clean == "CIK#":
            col_map["cik"] = i
        elif h_clean == "Primary Business Name":
            col_map["name"] = i
        elif h_clean == "5F(2)(a)":
            col_map["raum_disc"] = i
        elif h_clean == "5F(2)(b)":
            col_map["raum_non_disc"] = i
        elif h_clean == "5F(2)(c)":
            col_map["raum_total"] = i
        elif h_clean == "5F(2)(f)":
            col_map["num_accounts"] = i

    required = {"crd", "raum_total"}
    if not required.issubset(col_map.keys()):
        logger.error("ADV CSV missing required columns: %s", required - col_map.keys())
        return {}

    max_col = max(col_map.values())
    result: dict[str, dict] = {}

    for row in reader_iter:
        if len(row) <= max_col:
            continue

        crd = row[col_map["crd"]].strip()
        if not crd:
            continue

        result[crd] = {
            "crd": crd,
            "cik": row[col_map.get("cik", 0)].strip() if "cik" in col_map else "",
            "firm_name": row[col_map.get("name", 0)].strip() if "name" in col_map else "",
            "raum_discretionary": _parse_money(row[col_map.get("raum_disc", 0)]) if "raum_disc" in col_map else 0,
            "raum_non_discretionary": _parse_money(row[col_map.get("raum_non_disc", 0)]) if "raum_non_disc" in col_map else 0,
            "raum_total": _parse_money(row[col_map["raum_total"]]),
            "num_accounts": _parse_money(row[col_map.get("num_accounts", 0)]) if "num_accounts" in col_map else 0,
        }

    return result


def fetch_all_adv_raum() -> dict[str, dict]:
    """Download the latest SEC ADV bulk CSV and extract RAUM for all firms.

    Returns dict keyed by CRD number.
    Caches the parsed result in Supabase (category="adv_bulk", 90-day TTL).

    The underlying Form ADV data is filed annually by each adviser, so
    the bulk CSV content changes very slowly.  We cache aggressively.
    """
    # Check cache first — 90-day TTL, ADV data is essentially annual
    cached, is_fresh = supabase_cache.get_cached_with_stale("adv_bulk:latest")
    if isinstance(cached, dict) and cached.get("data") and is_fresh:
        logger.info("Using cached ADV bulk data (%d firms)", len(cached["data"]))
        return cached["data"]

    # Fetch fresh data
    zip_url = _build_adv_zip_url()

    logger.info("Downloading ADV bulk data from %s", zip_url)
    try:
        resp = httpx.get(zip_url, headers=_HEADERS, timeout=120, follow_redirects=True)
        resp.raise_for_status()
    except Exception as e:
        logger.error("Failed to download ADV bulk data: %s", e)
        # Return stale cache if available
        if isinstance(cached, dict) and cached.get("data"):
            logger.info("Using stale ADV cache (%d firms)", len(cached["data"]))
            return cached["data"]
        return {}

    # Extract CSV from zip
    try:
        z = zipfile.ZipFile(io.BytesIO(resp.content))
        csv_name = z.namelist()[0]
        with z.open(csv_name) as f:
            csv_content = f.read().decode("utf-8", errors="replace")
    except Exception as e:
        logger.error("Failed to extract ADV zip: %s", e)
        return {}

    result = _parse_raum_from_csv(csv_content)
    logger.info("Parsed ADV bulk data: %d firms with RAUM", len(result))

    # Cache the result
    try:
        _cache_upsert(
            cache_key="adv_bulk:latest",
            category="adv_bulk",
            data={"data": result, "source_url": zip_url, "fetched_at": datetime.now(timezone.utc).isoformat()},
            ttl_seconds=_TTL_ADV_BULK,
        )
    except Exception as e:
        logger.debug("Failed to cache ADV bulk data: %s", e)

    return result


def get_raum_for_crd(crd: str) -> dict | None:
    """Get RAUM data for a single CRD number.

    Returns dict with raum_total, raum_discretionary, etc. or None.
    """
    all_data = fetch_all_adv_raum()
    return all_data.get(crd)


# ═══════════════════════════════════════════════════════════════════════
# 2. XBRL — Public Company Cash from 10-K/10-Q
# ═══════════════════════════════════════════════════════════════════════

# GAAP concepts to try, in priority order (most comprehensive first)
_XBRL_CASH_CONCEPTS = [
    "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents",
    "CashAndCashEquivalentsAtCarryingValue",
    "CashAndCashEquivalents",
]

_XBRL_SHORT_TERM_CONCEPTS = [
    "ShortTermInvestments",
    "AvailableForSaleSecuritiesDebtSecurities",
    "MarketableSecuritiesCurrent",
]


def fetch_xbrl_cash(cik: str) -> list[dict] | None:
    """Fetch historical cash data from SEC XBRL API for a public company.

    Returns list of dicts sorted by period (newest first):
        [{"period": "2025-09-30", "form": "10-Q", "filed": "2025-11-03",
          "cash": 77111000000, "short_term_investments": 0, "total_liquid": 77111000000}]

    Returns None if the CIK has no XBRL data (not a public company filer).
    """
    # Check cache first — 45-day TTL, XBRL data is quarterly
    cache_key = f"xbrl:{cik}"
    cached, is_fresh = supabase_cache.get_cached_with_stale(cache_key)
    if isinstance(cached, dict) and cached.get("cash_history") is not None and is_fresh:
        history = cached["cash_history"]
        # Cached 404: no_data=True means this CIK has no XBRL filings
        if cached.get("no_data") and not history:
            return None
        return history

    padded_cik = cik.zfill(10)
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{padded_cik}.json"

    try:
        time.sleep(_MIN_REQUEST_GAP)
        resp = httpx.get(url, headers=_HEADERS, timeout=30)
        if resp.status_code == 404:
            logger.debug("No XBRL data for CIK %s (not a public filer)", cik)
            # Cache the 404 so we don't re-hit the API every sync cycle
            _cache_upsert(
                cache_key=cache_key,
                category="xbrl",
                data={"cash_history": [], "cik": cik, "no_data": True,
                      "fetched_at": datetime.now(timezone.utc).isoformat()},
                ttl_seconds=_TTL_XBRL,
            )
            return None
        resp.raise_for_status()
    except Exception as e:
        logger.warning("XBRL fetch failed for CIK %s: %s", cik, e)
        if isinstance(cached, dict) and cached.get("cash_history") is not None:
            return cached["cash_history"]
        return None

    data = resp.json()
    usgaap = data.get("facts", {}).get("us-gaap", {})

    # Extract cash entries
    cash_entries = _extract_xbrl_entries(usgaap, _XBRL_CASH_CONCEPTS)
    sti_entries = _extract_xbrl_entries(usgaap, _XBRL_SHORT_TERM_CONCEPTS)

    # Build period→values map
    periods: dict[str, dict] = {}

    for entry in cash_entries:
        period_end = entry.get("end", "")
        if not period_end:
            continue
        form = entry.get("form", "")
        if form not in ("10-K", "10-Q"):
            continue
        # Only keep entries with "frame" (deduplicated quarterly snapshots)
        if "frame" not in entry:
            continue

        key = period_end
        if key not in periods:
            periods[key] = {
                "period": period_end,
                "form": form,
                "filed": entry.get("filed", ""),
                "cash": 0,
                "short_term_investments": 0,
                "total_liquid": 0,
            }
        periods[key]["cash"] = max(periods[key]["cash"], entry.get("val", 0))

    for entry in sti_entries:
        period_end = entry.get("end", "")
        form = entry.get("form", "")
        if not period_end or form not in ("10-K", "10-Q") or "frame" not in entry:
            continue
        if period_end in periods:
            periods[period_end]["short_term_investments"] = max(
                periods[period_end]["short_term_investments"], entry.get("val", 0)
            )

    # Compute total_liquid and sort
    for p in periods.values():
        p["total_liquid"] = p["cash"] + p["short_term_investments"]

    result = sorted(periods.values(), key=lambda x: x["period"], reverse=True)

    # Keep last 12 quarters
    result = result[:12]

    # Cache result
    try:
        _cache_upsert(
            cache_key=cache_key,
            category="xbrl",
            data={"cash_history": result, "cik": cik, "fetched_at": datetime.now(timezone.utc).isoformat()},
            ttl_seconds=_TTL_XBRL,
        )
    except Exception as e:
        logger.debug("Failed to cache XBRL data for CIK %s: %s", cik, e)

    return result


def _extract_xbrl_entries(usgaap: dict, concepts: list[str]) -> list[dict]:
    """Try multiple GAAP concepts and return entries from the first one found."""
    for concept in concepts:
        if concept in usgaap:
            units = usgaap[concept].get("units", {})
            usd_entries = units.get("USD", [])
            if usd_entries:
                return usd_entries
    return []


# ═══════════════════════════════════════════════════════════════════════
# 3. Deployment Metrics
# ═══════════════════════════════════════════════════════════════════════


def compute_deployment_metrics(
    cik: str,
    thirteenf_value: int,
    raum: int | None,
    xbrl_cash_history: list[dict] | None = None,
) -> dict:
    """Compute deployment metrics for a single fund.

    Returns: {
        "cik": str,
        "thirteenf_value": int,
        "raum": int | None,
        "deployment_ratio": float | None,        # 13F value / RAUM (0.0–1.0+)
        "estimated_non_equity": int | None,       # RAUM - 13F value
        "non_equity_pct": float | None,           # (RAUM - 13F) / RAUM * 100
        "exact_cash": int | None,                 # Latest from XBRL
        "exact_cash_period": str | None,          # Period of exact cash figure
        "cash_pct_of_raum": float | None,         # exact_cash / RAUM * 100
        "cash_history": list[dict] | None,        # XBRL cash time series
        "data_source": "adv" | "xbrl" | "adv+xbrl" | None,
    }
    """
    result: dict = {
        "cik": cik,
        "thirteenf_value": thirteenf_value,
        "raum": raum,
        "deployment_ratio": None,
        "estimated_non_equity": None,
        "non_equity_pct": None,
        "exact_cash": None,
        "exact_cash_period": None,
        "cash_pct_of_raum": None,
        "cash_history": xbrl_cash_history,
        "data_source": None,
    }

    has_raum = raum is not None and raum > 0
    has_xbrl = xbrl_cash_history is not None and len(xbrl_cash_history) > 0

    if has_raum:
        result["deployment_ratio"] = round(thirteenf_value / raum, 4) if raum > 0 else None
        result["estimated_non_equity"] = raum - thirteenf_value
        result["non_equity_pct"] = round((raum - thirteenf_value) / raum * 100, 1) if raum > 0 else None
        result["data_source"] = "adv"

    if has_xbrl:
        latest = xbrl_cash_history[0]
        result["exact_cash"] = latest.get("total_liquid", latest.get("cash", 0))
        result["exact_cash_period"] = latest.get("period")
        if has_raum and raum > 0:
            result["cash_pct_of_raum"] = round(result["exact_cash"] / raum * 100, 1)
            result["data_source"] = "adv+xbrl"
        else:
            result["data_source"] = "xbrl"

    return result


# ═══════════════════════════════════════════════════════════════════════
# 4. Bulk Sync
# ═══════════════════════════════════════════════════════════════════════


def _deployment_cache_is_fresh() -> bool:
    """Check whether existing deployment cache is still usable.

    Rather than re-downloading the 50 MB ADV zip + hitting the XBRL API
    on every sync cycle, we do a lightweight Supabase query for a single
    deployment row's ``created_at`` and ``expires_at``.  If the row
    exists and is within its TTL, the whole set is still good (they
    were all written in the same pass with the same TTL).

    Falls back to created_at + _TTL_DEPLOYMENT if expires_at is null
    (handles rows written before the TTL columns were populated).
    """
    client = supabase_cache._get_client()
    if client is None:
        return False

    try:
        resp = (
            client.table("api_cache")
            .select("created_at, expires_at")
            .eq("category", "deployment")
            .limit(1)
            .execute()
        )
        rows = resp.data or []
    except Exception:
        return False

    if not rows:
        return False  # No data at all → must sync

    now = datetime.now(timezone.utc)
    row = rows[0]

    # Prefer explicit expires_at if present
    expires_at = row.get("expires_at")
    if expires_at:
        try:
            exp = datetime.fromisoformat(expires_at)
            return exp > now
        except (ValueError, TypeError):
            pass

    # Fallback: use created_at + deployment TTL
    created_at = row.get("created_at")
    if created_at:
        try:
            from datetime import timedelta

            created = datetime.fromisoformat(created_at)
            return (created + timedelta(seconds=_TTL_DEPLOYMENT)) > now
        except (ValueError, TypeError):
            pass

    return False


def sync_all_deployment_data(
    superinvestors: list,
    fund_cache: dict,
    *,
    force: bool = False,
) -> dict:
    """Sync deployment data for all superinvestors.

    Because all upstream inputs change infrequently (Form ADV annually,
    XBRL quarterly, 13F quarterly), this function starts with a cheap
    freshness check.  If the existing deployment cache is still within
    its 30-day TTL the entire sync is skipped — zero network calls.

    Pass ``force=True`` to bypass the freshness gate (e.g. from the
    manual /api/deployment/sync endpoint).

    Pipeline (when not skipped):
      1. Download ADV bulk CSV (single request, covers all CRDs)
      2. For each public company: fetch XBRL cash data
      3. Compute deployment metrics for each fund
      4. Cache combined results in Supabase

    Args:
        superinvestors: List of SuperinvestorInfo objects
        fund_cache:     Current fund_cache dict (CIK→fund data) for 13F values
        force:          Bypass the freshness check

    Returns: {"updated": N, "skipped": N, "failed": N, "skipped_reason": str | None}
    """
    # ── Freshness gate: skip entire sync if cache is still valid ──
    if not force and _deployment_cache_is_fresh():
        logger.info(
            "Deployment cache is fresh (TTL %dd) — skipping sync",
            _TTL_DEPLOYMENT // 86400,
        )
        return {
            "updated": 0,
            "skipped": len(superinvestors),
            "failed": 0,
            "skipped_reason": "cache_fresh",
        }

    updated = 0
    skipped = 0
    failed = 0

    # Step 1: Fetch all ADV RAUM data (single bulk download)
    # fetch_all_adv_raum() has its own 90-day cache so this is
    # usually a Supabase read, not a SEC download.
    logger.info("Fetching ADV bulk RAUM data...")
    all_raum = fetch_all_adv_raum()
    logger.info("ADV data: %d firms loaded", len(all_raum))

    # Step 2: Process each superinvestor
    for si in superinvestors:
        try:
            # Get 13F portfolio value from cache
            fund_data = fund_cache.get(si.cik, {})
            thirteenf_value = fund_data.get("total_value", 0)

            if thirteenf_value <= 0:
                skipped += 1
                continue

            # Get RAUM from ADV data
            raum = None
            if si.crd_number:
                adv_data = all_raum.get(si.crd_number)
                if adv_data:
                    raum = adv_data.get("raum_total", 0) or None

            # Try XBRL cash for ALL funds — the SEC XBRL API returns
            # 404 for non-public filers, which fetch_xbrl_cash handles
            # gracefully.  After the first sync, results are cached
            # (45-day TTL) so subsequent calls are just Supabase reads.
            xbrl_cash = fetch_xbrl_cash(si.cik)

            # Compute metrics
            # 13F total_value is converted from thousands to dollars at ingestion (client.py)
            metrics = compute_deployment_metrics(
                cik=si.cik,
                thirteenf_value=thirteenf_value,
                raum=raum,
                xbrl_cash_history=xbrl_cash,
            )

            # Add fund identity
            metrics["display_name"] = si.display_name
            metrics["fund_name"] = si.fund_name
            metrics["crd_number"] = si.crd_number or ""

            # Cache deployment metrics (30-day TTL — inputs change quarterly)
            cache_key = f"deployment:{si.cik}"
            _cache_upsert(
                cache_key=cache_key,
                category="deployment",
                data=metrics,
                ttl_seconds=_TTL_DEPLOYMENT,
            )
            updated += 1

        except Exception as e:
            logger.warning("Deployment sync failed for %s (CIK %s): %s", si.display_name, si.cik, e)
            failed += 1

    logger.info("Deployment sync: %d updated, %d skipped, %d failed", updated, skipped, failed)
    return {"updated": updated, "skipped": skipped, "failed": failed, "skipped_reason": None}


def load_all_deployment_data() -> dict[str, dict]:
    """Load all cached deployment metrics from Supabase.

    Returns dict keyed by CIK.
    """
    rows = supabase_cache.get_all_by_category("deployment", page_size=100)
    if not rows:
        return {}

    result: dict[str, dict] = {}
    for row in rows:
        key = row.get("cache_key", "")
        if not key.startswith("deployment:"):
            continue
        cik = key.replace("deployment:", "", 1)
        data = row.get("response_data")
        if isinstance(data, dict):
            result[cik] = data

    logger.info("Loaded %d deployment metrics from cache", len(result))
    return result


def build_deployment_leaderboard(
    deployment_data: dict[str, dict],
) -> list[dict]:
    """Build a sorted leaderboard of deployment data.

    Returns ALL funds that have cached deployment data, sorted by
    estimated cash value descending (highest cash first).  The cash
    value used for sorting is: exact_cash (XBRL) if available, else
    estimated_non_equity (AUM gap), else 0.
    """
    entries: list[dict] = []

    for cik, metrics in deployment_data.items():
        ratio = metrics.get("deployment_ratio")
        exact_cash = metrics.get("exact_cash")
        est_non_eq = metrics.get("estimated_non_equity")
        # Pick best available cash figure for sort key
        cash_sort = exact_cash if exact_cash is not None else (est_non_eq if est_non_eq is not None else 0)
        entry = {
            "cik": cik,
            "display_name": metrics.get("display_name", ""),
            "fund_name": metrics.get("fund_name", ""),
            "crd_number": metrics.get("crd_number", ""),
            "raum": metrics.get("raum", 0),
            "thirteenf_value": metrics.get("thirteenf_value", 0),
            "deployment_ratio": ratio,
            "non_equity_pct": metrics.get("non_equity_pct"),
            "estimated_non_equity": est_non_eq,
            "exact_cash": exact_cash,
            "exact_cash_period": metrics.get("exact_cash_period"),
            "data_source": metrics.get("data_source"),
            "_cash_sort": cash_sort,
        }
        entries.append(entry)

    # Sort by cash value DESC (highest cash first)
    entries.sort(key=lambda x: x["_cash_sort"], reverse=True)

    return entries
