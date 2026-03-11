"""US Treasury yield curve data from the Fiscal Data API.

Free, no API key required.
Source: https://api.fiscaldata.treasury.gov/

Provides:
  - Daily Treasury yield curve (1mo → 30yr)
  - National debt (debt to the penny)
"""

import csv
import io
import json
import logging
import threading
import time
import urllib.request
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

_DEBT_URL = (
    "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/"
    "v2/accounting/od/debt_to_penny"
)

_lock = threading.Lock()
_cache: dict[str, tuple[float, any]] = {}
_TTL = 3600 * 6  # 6 hours

# Treasury yield maturities we track
MATURITIES = ["1 Mo", "3 Mo", "6 Mo", "1 Yr", "2 Yr", "5 Yr", "7 Yr", "10 Yr", "20 Yr", "30 Yr"]


def _cached_or_fetch(cache_key: str, fetcher, ttl: int = _TTL):
    """Generic L1 → L2 → fetcher pattern."""
    # L1
    with _lock:
        cached = _cache.get(cache_key)
        if cached and time.time() - cached[0] < ttl:
            return cached[1]

    # L2
    try:
        from filings import supabase_cache
        l2 = supabase_cache.get_cached(cache_key)
        if l2:
            with _lock:
                _cache[cache_key] = (time.time(), l2)
            return l2
    except Exception:
        pass

    # L3: live fetch
    try:
        result = fetcher()
        if result:
            try:
                from filings import supabase_cache
                supabase_cache.set_cached(cache_key, result, ttl_hours=24)
            except Exception:
                pass
            with _lock:
                _cache[cache_key] = (time.time(), result)
        return result
    except Exception as exc:
        logger.warning("Treasury fetch failed for %s: %s", cache_key, exc)
        return None


def _fetch_yield_curve() -> dict | None:
    """Fetch current Treasury yield curve from treasury.gov XML/CSV feed.

    Uses the Fiscal Data API for average interest rates on Treasury securities.
    """
    try:
        # Use the daily treasury par yield curve rates
        today = datetime.now()
        year = today.year
        month = today.strftime("%m")

        # Fetch from treasury.gov CSV - daily par yield curve rates
        url = (
            f"https://home.treasury.gov/resource-center/data-chart-center/"
            f"interest-rates/daily-treasury-rates.csv/{year}/all"
            f"?type=daily_treasury_yield_curve&field_tdr_date_value_month={year}{month}"
            f"&page&_format=csv"
        )
        req = urllib.request.Request(url, headers={
            "User-Agent": "PaperPanda/1.0",
            "Accept": "text/csv",
        })
        with urllib.request.urlopen(req, timeout=15) as resp:
            raw = resp.read().decode("utf-8")

        if not raw.strip():
            # Try previous month
            prev = today - timedelta(days=30)
            year = prev.year
            month = prev.strftime("%m")
            url = (
                f"https://home.treasury.gov/resource-center/data-chart-center/"
                f"interest-rates/daily-treasury-rates.csv/{year}/all"
                f"?type=daily_treasury_yield_curve&field_tdr_date_value_month={year}{month}"
                f"&page&_format=csv"
            )
            req = urllib.request.Request(url, headers={
                "User-Agent": "PaperPanda/1.0",
                "Accept": "text/csv",
            })
            with urllib.request.urlopen(req, timeout=15) as resp:
                raw = resp.read().decode("utf-8")

        reader = csv.reader(io.StringIO(raw.strip()))
        rows = [row for row in reader if row]
        if len(rows) < 2:
            return None

        headers = [h.strip() for h in rows[0]]
        # Get last row (most recent date)
        last_row = [v.strip() for v in rows[-1]]

        # Map column names to our maturity labels
        col_map = {
            "1 Mo": "1 Mo", "2 Mo": "2 Mo", "3 Mo": "3 Mo", "4 Mo": "4 Mo",
            "6 Mo": "6 Mo", "1 Yr": "1 Yr", "2 Yr": "2 Yr", "3 Yr": "3 Yr",
            "5 Yr": "5 Yr", "7 Yr": "7 Yr", "10 Yr": "10 Yr",
            "20 Yr": "20 Yr", "30 Yr": "30 Yr",
        }

        date_str = last_row[0] if last_row else ""
        yields = {}
        for i, h in enumerate(headers):
            if h in col_map and i < len(last_row):
                try:
                    yields[col_map[h]] = float(last_row[i])
                except (ValueError, IndexError):
                    pass

        if not yields:
            return None

        # Build curve data
        curve = []
        for mat in MATURITIES:
            if mat in yields:
                curve.append({"maturity": mat, "yield": yields[mat]})

        return {
            "date": date_str,
            "curve": curve,
            "yields": yields,
            "inverted": yields.get("2 Yr", 0) > yields.get("10 Yr", 99),
        }

    except Exception as exc:
        logger.warning("Treasury yield curve fetch failed: %s", exc)
        return None


def _fetch_debt_data() -> dict | None:
    """Fetch national debt data from Fiscal Data API."""
    try:
        url = (
            f"{_DEBT_URL}?sort=-record_date&page[size]=30"
            f"&fields=record_date,tot_pub_debt_out_amt"
        )
        req = urllib.request.Request(url, headers={"User-Agent": "PaperPanda/1.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())

        records = data.get("data", [])
        if not records:
            return None

        result = []
        for rec in records:
            try:
                result.append({
                    "date": rec["record_date"],
                    "debt": float(rec["tot_pub_debt_out_amt"]),
                })
            except (KeyError, ValueError):
                continue

        result.reverse()
        latest = result[-1] if result else None
        return {
            "data": result,
            "latest": latest,
            "latest_trillions": round(latest["debt"] / 1e12, 2) if latest else None,
        }
    except Exception as exc:
        logger.warning("Debt data fetch failed: %s", exc)
        return None


def get_yield_curve() -> dict | None:
    """Get current Treasury yield curve (cached)."""
    return _cached_or_fetch("treasury:yield_curve", _fetch_yield_curve)


def get_debt_data() -> dict | None:
    """Get national debt data (cached)."""
    return _cached_or_fetch("treasury:debt", _fetch_debt_data)


def get_treasury_dashboard() -> dict:
    """Get all Treasury data for the macro dashboard."""
    yield_curve = get_yield_curve()
    debt = get_debt_data()
    return {
        "yield_curve": yield_curve,
        "debt": debt,
    }
