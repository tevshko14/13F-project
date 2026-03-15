"""Earnings Calendar — upcoming & recent earnings dates with BMO/AMC timing.

Fetches earnings calendar data from Finnhub (primary) and FMP (fallback),
providing a week-by-week or month-level view of upcoming earnings reports.

Data flow:
  1. Check in-memory L1 cache (1h TTL)
  2. Fetch from Finnhub /calendar/earnings (free tier, bulk)
     — via :func:`earnings.fetch_finnhub_calendar_raw` (shared cache)
  3. Fallback to FMP /earning_calendar (premium)
     — via :func:`earnings_scorecard.fmp_get` (shared helper)
  4. Enrich with company metadata (name, sector, logo availability)
     — via :func:`earnings_scorecard.build_company_lookup` (shared)

Feature-flagged: set EARNINGS_CALENDAR_ENABLED=1 to expose the page.
"""

from __future__ import annotations

import logging
import os
import time
from collections import defaultdict
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

# ── Configuration ────────────────────────────────────────────────
_TTL = 3600  # 1 hour

# ── Feature flag ─────────────────────────────────────────────────
FEATURE_ENABLED = os.environ.get("EARNINGS_CALENDAR_ENABLED", "1") == "1"

# ── In-memory cache ──────────────────────────────────────────────
_cal_cache: dict[str, tuple[float, list[dict]]] = {}
_MAX_CACHE = 50


# ── Public API ───────────────────────────────────────────────────

def get_earnings_calendar(
    start_date: str | None = None,
    end_date: str | None = None,
    weeks: int = 4,
) -> dict:
    """Return earnings calendar data for a date range.

    Returns::

        {
            "entries":    list[dict],   # individual earnings events
            "by_date":    dict,         # {date_str: [events]}
            "weeks":      list[dict],   # [{start, end, dates: [...]}]
            "stats":      dict,         # summary statistics
            "range":      dict,         # {start, end}
            "source":     str,          # "finnhub" | "fmp" | "mock"
        }
    """
    now = datetime.now()

    if not start_date:
        # Start from the Monday of this week
        days_since_monday = now.weekday()
        monday = now - timedelta(days=days_since_monday)
        start_date = monday.strftime("%Y-%m-%d")

    if not end_date:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = start_dt + timedelta(weeks=weeks) - timedelta(days=1)
        end_date = end_dt.strftime("%Y-%m-%d")

    cache_key = f"{start_date}:{end_date}"
    cached = _cal_cache.get(cache_key)
    if cached and (time.time() - cached[0]) < _TTL:
        return _build_response(cached[1], start_date, end_date, "cached")

    # Try Finnhub first, then FMP
    entries = _fetch_finnhub_calendar(start_date, end_date)
    source = "finnhub"

    if not entries:
        entries = _fetch_fmp_calendar(start_date, end_date)
        source = "fmp"

    if not entries:
        entries = _build_mock_entries(start_date, end_date)
        source = "mock"

    # Enrich with company metadata
    entries = _enrich_entries(entries)

    # Cache
    if len(_cal_cache) >= _MAX_CACHE:
        oldest = min(_cal_cache, key=lambda k: _cal_cache[k][0])
        _cal_cache.pop(oldest, None)
    _cal_cache[cache_key] = (time.time(), entries)

    return _build_response(entries, start_date, end_date, source)


def get_week_view(target_date: str | None = None) -> dict:
    """Get a single week of earnings, centered on target_date."""
    now = datetime.now()
    if target_date:
        dt = datetime.strptime(target_date, "%Y-%m-%d")
    else:
        dt = now

    days_since_monday = dt.weekday()
    monday = dt - timedelta(days=days_since_monday)
    friday = monday + timedelta(days=4)

    return get_earnings_calendar(
        start_date=monday.strftime("%Y-%m-%d"),
        end_date=friday.strftime("%Y-%m-%d"),
        weeks=1,
    )


def get_month_view(year: int | None = None, month: int | None = None) -> dict:
    """Get a full month of earnings for heatmap view."""
    now = datetime.now()
    y = year or now.year
    m = month or now.month

    start = datetime(y, m, 1)
    if m == 12:
        end = datetime(y + 1, 1, 1) - timedelta(days=1)
    else:
        end = datetime(y, m + 1, 1) - timedelta(days=1)

    return get_earnings_calendar(
        start_date=start.strftime("%Y-%m-%d"),
        end_date=end.strftime("%Y-%m-%d"),
    )


# ── Data fetchers ────────────────────────────────────────────────

def _fetch_finnhub_calendar(start: str, end: str) -> list[dict]:
    """Fetch from Finnhub /calendar/earnings bulk endpoint.

    Uses :func:`earnings.fetch_finnhub_calendar_raw` for the shared,
    cached HTTP call — avoids duplicate API requests when the earnings
    history module fetches the same data.
    """
    from filings.earnings import fetch_finnhub_calendar_raw

    raw = fetch_finnhub_calendar_raw(start, end)
    if not raw:
        return []

    entries = []
    for item in raw:
        symbol = item.get("symbol", "")
        if not symbol or "." in symbol:
            continue  # skip non-US tickers

        date = item.get("date", "")
        if not date:
            continue

        hour = item.get("hour", "")
        timing = _parse_timing(hour)

        entries.append({
            "ticker": symbol,
            "date": date,
            "timing": timing,
            "eps_estimate": item.get("epsEstimate"),
            "eps_actual": item.get("epsActual"),
            "revenue_estimate": item.get("revenueEstimate"),
            "revenue_actual": item.get("revenueActual"),
            "year": item.get("year"),
            "quarter": item.get("quarter"),
            "confirmed": hour != "",
        })

    logger.info(
        "Finnhub earnings calendar: %d entries for %s to %s",
        len(entries), start, end,
    )
    return entries


def _fetch_fmp_calendar(start: str, end: str) -> list[dict]:
    """Fallback: FMP ``/earning_calendar`` endpoint.

    Uses :func:`earnings_scorecard.fmp_get` for the shared HTTP helper
    (handles API key, error parsing, logging).
    """
    from filings.earnings_scorecard import fmp_get

    data = fmp_get("/earning_calendar", {"from": start, "to": end})
    if data is None or not isinstance(data, list):
        return []

    entries = []
    for item in data:
        symbol = item.get("symbol", "")
        if not symbol or "." in symbol:
            continue

        date = item.get("date", "")
        if not date:
            continue

        time_str = item.get("time", "")
        timing = "bmo" if time_str == "bmo" else "amc" if time_str == "amc" else "unknown"

        entries.append({
            "ticker": symbol,
            "date": date,
            "timing": timing,
            "eps_estimate": item.get("epsEstimated"),
            "eps_actual": item.get("eps"),
            "revenue_estimate": item.get("revenueEstimated"),
            "revenue_actual": item.get("revenue"),
            "year": item.get("fiscalDateEnding", "")[:4] if item.get("fiscalDateEnding") else None,
            "quarter": None,
            "confirmed": True,
        })

    logger.info(
        "FMP earnings calendar: %d entries for %s to %s",
        len(entries), start, end,
    )
    return entries


# ── Enrichment ───────────────────────────────────────────────────

def _enrich_entries(entries: list[dict]) -> list[dict]:
    """Add company name, sector, and logo availability to entries."""
    company_info = _get_company_lookup()

    for entry in entries:
        ticker = entry["ticker"]
        info = company_info.get(ticker, {})
        entry["name"] = info.get("name", "")
        entry["sector"] = info.get("sector", "")

        # Format estimates for display
        entry["eps_estimate_fmt"] = _fmt_eps(entry.get("eps_estimate"))
        entry["revenue_estimate_fmt"] = _fmt_revenue(entry.get("revenue_estimate"))
        entry["eps_actual_fmt"] = _fmt_eps(entry.get("eps_actual"))
        entry["revenue_actual_fmt"] = _fmt_revenue(entry.get("revenue_actual"))

        # Determine beat/miss if actuals available
        if entry.get("eps_actual") is not None and entry.get("eps_estimate") is not None:
            entry["beat_eps"] = entry["eps_actual"] >= entry["eps_estimate"]
        else:
            entry["beat_eps"] = None

        if entry.get("revenue_actual") is not None and entry.get("revenue_estimate") is not None:
            entry["beat_revenue"] = entry["revenue_actual"] >= entry["revenue_estimate"]
        else:
            entry["beat_revenue"] = None

    return entries


def _get_company_lookup() -> dict[str, dict]:
    """Build {symbol: {name, sector}} from market_data constituents.

    Delegates to :func:`earnings_scorecard.build_company_lookup`.
    """
    from filings.earnings_scorecard import build_company_lookup

    return build_company_lookup("sp500")


# ── Response builder ─────────────────────────────────────────────

def _build_response(
    entries: list[dict], start: str, end: str, source: str,
) -> dict:
    """Structure entries into a calendar-friendly response."""
    # Sort by date, then BMO before AMC
    timing_order = {"bmo": 0, "amc": 1, "unknown": 2}
    entries.sort(key=lambda e: (e["date"], timing_order.get(e["timing"], 2), e["ticker"]))

    # Group by date
    by_date: dict[str, list[dict]] = defaultdict(list)
    for entry in entries:
        by_date[entry["date"]].append(entry)

    # Build week structures
    weeks = _build_weeks(start, end, by_date)

    # Stats
    total = len(entries)
    bmo_count = sum(1 for e in entries if e["timing"] == "bmo")
    amc_count = sum(1 for e in entries if e["timing"] == "amc")
    confirmed = sum(1 for e in entries if e.get("confirmed"))

    # Find peak day
    peak_date = max(by_date, key=lambda d: len(by_date[d])) if by_date else ""
    peak_count = len(by_date[peak_date]) if peak_date else 0

    return {
        "entries": entries,
        "by_date": dict(by_date),
        "weeks": weeks,
        "stats": {
            "total": total,
            "bmo": bmo_count,
            "amc": amc_count,
            "confirmed": confirmed,
            "unconfirmed": total - confirmed,
            "peak_date": peak_date,
            "peak_count": peak_count,
            "unique_dates": len(by_date),
        },
        "range": {"start": start, "end": end},
        "source": source,
    }


def _build_weeks(
    start: str, end: str, by_date: dict[str, list[dict]],
) -> list[dict]:
    """Build a list of week dicts with daily breakdowns."""
    start_dt = datetime.strptime(start, "%Y-%m-%d")
    end_dt = datetime.strptime(end, "%Y-%m-%d")

    # Align to Monday
    days_since_monday = start_dt.weekday()
    week_start = start_dt - timedelta(days=days_since_monday)

    weeks = []
    while week_start <= end_dt:
        week_end = week_start + timedelta(days=4)  # Friday
        days = []
        week_total = 0

        for i in range(5):  # Mon-Fri
            day = week_start + timedelta(days=i)
            day_str = day.strftime("%Y-%m-%d")
            day_entries = by_date.get(day_str, [])
            bmo = [e for e in day_entries if e["timing"] == "bmo"]
            amc = [e for e in day_entries if e["timing"] == "amc"]
            other = [e for e in day_entries if e["timing"] == "unknown"]
            week_total += len(day_entries)

            days.append({
                "date": day_str,
                "day_name": day.strftime("%A"),
                "day_short": day.strftime("%a"),
                "month_day": day.strftime("%b %d"),
                "entries": day_entries,
                "bmo": bmo,
                "amc": amc,
                "other": other,
                "count": len(day_entries),
                "is_today": day.strftime("%Y-%m-%d") == datetime.now().strftime("%Y-%m-%d"),
            })

        weeks.append({
            "start": week_start.strftime("%Y-%m-%d"),
            "end": week_end.strftime("%Y-%m-%d"),
            "label": f"{week_start.strftime('%b %d')} – {week_end.strftime('%b %d, %Y')}",
            "days": days,
            "total": week_total,
        })

        week_start += timedelta(weeks=1)

    return weeks


# ── Formatting helpers ───────────────────────────────────────────

def _parse_timing(hour: str) -> str:
    """Parse Finnhub 'hour' field into bmo/amc/unknown."""
    h = (hour or "").lower().strip()
    if h in ("bmo", "before market open"):
        return "bmo"
    if h in ("amc", "after market close"):
        return "amc"
    return "unknown"


def _fmt_eps(val) -> str:
    if val is None:
        return "—"
    try:
        return f"${float(val):.2f}"
    except (ValueError, TypeError):
        return "—"


def _fmt_revenue(val) -> str:
    """Format revenue for display, returning ``—`` for None/invalid.

    Delegates to :func:`earnings._fmt_revenue` (returns ``""`` for None),
    substituting ``—`` for empty strings.
    """
    from filings.earnings import _fmt_revenue as _base_fmt_revenue

    result = _base_fmt_revenue(val)
    return result if result else "—"


# ── Mock data ────────────────────────────────────────────────────

_MOCK_EARNINGS = [
    ("AAPL", "Apple Inc.", "Technology", "bmo", 1.58, 98.3e9),
    ("MSFT", "Microsoft Corp.", "Technology", "amc", 2.94, 61.9e9),
    ("GOOGL", "Alphabet Inc.", "Communication Services", "amc", 1.89, 86.3e9),
    ("AMZN", "Amazon.com Inc.", "Consumer Cyclical", "amc", 1.14, 155.7e9),
    ("NVDA", "NVIDIA Corp.", "Technology", "amc", 0.82, 35.1e9),
    ("META", "Meta Platforms Inc.", "Communication Services", "amc", 5.33, 40.1e9),
    ("TSLA", "Tesla Inc.", "Consumer Cyclical", "amc", 0.73, 25.2e9),
    ("JPM", "JPMorgan Chase & Co.", "Financials", "bmo", 4.75, 41.7e9),
    ("V", "Visa Inc.", "Financials", "amc", 2.55, 9.1e9),
    ("JNJ", "Johnson & Johnson", "Healthcare", "bmo", 2.70, 21.4e9),
    ("UNH", "UnitedHealth Group", "Healthcare", "bmo", 6.91, 92.4e9),
    ("HD", "Home Depot Inc.", "Consumer Cyclical", "bmo", 3.81, 37.8e9),
    ("MA", "Mastercard Inc.", "Financials", "bmo", 3.30, 6.5e9),
    ("NFLX", "Netflix Inc.", "Communication Services", "amc", 4.52, 9.4e9),
    ("CRM", "Salesforce Inc.", "Technology", "amc", 2.45, 9.1e9),
    ("DIS", "Walt Disney Co.", "Communication Services", "amc", 1.22, 22.3e9),
    ("PFE", "Pfizer Inc.", "Healthcare", "bmo", 0.31, 14.2e9),
    ("XOM", "Exxon Mobil Corp.", "Energy", "bmo", 2.14, 84.3e9),
    ("BAC", "Bank of America", "Financials", "bmo", 0.82, 25.5e9),
    ("KO", "Coca-Cola Co.", "Consumer Defensive", "bmo", 0.72, 11.3e9),
    ("PEP", "PepsiCo Inc.", "Consumer Defensive", "bmo", 1.73, 22.5e9),
    ("AVGO", "Broadcom Inc.", "Technology", "amc", 10.99, 9.3e9),
    ("LLY", "Eli Lilly & Co.", "Healthcare", "bmo", 2.65, 9.6e9),
    ("ABBV", "AbbVie Inc.", "Healthcare", "bmo", 2.97, 13.9e9),
    ("WMT", "Walmart Inc.", "Consumer Defensive", "bmo", 1.56, 164.e9),
    ("MRK", "Merck & Co.", "Healthcare", "bmo", 1.87, 15.6e9),
    ("COST", "Costco Wholesale", "Consumer Defensive", "amc", 3.65, 58.4e9),
    ("AMD", "Advanced Micro Devices", "Technology", "amc", 0.77, 6.5e9),
    ("INTC", "Intel Corp.", "Technology", "amc", 0.13, 14.3e9),
    ("GS", "Goldman Sachs", "Financials", "bmo", 8.42, 12.5e9),
]


def _build_mock_entries(start: str, end: str) -> list[dict]:
    """Generate deterministic mock earnings entries for dev mode."""
    import hashlib

    start_dt = datetime.strptime(start, "%Y-%m-%d")
    end_dt = datetime.strptime(end, "%Y-%m-%d")

    entries = []
    current = start_dt
    mock_idx = 0

    while current <= end_dt:
        if current.weekday() < 5:  # Weekdays only
            # Deterministic count based on date
            h = int(hashlib.md5(current.strftime("%Y-%m-%d").encode()).hexdigest(), 16)
            count = 2 + (h % 5)  # 2-6 companies per day

            for _ in range(count):
                if mock_idx >= len(_MOCK_EARNINGS):
                    mock_idx = 0
                ticker, name, sector, timing, eps_est, rev_est = _MOCK_EARNINGS[mock_idx]
                mock_idx += 1

                entries.append({
                    "ticker": ticker,
                    "name": name,
                    "sector": sector,
                    "date": current.strftime("%Y-%m-%d"),
                    "timing": timing,
                    "eps_estimate": eps_est,
                    "eps_actual": None,
                    "revenue_estimate": rev_est,
                    "revenue_actual": None,
                    "year": current.year,
                    "quarter": (current.month - 1) // 3 + 1,
                    "confirmed": (h % 3) != 0,
                    "beat_eps": None,
                    "beat_revenue": None,
                    "eps_estimate_fmt": _fmt_eps(eps_est),
                    "revenue_estimate_fmt": _fmt_revenue(rev_est),
                    "eps_actual_fmt": "—",
                    "revenue_actual_fmt": "—",
                })

        current += timedelta(days=1)

    return entries
