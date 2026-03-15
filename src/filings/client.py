import logging
import os
import re

import httpx
from edgar import set_identity, find, Company, ThirteenF
from edgar.entity.search import CompanySearchResults

from filings.models import (
    SearchResult,
    Holding,
    FundInfo,
    HoldingChange,
    EnrichedHolding,
    ActivityItem,
    EnrichedActivityItem,
    ActivityCluster,
    GrandPortfolioEntry,
    StockHolder,
    StockDetail,
    StockQuarterEntry,
    StockQuarter,
    StockInfo,
)

# Set SEC identity (required for API access)
_identity = os.environ.get("SEC_IDENTITY", "13f-tool-user user@example.com")
set_identity(_identity)

_HEADERS = {"User-Agent": _identity}

logger = logging.getLogger(__name__)

# SEC 13F filings report values in *thousands* of dollars.
# The edgartools library returns raw XML values without conversion,
# so we must multiply by 1000 to get actual dollar amounts.
_SEC_13F_VALUE_MULTIPLIER = 1000

# ── Post-ingestion validation thresholds ─────────────────────────────
# A fund with 20+ holdings and total value under $10M (after x1000
# multiplier) is almost certainly wrong.  Log a warning so it gets
# caught during sync rather than silently serving bad data.
_MIN_HOLDINGS_FOR_TOTAL_CHECK = 20
_MIN_TOTAL_VALUE = 10_000_000       # $10M — fund with 20+ holdings below this is suspicious
_MIN_HOLDINGS_FOR_AVG_CHECK = 5
_MIN_VALUE_PER_HOLDING = 500_000    # $500K avg value per holding is suspicious floor


def _filing_total_value(tf) -> int:
    """Convert ThirteenF.total_value from thousands to actual dollars."""
    return int(tf.total_value) * _SEC_13F_VALUE_MULTIPLIER if tf.total_value else 0


def _row_value(row) -> int:
    """Convert a holdings DataFrame row's Value from thousands to actual dollars."""
    return int(row.Value) * _SEC_13F_VALUE_MULTIPLIER


def _validate_fund_values(
    cik: str, name: str, total_value: int, num_holdings: int
) -> None:
    """Log a warning if a fund's portfolio value looks anomalously low."""
    if num_holdings == 0 or total_value <= 0:
        return
    avg_per_holding = total_value / num_holdings
    if num_holdings >= _MIN_HOLDINGS_FOR_TOTAL_CHECK and total_value < _MIN_TOTAL_VALUE:
        logger.warning(
            "VALIDATION: Fund %s (CIK %s) has %d holdings but total value "
            "is only $%s — likely missing x1000 multiplier",
            name, cik, num_holdings, f"{total_value:,.0f}",
        )
    elif avg_per_holding < _MIN_VALUE_PER_HOLDING and num_holdings >= _MIN_HOLDINGS_FOR_AVG_CHECK:
        logger.warning(
            "VALIDATION: Fund %s (CIK %s) avg value per holding is $%s "
            "(%d holdings, $%s total) — unusually low",
            name, cik, f"{avg_per_holding:,.0f}", num_holdings,
            f"{total_value:,.0f}",
        )


def _validate_tickers(cik: str, name: str, holdings: list[dict]) -> None:
    """Log warnings for holdings with missing or invalid tickers."""
    missing = []
    for h in holdings:
        ticker = h.get("ticker")
        if not ticker:
            missing.append(h.get("issuer", "?"))
    if missing:
        logger.info(
            "TICKER: Fund %s (CIK %s) has %d/%d holdings without a valid ticker: %s",
            name, cik, len(missing), len(holdings),
            ", ".join(missing[:10]) + ("..." if len(missing) > 10 else ""),
        )


def _search_edgar_efts(query: str) -> list[SearchResult]:
    """Fallback: search EDGAR full-text search for 13F filers by name."""
    url = "https://efts.sec.gov/LATEST/search-index"
    params = {
        "q": f'"{query}"',
        "forms": "13F-HR",
        "dateRange": "custom",
        "startdt": "2023-01-01",
    }
    resp = httpx.get(url, params=params, headers=_HEADERS, timeout=15)
    resp.raise_for_status()
    data = resp.json()

    seen_ciks: set[str] = set()
    results: list[SearchResult] = []

    for hit in data.get("hits", {}).get("hits", []):
        for display_name in hit.get("_source", {}).get("display_names", []):
            # Format: "COMPANY NAME  (CIK 0001423053)"
            match = re.match(r"^(.+?)\s+\(CIK\s+(\d+)\)", display_name)
            if match:
                name = match.group(1).strip()
                cik = str(int(match.group(2)))  # strip leading zeros
                if cik not in seen_ciks:
                    seen_ciks.add(cik)
                    results.append(SearchResult(name=name, cik=cik, ticker=None))

    return results


def search_managers(query: str) -> list[SearchResult]:
    """Search for fund managers by name."""
    result = find(query)

    results: list[SearchResult] = []

    if result is not None:
        # find() returns CompanySearchResults for name queries
        if isinstance(result, CompanySearchResults):
            if not result.empty:
                for row in result.results.itertuples():
                    results.append(
                        SearchResult(
                            name=str(row.company),
                            cik=str(row.cik),
                            ticker=str(row.ticker)
                            if hasattr(row, "ticker") and row.ticker
                            else None,
                        )
                    )
        # find() may return a single Entity/Company for exact matches
        elif hasattr(result, "cik") and hasattr(result, "name"):
            ticker = None
            if hasattr(result, "tickers") and result.tickers:
                ticker = result.tickers[0]
            results.append(
                SearchResult(
                    name=str(result.name),
                    cik=str(result.cik),
                    ticker=ticker,
                )
            )

    # Also search EDGAR full-text search for 13F filers
    # This catches hedge funds and other 13F filers without tickers
    efts_results = _search_edgar_efts(query)
    seen_ciks = {r.cik for r in results}
    for r in efts_results:
        if r.cik not in seen_ciks:
            seen_ciks.add(r.cik)
            results.append(r)

    return results


def get_holdings(cik: str, top_n: int = 25) -> tuple[FundInfo, list[Holding]]:
    """Get the most recent 13F holdings for a fund by CIK."""
    company = Company(int(cik))
    filings = company.get_filings(form="13F-HR", amendments=False)

    if len(filings) == 0:
        raise ValueError(f"No 13F-HR filings found for CIK {cik} ({company.name})")

    latest = filings[0]
    tf = ThirteenF(latest)
    holdings_df = tf.holdings

    fund = FundInfo(
        name=tf.management_company_name or company.name,
        cik=cik,
        report_period=str(tf.report_period),
        filing_date=str(tf.filing_date),
        total_value=_filing_total_value(tf),
        total_holdings=len(holdings_df),
    )

    # Sort by value descending and take top N
    sorted_df = holdings_df.sort_values("Value", ascending=False).head(top_n)

    holdings = []
    for row in sorted_df.itertuples():
        holdings.append(
            Holding(
                issuer_name=str(row.Issuer),
                title_of_class=str(row.Class),
                cusip=str(row.Cusip),
                value=_row_value(row),
                shares=int(row.SharesPrnAmount),
                share_type=str(row.Type),
            )
        )

    return fund, holdings


def _compare_two_filings(current_df, previous_df) -> list[HoldingChange]:
    """Compare two filing DataFrames and return a list of HoldingChanges.

    This is the core diff algorithm used by both compare_quarters()
    and the multi-quarter history builder.
    """
    current_by_cusip = {}
    for row in current_df.itertuples():
        current_by_cusip[row.Cusip] = row

    previous_by_cusip = {}
    for row in previous_df.itertuples():
        previous_by_cusip[row.Cusip] = row

    all_cusips = set(current_by_cusip.keys()) | set(previous_by_cusip.keys())

    changes = []
    for cusip in all_cusips:
        curr = current_by_cusip.get(cusip)
        prev = previous_by_cusip.get(cusip)

        curr_shares = int(curr.SharesPrnAmount) if curr else 0
        prev_shares = int(prev.SharesPrnAmount) if prev else 0
        curr_value = _row_value(curr) if curr else 0
        prev_value = _row_value(prev) if prev else 0
        name = str(curr.Issuer) if curr else str(prev.Issuer)

        if prev is None:
            status = "NEW"
        elif curr is None:
            status = "CLOSED"
        elif curr_shares > prev_shares:
            status = "INCREASED"
        elif curr_shares < prev_shares:
            status = "DECREASED"
        else:
            status = "UNCHANGED"

        changes.append(
            HoldingChange(
                issuer_name=name,
                cusip=cusip,
                status=status,
                current_shares=curr_shares,
                previous_shares=prev_shares,
                share_change=curr_shares - prev_shares,
                current_value=curr_value,
                previous_value=prev_value,
            )
        )

    return changes


def _report_period_to_quarter_label(report_period: str) -> str:
    """Convert '09-30-2025' or '2025-09-30' to 'Q3 2025'."""
    parts = report_period.split("-")
    if len(parts[0]) == 4:
        # YYYY-MM-DD
        year, month = parts[0], int(parts[1])
    else:
        # MM-DD-YYYY
        month, year = int(parts[0]), parts[2]
    quarter = (month - 1) // 3 + 1
    return f"Q{quarter} {year}"


def compare_quarters(
    cik: str, top_n: int = 25
) -> tuple[FundInfo, FundInfo, list[HoldingChange]]:
    """Compare the latest two quarters of 13F holdings for a fund."""
    company = Company(int(cik))
    filings = company.get_filings(form="13F-HR", amendments=False)

    if len(filings) < 2:
        raise ValueError(
            f"Need at least 2 filings to compare, but CIK {cik} ({company.name}) "
            f"has {len(filings)}"
        )

    tf_current = ThirteenF(filings[0])
    tf_previous = ThirteenF(filings[1])

    current_info = FundInfo(
        name=tf_current.management_company_name or company.name,
        cik=cik,
        report_period=str(tf_current.report_period),
        filing_date=str(tf_current.filing_date),
        total_value=_filing_total_value(tf_current),
        total_holdings=len(tf_current.holdings),
    )
    previous_info = FundInfo(
        name=tf_previous.management_company_name or company.name,
        cik=cik,
        report_period=str(tf_previous.report_period),
        filing_date=str(tf_previous.filing_date),
        total_value=_filing_total_value(tf_previous),
        total_holdings=len(tf_previous.holdings),
    )

    changes = _compare_two_filings(tf_current.holdings, tf_previous.holdings)

    # Sort: NEW first, then CLOSED, then by absolute share change descending
    status_order = {
        "NEW": 0,
        "CLOSED": 1,
        "INCREASED": 2,
        "DECREASED": 3,
        "UNCHANGED": 4,
    }
    changes.sort(key=lambda c: (status_order.get(c.status, 5), -abs(c.share_change)))

    return current_info, previous_info, changes[:top_n]


# ── Ticker correction tables ──────────────────────────────────────
# The edgartools CUSIP→ticker mapping (ct.pq) is static and incomplete.
# These tables patch known gaps at two levels:
#
# 1. _CUSIP_OVERRIDES: CUSIPs missing from ct.pq entirely, or mapped to
#    a wrong/stale ticker.  Checked first — highest priority.
# 2. _TICKER_CORRECTIONS: ticker-level renames (FB→META, TWTR→X).
#    Applied after CUSIP lookup when the CUSIP *does* resolve but to an
#    outdated symbol.
#
# To add a new correction: identify whether the problem is a missing CUSIP
# or a stale ticker, and add to the appropriate table.

_CUSIP_OVERRIDES: dict[str, str] = {
    # Hilton Grand Vacations — CUSIP changed after spin-off, not in ct.pq
    "46321A104": "HGV",
    # Hilton Worldwide Holdings — alternate CUSIP not in ct.pq
    "432848101": "HLT",
    # Compagnie Financière Richemont — Swiss CUSIP, no US listing
    # Maps to the US-traded ADR (CFRUY) for display purposes
    "H25662105": "CFRUY",
}

_TICKER_CORRECTIONS: dict[str, str] = {
    # Ticker renames / migrations
    "FB": "META",        # Meta Platforms, June 2022
    "TWTR": "X",         # Twitter → X Corp, October 2023
    # edgartools padding artifacts (5-char field overflow)
    "BMNRD": "BMNR",
}

# Pre-compiled regex for ticker validation.  Valid tickers are 1-6
# alphanumeric characters, optionally with dots (e.g. BRK.A).
_VALID_TICKER_RE = re.compile(r'^[A-Z0-9.]{1,6}$', re.IGNORECASE)


def _is_valid_ticker(t: str) -> bool:
    """Check if a string looks like a valid ticker symbol.

    Rejects strings with spaces, special chars, >6 chars,
    or that are clearly truncated company names.
    """
    if not t:
        return False
    return bool(_VALID_TICKER_RE.match(t))


def _safe_ticker(row) -> str | None:
    """Extract and clean ticker from a DataFrame row.

    Resolution order:
    1. CUSIP override (from row.Cusip if present)
    2. edgartools Ticker column (from ct.pq CUSIP mapping)
    3. Ticker correction (rename table)
    4. Validation — reject malformed tickers (spaces, >6 chars, special chars)

    Returns None if no valid ticker can be resolved.
    """
    # 1. CUSIP-level override (highest priority)
    cusip = str(row.Cusip) if hasattr(row, "Cusip") else None
    if cusip:
        override = _CUSIP_OVERRIDES.get(cusip)
        if override:
            return override

    # 2. Extract from edgartools Ticker column
    t = None
    if hasattr(row, "Ticker"):
        val = row.Ticker
        if val and str(val).strip() not in ("", "nan", "None", "NaN"):
            t = str(val).strip()

    if t is None:
        return None

    # 3. Apply ticker-level corrections
    t = _TICKER_CORRECTIONS.get(t, t)

    # 4. Validate
    if not _is_valid_ticker(t):
        logger.debug(
            "Rejected invalid ticker %r (CUSIP %s, issuer %s)",
            t,
            cusip or "?",
            getattr(row, "Issuer", "?"),
        )
        return None

    return t


def get_fund_summary(cik: str, history_quarters: int = 8) -> dict:
    """Fetch all data for a single fund (for caching).
    Returns a dict with fund info, holdings, changes, and quarterly history."""
    company = Company(int(cik))
    filings = company.get_filings(form="13F-HR", amendments=False)

    if len(filings) == 0:
        raise ValueError(f"No 13F-HR filings found for CIK {cik}")

    tf = ThirteenF(filings[0])
    holdings_df = tf.holdings
    total_val = _filing_total_value(tf)

    sorted_df = holdings_df.sort_values("Value", ascending=False)

    top_holdings = []
    for row in sorted_df.head(10).itertuples():
        cusip = str(row.Cusip)
        top_holdings.append(
            {
                "issuer": str(row.Issuer),
                "ticker": _safe_ticker(row),
                "cusip": cusip,
                "value": _row_value(row),
                "shares": int(row.SharesPrnAmount),
            }
        )

    all_holdings = []
    for row in sorted_df.itertuples():
        val = _row_value(row)
        cusip = str(row.Cusip)
        all_holdings.append(
            {
                "issuer": str(row.Issuer),
                "ticker": _safe_ticker(row),
                "cusip": cusip,
                "value": val,
                "shares": int(row.SharesPrnAmount),
                "pct": round(val / total_val * 100, 2) if total_val > 0 else 0,
            }
        )

    # Multi-quarter change history
    num_pairs = min(history_quarters, len(filings) - 1)
    quarterly_changes = []
    flat_changes = []  # Backwards-compatible "changes" field (most recent only)

    for i in range(num_pairs):
        try:
            tf_newer = tf if i == 0 else ThirteenF(filings[i])
            tf_older = ThirteenF(filings[i + 1])

            change_list = _compare_two_filings(tf_newer.holdings, tf_older.holdings)

            period_label = _report_period_to_quarter_label(str(tf_newer.report_period))

            quarter_entry = {
                "period": period_label,
                "report_period": str(tf_newer.report_period),
                "filing_date": str(tf_newer.filing_date),
                "changes": [],
            }

            for c in change_list:
                if c.status == "UNCHANGED":
                    continue
                change_dict = {
                    "issuer": c.issuer_name,
                    "cusip": c.cusip,
                    "status": c.status,
                    "share_change": c.share_change,
                    "current_value": c.current_value,
                    "current_shares": c.current_shares,
                    "previous_shares": c.previous_shares,
                }
                quarter_entry["changes"].append(change_dict)

                # First pair populates flat changes for backwards compat
                if i == 0:
                    flat_changes.append(
                        {
                            "issuer": c.issuer_name,
                            "cusip": c.cusip,
                            "status": c.status,
                            "share_change": c.share_change,
                            "current_value": c.current_value,
                        }
                    )

            quarterly_changes.append(quarter_entry)
        except Exception:
            continue

    fund_name = tf.management_company_name or company.name
    _validate_fund_values(cik, fund_name, total_val, len(holdings_df))
    _validate_tickers(cik, fund_name, all_holdings)

    return {
        "name": fund_name,
        "cik": cik,
        "report_period": str(tf.report_period),
        "filing_date": str(tf.filing_date),
        "total_value": total_val,
        "total_holdings": len(holdings_df),
        "top_holdings": top_holdings,
        "all_holdings": all_holdings,
        "changes": flat_changes,
        "quarterly_changes": quarterly_changes,
    }


def get_enriched_holdings(
    cik: str, top_n: int = 25
) -> tuple[FundInfo, list[EnrichedHolding]]:
    """Get holdings with ticker, portfolio %, and activity status."""
    company = Company(int(cik))
    filings = company.get_filings(form="13F-HR", amendments=False)

    if len(filings) == 0:
        raise ValueError(f"No 13F-HR filings found for CIK {cik} ({company.name})")

    tf = ThirteenF(filings[0])
    holdings_df = tf.holdings
    total_val = _filing_total_value(tf)

    fund = FundInfo(
        name=tf.management_company_name or company.name,
        cik=cik,
        report_period=str(tf.report_period),
        filing_date=str(tf.filing_date),
        total_value=total_val,
        total_holdings=len(holdings_df),
    )

    # Get activity data from compare
    activity_by_cusip: dict[str, HoldingChange] = {}
    if len(filings) >= 2:
        try:
            _, _, change_list = compare_quarters(cik, top_n=9999)
            for c in change_list:
                activity_by_cusip[c.cusip] = c
        except Exception:
            pass

    sorted_df = holdings_df.sort_values("Value", ascending=False).head(top_n)

    holdings = []
    for row in sorted_df.itertuples():
        val = _row_value(row)
        cusip = str(row.Cusip)
        change = activity_by_cusip.get(cusip)
        activity = change.status if change and change.status != "UNCHANGED" else None
        share_change = change.share_change if change else 0

        # Map status to user-friendly labels
        activity_label = None
        if activity == "NEW":
            activity_label = "NEW BUY"
        elif activity == "INCREASED":
            activity_label = "ADD"
        elif activity == "DECREASED":
            activity_label = "REDUCE"
        elif activity == "CLOSED":
            activity_label = "SOLD"

        holdings.append(
            EnrichedHolding(
                issuer_name=str(row.Issuer),
                title_of_class=str(row.Class),
                cusip=cusip,
                value=val,
                shares=int(row.SharesPrnAmount),
                share_type=str(row.Type),
                ticker=_safe_ticker(row),
                pct_of_portfolio=round(val / total_val * 100, 2)
                if total_val > 0
                else 0,
                activity=activity_label,
                share_change=share_change,
            )
        )

    return fund, holdings


# ── Cache-first helpers ─────────────────────────────────────────────
# These reconstruct model objects from the cached dict produced by
# ``get_fund_summary()``, avoiding SEC API calls entirely.


def get_enriched_holdings_from_cache(
    fund_data: dict,
    cik: str,
    top_n: int = 25,
) -> tuple[FundInfo, list[EnrichedHolding]]:
    """Build FundInfo + EnrichedHoldings from cached fund data.

    Equivalent to ``get_enriched_holdings()`` but zero API calls.
    Returns the same types so templates work unchanged.
    """
    total_val = fund_data.get("total_value", 0)

    fund = FundInfo(
        name=fund_data.get("name", "Unknown"),
        cik=cik,
        report_period=fund_data.get("report_period", ""),
        filing_date=fund_data.get("filing_date", ""),
        total_value=total_val,
        total_holdings=fund_data.get("total_holdings", 0),
    )

    # Build activity lookup from flat changes
    status_labels = {
        "NEW": "NEW BUY",
        "CLOSED": "SOLD",
        "INCREASED": "ADD",
        "DECREASED": "REDUCE",
    }
    change_by_cusip: dict[str, dict] = {}
    for ch in fund_data.get("changes", []):
        change_by_cusip[ch["cusip"]] = ch

    holdings: list[EnrichedHolding] = []
    for h in fund_data.get("all_holdings", [])[:top_n]:
        val = h.get("value", 0)
        cusip = h.get("cusip", "")
        change = change_by_cusip.get(cusip)

        activity_label = None
        share_change = 0
        if change:
            activity_label = status_labels.get(change.get("status"))
            share_change = change.get("share_change", 0)

        holdings.append(
            EnrichedHolding(
                issuer_name=h.get("issuer", ""),
                title_of_class="COM",  # not stored in cache; safe default
                cusip=cusip,
                value=val,
                shares=h.get("shares", 0),
                share_type="SH",  # not stored in cache; safe default
                ticker=h.get("ticker"),
                pct_of_portfolio=h.get(
                    "pct", round(val / total_val * 100, 2) if total_val else 0
                ),
                activity=activity_label,
                share_change=share_change,
            )
        )

    return fund, holdings


def get_compare_from_cache(
    fund_data: dict,
    cik: str,
    top_n: int = 25,
) -> tuple[FundInfo, FundInfo | None, list[HoldingChange]]:
    """Build quarter-over-quarter comparison from cached fund data.

    Reconstructs the same types as ``compare_quarters()`` from
    the ``quarterly_changes`` stored by ``get_fund_summary()``.
    Zero API calls.

    Returns (current_info, previous_info, changes).
    ``previous_info`` may be ``None`` if only one quarter is cached.
    """
    quarterly = fund_data.get("quarterly_changes", [])

    current_info = FundInfo(
        name=fund_data.get("name", "Unknown"),
        cik=cik,
        report_period=fund_data.get("report_period", ""),
        filing_date=fund_data.get("filing_date", ""),
        total_value=fund_data.get("total_value", 0),
        total_holdings=fund_data.get("total_holdings", 0),
    )

    if not quarterly:
        return current_info, None, []

    # The first quarterly entry is current→previous diff
    latest_q = quarterly[0]

    # Build previous_info from second quarter if available
    previous_info = None
    if len(quarterly) >= 2:
        q2 = quarterly[1]
        previous_info = FundInfo(
            name=fund_data.get("name", "Unknown"),
            cik=cik,
            report_period=q2.get("report_period", ""),
            filing_date=q2.get("filing_date", ""),
            total_value=0,  # not stored per-quarter in cache
            total_holdings=0,
        )
    else:
        previous_info = FundInfo(
            name=fund_data.get("name", "Unknown"),
            cik=cik,
            report_period=latest_q.get("report_period", ""),
            filing_date=latest_q.get("filing_date", ""),
            total_value=0,
            total_holdings=0,
        )

    # Reconstruct HoldingChange objects from the latest quarter's changes
    status_order = {
        "NEW": 0,
        "CLOSED": 1,
        "INCREASED": 2,
        "DECREASED": 3,
        "UNCHANGED": 4,
    }
    changes: list[HoldingChange] = []
    for ch in latest_q.get("changes", []):
        changes.append(
            HoldingChange(
                issuer_name=ch.get("issuer", ""),
                cusip=ch.get("cusip", ""),
                status=ch.get("status", "UNCHANGED"),
                current_shares=ch.get("current_shares", 0),
                previous_shares=ch.get("previous_shares", 0),
                share_change=ch.get("share_change", 0),
                current_value=ch.get("current_value", 0),
                previous_value=0,  # not stored in quarterly cache
            )
        )

    changes.sort(key=lambda c: (status_order.get(c.status, 5), -abs(c.share_change)))
    return current_info, previous_info, changes[:top_n]


def build_activity_feed(
    cache_data: dict, superinvestors_by_cik: dict
) -> list[ActivityItem]:
    """Build activity feed from cached data. Zero API calls."""
    status_labels = {
        "NEW": "NEW BUY",
        "CLOSED": "SOLD",
        "INCREASED": "ADD",
        "DECREASED": "REDUCE",
    }

    activities = []
    for cik, fund_data in cache_data.items():
        si = superinvestors_by_cik.get(cik)
        if not si:
            continue
        display_name = si.display_name

        # Find tickers from holdings for matching
        ticker_by_cusip = {}
        for h in fund_data.get("all_holdings", []):
            ticker_by_cusip[h["cusip"]] = h.get("ticker")

        for change in fund_data.get("changes", []):
            label = status_labels.get(change["status"])
            if not label:
                continue
            activities.append(
                ActivityItem(
                    fund_display_name=display_name,
                    fund_cik=cik,
                    issuer_name=change["issuer"],
                    ticker=ticker_by_cusip.get(change["cusip"]),
                    cusip=change["cusip"],
                    action=label,
                    share_change=change["share_change"],
                    current_value=change["current_value"],
                    filing_date=fund_data.get("filing_date", ""),
                )
            )

    # Sort by absolute value descending
    activities.sort(key=lambda a: -abs(a.current_value))
    return activities


def build_enriched_activity_feed(
    cache_data: dict,
    superinvestors_by_cik: dict,
    price_data: dict[str, float] | None = None,
    timeframe: str = "ALL",
    ptype: str = "guru",
) -> tuple[list[ActivityCluster], list[EnrichedActivityItem], dict]:
    """Build enriched activity feed with conviction scores and clustering.

    Returns ``(clusters, solo_items, stats)``.

    *ptype* -- ``"guru"`` filters to funds with < 100 holdings
    (high-conviction).  ``"institutional"`` filters to funds with >= 100
    holdings (quant/diversified).

    *clusters* -- list of ``ActivityCluster`` where 2+ investors acted on the
    same ticker.  Sorted by investor_count desc, combined_value desc.

    *solo_items* -- remaining ``EnrichedActivityItem`` entries where only one
    investor acted.  Sorted by current_value desc.

    *stats* -- dict with sentiment summary and sector breakdown for the header.
    """
    from collections import defaultdict
    from datetime import datetime, timedelta

    status_labels = {
        "NEW": "NEW BUY",
        "CLOSED": "SOLD",
        "INCREASED": "ADD",
        "DECREASED": "REDUCE",
    }

    if price_data is None:
        price_data = {}

    # ── 1. Build all enriched items ──────────────────────────────────
    all_items: list[EnrichedActivityItem] = []

    for cik, fund_data in cache_data.items():
        si = superinvestors_by_cik.get(cik)
        if not si:
            continue

        # Portfolio type filter (anti-noise engine)
        total_holdings = fund_data.get("total_holdings", 0)
        if ptype == "guru" and total_holdings >= 100:
            continue
        elif ptype == "institutional" and total_holdings < 100:
            continue

        fund_aum = fund_data.get("total_value", 0)
        filing_date = fund_data.get("filing_date", "")

        # Build cusip -> portfolio weight + ticker maps
        weight_by_cusip: dict[str, float] = {}
        ticker_by_cusip: dict[str, str | None] = {}
        for h in fund_data.get("all_holdings", []):
            weight_by_cusip[h["cusip"]] = h.get("pct", 0.0)
            ticker_by_cusip[h["cusip"]] = h.get("ticker")

        # Use quarterly_changes (has current_shares/previous_shares) with
        # fallback to flat changes for backward compatibility
        quarterly = fund_data.get("quarterly_changes", [])
        changes_source = (
            quarterly[0]["changes"] if quarterly else fund_data.get("changes", [])
        )

        for change in changes_source:
            label = status_labels.get(change["status"])
            if not label:
                continue

            cusip = change["cusip"]
            ticker = ticker_by_cusip.get(cusip)
            pct_weight = weight_by_cusip.get(cusip, 0.0)
            conviction = abs(change["share_change"]) * pct_weight / 100.0

            current_price = price_data.get(ticker) if ticker else None

            # Compute pct share change for HEAVY ADD detection
            prev_shares = change.get("previous_shares", 0)
            if change["status"] == "NEW":
                pct_share_change = 100.0
            elif change["status"] == "CLOSED":
                pct_share_change = -100.0
            elif prev_shares and prev_shares > 0:
                pct_share_change = round(change["share_change"] / prev_shares * 100, 1)
            else:
                pct_share_change = None

            # Portfolio impact = trade value as % of fund AUM
            portfolio_impact = (
                round(change["current_value"] / fund_aum * 100, 2)
                if fund_aum > 0
                else 0.0
            )

            # Estimate dollar value of this specific trade
            current_shares = change.get("current_shares", 0)
            if current_price and abs(change["share_change"]) > 0:
                trade_value = abs(change["share_change"]) * current_price
            elif current_shares > 0 and change["current_value"] > 0:
                implied_price = change["current_value"] / current_shares
                trade_value = abs(change["share_change"]) * implied_price
            else:
                trade_value = float(abs(change["current_value"]))

            all_items.append(
                EnrichedActivityItem(
                    fund_display_name=si.display_name,
                    fund_cik=cik,
                    fund_aum=fund_aum,
                    issuer_name=change["issuer"],
                    ticker=ticker,
                    cusip=cusip,
                    action=label,
                    signal="",  # assigned below
                    share_change=change["share_change"],
                    current_value=change["current_value"],
                    portfolio_weight=round(pct_weight, 2),
                    conviction=round(conviction, 2),
                    filing_date=filing_date,
                    price_at_filing=None,
                    current_price=current_price,
                    price_change_pct=None,
                    fund_total_holdings=total_holdings,
                    portfolio_impact=portfolio_impact,
                    pct_share_change=pct_share_change,
                    trade_value=round(trade_value, 2),
                )
            )

    # ── 2. Filter by timeframe ───────────────────────────────────────
    if timeframe != "ALL" and all_items:
        now = datetime.now()
        if timeframe == "1W":
            cutoff = now - timedelta(days=7)
        elif timeframe == "1M":
            cutoff = now - timedelta(days=30)
        else:
            cutoff = None

        if cutoff:
            filtered = []
            for item in all_items:
                try:
                    fd = datetime.strptime(item.filing_date, "%Y-%m-%d")
                    if fd >= cutoff:
                        filtered.append(item)
                except (ValueError, TypeError):
                    filtered.append(item)  # keep items with unparseable dates
            all_items = filtered

    # ── 3. Assign signal labels (fixed >10% share change threshold) ──
    for item in all_items:
        if item.action == "NEW BUY":
            item.signal = "NEW ENTRY"
        elif item.action == "SOLD":
            item.signal = "LIQUIDATED"
        elif item.action == "ADD":
            if item.pct_share_change is not None and item.pct_share_change > 10.0:
                item.signal = "HEAVY ADD"
            else:
                item.signal = "ADD"
        elif item.action == "REDUCE":
            if item.pct_share_change is not None and abs(item.pct_share_change) > 10.0:
                item.signal = "TRIM"
            else:
                item.signal = "REDUCE"

    # ── 4. Build clusters (group by ticker or cusip) ─────────────────
    groups: dict[str, list[EnrichedActivityItem]] = defaultdict(list)
    for item in all_items:
        key = (item.ticker or item.cusip).upper()
        groups[key].append(item)

    clusters: list[ActivityCluster] = []
    solo_items: list[EnrichedActivityItem] = []

    for _key, items in groups.items():
        if len(items) >= 2:
            buy_count = sum(1 for i in items if i.action in ("NEW BUY", "ADD"))
            sell_count = sum(1 for i in items if i.action in ("SOLD", "REDUCE"))

            buy_part = f"{buy_count} Buying" if buy_count else ""
            sell_part = f"{sell_count} Selling" if sell_count else ""
            action_summary = " / ".join(filter(None, [buy_part, sell_part]))

            # Value-weighted sentiment (dollar flow, not transaction count)
            buy_value = sum(
                i.trade_value for i in items if i.action in ("NEW BUY", "ADD")
            )
            sell_value = sum(
                i.trade_value for i in items if i.action in ("SOLD", "REDUCE")
            )
            net_flow = buy_value - sell_value
            gross_flow = buy_value + sell_value

            if gross_flow > 0 and abs(net_flow) / gross_flow > 0.05:
                sentiment = "BULLISH" if net_flow > 0 else "BEARISH"
            else:
                sentiment = "NEUTRAL"

            convictions = [i.conviction for i in items]
            avg_conv = (
                round(sum(convictions) / len(convictions), 2) if convictions else 0
            )

            # Sort items within cluster by conviction desc
            items.sort(key=lambda i: -i.conviction)

            clusters.append(
                ActivityCluster(
                    ticker=items[0].ticker,
                    cusip=items[0].cusip,
                    issuer_name=items[0].issuer_name,
                    action_summary=action_summary,
                    net_sentiment=sentiment,
                    investor_count=len(items),
                    combined_value=sum(i.current_value for i in items),
                    avg_conviction=avg_conv,
                    items=items,
                    buy_value=round(buy_value, 2),
                    sell_value=round(sell_value, 2),
                    net_flow=round(net_flow, 2),
                )
            )
        else:
            solo_items.extend(items)

    # Sort clusters by investor_count desc, then combined_value desc
    clusters.sort(key=lambda c: (-c.investor_count, -c.combined_value))
    # Sort solo items by current_value desc
    solo_items.sort(key=lambda i: -abs(i.current_value))

    # ── 5. Compute stats ─────────────────────────────────────────────
    total = len(all_items)
    buy_count = sum(1 for i in all_items if i.action in ("NEW BUY", "ADD"))
    sell_count = sum(1 for i in all_items if i.action in ("SOLD", "REDUCE"))

    buying_pct = round(buy_count / total * 100) if total else 0
    selling_pct = round(sell_count / total * 100) if total else 0

    # Value-weighted sentiment (net dollar flow)
    total_buy_value = sum(
        i.trade_value for i in all_items if i.action in ("NEW BUY", "ADD")
    )
    total_sell_value = sum(
        i.trade_value for i in all_items if i.action in ("SOLD", "REDUCE")
    )
    net_dollar_flow = total_buy_value - total_sell_value
    gross_dollar_flow = total_buy_value + total_sell_value

    if gross_dollar_flow > 0 and abs(net_dollar_flow) / gross_dollar_flow > 0.05:
        value_sentiment = "BULLISH" if net_dollar_flow > 0 else "BEARISH"
    else:
        value_sentiment = "NEUTRAL"

    # Sector breakdown (cross-ref with S&P 500 constituents)
    sector_counts: dict[str, int] = defaultdict(int)
    try:
        from filings.market_data import get_sp500_constituents

        constituents = get_sp500_constituents()
        ticker_to_sector = {
            c["ticker"].upper(): c.get("sector", "Other") for c in constituents
        }
        for item in all_items:
            if item.ticker:
                sector = ticker_to_sector.get(item.ticker.upper(), "Other")
                sector_counts[sector] += 1
    except Exception:
        pass

    most_active_sector = (
        max(sector_counts, key=sector_counts.get, default="N/A")
        if sector_counts
        else "N/A"
    )

    consensus_count = len(clusters)

    stats = {
        "total_activities": total,
        "buying_pct": buying_pct,
        "selling_pct": selling_pct,
        "total_buy_value": round(total_buy_value, 2),
        "total_sell_value": round(total_sell_value, 2),
        "net_dollar_flow": round(net_dollar_flow, 2),
        "value_sentiment": value_sentiment,
        "most_active_sector": most_active_sector,
        "consensus_count": consensus_count,
    }

    return clusters, solo_items, stats


def build_ticker_ownership_map(
    cache_data: dict,
    superinvestors_by_cik: dict,
) -> dict[str, list[str]]:
    """Build a map of ticker -> list of superinvestor display names who hold it.

    Single pass through the cache. Returns only tickers held by at least one
    recognized superinvestor.  Zero API calls.
    """
    by_ticker: dict[str, list[str]] = {}
    for cik, fund_data in cache_data.items():
        si = superinvestors_by_cik.get(cik)
        if not si:
            continue
        for h in fund_data.get("all_holdings", []):
            t = h.get("ticker")
            if not t:
                continue
            t_upper = t.upper()
            by_ticker.setdefault(t_upper, []).append(si.display_name)
    return by_ticker


def build_grand_portfolio(
    cache_data: dict, superinvestors_by_cik: dict
) -> list[GrandPortfolioEntry]:
    """Build aggregated grand portfolio from cached data. Zero API calls."""
    # Group by CUSIP across all funds
    by_cusip: dict[str, dict] = {}
    total_aggregate = 0

    for cik, fund_data in cache_data.items():
        si = superinvestors_by_cik.get(cik)
        if not si:
            continue
        for h in fund_data.get("all_holdings", []):
            cusip = h["cusip"]
            val = h["value"]
            total_aggregate += val
            if cusip not in by_cusip:
                by_cusip[cusip] = {
                    "issuer_name": h["issuer"],
                    "ticker": h.get("ticker"),
                    "cusip": cusip,
                    "combined_value": 0,
                    "holders": [],
                    "weights": [],
                }
            by_cusip[cusip]["combined_value"] += val
            by_cusip[cusip]["holders"].append(si.display_name)
            by_cusip[cusip]["weights"].append(h.get("pct", 0))
            # Prefer a non-None ticker
            if h.get("ticker") and not by_cusip[cusip]["ticker"]:
                by_cusip[cusip]["ticker"] = h["ticker"]

    entries = []
    for data in by_cusip.values():
        weights = data["weights"]
        entries.append(
            GrandPortfolioEntry(
                issuer_name=data["issuer_name"],
                ticker=data["ticker"],
                cusip=data["cusip"],
                num_holders=len(data["holders"]),
                combined_value=data["combined_value"],
                pct_of_aggregate=round(
                    data["combined_value"] / total_aggregate * 100, 3
                )
                if total_aggregate > 0
                else 0,
                holders=data["holders"],
                avg_weight=sum(weights) / len(weights) if weights else 0,
            )
        )

    # Sort by number of holders (desc), then by combined value (desc)
    entries.sort(key=lambda e: (-e.num_holders, -e.combined_value))
    return entries


def build_stock_detail(
    lookup: str,
    cache_data: dict,
    superinvestors_by_cik: dict,
    *,
    by_cusip: bool = False,
) -> StockDetail | None:
    """Build stock detail from cached data. Zero API calls.

    Args:
        lookup: Ticker symbol (e.g. "AMZN") or CUSIP if by_cusip=True.
        cache_data: The full fund_cache dict (keyed by CIK).
        superinvestors_by_cik: Mapping from CIK to SuperinvestorInfo.
        by_cusip: If True, match by CUSIP instead of ticker.

    Returns:
        StockDetail or None if no fund holds this stock.
    """
    status_labels = {
        "NEW": "NEW BUY",
        "CLOSED": "SOLD",
        "INCREASED": "ADD",
        "DECREASED": "REDUCE",
    }

    lookup_upper = lookup.upper().strip()

    issuer_name = None
    ticker = None
    cusip = None
    holders: list[StockHolder] = []
    combined_value = 0

    for cik, fund_data in cache_data.items():
        si = superinvestors_by_cik.get(cik)
        if not si:
            continue

        total_val = fund_data.get("total_value", 0)

        # Build change lookup for this fund (cusip -> change dict)
        change_by_cusip: dict[str, dict] = {}
        for ch in fund_data.get("changes", []):
            change_by_cusip[ch["cusip"]] = ch

        for h in fund_data.get("all_holdings", []):
            # Match logic
            if by_cusip:
                if h["cusip"].upper() != lookup_upper:
                    continue
            else:
                h_ticker = h.get("ticker")
                if not h_ticker or h_ticker.upper() != lookup_upper:
                    continue

            # Found a match in this fund
            val = h["value"]
            combined_value += val

            # Capture stock-level info from first match
            if issuer_name is None:
                issuer_name = h["issuer"]
                cusip = h["cusip"]
                ticker = h.get("ticker")
            # Prefer a non-None ticker
            if h.get("ticker") and not ticker:
                ticker = h["ticker"]

            # Compute activity for this holding
            change = change_by_cusip.get(h["cusip"])
            activity_label = None
            share_change = 0
            if change:
                activity_label = status_labels.get(change["status"])
                share_change = change["share_change"]

            pct = h.get("pct", 0.0)
            if pct == 0 and total_val > 0:
                pct = round(val / total_val * 100, 2)

            holders.append(
                StockHolder(
                    fund_display_name=si.display_name,
                    fund_cik=cik,
                    pct_of_portfolio=pct,
                    value=val,
                    shares=h["shares"],
                    activity=activity_label,
                    share_change=share_change,
                )
            )

    if not holders:
        return None

    # Sort holders by value descending (largest position first)
    holders.sort(key=lambda h: -h.value)

    return StockDetail(
        issuer_name=issuer_name,
        ticker=ticker,
        cusip=cusip,
        num_holders=len(holders),
        combined_value=combined_value,
        holders=holders,
    )


def build_stock_history(
    lookup: str,
    cache_data: dict,
    superinvestors_by_cik: dict,
    *,
    by_cusip: bool = False,
) -> list[StockQuarter]:
    """Build multi-quarter activity history for a stock from cached data.

    Returns a list of StockQuarter objects, one per quarter that had any
    activity on this stock, sorted most recent first. Zero API calls.
    """
    status_labels = {
        "NEW": "NEW BUY",
        "CLOSED": "SOLD",
        "INCREASED": "ADD",
        "DECREASED": "REDUCE",
    }

    lookup_upper = lookup.upper().strip()

    # ── Phase 1: Build matching CUSIP set in a single pass ──
    # Index holdings by ticker and cusip for O(1) lookups
    matching_cusips: set[str] = set()
    for cik, fund_data in cache_data.items():
        for h in fund_data.get("all_holdings", []):
            if by_cusip:
                if h["cusip"].upper() == lookup_upper:
                    matching_cusips.add(h["cusip"])
            else:
                h_ticker = h.get("ticker")
                if h_ticker and h_ticker.upper() == lookup_upper:
                    matching_cusips.add(h["cusip"])
        # Also check quarterly_changes for CUSIPs (stock may have been sold)
        if by_cusip:
            for qc in fund_data.get("quarterly_changes", []):
                for ch in qc.get("changes", []):
                    if ch["cusip"].upper() == lookup_upper:
                        matching_cusips.add(ch["cusip"])

    if not matching_cusips:
        return []

    # ── Phase 2: Build quarters in a single pass with CUSIP set lookup (O(1)) ──
    quarters: dict[str, dict] = {}  # period -> {"report_date": ..., "entries": [...]}

    for cik, fund_data in cache_data.items():
        si = superinvestors_by_cik.get(cik)
        if not si:
            continue

        for qc in fund_data.get("quarterly_changes", []):
            period = qc["period"]
            report_date = qc.get("report_period", "")

            for ch in qc.get("changes", []):
                if ch["cusip"] not in matching_cusips:
                    continue

                label = status_labels.get(ch["status"])
                if not label:
                    continue

                prev_shares = ch.get("previous_shares", 0)
                pct_change = 0.0
                if prev_shares and prev_shares > 0:
                    pct_change = round((ch["share_change"] / prev_shares) * 100, 1)
                elif ch["status"] == "NEW":
                    pct_change = 100.0

                entry = StockQuarterEntry(
                    fund_display_name=si.display_name,
                    fund_cik=cik,
                    activity=label,
                    share_change=ch["share_change"],
                    pct_change=pct_change,
                )

                if period not in quarters:
                    quarters[period] = {
                        "report_date": report_date,
                        "entries": [],
                    }
                quarters[period]["entries"].append(entry)

    # Sort entries within each quarter: buys first, then adds, reduces, sells
    activity_order = {"NEW BUY": 0, "ADD": 1, "REDUCE": 2, "SOLD": 3}

    result = []
    for period, data in quarters.items():
        entries = data["entries"]
        entries.sort(
            key=lambda e: (activity_order.get(e.activity, 99), e.fund_display_name)
        )
        result.append(
            StockQuarter(
                period=period,
                report_date=data["report_date"],
                entries=entries,
            )
        )

    # Sort quarters most recent first
    def quarter_sort_key(sq: StockQuarter) -> tuple[int, int]:
        try:
            parts = sq.period.split()
            q_num = int(parts[0][1])
            year = int(parts[1])
            return (-year, -q_num)
        except (IndexError, ValueError):
            return (0, 0)

    result.sort(key=quarter_sort_key)

    return result


# ── yfinance info cache (single fetch per ticker, 1-hour TTL) ──
import time as _time
import threading as _threading

_yf_info_cache: dict[str, tuple[float, dict]] = {}
_yf_info_lock = _threading.Lock()
_YF_INFO_TTL = 3600  # 1 hour


def get_yfinance_info(ticker: str) -> dict:
    """Fetch yfinance info for a ticker with TTL cache. Single network call.

    This is the **canonical** cached accessor for ``yf.Ticker(ticker).info``.
    All modules should import this instead of making independent yfinance calls
    to avoid redundant HTTP requests for the same ticker.
    """
    now = _time.monotonic()
    with _yf_info_lock:
        cached = _yf_info_cache.get(ticker)
        if cached and now - cached[0] < _YF_INFO_TTL:
            return cached[1]

    try:
        import yfinance as yf
        from filings.market_data import _yf_session

        tk = yf.Ticker(ticker, session=_yf_session)
        info = tk.info or {}
    except Exception:
        info = {}

    # Overlay Tiingo real-time price if available (more reliable than yfinance)
    try:
        from filings import tiingo

        if tiingo.has_tiingo_key():
            tq = tiingo.get_quote(ticker)
            if tq and tq.get("last"):
                info["currentPrice"] = tq["last"]
                info["regularMarketPrice"] = tq["last"]
                if tq.get("prevClose"):
                    info["previousClose"] = tq["prevClose"]
                if tq.get("open"):
                    info["regularMarketOpen"] = tq["open"]
                if tq.get("high"):
                    info["dayHigh"] = tq["high"]
                if tq.get("low"):
                    info["dayLow"] = tq["low"]
                if tq.get("volume"):
                    info["volume"] = int(tq["volume"])
    except Exception:
        pass  # Tiingo overlay is best-effort

    with _yf_info_lock:
        # Only cache successful results for the full TTL;
        # cache empty/failed results for just 60s to allow quick retry.
        ttl_offset = 0 if info else (_YF_INFO_TTL - 60)
        _yf_info_cache[ticker] = (now - ttl_offset, info)
    return info


def _resolve_logo_domain_from_info(info: dict) -> str | None:
    """Extract company website domain from yfinance info dict."""
    from urllib.parse import urlparse

    website = info.get("website") or ""
    if website:
        parsed = urlparse(website)
        domain = parsed.netloc or parsed.path
        if domain.startswith("www."):
            domain = domain[4:]
        return domain or None
    return None


def resolve_stock_info(ticker: str, cache_data: dict) -> StockInfo:
    """Resolve a ticker to basic stock info.

    First checks 13F cache for issuer name (zero API calls for name).
    Falls back to yfinance for non-superinvestor-held tickers.
    Always tries to resolve logo_domain via yfinance.
    Always returns a StockInfo — never raises.
    """
    import logging

    logger = logging.getLogger(__name__)
    ticker_upper = ticker.upper().strip()

    # Single yfinance fetch (cached with 1h TTL)
    yf_info = get_yfinance_info(ticker_upper)
    logo_domain = _resolve_logo_domain_from_info(yf_info)

    # Extract SEO fields from yfinance (already fetched, zero extra cost)
    seo = {
        "long_business_summary": yf_info.get("longBusinessSummary"),
        "sector": yf_info.get("sector"),
        "industry": yf_info.get("industry"),
        "market_cap": yf_info.get("marketCap"),
        "trailing_pe": yf_info.get("trailingPE"),
        "forward_pe": yf_info.get("forwardPE"),
        "dividend_yield": yf_info.get("dividendYield"),
        "beta": yf_info.get("beta"),
        "fifty_two_week_high": yf_info.get("fiftyTwoWeekHigh"),
        "fifty_two_week_low": yf_info.get("fiftyTwoWeekLow"),
        "current_price": (
            yf_info.get("currentPrice") or yf_info.get("regularMarketPrice")
        ),
        "recommendation_key": yf_info.get("recommendationKey"),
        "exchange": yf_info.get("exchange"),
    }

    # 1. Try to find in 13F cache (any fund's holdings)
    for fund_data in cache_data.values():
        for h in fund_data.get("all_holdings", []):
            h_ticker = h.get("ticker")
            if h_ticker and h_ticker.upper() == ticker_upper:
                return StockInfo(
                    ticker=ticker_upper,
                    issuer_name=h.get("issuer"),
                    cusip=h.get("cusip"),
                    logo_domain=logo_domain,
                    **seo,
                )

    # 2. Fall back to yfinance for company name (already fetched, no extra call)
    name = yf_info.get("longName") or yf_info.get("shortName")

    # 3. Fall back to hardcoded S&P 500 list if yfinance returned nothing
    if not name:
        try:
            from filings.market_data import _FALLBACK_SP500

            for entry in _FALLBACK_SP500:
                if entry["ticker"].upper() == ticker_upper:
                    name = entry["name"]
                    break
        except Exception:
            pass

    return StockInfo(
        ticker=ticker_upper,
        issuer_name=name,
        cusip=None,
        logo_domain=logo_domain,
        **seo,
    )
