"""Financial statements from SEC EDGAR XBRL CompanyFacts API.

Fetches all US-GAAP facts for a public company in a single API call,
parses them into Income Statement / Balance Sheet / Cash Flow rows,
and derives Key Ratios.  Results are two-tier cached:

  L1  in-memory dict  (1 h TTL)  — instant on repeated page views
  L2  Supabase api_cache          (24 h TTL) — survives restarts

Data source (free, no API key):
  GET https://data.sec.gov/api/xbrl/companyfacts/CIK{padded_cik}.json

CIK resolution uses edgartools (already a project dependency).
"""

from __future__ import annotations

import logging
import os
import threading
import time
from datetime import datetime, timezone
from typing import Any

import httpx

from filings import supabase_cache

logger = logging.getLogger(__name__)

# ── SEC HTTP config (same as aum_data.py) ────────────────────────────
_IDENTITY = os.environ.get(
    "SEC_IDENTITY", "PaperPanda/1.0 (13f-tool contact@paperpanda.io)"
)
_HEADERS = {"User-Agent": _IDENTITY, "Accept": "application/json"}
_MIN_REQUEST_GAP = 0.5          # seconds between requests
_XBRL_BASE = "https://data.sec.gov/api/xbrl/companyfacts"

# ── Cache config ─────────────────────────────────────────────────────
_L1_TTL = 3_600                  # 1 hour in-memory
_L2_TTL = 24 * 3_600             # 24 hours in Supabase
_MAX_CACHE = 500
_lock = threading.Lock()
_mem_cache: dict[str, tuple[float, dict]] = {}   # ticker → (ts, result)
_cik_cache: dict[str, str | None] = {}           # ticker → CIK

# Number of historical periods to keep per statement
_MAX_PERIODS = 5

# ═════════════════════════════════════════════════════════════════════
# GAAP Concept Mappings  (priority-ordered — first match wins)
# ═════════════════════════════════════════════════════════════════════

_INCOME_CONCEPTS: dict[str, list[str]] = {
    "Revenue": [
        "Revenues",
        "RevenueFromContractWithCustomerExcludingAssessedTax",
        "SalesRevenueNet",
        "RevenueFromContractWithCustomerIncludingAssessedTax",
        "SalesRevenueGoodsNet",
    ],
    "Cost of Revenue": [
        "CostOfGoodsAndServicesSold",
        "CostOfRevenue",
        "CostOfGoodsSold",
    ],
    "Gross Profit": [
        "GrossProfit",
    ],
    "Operating Expenses": [
        "OperatingExpenses",
        "OperatingCostsAndExpenses",
    ],
    "R&D Expenses": [
        "ResearchAndDevelopmentExpense",
    ],
    "SG&A Expenses": [
        "SellingGeneralAndAdministrativeExpense",
    ],
    "Operating Income": [
        "OperatingIncomeLoss",
    ],
    "Interest Expense": [
        "InterestExpense",
        "InterestExpenseDebt",
    ],
    "Pre-Tax Income": [
        "IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest",
        "IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethodInvestments",
        "IncomeLossFromContinuingOperationsBeforeIncomeTaxesDomestic",
    ],
    "Income Tax": [
        "IncomeTaxExpenseBenefit",
    ],
    "Net Income": [
        "NetIncomeLoss",
        "ProfitLoss",
    ],
    "EPS (Basic)": [
        "EarningsPerShareBasic",
    ],
    "EPS (Diluted)": [
        "EarningsPerShareDiluted",
    ],
}

_BALANCE_CONCEPTS: dict[str, list[str]] = {
    "Cash & Equivalents": [
        "CashAndCashEquivalentsAtCarryingValue",
        "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents",
        "CashAndCashEquivalents",
    ],
    "Short-Term Investments": [
        "ShortTermInvestments",
        "AvailableForSaleSecuritiesDebtSecuritiesCurrent",
        "MarketableSecuritiesCurrent",
    ],
    "Accounts Receivable": [
        "AccountsReceivableNetCurrent",
        "AccountsReceivableNet",
    ],
    "Inventory": [
        "InventoryNet",
        "InventoryFinishedGoodsNetOfReserves",
    ],
    "Total Current Assets": [
        "AssetsCurrent",
    ],
    "PP&E (Net)": [
        "PropertyPlantAndEquipmentNet",
    ],
    "Goodwill": [
        "Goodwill",
    ],
    "Intangible Assets": [
        "IntangibleAssetsNetExcludingGoodwill",
        "FiniteLivedIntangibleAssetsNet",
    ],
    "Total Assets": [
        "Assets",
    ],
    "Accounts Payable": [
        "AccountsPayableCurrent",
        "AccountsPayableAndAccruedLiabilitiesCurrent",
    ],
    "Short-Term Debt": [
        "ShortTermBorrowings",
        "DebtCurrent",
        "CommercialPaper",
    ],
    "Total Current Liabilities": [
        "LiabilitiesCurrent",
    ],
    "Long-Term Debt": [
        "LongTermDebtNoncurrent",
        "LongTermDebt",
    ],
    "Total Liabilities": [
        "Liabilities",
    ],
    "Total Equity": [
        "StockholdersEquity",
        "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
    ],
}

_CASHFLOW_CONCEPTS: dict[str, list[str]] = {
    "Operating Cash Flow": [
        "NetCashProvidedByUsedInOperatingActivities",
    ],
    "Capital Expenditures": [
        "PaymentsToAcquirePropertyPlantAndEquipment",
        "PaymentsToAcquireProductiveAssets",
    ],
    "Depreciation & Amortization": [
        "DepreciationDepletionAndAmortization",
        "DepreciationAmortizationAndAccretionNet",
        "DepreciationAndAmortization",
    ],
    "Stock-Based Compensation": [
        "ShareBasedCompensation",
        "AllocatedShareBasedCompensationExpense",
        "ShareBasedCompensationExpense",
    ],
    "Investing Cash Flow": [
        "NetCashProvidedByUsedInInvestingActivities",
    ],
    "Debt Repayment": [
        "RepaymentsOfDebt",
        "RepaymentsOfLongTermDebt",
    ],
    "Share Repurchases": [
        "PaymentsForRepurchaseOfCommonStock",
        "PaymentsForRepurchaseOfEquity",
    ],
    "Dividends Paid": [
        "PaymentsOfDividendsCommonStock",
        "PaymentsOfDividends",
    ],
    "Financing Cash Flow": [
        "NetCashProvidedByUsedInFinancingActivities",
    ],
}

# Line items that should be visually emphasised (bold) in the UI
_SUBTOTAL_LABELS = {
    "Revenue", "Gross Profit", "Operating Income", "Net Income",
    "Total Current Assets", "Total Assets", "Total Current Liabilities",
    "Total Liabilities", "Total Equity",
    "Operating Cash Flow", "Investing Cash Flow", "Financing Cash Flow",
}

# EPS rows use per-share unit (USD/shares) instead of USD
_EPS_LABELS = {"EPS (Basic)", "EPS (Diluted)"}


# ═════════════════════════════════════════════════════════════════════
# CIK Resolution
# ═════════════════════════════════════════════════════════════════════

def _resolve_cik(ticker: str) -> str | None:
    """Map ticker → CIK string, cached in memory."""
    key = ticker.upper()
    with _lock:
        if key in _cik_cache:
            return _cik_cache[key]

    try:
        from edgar import Company
        company = Company(key)
        cik = str(company.cik)
    except Exception as exc:
        logger.debug("CIK resolution failed for %s: %s", key, exc)
        cik = None

    with _lock:
        _cik_cache[key] = cik
    return cik


# ═════════════════════════════════════════════════════════════════════
# XBRL Fetching
# ═════════════════════════════════════════════════════════════════════

def _fetch_xbrl_facts(ticker: str) -> dict | None:
    """Fetch the full CompanyFacts JSON for *ticker*.

    Two-tier cache: L1 in-memory (1 h), L2 Supabase (24 h).
    Returns the ``us-gaap`` sub-dict or ``None`` on failure.
    """
    key = ticker.upper()

    # ── L1: in-memory ────────────────────────────────────────────────
    with _lock:
        if key in _mem_cache:
            ts, data = _mem_cache[key]
            if time.time() - ts < _L1_TTL:
                return data

    # ── L2: Supabase ─────────────────────────────────────────────────
    cache_key = f"fundamentals:{key}"
    cached, is_fresh = supabase_cache.get_cached_with_stale(cache_key)
    if isinstance(cached, dict) and cached.get("usgaap") is not None:
        usgaap = cached["usgaap"]
        if cached.get("no_data"):
            # Cached 404
            with _lock:
                _mem_cache[key] = (time.time(), None)  # type: ignore[arg-type]
            return None
        with _lock:
            _mem_cache[key] = (time.time(), usgaap)
        if is_fresh:
            return usgaap
        # Stale — fall through to refresh, but we have a fallback

    # ── Fetch from SEC ───────────────────────────────────────────────
    cik = _resolve_cik(key)
    if cik is None:
        return None

    padded_cik = cik.zfill(10)
    url = f"{_XBRL_BASE}/CIK{padded_cik}.json"

    try:
        time.sleep(_MIN_REQUEST_GAP)
        resp = httpx.get(url, headers=_HEADERS, timeout=30)
        if resp.status_code == 404:
            logger.debug("No XBRL data for %s (CIK %s)", key, cik)
            supabase_cache.set_cached(
                cache_key=cache_key,
                category="fundamentals",
                data={"usgaap": {}, "no_data": True, "ticker": key,
                      "fetched_at": datetime.now(timezone.utc).isoformat()},
                ttl_seconds=_L2_TTL,
            )
            with _lock:
                _mem_cache[key] = (time.time(), None)  # type: ignore[arg-type]
            return None
        resp.raise_for_status()
    except Exception as exc:
        logger.warning("XBRL fetch failed for %s: %s", key, exc)
        # Return stale data if available
        if isinstance(cached, dict) and cached.get("usgaap"):
            usgaap = cached["usgaap"]
            with _lock:
                _mem_cache[key] = (time.time(), usgaap)
            return usgaap
        return None

    data = resp.json()
    usgaap = data.get("facts", {}).get("us-gaap", {})

    # Cache the raw us-gaap dict (not the full response — too large)
    # We only keep the concepts we actually need to save space
    trimmed = _trim_usgaap(usgaap)
    try:
        supabase_cache.set_cached(
            cache_key=cache_key,
            category="fundamentals",
            data={"usgaap": trimmed, "ticker": key,
                  "fetched_at": datetime.now(timezone.utc).isoformat()},
            ttl_seconds=_L2_TTL,
        )
    except Exception as exc:
        logger.debug("Failed to cache fundamentals for %s: %s", key, exc)

    with _lock:
        _mem_cache[key] = (time.time(), usgaap)
        if len(_mem_cache) > _MAX_CACHE:
            oldest = min(_mem_cache, key=lambda k: _mem_cache[k][0])
            _mem_cache.pop(oldest, None)

    return usgaap


def _trim_usgaap(usgaap: dict) -> dict:
    """Keep only the GAAP concepts we use, to reduce Supabase payload."""
    needed: set[str] = set()
    for concept_list in _INCOME_CONCEPTS.values():
        needed.update(concept_list)
    for concept_list in _BALANCE_CONCEPTS.values():
        needed.update(concept_list)
    for concept_list in _CASHFLOW_CONCEPTS.values():
        needed.update(concept_list)

    return {k: v for k, v in usgaap.items() if k in needed}


# ═════════════════════════════════════════════════════════════════════
# Statement Parsing
# ═════════════════════════════════════════════════════════════════════

def _extract_entries(usgaap: dict, concepts: list[str]) -> list[dict]:
    """Merge entries from ALL matching concepts (priority-ordered).

    Companies often switch GAAP concepts over time (e.g. Apple used
    ``Revenues`` through 2018 then switched to
    ``RevenueFromContractWithCustomerExcludingAssessedTax``).  We merge
    entries from every concept that exists, with earlier concepts taking
    priority when the same period appears in multiple concepts.
    """
    seen_periods: set[str] = set()
    merged: list[dict] = []

    for concept in concepts:
        if concept not in usgaap:
            continue
        units = usgaap[concept].get("units", {})
        entries = units.get("USD", []) or units.get("USD/shares", [])
        for entry in entries:
            # Deduplicate by (end, form, frame) — earlier concepts win
            period_key = (entry.get("end", ""), entry.get("form", ""), entry.get("frame", ""))
            if period_key not in seen_periods:
                seen_periods.add(period_key)
                merged.append(entry)

    return merged


def _extract_line_items(
    usgaap: dict,
    concept_map: dict[str, list[str]],
    form_filter: str,
) -> dict[str, dict[str, Any]]:
    """Extract line items as ``{label: {period: value, ...}}``.

    Args:
        form_filter: ``"10-K"`` for annual, ``"10-Q"`` for quarterly.
    """
    result: dict[str, dict[str, Any]] = {}

    for label, concepts in concept_map.items():
        entries = _extract_entries(usgaap, concepts)
        period_vals: dict[str, Any] = {}

        for entry in entries:
            period_end = entry.get("end", "")
            if not period_end:
                continue
            form = entry.get("form", "")
            if form != form_filter:
                continue
            # Deduplicate: only keep entries with "frame"
            if "frame" not in entry:
                continue

            val = entry.get("val")
            if val is not None:
                # Keep the largest value for each period (handles restatements)
                if period_end not in period_vals:
                    period_vals[period_end] = val
                else:
                    # For most items, take the value from the latest filing
                    period_vals[period_end] = val

        if period_vals:
            result[label] = period_vals

    return result


def _discover_periods(
    items: dict[str, dict[str, Any]],
    max_periods: int = _MAX_PERIODS,
) -> list[str]:
    """Find the most recent common periods across all line items."""
    # Collect all periods mentioned in any line item
    all_periods: set[str] = set()
    for period_vals in items.values():
        all_periods.update(period_vals.keys())

    # Sort descending (most recent first)
    sorted_periods = sorted(all_periods, reverse=True)

    if max_periods is not None:
        return sorted_periods[:max_periods]
    return sorted_periods


def _build_statement(
    items: dict[str, dict[str, Any]],
    periods: list[str],
    concept_map: dict[str, list[str]],
) -> list[dict]:
    """Build ordered rows for a financial statement.

    Returns:
        [{"label": "Revenue", "is_subtotal": True, "is_eps": False,
          "values": {"2024-12-31": 123456, "2023-12-31": 111222, ...}}, ...]
    """
    rows: list[dict] = []
    for label in concept_map:
        period_vals = items.get(label, {})
        values = {p: period_vals.get(p) for p in periods}

        # Skip rows that are completely empty
        if not any(v is not None for v in values.values()):
            continue

        rows.append({
            "label": label,
            "is_subtotal": label in _SUBTOTAL_LABELS,
            "is_eps": label in _EPS_LABELS,
            "values": values,
        })

    return rows


# ═════════════════════════════════════════════════════════════════════
# Ratio Computation
# ═════════════════════════════════════════════════════════════════════

def _safe_div(a: float | None, b: float | None) -> float | None:
    """Return a/b or None if b is zero/None."""
    if a is None or b is None or b == 0:
        return None
    return a / b


def _safe_pct(a: float | None, b: float | None) -> float | None:
    """Return (a/b)*100 or None."""
    r = _safe_div(a, b)
    return r * 100 if r is not None else None


def _compute_ratios(
    income_items: dict[str, dict[str, Any]],
    balance_items: dict[str, dict[str, Any]],
    cashflow_items: dict[str, dict[str, Any]],
    periods: list[str],
) -> list[dict]:
    """Compute key financial ratios for each period.

    Returns rows in the same format as _build_statement().
    """
    def _get(items: dict, label: str, period: str) -> float | None:
        return items.get(label, {}).get(period)

    ratio_defs: list[tuple[str, Any]] = []

    # ── Profitability ────────────────────────────────────────────────
    def gross_margin(p: str) -> float | None:
        return _safe_pct(_get(income_items, "Gross Profit", p),
                         _get(income_items, "Revenue", p))

    def operating_margin(p: str) -> float | None:
        return _safe_pct(_get(income_items, "Operating Income", p),
                         _get(income_items, "Revenue", p))

    def net_margin(p: str) -> float | None:
        return _safe_pct(_get(income_items, "Net Income", p),
                         _get(income_items, "Revenue", p))

    def roe(p: str) -> float | None:
        return _safe_pct(_get(income_items, "Net Income", p),
                         _get(balance_items, "Total Equity", p))

    def roa(p: str) -> float | None:
        return _safe_pct(_get(income_items, "Net Income", p),
                         _get(balance_items, "Total Assets", p))

    # ── Liquidity ────────────────────────────────────────────────────
    def current_ratio(p: str) -> float | None:
        return _safe_div(_get(balance_items, "Total Current Assets", p),
                         _get(balance_items, "Total Current Liabilities", p))

    def quick_ratio(p: str) -> float | None:
        ca = _get(balance_items, "Total Current Assets", p)
        inv = _get(balance_items, "Inventory", p) or 0
        cl = _get(balance_items, "Total Current Liabilities", p)
        if ca is None or cl is None or cl == 0:
            return None
        return (ca - inv) / cl

    # ── Leverage ─────────────────────────────────────────────────────
    def debt_to_equity(p: str) -> float | None:
        return _safe_div(_get(balance_items, "Total Liabilities", p),
                         _get(balance_items, "Total Equity", p))

    def interest_coverage(p: str) -> float | None:
        oi = _get(income_items, "Operating Income", p)
        ie = _get(income_items, "Interest Expense", p)
        if oi is None or ie is None or ie == 0:
            return None
        return oi / ie

    # ── Cash Flow ────────────────────────────────────────────────────
    def fcf(p: str) -> float | None:
        ocf = _get(cashflow_items, "Operating Cash Flow", p)
        capex = _get(cashflow_items, "Capital Expenditures", p)
        if ocf is None:
            return None
        return ocf - (capex or 0)

    def fcf_margin(p: str) -> float | None:
        f = fcf(p)
        rev = _get(income_items, "Revenue", p)
        return _safe_pct(f, rev)

    # ── Growth ───────────────────────────────────────────────────────
    def rev_growth(p: str) -> float | None:
        rev = _get(income_items, "Revenue", p)
        idx = periods.index(p) if p in periods else -1
        if idx < 0 or idx + 1 >= len(periods):
            return None
        prev_rev = _get(income_items, "Revenue", periods[idx + 1])
        if rev is None or prev_rev is None or prev_rev == 0:
            return None
        return ((rev - prev_rev) / abs(prev_rev)) * 100

    # ── Asset Efficiency ─────────────────────────────────────────────
    def asset_turnover(p: str) -> float | None:
        return _safe_div(_get(income_items, "Revenue", p),
                         _get(balance_items, "Total Assets", p))

    # Build ratio rows
    ratio_specs = [
        ("Gross Margin", gross_margin, True),
        ("Operating Margin", operating_margin, True),
        ("Net Margin", net_margin, True),
        ("ROE", roe, True),
        ("ROA", roa, True),
        ("Current Ratio", current_ratio, False),
        ("Quick Ratio", quick_ratio, False),
        ("Debt / Equity", debt_to_equity, False),
        ("Interest Coverage", interest_coverage, False),
        ("Revenue Growth YoY", rev_growth, True),
        ("Free Cash Flow", fcf, False),
        ("FCF Margin", fcf_margin, True),
        ("Asset Turnover", asset_turnover, False),
    ]

    rows: list[dict] = []
    for label, calc_fn, is_pct in ratio_specs:
        values = {}
        for p in periods:
            try:
                values[p] = calc_fn(p)
            except Exception:
                values[p] = None

        # Skip if entirely empty
        if not any(v is not None for v in values.values()):
            continue

        rows.append({
            "label": label,
            "is_subtotal": False,
            "is_pct": is_pct,
            "is_ratio": not is_pct,  # plain ratio like 1.5x
            "is_cash": label == "Free Cash Flow",
            "values": values,
        })

    return rows


# ═════════════════════════════════════════════════════════════════════
# Public API
# ═════════════════════════════════════════════════════════════════════

def get_fundamentals(ticker: str) -> dict | None:
    """Return structured financial statements for *ticker*.

    Returns dict with keys ``annual`` and ``quarterly``, each containing
    ``income``, ``balance``, ``cashflow``, ``ratios`` sub-dicts with
    ``periods`` (list[str]) and ``rows`` (list[dict]).

    Returns ``None`` if no XBRL data is available.
    """
    usgaap = _fetch_xbrl_facts(ticker)
    if not usgaap:
        return None

    result: dict[str, Any] = {"ticker": ticker.upper()}

    for freq, form_filter in [("annual", "10-K"), ("quarterly", "10-Q")]:
        income_items = _extract_line_items(usgaap, _INCOME_CONCEPTS, form_filter)
        balance_items = _extract_line_items(usgaap, _BALANCE_CONCEPTS, form_filter)
        cashflow_items = _extract_line_items(usgaap, _CASHFLOW_CONCEPTS, form_filter)

        # Discover common periods across all three statements
        all_items = {}
        all_items.update(income_items)
        all_items.update(balance_items)
        all_items.update(cashflow_items)
        periods = _discover_periods(all_items)

        if not periods:
            result[freq] = None
            continue

        income_rows = _build_statement(income_items, periods, _INCOME_CONCEPTS)
        balance_rows = _build_statement(balance_items, periods, _BALANCE_CONCEPTS)
        cashflow_rows = _build_statement(cashflow_items, periods, _CASHFLOW_CONCEPTS)
        ratio_rows = _compute_ratios(income_items, balance_items, cashflow_items, periods)

        result[freq] = {
            "income": {"periods": periods, "rows": income_rows},
            "balance": {"periods": periods, "rows": balance_rows},
            "cashflow": {"periods": periods, "rows": cashflow_rows},
            "ratios": {"periods": periods, "rows": ratio_rows},
        }

    result["fetched_at"] = datetime.now(timezone.utc).isoformat()

    # Return None if we got zero data
    if result.get("annual") is None and result.get("quarterly") is None:
        return None

    return result


# ═════════════════════════════════════════════════════════════════════
# Bulk Backfill Helpers  (no cache — used by scripts/backfill_fundamentals.py)
# ═════════════════════════════════════════════════════════════════════

def fetch_raw_xbrl(ticker: str) -> dict | None:
    """Fetch trimmed us-gaap facts from SEC.  No caching — for bulk backfill.

    Returns the trimmed XBRL dict (only the 52 concepts we use) or None
    if the ticker can't be resolved or has no XBRL data (404).
    """
    cik = _resolve_cik(ticker)
    if cik is None:
        return None

    padded_cik = cik.zfill(10)
    url = f"{_XBRL_BASE}/CIK{padded_cik}.json"

    time.sleep(_MIN_REQUEST_GAP)
    resp = httpx.get(url, headers=_HEADERS, timeout=30)
    if resp.status_code == 404:
        return None
    resp.raise_for_status()

    data = resp.json()
    usgaap = data.get("facts", {}).get("us-gaap", {})
    return _trim_usgaap(usgaap)


def build_full_statements(usgaap: dict, ticker: str = "") -> dict:
    """Build income/balance/cashflow/ratios with ALL historical periods.

    Same output shape as ``get_fundamentals()`` but with no period cap.
    Used by the backfill script to archive full history to cold storage.
    """
    result: dict[str, Any] = {"ticker": ticker.upper()}

    for freq, form_filter in [("annual", "10-K"), ("quarterly", "10-Q")]:
        income_items = _extract_line_items(usgaap, _INCOME_CONCEPTS, form_filter)
        balance_items = _extract_line_items(usgaap, _BALANCE_CONCEPTS, form_filter)
        cashflow_items = _extract_line_items(usgaap, _CASHFLOW_CONCEPTS, form_filter)

        all_items = {}
        all_items.update(income_items)
        all_items.update(balance_items)
        all_items.update(cashflow_items)
        periods = _discover_periods(all_items, max_periods=None)

        if not periods:
            result[freq] = None
            continue

        result[freq] = {
            "income":   {"periods": periods, "rows": _build_statement(income_items, periods, _INCOME_CONCEPTS)},
            "balance":  {"periods": periods, "rows": _build_statement(balance_items, periods, _BALANCE_CONCEPTS)},
            "cashflow": {"periods": periods, "rows": _build_statement(cashflow_items, periods, _CASHFLOW_CONCEPTS)},
            "ratios":   {"periods": periods, "rows": _compute_ratios(income_items, balance_items, cashflow_items, periods)},
        }

    result["fetched_at"] = datetime.now(timezone.utc).isoformat()

    if result.get("annual") is None and result.get("quarterly") is None:
        return {}

    return result


def get_full_history(ticker: str) -> dict | None:
    """Retrieve full historical financial data from cold storage.

    Returns the same shape as ``get_fundamentals()`` but with all periods.
    Returns ``None`` if the ticker hasn't been backfilled yet.
    """
    from filings import cold_storage
    path = f"fundamentals/{ticker.upper()}/full_history.json"
    return cold_storage.download_json(path)
