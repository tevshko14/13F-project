"""Funds page (v2 redesign).

Five routes:

  * ``/funds``                    -- index page (4 sub-panes: Funds list /
                                     Holdings / Activity / Capital Deployed)
  * ``/api/funds-index/holdings`` -- lazy partial for Holdings pane
  * ``/api/funds-index/activity`` -- lazy partial for Activity pane
  * ``/funds/detail``             -- redirect to /funds/{cik} with default CIK
  * ``/funds/{cik}``              -- per-fund detail (Portfolio / Activity /
                                     Performance / Sectors / Filings tabs)

All five render against the in-process ``fund_cache`` + ``deployment_cache``
populated at app startup -- zero upstream fetches on the warm path, so
hits are sub-200 ms across all sub-tabs.
"""

from __future__ import annotations

import asyncio
import functools
import logging
import re
from datetime import datetime

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from filings import supabase_cache
from filings.app_state import templates
from filings.cache_l2 import l2_cached as _l2_cached
from filings.concurrency import to_heavy, to_supabase
from filings.routers._redesign.helpers import (
    _bounded,
    _build_cusip_ticker_map,
    _format_dollars_compact,
    _nice_axis_step,
    _shell_context,
    _short_date,
)

logger = logging.getLogger(__name__)

router = APIRouter()



# Berkshire Hathaway is the demo fund the design ships with.
_DEFAULT_FUND_CIK = "1067983"

# Manager metadata for the page hero — keyed by CIK.  Auto-derived from
# the canonical superinvestor list with a small per-CIK overlay for cities
# (city/HQ data isn't carried in superinvestors.py yet — manual overrides
# for the most-visited funds; everything else falls back to the fund_name).
_FUND_CITY_OVERRIDES: dict[str, str] = {
    "1067983": "Omaha, NE",            # Berkshire
    "1336528": "New York, NY",         # Pershing Square
    "1649339": "Saratoga, CA",         # Scion
    "1079114": "New York, NY",         # Greenlight
    "1040273": "New York, NY",         # Third Point
    "1656456": "Miami Beach, FL",      # Appaloosa (David Tepper)
    "1061768": "Boston, MA",           # Baupost
    "1135730": "New York, NY",         # Coatue
    "1423053": "Miami, FL",            # Citadel
    "1167483": "New York, NY",         # Tiger Global
    "1647251": "London, UK",           # TCI
    "1166559": "Seattle, WA",          # Gates Foundation
    "1418814": "San Francisco, CA",    # ValueAct
    "1037389": "East Setauket, NY",    # Renaissance
    "949509":  "Los Angeles, CA",      # Oaktree
    "1350694": "Westport, CT",         # Bridgewater
    "1061165": "Greenwich, CT",        # Lone Pine
    "1103804": "Greenwich, CT",        # Viking
    "934639":  "Dallas, TX",           # Maverick
    "1345471": "New York, NY",         # Trian
    "1358706": "Boston, MA",           # Abrams Capital
    "921669":  "New York, NY",         # Icahn
    "200217":  "San Francisco, CA",    # Dodge & Cox
    "1536411": "New York, NY",         # Duquesne
}


def _icon_from_fund_name(fund_name: str) -> str:
    """Derive a 3-letter chrome icon from a fund name.

    "Berkshire Hathaway" → "BRK"
    "Pershing Square"    → "PSC"
    "Scion Asset Mgmt"   → "SCN"
    Falls back to first 3 alphas if no decent split.
    """
    if not fund_name:
        return "FND"
    words = [w for w in re.split(r"[^A-Za-z]+", fund_name) if w]
    if not words:
        return "FND"
    if len(words) == 1:
        return words[0][:3].upper()
    # Take first letter of first word + 2 from second-most-distinctive word
    first = words[0]
    second = words[1] if len(words) > 1 else ""
    candidate = (first[:1] + second[:2]).upper() if second else first[:3].upper()
    return candidate or "FND"


def _build_fund_meta() -> dict[str, dict]:
    """Auto-derive page-hero metadata for every superinvestor on file.

    Reads from filings.superinvestors.SUPERINVESTORS_BY_CIK so the lookup
    automatically grows with the master list.
    """
    try:
        from filings.superinvestors import SUPERINVESTORS_BY_CIK
    except Exception:
        return {}
    meta: dict[str, dict] = {}
    for cik, info in SUPERINVESTORS_BY_CIK.items():
        meta[cik] = {
            "manager": info.display_name or info.fund_name or "",
            "city":    _FUND_CITY_OVERRIDES.get(cik, ""),
            "icon":    _icon_from_fund_name(info.fund_name or info.display_name),
        }
    return meta


_FUND_META: dict[str, dict] = _build_fund_meta()


def _format_dollars(v: float | int | None, *, full=False) -> str:
    """Dollar formatter — compact ($312.4B / $11.4B / $83M) by default,
    or fully unabbreviated comma-separated ($312,400,000,000) when
    ``full=True``.  Used in places (the All Holdings table) where the
    user expects to see exact dollar values, not the rounded compact form.
    """
    if v is None:
        return "—"
    v = float(v)
    if full:
        return f"${v:,.0f}"
    if v >= 1e12:
        return f"${v / 1e12:.2f}T"
    if v >= 1e9:
        return f"${v / 1e9:.1f}B"
    if v >= 1e6:
        return f"${v / 1e6:.1f}M"
    if v >= 1e3:
        return f"${v / 1e3:.1f}K"
    return f"${v:,.0f}"


def _format_shares(v: int | None) -> str:
    """Comma-separated share count, no decimal."""
    if v is None:
        return "—"
    return f"{int(v):,}"


def _quarter_label(report_period: str) -> str:
    """Convert "2026-03-31" → "Q1 2026"."""
    if not report_period:
        return ""
    try:
        d = datetime.fromisoformat(report_period[:10])
        q = (d.month - 1) // 3 + 1
        return f"Q{q} {d.year}"
    except Exception:
        return report_period


async def _fetch_fund_data(cik: str) -> dict | None:
    """Read 13F fund summary from L2 cache (Supabase) directly.

    We don't import _get_fund_data() from web.py to avoid pulling in the
    full app-state machinery; supabase_cache.get_cached_with_stale is the
    L2 source of truth and is what _get_fund_data() falls back to anyway.
    """
    cik_norm = (cik or "").lstrip("0") or cik
    try:
        from filings import supabase_cache
        data, _fresh = await to_heavy(
            supabase_cache.get_cached_with_stale, f"13f:{cik_norm}"
        )
        return data if isinstance(data, dict) else None
    except Exception as exc:
        logger.warning("Funds L2 cache miss for CIK=%s: %s", cik, exc)
        return None


def _funds_kpi_strip(fund: dict, history: list[dict] | None = None) -> list[dict]:
    """Build the KPI strip from real data only — every cell is derived from
    the 13F payload + AUM history.  No placeholder em-dash KPIs."""
    aum = fund.get("total_value")
    positions = fund.get("total_holdings")
    holdings = fund.get("all_holdings") or []
    qchanges = fund.get("quarterly_changes") or []

    top10_value = sum(h.get("value") or 0 for h in holdings[:10])
    top10_pct = (top10_value / aum * 100) if aum else None

    # New positions opened this quarter (from the most-recent quarter's diff).
    new_positions_q = sum(
        1 for c in (qchanges[0].get("changes") or []) if (c.get("status") or "").upper() in ("NEW", "NEWLY ADDED")
    ) if qchanges else 0

    # 4-quarter rolling turnover proxy: sum of |share_change| / sum(current_shares)
    # across the last 4 quarters.  A real Turnover-TTM needs full position
    # tracking, but this rolls-up the same trades the table already shows.
    chg_4q  = sum(abs(c.get("share_change") or 0)
                  for q in qchanges[:4] for c in (q.get("changes") or []))
    held_4q = sum(int(h.get("shares") or 0) for h in holdings) or 1
    turnover_pct = (chg_4q / held_4q * 100) if chg_4q else None

    # AUM QoQ delta from the history series (last two points).
    qoq_pct = None
    qoq_up = None
    if history and len(history) >= 2:
        prev = history[-2].get("total_value") or 0
        curr = history[-1].get("total_value") or 0
        if prev > 0:
            qoq_pct = (curr - prev) / prev * 100
            qoq_up  = qoq_pct >= 0

    return [
        {"label": "AUM",            "value": _format_dollars(aum),
         "delta": (f"{qoq_pct:+.1f}% QoQ" if qoq_pct is not None else None),
         "up":    qoq_up},
        {"label": "Positions",      "value": str(positions) if positions else "—",
         "delta": (f"+{new_positions_q} new" if new_positions_q else None),
         "up":    (True if new_positions_q else None)},
        {"label": "Top 10 conc.",   "value": (f"{top10_pct:.1f}%" if top10_pct else "—"),
         "delta": None, "up": None},
        {"label": "Turnover · 4q",  "value": (f"{turnover_pct:.1f}%" if turnover_pct is not None else "—"),
         "delta": None, "up": None},
    ]


def _funds_concentration_donut(fund: dict) -> dict:
    """v1's Portfolio Concentration donut — top 10 holdings as wedges + "Other"
    bucket for the long tail.  Returns a payload the template walks to render
    an SVG donut + legend."""
    aum = fund.get("total_value") or 0
    holdings = fund.get("all_holdings") or []
    if not aum or not holdings:
        return {"have_data": False, "wedges": [], "legend": []}

    palette = [
        "#3b82f6",  # blue (AAPL-style anchor)
        "#22c55e",  # green
        "#f97316",  # orange
        "#a855f7",  # purple
        "#ef4444",  # red
        "#14b8a6",  # teal
        "#eab308",  # yellow
        "#8b5cf6",  # violet
        "#94a3b8",  # slate
        "#ec4899",  # pink
    ]
    top10 = holdings[:10]
    rest_value = sum(h.get("value") or 0 for h in holdings[10:])

    wedges: list[dict] = []
    legend: list[dict] = []
    cumulative = 0.0  # 0..1 fraction along the donut path.

    cx, cy, r_outer, r_inner = 100.0, 100.0, 80.0, 50.0

    def _arc(start_frac: float, end_frac: float, color: str) -> str:
        """Return an SVG path for a donut wedge between two fractional angles."""
        import math
        a0 = -math.pi / 2 + start_frac * 2 * math.pi
        a1 = -math.pi / 2 + end_frac   * 2 * math.pi
        large = 1 if (end_frac - start_frac) > 0.5 else 0
        x0o, y0o = cx + r_outer * math.cos(a0), cy + r_outer * math.sin(a0)
        x1o, y1o = cx + r_outer * math.cos(a1), cy + r_outer * math.sin(a1)
        x0i, y0i = cx + r_inner * math.cos(a0), cy + r_inner * math.sin(a0)
        x1i, y1i = cx + r_inner * math.cos(a1), cy + r_inner * math.sin(a1)
        return (
            f"M {x0o:.2f} {y0o:.2f} "
            f"A {r_outer} {r_outer} 0 {large} 1 {x1o:.2f} {y1o:.2f} "
            f"L {x1i:.2f} {y1i:.2f} "
            f"A {r_inner} {r_inner} 0 {large} 0 {x0i:.2f} {y0i:.2f} Z"
        )

    for i, h in enumerate(top10):
        val = h.get("value") or 0
        if not val:
            continue
        frac = val / aum
        color = palette[i % len(palette)]
        wedges.append({"d": _arc(cumulative, cumulative + frac, color), "color": color})
        legend.append({
            "ticker": (h.get("ticker") or "—").upper(),
            "pct":    f"{frac * 100:.1f}%",
            "color":  color,
        })
        cumulative += frac

    if rest_value > 0:
        frac = rest_value / aum
        color = "var(--pp-line2)"
        wedges.append({"d": _arc(cumulative, cumulative + frac, color), "color": color})
        legend.append({"ticker": "Other", "pct": f"{frac * 100:.1f}%", "color": color})

    return {"have_data": True, "wedges": wedges, "legend": legend, "viewbox": "0 0 200 200"}


def _funds_position_changes_quarters(fund: dict) -> list[dict]:
    """v1's Position Changes — emit one entry per quarter with its formatted
    change rows ready to render.  Used by the Activity tab quarter selector."""
    qchanges = fund.get("quarterly_changes") or []
    out: list[dict] = []
    for q in qchanges:
        rows: list[dict] = []
        for c in (q.get("changes") or []):
            status = (c.get("status") or "").upper()
            share_chg = c.get("share_change") or 0
            cur_shares = c.get("current_shares") or 0
            prev_shares = c.get("previous_shares") or 0
            sign = "+" if share_chg > 0 else ("-" if share_chg < 0 else "")
            magnitude = abs(share_chg)
            if magnitude >= 1e6:
                chg_str = f"{sign}{magnitude / 1e6:.1f}M"
            elif magnitude >= 1e3:
                chg_str = f"{sign}{magnitude / 1e3:.0f}K"
            else:
                chg_str = f"{sign}{magnitude:,}" if magnitude else "—"
            pct_str = "—"
            if prev_shares > 0:
                pct = share_chg / prev_shares * 100
                pct_str = f"{pct:+.1f}%"
            elif status in ("NEW", "NEWLY ADDED"):
                pct_str = "New"
            rows.append({
                "ticker":     c.get("ticker") or (c.get("issuer", "")[:6].upper() or "—"),
                "name":       c.get("issuer") or "",
                "action":     status,
                "share_chg":  chg_str,
                "share_chg_up": share_chg > 0,
                "pct_str":    pct_str,
                "pct_up":     (share_chg > 0) if share_chg else None,
                "shares_now": _format_shares(cur_shares),
                "value_now":  _format_dollars(c.get("current_value")),
            })
        rows.sort(key=lambda r: abs((r.get("share_chg_up") or False) and 1 or -1), reverse=False)
        out.append({
            "label":       _quarter_label(q.get("report_period", "")),
            "report_date": q.get("report_period", ""),
            "filing_date": q.get("filing_date", ""),
            "trade_count": len(rows),
            "rows":        rows,
        })
    return out


# ── Capital Deployed (v1 module) — 13F equity + cash & equivalents ───────

async def _funds_capital_deployed(cik: str) -> dict:
    """Fetch the cached deployment metrics for one CIK.

    Reads from the same Supabase rows the v1 page uses
    (``aum_data.load_all_deployment_data`` / ``deployment:{cik}``).
    """
    cik_norm = (cik or "").lstrip("0") or cik
    try:
        result = await to_supabase(
            supabase_cache.get_cached_with_stale, f"deployment:{cik_norm}",
        )
        cached, _fresh = result if result is not None else (None, False)
    except Exception as exc:
        logger.warning("Capital deployed fetch failed for CIK=%s: %s", cik, exc)
        return {}
    return cached if isinstance(cached, dict) else {}


def _funds_capital_panel(deployment: dict, fund: dict) -> dict:
    """Format the deployment payload into a 4-cell capital panel.

    Falls back to the in-fund 13F value when the deployment row is missing
    so the page still surfaces the 13F equity number every CIK has.
    """
    raum = (deployment or {}).get("raum")
    thirteenf = (deployment or {}).get("thirteenf_value") or fund.get("total_value")
    exact_cash = (deployment or {}).get("exact_cash")
    est_non_eq = (deployment or {}).get("estimated_non_equity")
    deployment_ratio = (deployment or {}).get("deployment_ratio")
    cash_period = (deployment or {}).get("exact_cash_period")
    data_source = (deployment or {}).get("data_source")

    cash_value = exact_cash if exact_cash is not None else est_non_eq
    cash_label = "Cash · 10-K/Q" if exact_cash is not None else (
        "Est. non-equity" if est_non_eq is not None else "Cash & Equiv"
    )
    cash_sub = (
        f"as of {cash_period}" if exact_cash is not None and cash_period
        else ("AUM gap (RAUM − 13F)" if est_non_eq is not None else None)
    )

    cells = [
        {"label": "13F Equity", "value": _format_dollars(thirteenf),
         "sub":   "from 13F-HR holdings"},
        {"label": cash_label,   "value": _format_dollars(cash_value),
         "sub":   cash_sub},
    ]
    if raum:
        cells.append({"label": "RAUM",
                      "value": _format_dollars(raum),
                      "sub":   "Form ADV reported AUM"})
    if deployment_ratio is not None:
        cells.append({
            "label": "Deployment ratio",
            "value": f"{deployment_ratio * 100:.1f}%",
            "sub":   "13F / RAUM",
        })

    return {
        "have_data": bool(thirteenf),
        "cells":     cells,
        "data_source": data_source,
    }


# ── SEC Filings list (Filings tab, mirrors stock-page pattern) ──────────

# Form-type → human-readable description.  Match by prefix so variants
# (10-K/A, 13F-HR/A, etc.) inherit the same kind.
_FILING_KIND_PREFIX: list[tuple[str, str]] = [
    ("13F",   "Quarterly holdings"),
    ("10-K",  "Annual report"),
    ("10-Q",  "Quarterly report"),
    ("8-K",   "Material event"),
    ("DEF ",  "Proxy statement"),
    ("SC 13",       "Beneficial ownership"),
    ("SCHEDULE 13", "Beneficial ownership"),
    ("S-1",   "Registration"),
    ("S-3",   "Registration"),
    ("S-4",   "Registration"),
    ("4",     "Insider transaction"),
    ("3",     "Insider transaction"),
    ("5",     "Insider transaction"),
    ("144",   "Notice of sale"),
]


def _filing_kind(form: str) -> str:
    f = (form or "").upper()
    for prefix, label in _FILING_KIND_PREFIX:
        if f.startswith(prefix):
            return label
    return "Filing"


def _filing_row_from_df(row, cik_norm: str) -> dict:
    """Convert one row of `EntityFilings.to_pandas()` into the template shape."""
    form = str(row.get("form", "") or "").strip()
    filing_date = str(row.get("filing_date", "") or "")
    accession = str(row.get("accession_number", "") or "").strip()
    acc_clean = accession.replace("-", "")
    url = (
        f"https://www.sec.gov/Archives/edgar/data/{cik_norm}/{acc_clean}/" if acc_clean
        else f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik_norm}&type={form}"
    )
    return {
        "form":         form,
        "kind":         _filing_kind(form),
        "filing_date":  filing_date,
        "filing_label": _short_date(filing_date) or filing_date,
        "accession":    accession,
        "url":          url,
    }


async def _funds_filings_list(cik: str, *, limit: int = 25) -> list[dict]:
    """List the most-recent SEC filings for *cik* via edgartools.

    L2-cached (24h) — filings only land a few times per quarter.
    """
    cik_norm = (cik or "").lstrip("0") or cik

    def _compute() -> list[dict]:
        try:
            # filings.client sets the SEC User-Agent identity at import-time
            # via set_identity() — required before any edgartools call.
            from filings import client as _client  # noqa: F401
            from edgar import Company
        except Exception:
            return []
        try:
            company = Company(int(cik_norm))
            # `.to_pandas()` sidesteps the per-attribute pyarrow access that
            # raises 'ChunkedArray has no as_py' under newer pyarrow.
            df = company.get_filings().to_pandas().head(limit)
        except Exception as exc:
            logger.warning("Funds filings: edgar load failed for CIK=%s: %s", cik, exc)
            return []
        return [_filing_row_from_df(row, cik_norm) for _, row in df.iterrows()]

    return await _l2_cached(
        key=f"funds:filings:{cik_norm}:v1",
        ttl_seconds=24 * 3600,
        compute=_compute,
        category="funds_filings",
    ) or []


def _qoq_pct_for_holding(h: dict, changes_by_cusip: dict[str, dict]) -> float | None:
    """Resolve the QoQ share-count change for a holding.

    Looks up `changes` (most-recent quarter's diff vs prior) by cusip; the
    `share_change` is signed.  Returns the percentage change relative to
    the prior quarter's share count, or None if unchanged / unknown.
    """
    cusip = h.get("cusip")
    if not cusip:
        return None
    rec = changes_by_cusip.get(cusip)
    if not rec:
        return None
    share_chg = rec.get("share_change") or 0
    prev = rec.get("previous_shares") or 0
    if prev <= 0 or share_chg == 0:
        # NEW positions report previous_shares=0; show as "NEW" via the tag below.
        return None
    return float(share_chg) / float(prev)


def _funds_holdings_table(
    fund: dict,
    top_n: int = 10,
    *,
    prices_by_ticker: dict[str, dict] | None = None,
    spark_by_ticker:  dict[str, list[float]] | None = None,
) -> list[dict]:
    """Format top-N holdings for the dense table.

    `prices_by_ticker` shape: {"AAPL": {"price": 232.71, "pct_change": -0.83}, ...}
    `spark_by_ticker`  shape: {"AAPL": [0.32, 0.38, ...], ...}
    Both are optional — pass None to render placeholders.
    """
    aum = fund.get("total_value") or 0
    holdings = fund.get("all_holdings") or []
    prices_by_ticker = prices_by_ticker or {}
    spark_by_ticker = spark_by_ticker or {}

    # cusip → {share_change, previous_shares, status}.  Pulled from the
    # most-recent quarter's `changes` list.  Used for the QoQ Δ column.
    changes_by_cusip: dict[str, dict] = {}
    qchanges = fund.get("quarterly_changes") or []
    if qchanges:
        for c in (qchanges[0].get("changes") or []):
            cusip = c.get("cusip")
            if cusip:
                changes_by_cusip[cusip] = c

    rows = []
    for i, h in enumerate(holdings[:top_n], start=1):
        val = h.get("value") or 0
        port_pct = (val / aum) if aum else 0
        ticker = (h.get("ticker") or "").upper() or "—"

        # QoQ — preferred resolution: actual share_change pct vs prior shares.
        qoq_pct = _qoq_pct_for_holding(h, changes_by_cusip)
        rec = changes_by_cusip.get(h.get("cusip") or "")
        is_new = bool(rec and rec.get("status") in ("NEW", "NEWLY ADDED"))
        if is_new:
            qoq_str = "NEW"
            qoq_kind = "new"          # gets the coral "NEW" chip
        elif qoq_pct is None:
            qoq_str = "—"
            qoq_kind = "neutral"
        else:
            qoq_str = f"{'+' if qoq_pct > 0 else ''}{qoq_pct * 100:.1f}%"
            qoq_kind = "added" if qoq_pct > 0 else "reduced"

        price_rec = prices_by_ticker.get(ticker) or {}
        last_v = price_rec.get("price")
        day_v  = price_rec.get("pct_change")  # already in % (e.g. -0.83)
        spark_series = spark_by_ticker.get(ticker)

        rows.append({
            "rank":   i,
            "ticker": ticker,
            "name":   h.get("issuer") or "",
            "shares": _format_shares(h.get("shares")),
            "value":  _format_dollars(val),
            "port":   port_pct,         # 0..1 for bar width
            "port_pct_str": f"{port_pct * 100:.1f}%",
            "qoq":         qoq_str,
            "qoq_kind":    qoq_kind,    # "added" | "reduced" | "new" | "neutral"
            "last":        f"{last_v:.2f}" if isinstance(last_v, (int, float)) else None,
            "day":         f"{day_v:+.2f}%" if isinstance(day_v, (int, float)) else None,
            "day_up":      (day_v >= 0) if isinstance(day_v, (int, float)) else None,
            "spark":       spark_series,
            "spark_up":    bool(spark_series and len(spark_series) > 1 and spark_series[-1] >= spark_series[0]),
        })
    return rows


def _funds_changes_split(fund: dict) -> tuple[list[dict], list[dict]]:
    """Split the most-recent quarter's `changes` into adds/news vs cuts/exits."""
    changes = fund.get("changes") or []
    adds, cuts = [], []
    for c in changes:
        status = c.get("status", "")
        share_chg = c.get("share_change") or 0
        # Sign formatter: +22.4M sh / -92.4M sh / 1.27M sh (NEW, no sign needed but pos).
        sign = "+" if share_chg > 0 else ("-" if share_chg < 0 else "")
        magnitude = abs(share_chg)
        if magnitude >= 1e6:
            chg_str = f"{sign}{magnitude / 1e6:.1f}M sh"
        elif magnitude >= 1e3:
            chg_str = f"{sign}{magnitude / 1e3:.0f}K sh"
        else:
            chg_str = f"{sign}{magnitude:,} sh"

        row = {
            "ticker": c.get("ticker") or c.get("issuer", "")[:6].upper(),
            "name":   c.get("issuer") or "",
            "action": status,
            "val":    chg_str,
        }
        if status in ("ADDED", "NEW", "ADD"):
            adds.append(row)
        elif status in ("REDUCED", "EXITED", "CUT", "EXIT"):
            cuts.append(row)
    return adds[:4], cuts[:4]


# GICS sector → palette mapping for the allocation panel.  Values are CSS
# variable references so light/dark mode swap automatically.  "Other" /
# anything missing collapses to a neutral border colour.
_FUNDS_SECTOR_COLORS: dict[str, str] = {
    "Information Technology": "var(--pp-accent)",
    "Communication Services": "var(--pp-ink)",
    "Financials":             "var(--pp-up)",
    "Health Care":            "var(--pp-down)",
    "Consumer Discretionary": "var(--pp-dim)",
    "Consumer Staples":       "var(--pp-up)",
    "Energy":                 "var(--pp-down)",
    "Industrials":            "var(--pp-ink)",
    "Materials":              "var(--pp-dim2)",
    "Utilities":              "var(--pp-line2)",
    "Real Estate":            "var(--pp-line2)",
    "Other":                  "var(--pp-line2)",
}


def _build_ticker_sector_map() -> dict[str, str]:
    """Build a ticker → GICS-sector lookup from S&P 500 + NASDAQ 100 data.

    Cached implicitly by `market_data.get_sp500_constituents` (24h memory).
    Returns roughly 500 tickers mapped — enough to cover ~80% of any
    superinvestor's top holdings; the rest fall under "Other".
    """
    try:
        from filings import market_data
    except Exception:
        return {}
    out: dict[str, str] = {}
    for source in (
        market_data.get_sp500_constituents() or [],
        market_data.get_nasdaq100_constituents() or [],
    ):
        for c in source:
            t = (c.get("ticker") or "").upper()
            sec = c.get("sector") or ""
            if t and sec and t not in out:
                out[t] = sec
    return out


def _funds_sectors_breakdown(fund: dict) -> list[dict]:
    """Aggregate holdings by GICS sector for the allocation panel.

    Tickers we can't resolve roll into "Other" so the bars still sum to ~100%.
    Returns at most 6 rows ordered by allocation (largest first).
    """
    holdings = fund.get("all_holdings") or []
    aum = fund.get("total_value") or 0
    if not holdings or not aum:
        return []
    ticker_to_sector = _build_ticker_sector_map()
    by_sector: dict[str, float] = {}
    for h in holdings:
        ticker = (h.get("ticker") or "").upper()
        val = h.get("value") or 0
        if not val:
            continue
        sector = ticker_to_sector.get(ticker, "Other")
        by_sector[sector] = by_sector.get(sector, 0.0) + float(val)
    # Pull "Other" out of the named buckets so the long-tail rollup doesn't
    # create a duplicate row when the cap (>5) trips.
    other_pct = (by_sector.pop("Other", 0.0) / aum) if aum else 0.0
    rows = [
        {
            "name":  name,
            "pct":   total / aum,
            "color": _FUNDS_SECTOR_COLORS.get(name, "var(--pp-line2)"),
        }
        for name, total in by_sector.items()
    ]
    rows.sort(key=lambda r: r["pct"], reverse=True)
    # Cap at 5 named rows + 1 "Other" — long tail beyond 5 rolls into Other.
    if len(rows) > 5:
        other_pct += sum(r["pct"] for r in rows[5:])
        rows = rows[:5]
    if other_pct > 0:
        rows.append({"name": "Other", "pct": other_pct, "color": _FUNDS_SECTOR_COLORS["Other"]})
    return rows


def _funds_holdings_market_join(holdings: list[dict]) -> tuple[dict[str, dict], dict[str, list[float]]]:
    """Fetch current prices + sparkline series for the given holdings.

    Synchronous (uses market_data's in-memory caches).  Wrap calls in
    `to_heavy` from the route for cold paths where these may pull from
    yfinance.  Always returns dicts (possibly empty) so callers can safely
    pass the results into `_funds_holdings_table`.
    """
    tickers = sorted({(h.get("ticker") or "").upper() for h in holdings if h.get("ticker")})
    if not tickers:
        return {}, {}
    try:
        from filings import market_data
    except Exception:
        return {}, {}

    # 1. Day-percent + price for each S&P 500 holding (covers ~80% of typical
    #    13F top-10s; the rest fall back to current_prices_batch).
    sp500 = market_data.get_sp500_market_data("1D") or {}
    prices_by_ticker: dict[str, dict] = {
        t: {"price": v.get("price"), "pct_change": v.get("pct_change")}
        for t, v in sp500.items()
        if isinstance(v, dict) and t in tickers
    }

    # 2. Fill in missing prices via current-prices batch (no day-pct, just last).
    missing = [t for t in tickers if t not in prices_by_ticker]
    if missing:
        try:
            extra = market_data.get_current_prices_batch(missing) or {}
            for t, p in extra.items():
                prices_by_ticker[t] = {"price": p, "pct_change": None}
        except Exception:
            pass

    # 3. Normalized sparkline points (~1 month of close data; column header
    #    relabelled "1M" since that's all we cache for free today).
    spark_by_ticker: dict[str, list[float]] = {}
    try:
        spark_by_ticker = market_data.get_sparkline_points(tickers, num_points=24) or {}
    except Exception:
        pass

    return prices_by_ticker, spark_by_ticker


# ── AUM history (multi-quarter) ────────────────────────────────────────────
# Reconstructs total_value per historical 13F-HR by walking edgartools
# directly.  Cached in L2 with 24h TTL — the underlying filings are
# immutable once filed, so a long TTL is safe.

_FUNDS_AUM_HISTORY_TTL_SECONDS = 24 * 3600
_FUNDS_AUM_HISTORY_QUARTERS = 12   # ~3 years of quarterly cadence


def _fund_aum_history_compute(cik: str) -> list[dict]:
    """Synchronous: pull last N 13F-HRs via edgartools and total each one.

    Returns list of {report_period, filing_date, total_value} ordered
    oldest → newest.  Empty list if edgar can't reach the filer.

    `tf.total_value` triggers a pyarrow incompatibility in some edgartools
    versions ("ChunkedArray has no attribute as_py"); we compute the total
    directly from the holdings frame's Value column instead, which is the
    same number ``ThirteenF.total_value`` would yield.
    """
    try:
        from edgar import Company, ThirteenF
        from filings.client import _detect_multiplier
    except Exception:
        return []

    try:
        company = Company(int(cik))
        filings = company.get_filings(form="13F-HR", amendments=False)
    except Exception as exc:
        logger.warning("AUM history: edgar load failed for CIK=%s: %s", cik, exc)
        return []

    if not filings:
        return []

    n = min(len(filings), _FUNDS_AUM_HISTORY_QUARTERS)
    out: list[dict] = []
    for i in range(n):
        try:
            tf = ThirteenF(filings[i])
            holdings_df = tf.holdings
            if holdings_df is None or len(holdings_df) == 0:
                continue
            mult = _detect_multiplier(holdings_df)
            total = int(holdings_df["Value"].sum()) * mult
            if not total:
                continue
            out.append({
                "report_period": str(tf.report_period),
                "filing_date":   str(tf.filing_date),
                "total_value":   total,
            })
        except Exception as exc:
            logger.debug("AUM history: skipping filing %d for CIK=%s: %s", i, cik, exc)
            continue
    out.sort(key=lambda r: r["report_period"])
    return out


async def _fund_aum_history(cik: str) -> list[dict]:
    """L2-cached AUM history series for the funds page.

    Cache key: ``funds:aum_history:{cik_norm}:v1``.  Stale data is fine —
    historical 13Fs don't change once filed.
    """
    cik_norm = (cik or "").lstrip("0") or cik
    return await _l2_cached(
        key=f"funds:aum_history:{cik_norm}:v1",
        ttl_seconds=_FUNDS_AUM_HISTORY_TTL_SECONDS,
        compute=lambda: _fund_aum_history_compute(cik),
        category="funds_aum_history",
    )


# `_format_dollars_compact` moved to _redesign.helpers (used by funds + insiders).


def _funds_aum_chart_payload(history: list[dict]) -> dict:
    """Convert AUM history into the data the SVG template needs.

    Produces SVG point pairs + nice-rounded y-axis labels in a 600×220
    viewBox.  The y-axis is anchored to round dollar values (e.g. $250B /
    $300B / $350B for Berkshire) so the absolute scale is visible — the
    earlier auto-normalized version made every fund's curve look the
    same shape regardless of magnitude.  The latest point's QoQ delta is
    precomputed so the panel header can colourize it.
    """
    if not history:
        return {
            "have_data": False,
            "points":     [],
            "ticks":      [],
            "y_labels":   [],
            "grid_ys":    [],
            "qoq_str":    "",
            "qoq_up":     None,
            "fill_d":     "",
            "line_d":     "",
        }

    # ViewBox sized 1500×240 to match the typical full-width container's
    # natural ratio (~6:1).  With `preserveAspectRatio="none"` the SVG
    # stretches to fill the container; if the viewBox aspect ratio matches,
    # that stretch is near-identity and circles stay round / slopes accurate.
    width, height = 1500.0, 240.0
    pad_top, pad_bot = 16.0, 12.0
    plot_h = height - pad_top - pad_bot
    n = len(history)

    vals = [r["total_value"] for r in history]
    lo_raw, hi_raw = min(vals), max(vals)
    rng = hi_raw - lo_raw if hi_raw > lo_raw else 1.0

    # Round y-axis bounds to a "nice" step so labels read $250B / $300B /
    # $350B instead of $258.5B / $303.4B / $348.2B.
    step = _nice_axis_step(rng, target_steps=4)
    import math
    lo = math.floor(lo_raw / step) * step
    hi = math.ceil (hi_raw / step) * step
    if hi == lo:
        hi = lo + step
    plot_rng = hi - lo

    def _y_for(v: float) -> float:
        return pad_top + (1.0 - (v - lo) / plot_rng) * plot_h

    points: list[tuple[float, float]] = []
    for i, v in enumerate(vals):
        x = (i / max(n - 1, 1)) * width
        points.append((round(x, 1), round(_y_for(v), 1)))

    line_d = " ".join(("M" if i == 0 else "L") + f"{x} {y}" for i, (x, y) in enumerate(points))
    fill_d = f"{line_d} L {width:.1f} {height:.1f} L 0 {height:.1f} Z"

    # Y-axis labels at every nice step between lo and hi.  Cap at 5 rows
    # so we don't crowd a small panel.
    y_labels: list[dict] = []
    grid_ys:  list[float] = []
    v = lo
    while v <= hi + step / 2:
        y_labels.append({
            "label": _format_dollars_compact(v),
            "y":     round(_y_for(v), 1),
        })
        grid_ys.append(round(_y_for(v), 1))
        v += step
    if len(y_labels) > 5:
        # Keep first / mid / last when too many.
        y_labels = [y_labels[0], y_labels[len(y_labels) // 2], y_labels[-1]]
        grid_ys  = [g["y"] for g in y_labels]

    # X-axis tick labels — ~6 evenly-spaced (incl. first + last).
    target_ticks = 6
    step_x = max(1, (n - 1) // (target_ticks - 1)) if n > 1 else 1
    tick_idxs = sorted(set([0] + list(range(0, n, step_x)) + [n - 1]))
    ticks = [{
        "label": _quarter_label(history[i]["report_period"]).replace(" 20", " '"),
        "x":     points[i][0],
    } for i in tick_idxs]

    # QoQ delta from the last two points.
    qoq_str = ""
    qoq_up: bool | None = None
    if n >= 2:
        delta = vals[-1] - vals[-2]
        sign = "+" if delta >= 0 else "-"
        if abs(delta) >= 1e9:
            qoq_str = f"{sign}${abs(delta) / 1e9:.1f}B QoQ"
        elif abs(delta) >= 1e6:
            qoq_str = f"{sign}${abs(delta) / 1e6:.0f}M QoQ"
        else:
            qoq_str = f"{sign}${abs(delta):,.0f} QoQ"
        qoq_up = delta >= 0

    # Per-point hover payload — quarter label + value + QoQ delta vs prior
    # point (None for the first row).  Serialized to JSON in the template
    # so the mousemove handler can look up the nearest point in O(1).
    chart_history: list[dict] = []
    for i, (h, p) in enumerate(zip(history, points)):
        prev_v = vals[i - 1] if i > 0 else None
        delta_pct = None
        if prev_v and prev_v > 0:
            delta_pct = (vals[i] - prev_v) / prev_v * 100
        chart_history.append({
            "x":          p[0],
            "y":          p[1],
            "value":      vals[i],
            "value_str":  _format_dollars_compact(vals[i]),
            "quarter":    _quarter_label(h["report_period"]),
            "filed":      h.get("filing_date", ""),
            "delta_pct":  delta_pct,
        })

    return {
        "have_data":     True,
        "points":        [{"x": x, "y": y} for x, y in points],
        "ticks":         ticks,
        "y_labels":      y_labels,
        "grid_ys":       grid_ys,
        "qoq_str":       qoq_str,
        "qoq_up":        qoq_up,
        "fill_d":        fill_d,
        "line_d":        line_d,
        "vb_width":      width,
        "vb_height":     height,
        "chart_history": chart_history,
    }


def _funds_recent_activity_with_aum(fund: dict, history: list[dict]) -> list[dict]:
    """Activity timeline with real AUM Δ when we have multi-quarter history.

    Pairs each `quarterly_changes` entry with the corresponding AUM-then
    record (matched by report_period).  When two consecutive AUM points
    are available, fills the Δ; otherwise leaves it as an em-dash.
    """
    qchanges = fund.get("quarterly_changes") or []
    history = history or []
    by_period = {h["report_period"]: h["total_value"] for h in history}
    history_periods = [h["report_period"] for h in history]

    rows = []
    for i, q in enumerate(qchanges[:4]):
        changes = q.get("changes") or []
        adds = sum(1 for c in changes if c.get("status") in ("ADDED", "NEW", "ADD"))
        cuts = sum(1 for c in changes if c.get("status") in ("REDUCED", "EXITED", "CUT", "EXIT"))

        period = q.get("report_period", "")
        aum_str, aum_up = "—", None
        if period in by_period:
            aum_now = by_period[period]
            try:
                idx = history_periods.index(period)
                if idx > 0:
                    prev = history[idx - 1]["total_value"]
                    if prev > 0:
                        delta_pct = (aum_now - prev) / prev * 100
                        sign = "+" if delta_pct >= 0 else ""
                        aum_str = f"{sign}{delta_pct:.1f}%"
                        aum_up = delta_pct >= 0
            except ValueError:
                pass

        rows.append({
            "quarter":     _quarter_label(period),
            "filing_date": q.get("filing_date", ""),
            "count_str":   f"+{adds} / -{cuts}",
            "current":     i == 0,
            "aum_delta":   aum_str,
            "aum_up":      aum_up,
        })
    return rows


_FUND_TABS = ("Portfolio", "Activity", "Performance", "Sectors", "Filings")


def _funds_index_rows(request: Request) -> list[dict]:
    """Build the sortable summary row for every cached superinvestor.

    Sources from `request.app.state.fund_cache` (populated at app startup;
    invalidated only when the cache reference changes).  Each row carries
    everything the index template renders — manager + fund name, portfolio
    value, holdings count, top-5 tickers + their stored logo IDs, filing
    date — sized to the v2 design token system (no extra fetches).
    """
    fund_cache = getattr(request.app.state, "fund_cache", {}) or {}
    if not fund_cache:
        return []
    try:
        from filings.superinvestors import SUPERINVESTORS_BY_CIK
    except Exception:
        return []

    rows: list[dict] = []
    for cik, info in SUPERINVESTORS_BY_CIK.items():
        cached = fund_cache.get(cik)
        if not cached:
            continue
        # `top_holdings` was dropped from the in-memory cache slim; read
        # the same prefix off `all_holdings` (value-sorted desc).
        top_tickers = [
            h.get("ticker") for h in (cached.get("all_holdings") or [])[:5]
            if h.get("ticker")
        ]
        rows.append({
            "cik":            cik,
            "manager":        info.display_name or info.fund_name or "—",
            "fund_name":      cached.get("name") or info.fund_name or "—",
            "value":          float(cached.get("total_value") or 0),
            "value_str":      _format_dollars(cached.get("total_value")),
            "holdings_n":     int(cached.get("total_holdings") or 0),
            "top_tickers":    top_tickers,
            "filing_date":    cached.get("filing_date", ""),
            "report_period":  cached.get("report_period", ""),
            "icon":           _icon_from_fund_name(cached.get("name") or info.fund_name or ""),
            "href":           f"/funds/{cik}",
        })
    rows.sort(key=lambda r: r["value"], reverse=True)
    return rows


# ── Funds index payload helpers ──────────────────────────────────────────
#
# Four sub-tabs (Funds / Holdings / Activity / Capital Deployed) all
# render against the same in-process ``fund_cache``.  Each helper below
# transforms the raw cache into the shape the corresponding pane needs.
# Mirrors v1's `/funds?view=…` page but with the v2 design system.


def _funds_holdings_consensus(grand_portfolio, n: int = 10) -> list[dict]:
    """Top-N stocks by holder count — drives the Consensus Leaders bar chart."""
    out: list[dict] = []
    max_holders = max((e.num_holders for e in grand_portfolio[:n]), default=1)
    for e in grand_portfolio[:n]:
        out.append({
            "ticker":          e.ticker or (e.cusip or "")[:6],
            "name":            e.issuer_name or "",
            "num_holders":     e.num_holders,
            "combined_value":  e.combined_value,
            "combined_value_str": _format_dollars(e.combined_value),
            "avg_weight":      round(e.avg_weight or 0, 1),
            "top_holders":     (e.holders or [])[:3],
            "bar_pct":         round(e.num_holders / max(max_holders, 1) * 100, 1),
        })
    return out


def _funds_holdings_momentum(most_added, n: int = 15) -> list[dict]:
    """Recent quarter momentum — top stocks added by superinvestors."""
    out: list[dict] = []
    max_adds = max((m.get("add_count", 0) for m in most_added[:n]), default=1)
    for m in most_added[:n]:
        out.append({
            "ticker":     m.get("ticker") or (m.get("cusip", "")[:6]),
            "name":       m.get("issuer_name", ""),
            "add_count":  m.get("add_count", 0),
            "adders":     [a for a in (m.get("adders") or [])[:5]],
            "value_str":  _format_dollars(m.get("total_value", 0)),
            "bar_pct":    round(m.get("add_count", 0) / max(max_adds, 1) * 100, 1),
        })
    return out


def _funds_index_holdings_table(grand_portfolio, top_n: int = 100) -> list[dict]:
    """Top-N stocks sorted by holder count — drives the funds-index All
    Holdings table.

    NB: distinct from the older `_funds_holdings_table` helper used by
    the fund detail page; renamed here to avoid the same shadow that
    collided `_funds_capital_deployed`.  ``pct_of_aggregate`` comes
    pre-computed off each entry from `client.build_grand_portfolio`,
    so no aggregate sum needed at this layer."""
    rows: list[dict] = []
    for i, e in enumerate(grand_portfolio[:top_n], start=1):
        rows.append({
            "rank":                i,
            "ticker":              e.ticker or (e.cusip or "")[:6],
            "name":                e.issuer_name or "",
            "num_holders":         e.num_holders,
            "combined_value":      e.combined_value,
            "combined_value_str":  _format_dollars(e.combined_value, full=True),
            "pct_of_aggregate":    round(e.pct_of_aggregate or 0, 2),
            "top_holders":         (e.holders or [])[:3],
            "more_holders":        max(0, len(e.holders or []) - 3),
        })
    return rows


_ACTIVITY_STATUS = {
    "NEW":      ("BUY",  True),
    "ADDED":    ("ADD",  True),
    "INCREASED": ("ADD", True),
    "REDUCED":  ("TRIM", False),
    "DECREASED": ("TRIM", False),
    "EXITED":   ("SELL", False),
    "SOLD":     ("SELL", False),
    "BOUGHT":   ("BUY",  True),
}


def _classify_action(raw_status: str) -> tuple[str, bool]:
    """Map raw 13F status → (display label, is_buy)."""
    return _ACTIVITY_STATUS.get((raw_status or "").upper(), (raw_status or "—", False))


_FUNDS_ACTIVITY_TOP_N = 50


def _funds_activity_consensus(fund_cache: dict, superinvestors_by_cik: dict,
                              *, cusip_to_ticker: dict | None = None) -> dict:
    """Aggregate every 13F change across all funds, grouped by ticker.

    Returns ``{moves: list, summary: dict}`` where each move row carries
    buyer/seller counts, net dollar flow, sentiment label, and the full
    per-fund detail list (for the expand-row pattern).

    `changes` rows often carry only a CUSIP (no ticker); resolve via the
    cross-fund CUSIP→ticker map so the consensus group keys collapse on
    the same security instead of fragmenting across CUSIP prefixes.

    Caller can pass a pre-built ``cusip_to_ticker`` map (e.g. the one
    cached on ``app.state._pp_redesign_cusip_ticker``) to skip the
    rebuild — saves ~50-150ms on warm Activity-tab hits.

    Truncates to the top ``_FUNDS_ACTIVITY_TOP_N`` tickers in Python
    BEFORE per-ticker trade sorting, so we don't sort ~5,950 throwaway
    trade lists that the template will discard via slice anyway.
    """
    if cusip_to_ticker is None:
        cusip_to_ticker = _build_cusip_ticker_map(fund_cache or {})

    by_ticker: dict[str, dict] = {}
    total_buy = 0.0
    total_sell = 0.0
    total_activities = 0

    for cik, fund_data in (fund_cache or {}).items():
        si = superinvestors_by_cik.get(cik)
        if not si:
            continue
        for c in (fund_data.get("changes") or []):
            raw = (c.get("status") or "").upper()
            if not raw or raw == "UNCHANGED":
                continue
            cusip = c.get("cusip", "") or ""
            ticker = c.get("ticker") or cusip_to_ticker.get(cusip)
            # No ticker resolution → skip (un-traded names crowd the list
            # without value; users wouldn't recognise raw CUSIP prefixes).
            if not ticker:
                continue
            action, is_buy = _classify_action(raw)
            value = float(c.get("current_value") or 0)
            shares = float(c.get("share_change") or 0)
            total_activities += 1
            if is_buy:
                total_buy += value
            else:
                total_sell += value

            slot = by_ticker.setdefault(ticker, {
                "ticker":      ticker,
                "issuer":      c.get("issuer", ""),
                "buy_count":   0,
                "sell_count":  0,
                "net_flow":    0.0,
                "trades":      [],
            })
            slot["buy_count"]  += 1 if is_buy else 0
            slot["sell_count"] += 0 if is_buy else 1
            # Net flow uses signed direction so dollars cancel within each ticker
            slot["net_flow"]   += value if is_buy else -value
            slot["trades"].append({
                "fund_name":    si.display_name,
                "cik":          cik,
                "action":       action,
                "is_buy":       is_buy,
                "share_change": shares,
                "value":        value,
                "value_str":    _format_dollars(value),
                "share_change_str": f"{shares:+,.0f}" if shares else "—",
                "raw_status":   raw,
            })

    # Cap to the top-N tickers BEFORE per-ticker trade sorting + sentiment
    # decoration.  ~6,000 raw tickers but only `_FUNDS_ACTIVITY_TOP_N` ever
    # render — sorting trade lists for the other ~5,950 was pure waste.
    total_consensus = len(by_ticker)
    moves_raw = sorted(
        by_ticker.values(),
        key=lambda m: -(m["buy_count"] + m["sell_count"]),
    )[:_FUNDS_ACTIVITY_TOP_N]

    moves: list[dict] = []
    for m in moves_raw:
        # Per-fund detail list sorted by absolute dollar move so the
        # biggest mover appears first when the user expands the row.
        m["trades"].sort(key=lambda t: -abs(t["value"]))
        if m["net_flow"] > 0:
            sentiment = ("BULLISH", "up")
        elif m["net_flow"] < 0:
            sentiment = ("BEARISH", "down")
        else:
            sentiment = ("NEUTRAL", "dim")
        m["sentiment_label"] = sentiment[0]
        m["sentiment_tone"]  = sentiment[1]
        m["net_flow_str"]    = (
            ("+" if m["net_flow"] >= 0 else "−") + _format_dollars(abs(m["net_flow"])).lstrip("$")
        )
        m["activity_n"]      = m["buy_count"] + m["sell_count"]
        moves.append(m)

    net_flow = total_buy - total_sell
    summary = {
        "value_sentiment":  "BULLISH" if net_flow > 0 else ("BEARISH" if net_flow < 0 else "NEUTRAL"),
        "value_tone":       "up" if net_flow > 0 else ("down" if net_flow < 0 else "dim"),
        "net_flow":         net_flow,
        "net_flow_str":     ("+" if net_flow >= 0 else "−") + _format_dollars(abs(net_flow)).lstrip("$"),
        "buying_str":       _format_dollars(total_buy),
        "selling_str":      _format_dollars(total_sell),
        "consensus_count":  total_consensus,
        "total_activities": total_activities,
    }
    return {"moves": moves, "summary": summary}


def _funds_capital_deployed_rows(deployment_data: dict) -> list[dict]:
    """Sortable rows for the funds-index Capital Deployed pane.

    NB: distinct from the older `_funds_capital_deployed(cik)` helper a
    few hundred lines up, which fetches per-fund deployment for the
    detail page.  Same domain, different consumer; renamed here to
    avoid the shadow that broke `/_v2/funds/{cik}` after this batch.

    Wraps `aum_data.build_deployment_leaderboard` and shapes each entry
    into the row format the template wants — formatted dollar strings,
    deployed-pct progress bar, source pill, and a stable sort key.
    """
    if not deployment_data:
        return []
    from filings import aum_data
    leaderboard = aum_data.build_deployment_leaderboard(deployment_data)

    rows: list[dict] = []
    for i, e in enumerate(leaderboard, start=1):
        # `deployment_ratio` is stored as a fraction (e.g. 0.46 = 46%);
        # multiply for the percentage display.
        ratio_raw = e.get("deployment_ratio")
        ratio_pct = round(float(ratio_raw) * 100, 1) if ratio_raw is not None else None
        # Deployed-bar tone: green if mostly deployed (≥ 60%), amber middle,
        # red when most cash sits outside the 13F equity book.
        if ratio_pct is None:
            tone = "dim"
        elif ratio_pct >= 60:
            tone = "up"
        elif ratio_pct >= 30:
            tone = "warn"
        else:
            tone = "down"

        cash = e.get("exact_cash") or e.get("estimated_non_equity")
        rows.append({
            "rank":               i,
            "cik":                e.get("cik", ""),
            "manager":            e.get("display_name") or e.get("fund_name") or "—",
            "fund_name":          e.get("fund_name") or "—",
            "raum":               e.get("raum") or 0,
            "raum_str":           _format_dollars(e.get("raum")),
            "thirteenf_value":    e.get("thirteenf_value") or 0,
            "thirteenf_value_str": _format_dollars(e.get("thirteenf_value")),
            "deployment_pct":     ratio_pct,
            "deployment_tone":    tone,
            "cash":               cash,
            "cash_str":           _format_dollars(cash) if cash else "—",
            "data_source":        (e.get("data_source") or "").upper(),
        })
    return rows


@router.get("/funds", response_class=HTMLResponse)
async def preview_funds_index(request: Request, view: str = "Funds"):
    """Funds index — 4 sub-panes (Funds list / Holdings / Activity /
    Capital Deployed).  Lands here when a user clicks "Funds" in the side
    nav.  Each fund row deep-links into ``/_v2/funds/{cik}``.

    All four panes render against the in-process ``fund_cache`` +
    ``deployment_cache`` populated at app startup — zero upstream fetches
    on this route, so warm hits are sub-200 ms across all sub-tabs.
    """
    valid_views = {"Funds", "Holdings", "Activity", "Capital Deployed"}
    # Accept legacy lowercase v1 view values so old bookmarks land on the
    # right tab (e.g. /funds?view=holdings → Holdings).
    legacy_alias = {
        "funds": "Funds", "holdings": "Holdings",
        "activity": "Activity", "deployment": "Capital Deployed",
    }
    if view not in valid_views:
        view = legacy_alias.get(view.lower(), "Funds")

    rows = _funds_index_rows(request)
    fund_cache       = getattr(request.app.state, "fund_cache", {}) or {}
    deployment_cache = getattr(request.app.state, "deployment_cache", {}) or {}

    cache_age = ""
    if fund_cache:
        try:
            from filings import cache as _cache_mod
            cache_age = _cache_mod.get_cache_age_str(fund_cache)
        except Exception:
            cache_age = ""

    # Holdings + Activity panes are now lazy-loaded on tab click via
    # `/api/funds-index/holdings` and `/api/funds-index/activity` (see
    # partial handlers below).  We only fetch their data here when the
    # user lands directly on `?view=Holdings` or `?view=Activity` — that
    # way the default Funds landing skips ~890ms of aggregation work and
    # ~750KB of HTML render.
    grand_portfolio: list = []
    most_added:      list = []
    activity_payload: dict = {"moves": [], "summary": {}}

    if fund_cache and view in ("Holdings", "Activity"):
        try:
            holdings_data, activity_data = await _fetch_funds_panes(
                request, fund_cache,
                need_holdings=(view == "Holdings"),
                need_activity=(view == "Activity"),
            )
            grand_portfolio, most_added = holdings_data
            activity_payload = activity_data or activity_payload
        except Exception as exc:
            logger.warning("Funds index aggregation failed: %s", exc)

    ctx = {
        "request":       request,
        **(await _shell_context(request, "Funds")),
        "page_title":    "Funds",
        # Tab navigation
        "funds_index_tabs":  ["Funds", "Holdings", "Activity", "Capital Deployed"],
        "funds_index_view":  view,
        # Funds pane (sortable list)
        "funds_index":   rows,
        "funds_count":   len(rows),
        "funds_cache_age": cache_age,
        # Holdings pane — only populated on direct ?view=Holdings link.
        "funds_consensus":    _funds_holdings_consensus(grand_portfolio, n=10),
        "funds_momentum":     _funds_holdings_momentum(most_added, n=15),
        "funds_holdings_all": _funds_index_holdings_table(grand_portfolio, top_n=100),
        # Activity pane — only populated on direct ?view=Activity link.
        "funds_activity_moves":   activity_payload.get("moves", []),
        "funds_activity_summary": activity_payload.get("summary", {}),
        # Capital Deployed pane (cheap — reads cached deployment_cache directly)
        "funds_capital_rows": _funds_capital_deployed_rows(deployment_cache),
    }
    return templates.TemplateResponse("_redesign/funds_index.html", ctx)


async def _fetch_funds_panes(
    request: Request,
    fund_cache: dict,
    *,
    need_holdings: bool,
    need_activity: bool,
) -> tuple[tuple[list, list], dict | None]:
    """Run only the aggregations needed for the requested panes.

    Returns ``((grand_portfolio, most_added), activity_payload_or_None)``.
    `build_grand_portfolio` and `build_most_added_table` are memoized on
    ``id(fund_cache)``; `_funds_activity_consensus_sync` likewise after
    the Wave 1 memoization pass.  So sequential `view=Holdings` then
    `view=Activity` requests share their respective memoized results.
    """
    from filings import client, market_data
    from filings.superinvestors import SUPERINVESTORS_BY_CIK

    tasks = []
    if need_holdings:
        # Heavy iteration + yfinance fanout (503 S&P tickers) -- route
        # through to_heavy so it lands on the heavy pool, not default.
        tasks.append(to_heavy(client.build_grand_portfolio,
                              fund_cache, SUPERINVESTORS_BY_CIK, timeout=30.0))
        tasks.append(to_heavy(market_data.build_most_added_table,
                              fund_cache, SUPERINVESTORS_BY_CIK, timeout=60.0))
    if need_activity:
        tasks.append(to_heavy(_funds_activity_consensus_sync,
                              request, fund_cache, SUPERINVESTORS_BY_CIK,
                              timeout=15.0))

    results = await asyncio.gather(*tasks) if tasks else []
    i = 0
    grand_portfolio: list = []
    most_added:      list = []
    activity_payload: dict | None = None
    if need_holdings:
        grand_portfolio = results[i]; i += 1
        most_added      = results[i]; i += 1
    if need_activity:
        activity_payload = results[i]
    return (grand_portfolio, most_added), activity_payload


@router.get("/api/funds-index/holdings", response_class=HTMLResponse)
async def preview_funds_holdings_partial(request: Request):
    """Lazy-loaded Holdings pane — fetched by the funds-index page on
    first activation of the Holdings tab.  Renders just the partial
    template (no app shell)."""
    fund_cache = getattr(request.app.state, "fund_cache", {}) or {}
    grand_portfolio: list = []
    most_added:      list = []
    if fund_cache:
        try:
            (grand_portfolio, most_added), _ = await _fetch_funds_panes(
                request, fund_cache, need_holdings=True, need_activity=False,
            )
        except Exception as exc:
            logger.warning("Funds holdings partial failed: %s", exc)
    return templates.TemplateResponse(
        "_redesign/partials/funds_holdings.html",
        {
            "request": request,
            "funds_consensus":    _funds_holdings_consensus(grand_portfolio, n=10),
            "funds_momentum":     _funds_holdings_momentum(most_added, n=15),
            "funds_holdings_all": _funds_index_holdings_table(grand_portfolio, top_n=100),
        },
    )


@router.get("/api/funds-index/activity", response_class=HTMLResponse)
async def preview_funds_activity_partial(request: Request):
    """Lazy-loaded Activity pane — fetched on first Activity-tab click."""
    fund_cache = getattr(request.app.state, "fund_cache", {}) or {}
    activity_payload: dict = {"moves": [], "summary": {}}
    if fund_cache:
        try:
            _, activity_payload = await _fetch_funds_panes(
                request, fund_cache, need_holdings=False, need_activity=True,
            )
            activity_payload = activity_payload or {"moves": [], "summary": {}}
        except Exception as exc:
            logger.warning("Funds activity partial failed: %s", exc)
    return templates.TemplateResponse(
        "_redesign/partials/funds_activity.html",
        {
            "request": request,
            "funds_activity_moves":   activity_payload.get("moves", []),
            "funds_activity_summary": activity_payload.get("summary", {}),
        },
    )


_funds_activity_consensus_memo: tuple[int, dict] | None = None


def _funds_activity_consensus_sync(request: Request,
                                    fund_cache: dict,
                                    superinvestors_by_cik: dict) -> dict:
    """Sync wrapper around `_funds_activity_consensus` for ``to_thread``.

    Memoized on ``id(fund_cache)`` — the fund cache is rebuilt as a new
    dict object on each sweep, so identity is a reliable invalidation
    key and matches the pattern used by `build_grand_portfolio` and
    `build_most_added_table`.  Uses the request-scoped CUSIP→ticker map
    cached on ``app.state._pp_redesign_cusip_ticker``.
    """
    global _funds_activity_consensus_memo
    cache_id = id(fund_cache)
    if _funds_activity_consensus_memo and _funds_activity_consensus_memo[0] == cache_id:
        return _funds_activity_consensus_memo[1]

    cmap = getattr(request.app.state, "_pp_redesign_cusip_ticker", None)
    if cmap is None:
        cmap = _build_cusip_ticker_map(fund_cache)
        try:
            request.app.state._pp_redesign_cusip_ticker = cmap
        except Exception:
            pass
    result = _funds_activity_consensus(fund_cache, superinvestors_by_cik, cusip_to_ticker=cmap)
    _funds_activity_consensus_memo = (cache_id, result)
    return result


@router.get("/funds/detail", response_class=HTMLResponse)
async def preview_fund_detail_legacy(
    request: Request,
    cik: str = _DEFAULT_FUND_CIK,
    tab: str = "Portfolio",
):
    """Legacy `?cik=` deep-link entry point.

    Registered BEFORE the `/funds/{cik}` catch-all so FastAPI matches the
    exact path first — otherwise ``cik="detail"`` would leak into the
    detail handler and blow up int parsing downstream.

    Old internal links carrying `/_v2/funds?cik=…` were replaced with the
    canonical `/_v2/funds/{cik}` path; this route covers any external /
    bookmarked links that still point at the query-string form.
    """
    return await _render_fund_detail(request, cik=cik, tab=tab)


@router.get("/funds/{cik}", response_class=HTMLResponse)
async def preview_fund_detail_by_cik(
    request: Request,
    cik: str,
    tab: str = "Portfolio",
):
    """Funds detail — pretty-URL form `/_v2/funds/{cik}`.

    Delegates to `_render_fund_detail` so the legacy `?cik=` path keeps
    working for any external links cached during the redesign window.
    """
    return await _render_fund_detail(request, cik=cik, tab=tab)


async def _render_fund_detail(request: Request, *, cik: str, tab: str):
    """Funds detail — 5 tabs (Portfolio / Activity / Performance / Sectors /
    Filings).  ``cik`` selects the fund; ``tab`` deep-links the active pane."""
    if tab not in _FUND_TABS:
        tab = "Portfolio"
    bounded = functools.partial(_bounded, page="Funds page")

    fund = await _fetch_fund_data(cik)
    if not fund:
        fund = {
            "name": "—", "cik": cik, "report_period": "", "filing_date": "",
            "total_value": 0, "total_holdings": 0,
            "top_holdings": [], "all_holdings": [],
            "changes": [], "quarterly_changes": [],
        }

    meta = _FUND_META.get(cik.lstrip("0") or cik) or _FUND_META.get(cik) or {}
    adds, cuts = _funds_changes_split(fund)
    top10 = (fund.get("all_holdings") or [])[:10]

    history, market_join, deployment, filings = await asyncio.gather(
        bounded(_fund_aum_history(cik),                        timeout=8.0,
                 fallback=[],          name="aum_history"),
        bounded(to_heavy(_funds_holdings_market_join, top10),  timeout=4.0,
                 fallback=({}, {}),    name="holdings_market_join"),
        bounded(_funds_capital_deployed(cik),                  timeout=3.0,
                 fallback={},          name="capital_deployed"),
        bounded(_funds_filings_list(cik, limit=25),            timeout=8.0,
                 fallback=[],          name="filings"),
    )
    # _l2_cached returns None on hard compute failure; normalise.
    history = history or []
    prices_by_ticker, spark_by_ticker = market_join or ({}, {})

    aum_chart = _funds_aum_chart_payload(history)
    activity  = _funds_recent_activity_with_aum(fund, history)

    ctx = {
        "request": request,
        **(await _shell_context(request, "Funds")),
        # Hero
        "fund_icon":          meta.get("icon") or _icon_from_fund_name(fund.get("name", "")),
        "fund_cik":           fund.get("cik") or cik,
        "fund_name":          fund.get("name") or "—",
        "fund_manager":       meta.get("manager") or "",
        "fund_city":          meta.get("city") or "",
        "fund_filing_date":   fund.get("filing_date", ""),
        "fund_report_period": fund.get("report_period", ""),
        "fund_quarter_label": _quarter_label(fund.get("report_period", "")),
        # Tabs
        "fund_tabs":     list(_FUND_TABS),
        "funds_tab":     tab,
        # Portfolio tab payloads
        "funds_kpi":           _funds_kpi_strip(fund, history),
        "funds_concentration": _funds_concentration_donut(fund),
        "funds_holdings":      _funds_holdings_table(
            fund, top_n=10,
            prices_by_ticker=prices_by_ticker,
            spark_by_ticker=spark_by_ticker,
        ),
        "funds_pos_total":     fund.get("total_holdings") or 0,
        # Activity tab payloads
        "funds_activity":      activity,
        "funds_quarter_diffs": _funds_position_changes_quarters(fund),
        "funds_adds":          adds,
        "funds_cuts":          cuts,
        # Performance tab payloads
        "funds_aum_chart":     aum_chart,
        "funds_capital":       _funds_capital_panel(deployment, fund),
        # Sectors tab payload
        "funds_sectors":       _funds_sectors_breakdown(fund),
        # Filings tab payload
        "funds_filings":       filings,
    }
    return templates.TemplateResponse("_redesign/funds.html", ctx)
