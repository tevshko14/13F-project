"""Insiders page (v2 redesign).

One route -- ``/insiders`` -- with two tabs:
  * **Filings**  -- filtered recent trades (direction + role + plan filters)
  * **Clusters** -- per-ticker rollups of 3+ same-direction insiders

Both tabs read from ``filings.insider_trading`` which is Supabase-first
(hot table + cold archive; OpenInsider scrape is L3 fallback only).
Trade-list fetches go through ``to_supabase`` so a slow yfinance day
can't saturate them via the heavy pool.
"""

from __future__ import annotations

import asyncio
import functools
import logging
import math
from datetime import datetime, timedelta
from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from filings.app_state import templates
from filings.concurrency import to_light, to_supabase
from filings.routers._redesign.helpers import (
    _bounded,
    _format_compact_dollars,
    _format_dollars_compact,
    _insiders_action,
    _insiders_format_title,
    _nice_axis_step,
    _shell_context,
    GracefulRoute,
)

logger = logging.getLogger(__name__)

router = APIRouter(route_class=GracefulRoute)


# ── Trade-type classifiers ───────────────────────────────────────────


def _insiders_plan(trade_type: str) -> str:
    """Open-market vs scheduled.  OpenInsider doesn't always carry the
    10b5-1 marker on the global feed; we tag "10b5-1" only when explicitly
    present and otherwise default to "open" so the column is populated.
    """
    s = (trade_type or "").lower()
    if "10b5-1" in s or "10b5" in s:
        return "10b5-1"
    return "open"


# Coarse role bucket used by the Filings tab role filter.  Maps every
# OpenInsider title segment to one of the chip values.  Anything that
# doesn't match a named bucket falls into "Other" so the filter still
# accounts for it without dropping rows from "All".
def _insiders_role_key(title: str) -> str:
    """Map a raw OpenInsider title to one of the role-filter buckets:
    'CEO', 'CFO', 'Director', '10pct', or 'Other'."""
    t = (title or "").lower()
    if not t:
        return "Other"
    # Title strings can list multiple roles, e.g. "Pres & CEO, Director" —
    # match the most senior-looking bucket first.
    if "ceo" in t or "chief executive" in t:
        return "CEO"
    if "cfo" in t or "chief financial" in t:
        return "CFO"
    if "10%" in t or "10 percent" in t or "ten percent" in t or "10 pct" in t:
        return "10pct"
    if "director" in t:
        return "Director"
    return "Other"


# ── KPI strip helpers ────────────────────────────────────────────────


def _insiders_kpi_strip_empty() -> list[dict]:
    """KPI strip when no upstream trade data is available — every cell em-dashed."""
    return [
        {"label": "Filings",          "value": "—", "delta": None, "up": None},
        {"label": "Net flow",         "value": "—", "delta": None, "up": None},
        {"label": "Buy / Sell ratio", "value": "—", "delta": None, "up": None},
        {"label": "Active clusters",  "value": "—", "delta": None, "up": None},
        {"label": "Top buyer",        "value": "—", "delta": None, "up": None},
        {"label": "Top seller",       "value": "—", "delta": None, "up": None},
    ]


def _insiders_notable(rows: list[dict]) -> dict | None:
    """Pick the most notable open-market BUY for the coral-bordered callout."""
    candidates = [r for r in rows if r.get("flag")]
    if not candidates:
        return None
    r = candidates[0]
    return {
        "person":      r["person"],
        "role":        r["role"],
        "ticker":      r["ticker"],
        "value":       r["value"],
        "shares":      r["shares"],
        "price":       r["price"],
        "date":        r["date"],
        # Headline construction is left for the template — pass parts only.
    }


# ── Clusters tab helpers ─────────────────────────────────────────────


async def _fetch_insider_trades_wide(count: int = 200) -> list:
    """Pull a wider window of insider trades for the new aggregation tabs.

    Reuses the same scraper/Supabase layer as the Filings tab fetcher, just
    with a larger count budget.  Returns the raw ``InsiderTrade`` list so
    each builder can roll up by ticker / insider / sector independently.
    """
    try:
        from filings import insider_trading
        # Supabase-first read -- route off the heavy pool so a slow
        # yfinance day can't queue this behind stuck upstream work.
        return await to_supabase(
            insider_trading.get_latest_insider_trades, "", count, "",
        ) or []
    except Exception as exc:
        logger.warning("Insider wide-window fetch failed: %s", exc)
        return []


def _insider_value_dollars(trade) -> float:
    """Parse the OpenInsider ``value`` string into an absolute dollar float."""
    try:
        from filings.insider_trading import parse_dollar_value
    except Exception:
        return 0.0
    try:
        return abs(parse_dollar_value(trade.value))
    except Exception:
        return 0.0


def _insider_is_buy(trade) -> bool:
    """OpenInsider trade_type → bool BUY / SELL."""
    return "Purchase" in (trade.trade_type or "")


def _shorten_role(title: str) -> str:
    """Compact role label — preserves CEO/CFO/Director, abbreviates the rest."""
    if not title:
        return "—"
    t = title.strip()
    aliases = {
        "Chief Executive Officer":  "CEO",
        "Chief Financial Officer":  "CFO",
        "Chief Operating Officer":  "COO",
        "Chief Technology Officer": "CTO",
        "Chief Legal Officer":      "CLO",
        "Chief Marketing Officer":  "CMO",
        "President":                "President",
        "Director":                 "Director",
    }
    for long_, short_ in aliases.items():
        if long_.lower() in t.lower():
            return short_
    if "10%" in t or "Owner" in t:
        return "10% own."
    return t[:14]


def _net_format(net: float) -> str:
    if not net:
        return "—"
    sign = "+" if net > 0 else "-"
    return f"{sign}{_format_compact_dollars(abs(net))}"


def _insiders_clusters_panel(trades: list, top_n: int = 4) -> list[dict]:
    """Group trades by ticker; surface tickers with 3+ insiders all trading
    the same direction inside the window.  Returns the densest 4 cards by
    aggregate dollar volume — matches the design's 2x2 grid."""

    by_ticker: dict[str, dict] = {}
    for tr in trades:
        if not tr.ticker:
            continue
        key = tr.ticker.upper()
        if key not in by_ticker:
            by_ticker[key] = {
                "ticker":   key,
                "name":     tr.company_name or "",
                "buys":     [],
                "sells":    [],
                "buy_value":  0.0,
                "sell_value": 0.0,
            }
        bucket = by_ticker[key]
        v = _insider_value_dollars(tr)
        if _insider_is_buy(tr):
            bucket["buys"].append(tr)
            bucket["buy_value"] += v
        else:
            bucket["sells"].append(tr)
            bucket["sell_value"] += v

    clusters: list[dict] = []
    for key, b in by_ticker.items():
        # Cluster = ≥3 distinct insiders trading the same direction.
        if len(b["buys"]) >= 3:
            members = b["buys"]
            direction = "BUY"
            volume = b["buy_value"]
        elif len(b["sells"]) >= 3:
            members = b["sells"]
            direction = "SELL"
            volume = b["sell_value"]
        else:
            continue

        # Dedupe by insider name; keep the largest individual trade per person.
        per_person: dict[str, dict] = {}
        for m in members:
            person = m.insider_name or "—"
            mv = _insider_value_dollars(m)
            existing = per_person.get(person)
            if existing is None or mv > existing["v_raw"]:
                per_person[person] = {
                    "p":  person,
                    "r":  _shorten_role((m.title or "").split(",")[0].strip()),
                    "v":  _format_compact_dollars(mv),
                    "v_raw": mv,
                    "d":  (m.trade_date or m.filing_date or "")[:10],
                }
        members_list = sorted(per_person.values(), key=lambda x: x["v_raw"], reverse=True)[:6]

        clusters.append({
            "ticker":     key,
            "name":       b["name"],
            "direction":  direction,
            "count":      len(per_person),
            "value":      _format_compact_dollars(volume),
            "_volume":    volume,        # sort key — not consumed by the template
            "members":    members_list,
        })

    clusters.sort(key=lambda c: (c["count"], c["_volume"]), reverse=True)
    return clusters[:top_n]


def _insiders_kpi_strip_real(trades: list) -> list[dict]:
    """Real KPI strip from the active trade window. Empty data → every cell
    em-dashed; we never emit fake fallback numbers."""
    from collections import Counter
    if not trades:
        return _insiders_kpi_strip_empty()

    buys = [t for t in trades if _insider_is_buy(t)]
    sells = [t for t in trades if not _insider_is_buy(t)]
    buy_v = sum(_insider_value_dollars(t) for t in buys)
    sell_v = sum(_insider_value_dollars(t) for t in sells)
    net = buy_v - sell_v
    bs_ratio = (buy_v / sell_v) if sell_v else 0

    buy_counts = Counter((t.ticker or "").upper() for t in buys if t.ticker)
    sell_counts = Counter((t.ticker or "").upper() for t in sells if t.ticker)
    top_buyer  = buy_counts.most_common(1)[0][0] if buy_counts else "—"
    top_seller = sell_counts.most_common(1)[0][0] if sell_counts else "—"

    # Active clusters — tickers w/ 3+ same-direction insiders.
    from collections import defaultdict
    cluster_count = 0
    by_ticker_dirs: dict[str, dict[str, set]] = defaultdict(lambda: {"buys": set(), "sells": set()})
    for t in trades:
        if not t.ticker:
            continue
        key = t.ticker.upper()
        if _insider_is_buy(t):
            by_ticker_dirs[key]["buys"].add(t.insider_name)
        else:
            by_ticker_dirs[key]["sells"].add(t.insider_name)
    for sets in by_ticker_dirs.values():
        if len(sets["buys"]) >= 3 or len(sets["sells"]) >= 3:
            cluster_count += 1

    return [
        {"label": "Filings",                     "value": f"{len(trades):,}",       "delta": None,  "up": None},
        {"label": "Net flow",                    "value": _net_format(net),         "delta": None,  "up": (net >= 0)},
        {"label": "Buy / Sell ratio",            "value": f"{bs_ratio:.2f}",        "delta": None,  "up": (bs_ratio >= 1)},
        {"label": "Active clusters",             "value": str(cluster_count),       "delta": None,  "up": None},
        {"label": "Top buyer",                   "value": top_buyer,                "delta": None,  "up": None},
        {"label": "Top seller",                  "value": top_seller,               "delta": None,  "up": None},
    ]


# ── Filings tab — direction / window / role / plan filtering ────────


_INSIDERS_DIRECTIONS = {
    "latest":    {"label": "Latest",    "trade_type": ""},
    "purchases": {"label": "Purchases", "trade_type": "p"},
    "sales":     {"label": "Sales",     "trade_type": "s"},
}
_INSIDERS_WINDOWS: dict[str, dict[str, Any]] = {
    "today":   {"label": "Today",         "days": 1,   "kpi": "today"},
    "7d":      {"label": "Last 7 days",   "days": 7,   "kpi": "7d"},
    "30d":     {"label": "Last 30 days",  "days": 30,  "kpi": "30d"},
    "quarter": {"label": "This quarter",  "days": None,"kpi": "QTD"},  # special: from quarter start
}
_INSIDERS_ROLE_KEYS = ("All", "CEO", "CFO", "Director", "10pct", "Other")
_INSIDERS_PLAN_KEYS = ("All", "open", "10b5-1")

_VALID_INSIDER_DIRECTIONS = tuple(_INSIDERS_DIRECTIONS.keys())
_VALID_INSIDER_WINDOWS    = tuple(_INSIDERS_WINDOWS.keys())
# Clusters tab sub-pill — filter the per-ticker cluster list by direction.
_INSIDERS_CLUSTER_KEYS = ("All", "BUY", "SELL")


def _insiders_window_since(window_key: str) -> str:
    """Resolve a window key to a 'YYYY-MM-DD' since-date for the OpenInsider
    fetcher.  'quarter' anchors to the start of the current calendar quarter."""
    today = datetime.now()
    if window_key == "quarter":
        q_start_month = ((today.month - 1) // 3) * 3 + 1
        return today.replace(month=q_start_month, day=1).strftime("%Y-%m-%d")
    days = _INSIDERS_WINDOWS.get(window_key, _INSIDERS_WINDOWS["30d"])["days"]
    if days is None or days <= 0:
        return ""
    return (today - timedelta(days=days)).strftime("%Y-%m-%d")


async def _fetch_insiders_filtered(*, direction: str, window: str, count: int = 200) -> list:
    """Pull insider trades filtered server-side by trade_type + since_date.

    Role + Plan are parsed from the OpenInsider title / trade_type strings
    after the fetch — the upstream API doesn't expose those as filters.
    """
    trade_type = _INSIDERS_DIRECTIONS.get(direction, _INSIDERS_DIRECTIONS["latest"])["trade_type"]
    since      = _insiders_window_since(window)
    try:
        from filings import insider_trading
        # Supabase-first read -- to_supabase pool keeps this fast even
        # when yfinance is saturating the heavy pool.
        return await to_supabase(
            insider_trading.get_latest_insider_trades, trade_type, count, since,
        ) or []
    except Exception as exc:
        logger.warning("Insiders filtered fetch failed: %s", exc)
        return []


def _insiders_apply_role_plan(trades: list, role: str, plan: str) -> list:
    """Post-filter the returned list by role + plan (no upstream support)."""
    if role == "All" and plan == "All":
        return trades
    out = []
    for t in trades:
        if role != "All" and _insiders_role_key(t.title) != role:
            continue
        if plan != "All" and _insiders_plan(t.trade_type) != plan:
            continue
        out.append(t)
    return out


def _insiders_format_filtered_rows(trades: list, *, max_filings: int = 200) -> list[dict]:
    """Same shape as the existing rows — re-formatted from the filtered set.
    Used when we apply server-side filters."""
    rows = []
    for tr in trades[:max_filings]:
        action = _insiders_action(tr.trade_type)
        plan   = _insiders_plan(tr.trade_type)
        rows.append({
            "person":       tr.insider_name,
            "role":         _insiders_format_title(tr.title),
            "ticker":       (tr.ticker or "").upper(),
            "company_name": getattr(tr, "company_name", "") or "",
            "action":       action,
            "shares":       tr.qty or "—",
            "price":        tr.price or "—",
            "value":        tr.value or "—",
            "plan":         plan,
            "date":         (tr.trade_date or tr.filing_date or "")[:10],
            "sec_url":      getattr(tr, "sec_url", "") or "",
            "title":        tr.title or "",
            "flag":         action == "BUY" and plan == "open",
        })
    return rows


def _insiders_group_by_ticker(rows: list[dict]) -> list[dict]:
    """Collapse individual filings into one row per ticker for the dense
    summary view — each group carries the count + total $ + the
    constituent filings (revealed when the row is expanded)."""
    from collections import OrderedDict
    groups: "OrderedDict[str, dict]" = OrderedDict()
    for r in rows:
        tk = r.get("ticker") or "—"
        bucket = groups.setdefault(tk, {
            "ticker":     tk,
            "name":       "",
            "filings":    [],
            "buy_count":  0,
            "sell_count": 0,
            "buy_value":  0.0,
            "sell_value": 0.0,
        })
        bucket["filings"].append(r)
        # Best-available company name from the first row that carries one.
        if not bucket["name"]:
            bucket["name"] = (r.get("company_name") or r.get("name") or "")
        try:
            from filings.insider_trading import parse_dollar_value
            v = abs(parse_dollar_value(r.get("value") or ""))
        except Exception:
            v = 0.0
        if r.get("action") == "BUY":
            bucket["buy_count"]  += 1
            bucket["buy_value"]  += v
        else:
            bucket["sell_count"] += 1
            bucket["sell_value"] += v

    out = []
    for tk, b in groups.items():
        net = b["buy_value"] - b["sell_value"]
        # Summary tag: dominant direction + count.
        if b["buy_count"] and not b["sell_count"]:
            tag_kind  = "BUY"
            tag_label = f"{b['buy_count']} Buy" + ("s" if b["buy_count"] > 1 else "")
        elif b["sell_count"] and not b["buy_count"]:
            tag_kind  = "SELL"
            tag_label = f"{b['sell_count']} Sale" + ("s" if b["sell_count"] > 1 else "")
        else:
            tag_kind  = "MIXED"
            tag_label = f"{b['buy_count']} Buy / {b['sell_count']} Sale"
        out.append({
            "ticker":     tk,
            "name":       b["name"],
            "tag_kind":   tag_kind,
            "tag_label":  tag_label,
            "net":        net,
            "net_str":    _net_format(net) if (b["buy_value"] or b["sell_value"]) else "—",
            "filings":    b["filings"],
            "filing_n":   len(b["filings"]),
        })
    return out


def _insiders_momentum_chart(trades: list, *, top_buys: int = 5, top_sells: int = 5) -> dict:
    """Bar-chart payload for the Insider Momentum panel.

    Buckets trades by ticker, picks the top-N buys (positive net dollar) +
    top-N sells (negative net dollar), then computes SVG-ready bar geometry.
    Mirrors the v1 'Insider Momentum' module.
    """
    if not trades:
        return {"have_data": False, "bars": [], "y_labels": [], "grid_ys": []}

    from collections import defaultdict
    by_t: dict[str, dict] = defaultdict(lambda: {"buy": 0.0, "sell": 0.0})
    for tr in trades:
        tk = (tr.ticker or "").upper()
        if not tk:
            continue
        v = _insider_value_dollars(tr)
        if _insider_is_buy(tr):
            by_t[tk]["buy"]  += v
        else:
            by_t[tk]["sell"] += v
    if not by_t:
        return {"have_data": False, "bars": [], "y_labels": [], "grid_ys": []}

    nets = [(tk, b["buy"] - b["sell"]) for tk, b in by_t.items()]
    nets.sort(key=lambda x: x[1], reverse=True)
    buys  = [(tk, n) for tk, n in nets if n > 0][:top_buys]
    sells = [(tk, n) for tk, n in nets if n < 0][-top_sells:]
    sells.reverse()  # most-negative first → matches v1 ordering
    # `bars` order: buys (descending), then sells (most-negative first → least).
    ordered = buys + sells
    if not ordered:
        return {"have_data": False, "bars": [], "y_labels": [], "grid_ys": []}

    # Y-axis: anchor zero at the chart midline so positive bars grow up and
    # negative bars grow down.  ViewBox 1500×280 matches a typical full-width
    # container (~5.4:1) so `preserveAspectRatio="none"` is near-identity.
    width, height = 1500.0, 280.0
    pad_top, pad_bot, pad_left = 18.0, 28.0, 78.0
    plot_w = width - pad_left
    plot_h = height - pad_top - pad_bot

    max_val = max(abs(n) for _, n in ordered) or 1.0
    # Round up to a "nice" step so y labels read cleanly.
    step = _nice_axis_step(max_val * 2, target_steps=4)
    rng_top = math.ceil(max_val / step) * step
    rng_bot = -rng_top

    def _y_for(v: float) -> float:
        return pad_top + (1.0 - (v - rng_bot) / (rng_top - rng_bot)) * plot_h

    zero_y = _y_for(0)

    n_bars = len(ordered)
    bar_gap = 14.0
    bar_w = max(8.0, (plot_w - bar_gap * (n_bars + 1)) / n_bars)

    bars: list[dict] = []
    for i, (tk, n) in enumerate(ordered):
        x = pad_left + bar_gap + i * (bar_w + bar_gap)
        y_top = _y_for(n) if n >= 0 else zero_y
        y_bot = zero_y if n >= 0 else _y_for(n)
        bars.append({
            "ticker":  tk,
            "value":   n,
            "is_buy":  n >= 0,
            "value_str": _net_format(n),
            "x":       round(x, 1),
            "y":       round(y_top, 1),
            "w":       round(bar_w, 1),
            "h":       round(max(y_bot - y_top, 1.0), 1),
            "label_x": round(x + bar_w / 2, 1),
        })

    # Y-axis: 5 lines from -rng to +rng inclusive.
    y_labels: list[dict] = []
    grid_ys:  list[float] = []
    v = rng_bot
    while v <= rng_top + step / 2:
        y_pos = round(_y_for(v), 1)
        y_labels.append({"label": _format_dollars_compact(v), "y": y_pos})
        grid_ys.append(y_pos)
        v += step

    return {
        "have_data": True,
        "bars":      bars,
        "y_labels":  y_labels,
        "grid_ys":   grid_ys,
        "vb_width":  width,
        "vb_height": height,
        "zero_y":    round(zero_y, 1),
        "left_pad":  pad_left,
    }


# ── Route ─────────────────────────────────────────────────────────────


@router.get("/insiders", response_class=HTMLResponse)
async def preview_insiders(
    request: Request,
    direction: str = "latest",
    role:      str = "All",
    plan:      str = "All",
    cluster:   str = "All",
):
    """Insiders page — Filings tab honours `?direction=&role=&plan=` query
    params; Clusters tab honours `?cluster=All|BUY|SELL`.  Every filter is
    server-rendered (no client-side decoration).

    The Filings tab + momentum chart use a fixed 30-day lookback; the wider
    6-month set still drives Clusters / People / Companies tabs.
    """
    window = "30d"  # fixed window for Filings + momentum (no UI control)
    if direction not in _VALID_INSIDER_DIRECTIONS: direction = "latest"
    if role      not in _INSIDERS_ROLE_KEYS:       role      = "All"
    if plan      not in _INSIDERS_PLAN_KEYS:       plan      = "All"
    if cluster   not in _INSIDERS_CLUSTER_KEYS:    cluster   = "All"

    bounded = functools.partial(_bounded, page="Insiders page")

    # Filings tab uses the user-selected filters on the recent 200-trade
    # pull; Clusters tab uses a wider always-on pull so the aggregation is
    # stable regardless of what's in the filter bar.
    filings_trades, wide_trades = await asyncio.gather(
        bounded(_fetch_insiders_filtered(direction=direction, window=window, count=200),
                timeout=8.0, fallback=[], name="filings"),
        bounded(_fetch_insider_trades_wide(200),
                timeout=8.0, fallback=[], name="wide"),
    )

    # Apply role + plan filters server-side (OpenInsider doesn't expose them).
    filings_trades = _insiders_apply_role_plan(filings_trades, role, plan)

    rows = _insiders_format_filtered_rows(filings_trades)
    grouped = _insiders_group_by_ticker(rows)

    # Momentum chart + KPI strip reflect the active Filings filter set.
    momentum_chart = _insiders_momentum_chart(filings_trades)

    # Pull the full cluster list (not just the top 4) so we can server-side
    # filter by direction before slicing to the 2x2 grid.  Wrapped in
    # ``bounded`` because ``to_light`` raises TimeoutError on hang; an
    # ungated raise here would crash the whole /insiders page when the
    # cluster build (~6s on big trade volumes) slips past its deadline.
    clusters_all = await bounded(
        to_light(_insiders_clusters_panel, wide_trades, 99),
        timeout=6.0, fallback=[], name="clusters",
    ) if wide_trades else []

    # Cluster sub-pill: filter by direction (BUY/SELL/All), then take top 4.
    if cluster == "All":
        clusters_panel = clusters_all[:4]
    else:
        clusters_panel = [c for c in clusters_all if c["direction"] == cluster][:4]
    clusters_buy_total  = sum(1 for c in clusters_all if c["direction"] == "BUY")
    clusters_sell_total = sum(1 for c in clusters_all if c["direction"] == "SELL")

    ctx = {
        "request":          request,
        **(await _shell_context(request, "Insiders")),
        # KPI strip + filtered Filings tab payload
        "insiders_kpi":         _insiders_kpi_strip_real(filings_trades),
        "insiders_rows":        rows,
        "insiders_grouped":     grouped,
        "insiders_total":       len(filings_trades),
        "insiders_total_str":   f"{len(filings_trades):,}",
        "insiders_notable":     _insiders_notable(rows),
        "insiders_momentum":    momentum_chart,
        # Active filter state
        "insiders_direction":   direction,
        "insiders_directions":  [(k, v["label"]) for k, v in _INSIDERS_DIRECTIONS.items()],
        "insiders_role":        role,
        "insiders_role_keys":   _INSIDERS_ROLE_KEYS,
        "insiders_plan":        plan,
        "insiders_plan_keys":   _INSIDERS_PLAN_KEYS,
        # Clusters tab (independent of Filings filter)
        "insiders_clusters":    clusters_panel,
        "insiders_cluster":     cluster,
        "insiders_cluster_keys": _INSIDERS_CLUSTER_KEYS,
        "insiders_clusters_buy_count":  clusters_buy_total,
        "insiders_clusters_sell_count": clusters_sell_total,
        "insiders_clusters_all_count":  len(clusters_all),
        "insiders_tab":         "Filings",
    }
    return templates.TemplateResponse("_redesign/insiders.html", ctx)
