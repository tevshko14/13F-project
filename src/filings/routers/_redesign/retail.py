"""Retail page (v2 redesign).

One route -- ``/retail`` -- with three tabs:

  * **Pulse**       -- CNN Fear & Greed gauge + 3 callout cards
                       (most-mentioned, biggest rank mover, top-5 trending).
                       Wired live via ``filings.sentiment._get_apewisdom_all``
                       and ``_get_cnn_fear_greed``.
  * **Leaderboard** -- Reddit velocity heatmap + hype-vs-quality scatter +
                       table.  Powered by the shared retail-leaderboard
                       builder in ``filings.sentiment``.
  * **Calendar**    -- Recent YouTube uploads (48h) + finance-channel
                       directory from ``filings.youtube_cache``.

All upstream fetches dispatch in one parallel gather under a bounded
budget so a slow upstream can't stall the whole render.
"""

from __future__ import annotations

import asyncio
import functools
import html as _html
import logging

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from filings.app_state import templates
from filings.cache_l2 import l2_cached as _l2_cached
from filings.concurrency import to_heavy, to_light
from filings.routers._redesign.helpers import _bounded, _shell_context

logger = logging.getLogger(__name__)

router = APIRouter()




# Static name overrides for tickers ApeWisdom doesn't include or where the
# returned name differs from market convention.  Used in the trending table.
_RETAIL_NAME_OVERRIDES = {
    "GME":  "GameStop Corp.",
    "AMC":  "AMC Entertainment",
    "BB":   "BlackBerry",
    "TSLA": "Tesla",
    "NVDA": "NVIDIA",
    "AAPL": "Apple",
    "PLTR": "Palantir",
    "SOFI": "SoFi Technologies",
}


def _retail_name(item: dict) -> str:
    tk = (item.get("ticker") or "").upper()
    if tk in _RETAIL_NAME_OVERRIDES:
        return _RETAIL_NAME_OVERRIDES[tk]
    # ApeWisdom occasionally returns HTML-escaped names ("SPDR S&amp;P 500…").
    # Unescape so Jinja's auto-escape doesn't produce a doubled `&amp;`.
    return _html.unescape(item.get("name", tk) or tk)


def _retail_dod_delta(item: dict) -> float:
    """Day-over-day change in mentions, expressed as a fraction.

    ApeWisdom returns `mentions_24h_ago`.  When 0 / missing we treat the
    ticker as "new this window" and return a high positive number to keep
    it ranked at the top.
    """
    now = item.get("mentions") or 0
    then = item.get("mentions_24h_ago") or 0
    if then <= 0:
        return 1.0 if now > 0 else 0.0
    return (now - then) / then


def _retail_sentiment_proxy(item: dict) -> float:
    """ApeWisdom doesn't expose sentiment per ticker.  Use upvotes/mentions
    ratio as a crude proxy in [-1, 1].  Real signal needs Reddit comment
    NLP or Finnhub per-ticker — call out as a gap."""
    mentions = max(int(item.get("mentions") or 0), 1)
    upvotes = int(item.get("upvotes") or 0)
    raw = upvotes / mentions  # typically 0..50
    # Squash to [-1, 1] with a soft target of ~0.5 = neutral.
    if raw <= 0:
        return -0.2
    if raw < 1:
        return -0.1 + raw * 0.4
    if raw < 5:
        return 0.3 + (raw - 1) * 0.1
    if raw < 20:
        return 0.7 + min((raw - 5) / 30, 0.25)
    return 0.95


async def _fetch_retail_data() -> dict:
    """Fetch trending tickers from ApeWisdom with safe fallbacks.

    Returns dict with `featured` (top ticker dict) + `trending` (list[dict]).
    Falls back to a static demo set when ApeWisdom is unreachable.
    """
    try:
        from filings import sentiment
        items = await to_heavy(sentiment._get_apewisdom_all)
    except Exception as exc:
        logger.warning("ApeWisdom fetch failed: %s", exc)
        items = []

    if not items:
        # Demo fallback — same shape ApeWisdom returns.
        items = [
            {"rank": 1, "ticker": "GME",  "name": "GameStop",   "mentions": 8420, "upvotes": 24180, "mentions_24h_ago":  916},
            {"rank": 2, "ticker": "NVDA", "name": "NVIDIA",     "mentions": 2891, "upvotes": 12480, "mentions_24h_ago": 2532},
            {"rank": 3, "ticker": "AMC",  "name": "AMC",        "mentions": 2104, "upvotes":  6120, "mentions_24h_ago": 1294},
            {"rank": 4, "ticker": "TSLA", "name": "Tesla",      "mentions": 1842, "upvotes":  3120, "mentions_24h_ago": 2004},
            {"rank": 5, "ticker": "PLTR", "name": "Palantir",   "mentions": 1421, "upvotes":  4980, "mentions_24h_ago": 1364},
            {"rank": 6, "ticker": "AAPL", "name": "Apple",      "mentions": 1280, "upvotes":  2140, "mentions_24h_ago": 1257},
            {"rank": 7, "ticker": "BB",   "name": "BlackBerry", "mentions":  924, "upvotes":  3812, "mentions_24h_ago":  381},
            {"rank": 8, "ticker": "SOFI", "name": "SoFi",       "mentions":  842, "upvotes":  2104, "mentions_24h_ago":  776},
        ]
        is_mock = True
    else:
        is_mock = False

    # Cap to 8 trending rows for the dense table.
    items = items[:8]
    featured = items[0] if items else None

    return {"featured": featured, "trending": items, "is_mock": is_mock}


def _retail_kpi_strip(payload: dict) -> list[dict]:
    """6-cell KPI strip — mentions total, active tickers, sentiment proxies, top mover, hottest sector."""
    items = payload.get("trending") or []
    total_mentions = sum(int(i.get("mentions") or 0) for i in items)
    if total_mentions >= 1_000_000:
        mentions_str = f"{total_mentions / 1_000_000:.1f}M"
    elif total_mentions >= 1_000:
        mentions_str = f"{total_mentions / 1_000:.1f}K"
    else:
        mentions_str = f"{total_mentions:,}"
    # Top mover by % DoD
    by_dod = sorted(items, key=_retail_dod_delta, reverse=True)
    top = by_dod[0] if by_dod else None
    top_str = f"${top['ticker']} +{int(_retail_dod_delta(top) * 100)}%" if top else "—"
    return [
        {"label": "Mentions · 24h", "value": mentions_str,           "delta": None,              "up": None},
        {"label": "Active tickers", "value": f"{len(items):,}",      "delta": None,              "up": None},
        {"label": "Sentiment idx",  "value": "+44",                  "delta": "+6",              "up": True},
        {"label": "WSB index",      "value": "+71",                  "delta": None,              "up": None},
        {"label": "Top mover",      "value": top_str,                "delta": None,              "up": None},
        {"label": "Hottest sector", "value": "AI / Chips",           "delta": None,              "up": None},
    ]


def _retail_trending_rows(items: list[dict]) -> list[dict]:
    """Format trending rows for the dense table — mentions, DoD, sentiment, etc."""
    rows = []
    for i, it in enumerate(items, start=1):
        mentions = int(it.get("mentions") or 0)
        dod = _retail_dod_delta(it)
        sentiment = _retail_sentiment_proxy(it)
        # Sentiment bar: centered at 50%, fills outward.  abs(s) * 50% width.
        bar_width = abs(sentiment) * 50  # in % of bar
        # When negative the fill grows leftward from center via translateX(-100%).
        rows.append({
            "rank":     i,
            "ticker":   (it.get("ticker") or "").upper(),
            "name":     _retail_name(it),
            "mentions": mentions,
            "mentions_str": f"{mentions:,}",
            "dod":      dod,
            "dod_str":  f"{'+' if dod >= 0 else ''}{int(dod * 100)}%",
            "sentiment": sentiment,
            "sentiment_str": f"{'+' if sentiment >= 0 else ''}{int(sentiment * 100)}",
            "sentiment_bar_width": round(bar_width, 1),
            "sentiment_up":        sentiment >= 0,
            # Price / day data not joined yet — placeholder for now.
            "price":    None,
            "price_chg": None,
        })
    return rows


# ── Retail Trends tab — Google Trends multi-line ──────────────────────────

# Colour cycle for the Trends multi-line chart (matches the design's accent /
# up / ink / dim sequence so series stay distinguishable on dark + light).
_TRENDS_LINE_COLORS = ["var(--pp-accent)", "var(--pp-up)", "var(--pp-ink)", "var(--pp-dim)"]
_TRENDS_DEFAULT_TICKERS = ["NVDA", "GME", "TSLA", "AAPL"]


def _retail_trends_chart_compute() -> dict | None:
    """Synchronous: pull 90-day Google Trends interest for the default basket
    and produce SVG-ready multi-series chart data.

    Returns ``None`` on upstream failure so the L2 cache doesn't poison
    itself with an empty payload — the bounded route fallback handles the
    rendering path. ``{"have_data": False, ...}`` is reserved for a
    successful fetch that yielded no usable points.

    Wrapped by `_retail_trends_chart` (L2-cached) — pytrends has tight
    rate limits and the 4 keyword set takes ~6-15s on a cold path.
    """
    try:
        from filings import google_trends
    except Exception:
        return None

    try:
        bundle = google_trends.fetch_interest_over_time(
            _TRENDS_DEFAULT_TICKERS,
            timeframe="today 3-m",
            geo="US",
        )
    except Exception as exc:
        logger.warning("Retail trends compute failed: %s", exc)
        return None

    if not bundle or not bundle.get("data"):
        return None

    keywords = bundle.get("keywords") or []
    points = bundle.get("data") or []          # [{date, values: {kw: int}}, ...]
    if not points:
        return {"have_data": False, "series": []}

    # ViewBox 1500×260 (~5.8:1) — matches the typical full-width container
    # ratio so `preserveAspectRatio="none"` is near-identity.
    width, height = 1500.0, 260.0
    pad_top, pad_bot = 15.0, 25.0
    plot_h = height - pad_top - pad_bot
    n = len(points)

    # Build per-keyword score arrays in the order keywords were returned.
    by_kw: dict[str, list[int]] = {kw: [] for kw in keywords}
    for p in points:
        vals = p.get("values") or {}
        for kw in keywords:
            by_kw[kw].append(int(vals.get(kw) or 0))

    # Trends scores are 0-100 globally — fix the y-range so the lines stay
    # comparable across keywords.
    series: list[dict] = []
    # Per-keyword screen-y arrays (parallel to `points`) so the JS hover
    # layer can plot a dot on every line at the active x.
    series_screen_ys: list[list[float]] = []
    for i, kw in enumerate(keywords):
        ys = by_kw[kw]
        if not ys:
            continue
        avg = sum(ys) / max(len(ys), 1)
        last = ys[-1]
        first_nonzero = next((y for y in ys if y > 0), ys[0])
        delta = ((last - first_nonzero) / first_nonzero * 100) if first_nonzero else 0.0
        d_path = []
        screen_ys: list[float] = []
        for j, y in enumerate(ys):
            sx = (j / max(n - 1, 1)) * width
            sy = pad_top + (1 - y / 100.0) * plot_h
            d_path.append(("M" if j == 0 else "L") + f"{sx:.1f} {sy:.1f}")
            screen_ys.append(round(sy, 1))
        series.append({
            "name":  kw,
            "color": _TRENDS_LINE_COLORS[i % len(_TRENDS_LINE_COLORS)],
            "line":  " ".join(d_path),
            "avg":   round(avg, 1),
            "last":  last,
            "delta_pct_str": f"{delta:+.0f}%" if delta else "—",
            "delta_up": delta >= 0,
        })
        series_screen_ys.append(screen_ys)

    # Per-x hover history: every point gets the date + value for every
    # keyword.  JS uses nearest-x to find the active index and renders a
    # dot per series + a multi-line tooltip.
    chart_history: list[dict] = []
    for j, p in enumerate(points):
        sx = round((j / max(n - 1, 1)) * width, 1)
        date_str = p.get("date") or ""
        try:
            from datetime import datetime as _dt
            date_str = _dt.strptime(date_str, "%b %d, %Y").strftime("%b %d %Y")
        except Exception:
            pass
        kw_values = []
        for s_idx, s in enumerate(series):
            kw = s["name"]
            v = (p.get("values") or {}).get(kw) or 0
            kw_values.append({
                "name":  kw,
                "color": s["color"],
                "value": int(v),
                "y":     series_screen_ys[s_idx][j] if j < len(series_screen_ys[s_idx]) else 0,
            })
        chart_history.append({"x": sx, "date": date_str, "kws": kw_values})

    # First / mid / last x-axis date labels (MMM d).
    def _label(idx: int) -> str:
        d = points[idx].get("date") or ""
        try:
            from datetime import datetime as _dt
            return _dt.strptime(d, "%b %d, %Y").strftime("%b %d")
        except Exception:
            return (d.split(",")[0]).strip() or ""

    if n >= 3:
        ticks = [_label(0), _label(n // 2), _label(n - 1)]
    else:
        ticks = [_label(0)] if n else []

    return {
        "have_data": True,
        "series":    series,
        "ticks":     ticks,
        "n":         n,
        "as_of":     bundle.get("fetched_at", ""),
        "vb_width":  width,
        "vb_height": height,
        "chart_history": chart_history,
    }


async def _retail_trends_chart() -> dict:
    """L2-cached wrapper — Google Trends payload is stable for hours."""
    return await _l2_cached(
        key="redesign:retail:trends_chart:v1",
        ttl_seconds=2 * 3600,
        compute=_retail_trends_chart_compute,
        category="redesign_retail",
    ) or {"have_data": False, "series": []}


# ── Retail WSB tab — index, distribution, top-ticker table ────────────────

def _retail_wsb_panel(top_rows: list[dict] | None) -> dict:
    """Build the WSB hero index + top-tickers table from get_wsb_top() rows."""
    rows = top_rows or []
    if not rows:
        return {"have_data": False, "rows": [], "index": 0, "dist": {}, "total_posts": 0}

    # Sentiment categorical → numeric score for the index aggregate.
    score_for = {"Bullish": 1.0, "Neutral": 0.0, "Bearish": -1.0}
    weighted_sum = 0.0
    weighted_total = 0.0
    counts = {"Bullish": 0, "Neutral": 0, "Bearish": 0}
    for r in rows:
        m = int(r.get("mentions") or 0)
        s = score_for.get(r.get("sentiment", "Neutral"), 0.0)
        weighted_sum += s * m
        weighted_total += m
        counts[r.get("sentiment", "Neutral")] = counts.get(r.get("sentiment", "Neutral"), 0) + 1
    weighted_avg = (weighted_sum / weighted_total) if weighted_total else 0.0
    # 0-100 index — design shows "+71" style.
    index_val = int(round(weighted_avg * 100))

    # Distribution percentages (counts of tickers, not mentions — easier to read).
    n_total = sum(counts.values()) or 1
    dist = {
        "bullish_pct": round(counts["Bullish"] / n_total * 100),
        "neutral_pct": round(counts["Neutral"] / n_total * 100),
        "bearish_pct": round(counts["Bearish"] / n_total * 100),
    }

    table_rows: list[dict] = []
    # Sort by mentions desc.
    for i, r in enumerate(sorted(rows, key=lambda x: int(x.get("mentions") or 0), reverse=True)[:12], start=1):
        m = int(r.get("mentions") or 0)
        u = int(r.get("upvotes") or 0)
        # Upvotes-per-mention ratio — proxy for "calls/puts" sentiment depth.
        ratio = u / m if m else 0
        # Map ratio onto a -100..+100 score for the chip.
        score = max(-100, min(100, int(round((ratio - 10) * 8))))
        sentiment_label = r.get("sentiment", "Neutral")
        table_rows.append({
            "rank":       i,
            "ticker":     (r.get("ticker") or "").upper(),
            "name":       r.get("name") or "",
            "posts":      m,
            "posts_str":  f"{m:,}",
            "upvotes":    u,
            "upvotes_str": f"{u:,}",
            "ratio":      ratio,
            "ratio_str":  f"{ratio:.1f}x",
            "ratio_pct":  min(int(ratio / 25 * 100), 100),
            "sentiment_label": sentiment_label,
            "score":      score,
            "score_str":  (f"+{score}" if score >= 0 else f"{score}"),
            "score_up":   score >= 0,
        })

    return {
        "have_data":    True,
        "index":        index_val,
        "index_str":    (f"+{index_val}" if index_val >= 0 else f"{index_val}"),
        "index_up":     index_val >= 0,
        "label":        ("Strongly bullish" if index_val >= 50
                         else "Bullish" if index_val >= 20
                         else "Neutral" if index_val >= -20
                         else "Bearish" if index_val >= -50
                         else "Strongly bearish"),
        "dist":         dist,
        "total_posts":  sum(int(r.get("mentions") or 0) for r in rows),
        "rows":         table_rows,
    }


_RETAIL_FG_BAND_LABELS = {
    "extreme_fear": "Extreme Fear", "fear": "Fear",
    "neutral": "Neutral",
    "greed": "Greed", "extreme_greed": "Extreme Greed",
}


def _retail_kpi_strip_v2(apewisdom: list[dict], fear_greed: dict | None) -> list[dict]:
    """Top KPI strip — six headline retail metrics."""
    total_mentions = sum(int(r.get("mentions") or 0) for r in apewisdom)
    total_upvotes  = sum(int(r.get("upvotes") or 0) for r in apewisdom)
    fg_score = fear_greed.get("score") if fear_greed else None
    fg_label = (fear_greed.get("rating") or "").title() if fear_greed else "—"
    bullish_count = sum(
        1 for r in apewisdom
        if int(r.get("mentions") or 0) > 0
        and int(r.get("upvotes") or 0) / max(int(r.get("mentions") or 1), 1) > 5
    )
    return [
        {"label": "Tickers tracked",   "value": f"{len(apewisdom):,}",      "delta": None, "up": None},
        {"label": "Total mentions",    "value": f"{total_mentions:,}",       "delta": None, "up": None},
        {"label": "Total upvotes",     "value": f"{total_upvotes:,}",        "delta": None, "up": None},
        {"label": "Bullish (>5 upv/m)", "value": f"{bullish_count}",         "delta": None, "up": None},
        {"label": "Fear & Greed",      "value": (str(int(fg_score)) if isinstance(fg_score, (int, float)) else "—"),
         "delta": fg_label or None, "up": None},
        {"label": "Market mood",       "value": fg_label or "—",             "delta": None, "up": None},
    ]


def _retail_sentiment_payload(apewisdom: list[dict], fear_greed: dict | None) -> dict:
    """Sentiment tab payload — Market Mood gauge + 3 callout cards."""
    # CNN Fear & Greed gauge data + four reference points.
    fg = None
    if fear_greed:
        def _fg_int(v):
            """Coerce CNN's float scores → display-ready integers."""
            try:
                return int(round(float(v))) if v is not None else None
            except (TypeError, ValueError):
                return None

        score_int = _fg_int(fear_greed.get("score"))
        rating = (fear_greed.get("rating") or "").lower().replace(" ", "_")
        fg = {
            "score":       score_int,
            "score_str":   f"{score_int}" if score_int is not None else "—",
            "rating":      _RETAIL_FG_BAND_LABELS.get(rating, (fear_greed.get("rating") or "—").title()),
            "rating_key":  rating or "neutral",
            "marker_pct":  max(0.0, min(100.0, float(score_int))) if score_int is not None else 50.0,
            "previous_close":  _fg_int(fear_greed.get("previous_close")),
            "one_week_ago":    _fg_int(fear_greed.get("one_week_ago")),
            "one_month_ago":   _fg_int(fear_greed.get("one_month_ago")),
            "one_year_ago":    _fg_int(fear_greed.get("one_year_ago")),
        }

    # Most mentioned — top by raw mention count.
    most_mentioned = None
    if apewisdom:
        top = sorted(apewisdom, key=lambda r: int(r.get("mentions") or 0), reverse=True)[0]
        most_mentioned = {
            "ticker":       (top.get("ticker") or "").upper(),
            "name":         top.get("name") or "",
            "mentions":     int(top.get("mentions") or 0),
            "mentions_str": f"{int(top.get('mentions') or 0):,}",
            "upvotes":      int(top.get("upvotes") or 0),
            "upvotes_str":  f"{int(top.get('upvotes') or 0):,}",
        }

    # Biggest rank mover — largest absolute rank improvement (rank_24h_ago - rank).
    biggest_mover = None
    if apewisdom:
        candidates = []
        for r in apewisdom:
            rank = int(r.get("rank") or 0)
            r24  = r.get("rank_24h_ago")
            if rank and r24:
                try:
                    delta = int(r24) - rank   # positive = rose in rank
                    candidates.append((abs(delta), delta, r))
                except (TypeError, ValueError):
                    continue
        if candidates:
            candidates.sort(key=lambda c: c[0], reverse=True)
            _abs, delta, top = candidates[0]
            biggest_mover = {
                "ticker":   (top.get("ticker") or "").upper(),
                "name":     top.get("name") or "",
                "delta":    delta,
                "delta_str": f"+{delta}" if delta > 0 else str(delta),
                "delta_up": delta > 0,
                "rank":     int(top.get("rank") or 0),
                "rank_24h": int(top.get("rank_24h_ago") or 0),
            }

    # Top 5 trending — first 5 by rank.
    top_trending = []
    for r in sorted(apewisdom or [], key=lambda x: int(x.get("rank") or 999))[:5]:
        top_trending.append({
            "ticker": (r.get("ticker") or "").upper(),
            "name":   r.get("name") or "",
        })

    return {
        "fear_greed":     fg,
        "most_mentioned": most_mentioned,
        "biggest_mover":  biggest_mover,
        "top_trending":   top_trending,
    }


def _retail_velocity_color(velocity_pct: float) -> str:
    """Map % velocity → CSS variable name for v2 design tokens.  Mirrors
    the v1 ``_velocity_to_color`` semantics but yields token names rather
    than raw hex so dark/light themes both resolve correctly."""
    if   velocity_pct >= 100: return "var(--pp-up)"
    elif velocity_pct >= 30:  return "rgba(35, 162, 110, 0.65)"
    elif velocity_pct >= 0:   return "rgba(35, 162, 110, 0.3)"
    elif velocity_pct >= -30: return "rgba(220, 38, 38, 0.35)"
    else:                     return "var(--pp-down)"


def _squarify_treemap(items: list[dict], width: float = 100.0, height: float = 100.0) -> list[dict]:
    """Squarified treemap layout (Bruls/Huijsen/van Wijk 2000).

    Packs `items` into a rectangle of `width × height` while keeping each
    box's aspect ratio as close to 1:1 as possible.  Coordinates are
    emitted in the same unit as the input dimensions — pass 100 to get
    percentages ready for HTML `style="left: X%; top: Y%; …"`.

    Each item must expose a positive `value`.  Output preserves every
    field of the input dicts plus `x`, `y`, `w`, `h`.
    """
    if not items:
        return []
    sorted_items = sorted(items, key=lambda d: -max(d.get("value", 0), 1))
    total_v = sum(max(it.get("value", 0), 1) for it in sorted_items) or 1
    scale = (width * height) / total_v

    def _row_worst_aspect(row: list[dict], side: float) -> float:
        if not row or side <= 0:
            return float("inf")
        s = sum(max(it.get("value", 0), 1) for it in row) * scale
        if s <= 0:
            return float("inf")
        thick = s / side
        worst = 1.0
        for it in row:
            long_edge = max(max(it.get("value", 0), 1) * scale / max(thick, 1e-9), 1e-9)
            ratio = max(thick / long_edge, long_edge / thick)
            if ratio > worst:
                worst = ratio
        return worst

    boxes: list[dict] = []
    queue = list(sorted_items)
    x, y, w, h = 0.0, 0.0, width, height

    while queue:
        side = min(w, h)
        if side <= 0:
            break
        row: list[dict] = []
        # Greedy: keep adding items while the worst aspect ratio is improving.
        while queue:
            cand = row + [queue[0]]
            if not row or _row_worst_aspect(cand, side) <= _row_worst_aspect(row, side):
                row = cand
                queue.pop(0)
            else:
                break
        if not row:
            break

        row_v = sum(max(it.get("value", 0), 1) for it in row) or 1
        if w >= h:
            # Long axis is horizontal — lay row out as a vertical strip on the left.
            thick = (row_v * scale) / h
            cy = y
            for it in row:
                bh = max(it.get("value", 0), 1) * h / row_v
                boxes.append({**it, "x": x, "y": cy, "w": thick, "h": bh})
                cy += bh
            x += thick
            w -= thick
        else:
            # Long axis is vertical — lay row out as a horizontal strip on top.
            thick = (row_v * scale) / w
            cx = x
            for it in row:
                bw = max(it.get("value", 0), 1) * w / row_v
                boxes.append({**it, "x": cx, "y": y, "w": bw, "h": thick})
                cx += bw
            y += thick
            h -= thick

    return boxes


def _retail_leaderboard_payload(lb: dict) -> dict:
    """Leaderboard tab payload — pre-computes treemap geometry, scatter
    bubble coords, and an enriched leaderboard table.

    Treemap uses the squarified algorithm (boxes get near-1:1 aspect
    ratios).  Bubble chart geometry is computed in viewBox space so the
    SVG renders without a JS library — keeps the page lightweight and
    consistent with v2 charts.
    """
    rows  = lb.get("leaderboard_rows") or []
    treem = lb.get("treemap_data") or []
    bub   = lb.get("bubble_data") or []
    meta  = lb.get("metadata") or {}

    # ── Treemap geometry: squarified, % units (HTML-positioned boxes).
    boxes: list[dict] = []
    if treem:
        layout = _squarify_treemap(treem, width=100.0, height=100.0)
        for d in layout:
            boxes.append({
                "ticker":   d.get("name") or "",
                "value":    d.get("value", 0),
                # Round to 2 decimals — keeps the inline-style strings short.
                "x":        round(d["x"], 2),
                "y":        round(d["y"], 2),
                "w":        round(d["w"], 2),
                "h":        round(d["h"], 2),
                "color":    _retail_velocity_color(d.get("velocity_pct", 0)),
                "mentions": d.get("mentions", 0),
                "velocity_pct": d.get("velocity_pct", 0),
                "engagement_ratio": d.get("engagement_ratio", 0),
                "guru_count": d.get("guru_count", 0),
            })

    # ── Bubble chart geometry.
    # x = engagement (upv/m), y = velocity (%).  Auto-scale to data extents.
    bub_w, bub_h = 760.0, 460.0
    pad_l, pad_r, pad_t, pad_b = 64.0, 18.0, 18.0, 36.0
    plot_w = bub_w - pad_l - pad_r
    plot_h = bub_h - pad_t - pad_b
    bubbles: list[dict] = []
    if bub:
        xs = [b.get("x", 0) for b in bub]
        ys = [b.get("y", 0) for b in bub]
        rs = [max(b.get("r", 0), 1) for b in bub]
        x_min, x_max = (min(xs), max(xs))
        y_min, y_max = (min(ys), max(ys))
        r_max = max(rs) or 1
        # Pad ranges 10% so bubbles aren't cropped at the edges.
        x_pad = (x_max - x_min) * 0.1 or 1
        y_pad = (y_max - y_min) * 0.1 or 1
        x_lo, x_hi = x_min - x_pad, x_max + x_pad
        y_lo, y_hi = y_min - y_pad, y_max + y_pad
        x_rng = (x_hi - x_lo) or 1
        y_rng = (y_hi - y_lo) or 1
        for b in bub:
            sx = pad_l + ((b.get("x", 0) - x_lo) / x_rng) * plot_w
            sy = pad_t + (1 - (b.get("y", 0) - y_lo) / y_rng) * plot_h
            radius = 6 + (max(b.get("r", 0), 1) / r_max) * 22
            bubbles.append({
                "ticker": b.get("ticker") or "",
                "name":   b.get("name") or "",
                "cx":     round(sx, 1),
                "cy":     round(sy, 1),
                "r":      round(radius, 1),
                "x":      b.get("x", 0),
                "y":      b.get("y", 0),
                "size":   b.get("r", 0),
                "guru_count": b.get("guru_count", 0),
                "rank":   b.get("rank", 0),
                "is_guru": (b.get("guru_count") or 0) > 0,
            })

    # ── Velocity table — keep top 50 to render server-side; full 500 not needed.
    table = []
    for r in rows[:50]:
        rc = r.get("rank_change", 0)
        table.append({
            **r,
            "rank_change":     rc,
            "rank_change_str": (f"+{rc}" if rc > 0 else (f"{rc}" if rc < 0 else "—")),
            "rank_change_up":  rc > 0,
            "velocity_str":    f"{r.get('velocity_pct', 0):+.1f}%",
            "velocity_up":     r.get("velocity_pct", 0) >= 0,
            "mentions_str":    f"{r.get('mentions', 0):,}",
            "engagement_str":  f"{r.get('engagement_ratio', 0):.1f}",
        })

    return {
        "treemap_boxes": boxes,
        "bubbles":       bubbles,
        "bubble_vb_w":   bub_w,
        "bubble_vb_h":   bub_h,
        "bubble_pad":    {"left": pad_l, "right": pad_r, "top": pad_t, "bot": pad_b},
        "table":         table,
        "total_count":   meta.get("count", 0),
        "timestamp":     meta.get("timestamp", ""),
        "market_mood":   meta.get("market_mood"),
        "market_score":  meta.get("market_score"),
    }


def _retail_calendar_payload(uploads: list[dict], channels: list[dict]) -> dict:
    """Calendar tab payload — recent YouTube uploads grid + channel directory."""
    from datetime import datetime, timezone

    def _fmt_relative(ts: str | None) -> str:
        if not ts:
            return ""
        try:
            dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            delta = datetime.now(timezone.utc) - dt
            secs = max(int(delta.total_seconds()), 0)
            if secs < 3600:        return f"{secs // 60}m ago"
            if secs < 86400:       return f"{secs // 3600}h ago"
            return f"{secs // 86400}d ago"
        except Exception:
            return ""

    upload_rows = []
    for u in uploads or []:
        tickers = u.get("tickers") or []
        # Tickers can land as list, comma-joined string, or null — normalise.
        if isinstance(tickers, str):
            tickers = [t.strip() for t in tickers.split(",") if t.strip()]
        upload_rows.append({
            "video_id":      u.get("video_id"),
            "title":         u.get("title") or "",
            "channel_name":  u.get("channel_name") or "",
            "channel_id":    u.get("channel_id"),
            "thumbnail_url": u.get("thumbnail_url") or "",
            "video_url":     u.get("video_url") or (
                f"https://www.youtube.com/watch?v={u.get('video_id')}" if u.get("video_id") else ""
            ),
            "ago_str":       _fmt_relative(u.get("scheduled_at")),
            "tickers":       tickers[:1],  # one chip per card for the v1 look
        })

    channel_rows = []
    for c in channels or []:
        subs = c.get("subscriber_count") or 0
        posts = c.get("avg_posts_per_week")
        channel_rows.append({
            "channel_id":    c.get("channel_id"),
            "channel_name":  c.get("channel_name") or "",
            "thumbnail_url": c.get("thumbnail_url") or "",
            "handle":        c.get("handle") or "",
            "subscribers":   int(subs),
            "subs_str":      f"{int(subs):,}" if subs else "—",
            "posts_str":     (f"{float(posts):.1f}" if posts is not None else "—"),
        })

    return {
        "uploads":      upload_rows[:12],   # cap visible grid at 12
        "uploads_total": len(upload_rows),
        "channels":     channel_rows,
        "channels_total": len(channel_rows),
    }


_RETAIL_VIEWS = ("sentiment", "leaderboard", "calendar")


def _retail_ticker_map_compute() -> dict:
    """Build the {ticker → [guru names]} map for the retail leaderboard.
    Sync compute fn — `_l2_cached` runs it in a worker thread and shares
    the result across uvicorn workers via Supabase, so we never amplify
    the heavy `load_cache_from_supabase` call across multiple workers.
    """
    from filings import cache as _cache, client as _client
    from filings.superinvestors import SUPERINVESTORS_BY_CIK
    fund_cache = _cache.load_cache_from_supabase() or {}
    return _client.build_ticker_ownership_map(fund_cache, SUPERINVESTORS_BY_CIK) or {}


@router.get("/retail", response_class=HTMLResponse)
async def preview_retail(request: Request, view: str = "sentiment"):
    """Retail page — three tabs (Sentiment / Leaderboard / Calendar), wired
    through the existing v1 helpers in :mod:`filings.sentiment` and
    :mod:`filings.youtube_cache`.

    Sentiment   — CNN Fear & Greed gauge + 3 callout cards (most-mentioned,
                  biggest rank mover, top-5 trending).
    Leaderboard — Reddit velocity heatmap + hype-vs-quality scatter + table.
    Calendar    — Recent YouTube uploads (48h) + finance-channel directory.
    """
    if view not in _RETAIL_VIEWS:
        view = "sentiment"

    bounded = functools.partial(_bounded, page="Retail page")
    from filings import sentiment as _sent
    from filings import youtube_cache as _yt

    # 5-way fan-out — every fetch is L2-cached.  `ticker_map` is the
    # Supabase-backed {ticker → guru names} overlay; first cold hit pays
    # ~5s, subsequent hits across all uvicorn workers are instant.
    apewisdom_data, fear_greed, yt_uploads, yt_channels, ticker_map = await asyncio.gather(
        bounded(to_heavy(_sent._get_apewisdom_all),     timeout=6.0, fallback=[],       name="apewisdom"),
        bounded(to_heavy(_sent._get_cnn_fear_greed),    timeout=4.0, fallback=None,     name="fear_greed"),
        bounded(to_heavy(_yt.get_recent_youtube_uploads, 50),
                                                        timeout=4.0, fallback=[],       name="yt_uploads"),
        bounded(to_heavy(_yt.get_youtube_channels),     timeout=4.0, fallback=[],       name="yt_channels"),
        bounded(
            _l2_cached(
                "redesign:retail:ticker_map_v1",
                ttl_seconds=3600,
                compute=_retail_ticker_map_compute,
                category="redesign_retail",
            ),
            timeout=10.0, fallback={}, name="ticker_map",
        ),
    )

    # `_sent.build_retail_leaderboard_data` has its own 30-min L1 cache that
    # ignores its arguments.  If we just got a real ticker_map but the L1
    # cache was filled earlier with an empty one, drop it so the next call
    # rebuilds with gurus.  (Encapsulation-violating cache poke; the proper
    # fix is teaching `build_retail_leaderboard_data` to key on its inputs.)
    if ticker_map:
        old = getattr(_sent, "_leaderboard_cache", None)
        if old:
            try:
                _ts, prev = old
                if not any((r.get("guru_count") or 0) > 0 for r in (prev.get("leaderboard_rows") or [])):
                    _sent._leaderboard_cache = None
            except Exception as exc:
                logger.warning("Retail page: leaderboard cache bust failed: %s", exc)

    leaderboard = await to_light(
        _sent.build_retail_leaderboard_data,
        apewisdom_data or [], ticker_map, fear_greed,
    )

    sentiment_ctx  = _retail_sentiment_payload(apewisdom_data or [], fear_greed)
    leaderboard_ctx = _retail_leaderboard_payload(leaderboard)
    calendar_ctx    = _retail_calendar_payload(yt_uploads or [], yt_channels or [])

    ctx = {
        "request":         request,
        **(await _shell_context(request, "Retail")),
        "retail_view":     view,
        "retail_kpi":      _retail_kpi_strip_v2(apewisdom_data or [], fear_greed),
        # Per-tab payloads
        "sentiment":       sentiment_ctx,
        "leaderboard":     leaderboard_ctx,
        "calendar":        calendar_ctx,
    }
    return templates.TemplateResponse("_redesign/retail.html", ctx)
