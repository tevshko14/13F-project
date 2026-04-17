"""Unusual Options Activity screener — data model, detection, and 3-tier cache.

Detects options contracts where daily volume ≥ 5× open interest,
classifies by sentiment (bullish calls / bearish puts), and surfaces
the results via a sortable feed and sector heatmap.

Data flow:
    L1 — in-memory dict (5-min TTL, sub-ms reads)
    L2 — Supabase ``unusual_options_activity`` table (7-day retention)
    L3 — live yfinance scan (expensive, only when DB is empty/unavailable)
"""

from __future__ import annotations

import logging
import math
import os
import threading
import time
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timezone

from filings import supabase_cache
from filings.caching import TTLCache

logger = logging.getLogger(__name__)

# ── Configuration ────────────────────────────────────────────────────────────

_VOL_OI_THRESHOLD = 5       # minimum volume / open_interest ratio
_MIN_OI = 10                # ignore contracts with tiny open interest
_MIN_VOLUME = 50            # ignore contracts with negligible volume
_MIN_PREMIUM = int(os.environ.get("OPTIONS_MIN_PREMIUM", "100000"))  # $100K floor
_FEED_TTL = 300             # 5 min L1 cache for the feed
_HEATMAP_TTL = 300          # 5 min L1 cache for heatmap data
_CLUSTER_TTL = 300          # 5 min L1 cache for cluster data

# ── In-memory L1 cache ──────────────────────────────────────────────────────

_lock = threading.Lock()                              # guards single-entry caches below
_feed_cache = TTLCache(ttl=_FEED_TTL, max_size=100)
_heatmap_cache: tuple[float, list[dict]] | None = None
_cluster_cache: tuple[float, list[dict]] | None = None


# ── Data model ──────────────────────────────────────────────────────────────


@dataclass
class UnusualOption:
    """Single unusual options contract flagged by the screener."""

    contract_symbol: str
    ticker: str
    company_name: str
    sector: str
    option_type: str            # "call" or "put"
    strike: float
    expiry: str                 # ISO date
    dte: int
    volume: int
    open_interest: int
    vol_oi_ratio: float
    bid: float | None
    ask: float | None
    last_price: float | None
    implied_vol: float | None
    underlying_price: float | None
    premium_est: float
    sentiment: str              # "bullish" or "bearish"
    scan_date: str              # ISO date (YYYY-MM-DD)
    fetched_at: str             # ISO datetime

    # ── Phase 1B: OI delta tracking ──
    oi_prev: int | None = None
    oi_delta: int | None = None
    oi_delta_pct: float | None = None
    is_new_positioning: bool = False

    # ── Phase 1C: Near-expiry urgency ──
    urgency_score: float = 1.0

    # ── Phase 1D: Moneyness / OTM bias ──
    moneyness: float | None = None
    moneyness_label: str = ""
    otm_score: float = 1.0

    # ── Phase 3: Greeks (populated by Tradier, None from yfinance) ──
    delta: float | None = None
    gamma: float | None = None
    theta: float | None = None
    vega: float | None = None

    def to_db_row(self) -> dict:
        """Convert to a dict suitable for Supabase upsert."""
        return asdict(self)


# ── Detection logic ─────────────────────────────────────────────────────────


def detect_unusual(
    ticker: str,
    company_name: str,
    sector: str,
    chain_calls,
    chain_puts,
    underlying_price: float | None,
    fetched_at: str,
) -> list[UnusualOption]:
    """Scan a single ticker's options chain for unusual activity.

    Args:
        ticker:           Uppercase ticker symbol.
        company_name:     Company display name.
        sector:           GICS sector string.
        chain_calls:      pandas DataFrame from ``yf.Ticker.option_chain().calls``.
        chain_puts:       pandas DataFrame from ``yf.Ticker.option_chain().puts``.
        underlying_price: Current stock price (may be None).
        fetched_at:       ISO timestamp of the fetch.

    Returns:
        List of UnusualOption objects where vol ≥ 5× OI.
    """
    results: list[UnusualOption] = []
    today = date.today()

    for option_type, df in [("call", chain_calls), ("put", chain_puts)]:
        if df is None or df.empty:
            continue
        for _, row in df.iterrows():
            vol = _safe_int(row.get("volume"))
            oi = _safe_int(row.get("openInterest"))

            if oi < _MIN_OI or vol < _MIN_VOLUME:
                continue
            if vol < _VOL_OI_THRESHOLD * oi:
                continue

            ratio = round(vol / oi, 2) if oi > 0 else 0.0

            bid = _safe_float(row.get("bid"))
            ask = _safe_float(row.get("ask"))
            last = _safe_float(row.get("lastPrice"))
            iv = _safe_float(row.get("impliedVolatility"))
            strike = float(row.get("strike", 0))

            # Estimate premium: volume × midpoint × 100 shares/contract
            mid = (bid + ask) / 2 if bid is not None and ask is not None else (last or 0)
            premium = round(vol * mid * 100, 2)

            # Phase 1A: Premium floor — skip low-dollar noise
            if premium < _MIN_PREMIUM:
                continue

            # Sentiment: call → bullish, put → bearish
            sentiment = "bullish" if option_type == "call" else "bearish"

            # Expiry and DTE
            expiry_str = str(row.get("expiry", "")) if "expiry" in row.index else ""
            # yfinance option_chain doesn't include expiry per row — it's the
            # date passed to option_chain().  We'll set it in the sync worker.
            contract = str(row.get("contractSymbol", ""))

            # Parse DTE from the contract symbol's embedded date if expiry not set
            dte = 0
            if expiry_str:
                try:
                    exp_date = date.fromisoformat(expiry_str)
                    dte = (exp_date - today).days
                except (ValueError, TypeError):
                    pass
            dte = max(dte, 0)

            # Phase 1C: Near-expiry urgency weighting
            if dte == 0:
                urgency = 2.0
            elif dte <= 7:
                urgency = 1.0 + (1.0 - dte / 7.0)
            else:
                urgency = 1.0

            # Phase 1D: Moneyness / OTM bias scoring
            moneyness_val: float | None = None
            moneyness_label = ""
            otm_score = 1.0
            if underlying_price and underlying_price > 0 and strike > 0:
                if option_type == "call":
                    moneyness_val = round(strike / underlying_price, 4)
                else:
                    moneyness_val = round(underlying_price / strike, 4)

                if moneyness_val < 0.95:
                    moneyness_label = "Deep ITM"
                    otm_score = 0.8
                elif moneyness_val < 1.0:
                    moneyness_label = "ITM"
                    otm_score = 1.0
                elif moneyness_val < 1.02:
                    moneyness_label = "ATM"
                    otm_score = 1.0
                elif moneyness_val < 1.10:
                    moneyness_label = "OTM"
                    otm_score = 1.25
                else:
                    moneyness_label = "Deep OTM"
                    otm_score = 1.5

            # Phase 3: Extract greeks if present in DataFrame (Tradier provides them)
            opt_delta = _safe_float(row.get("delta"))
            opt_gamma = _safe_float(row.get("gamma"))
            opt_theta = _safe_float(row.get("theta"))
            opt_vega = _safe_float(row.get("vega"))

            results.append(
                UnusualOption(
                    contract_symbol=contract,
                    ticker=ticker,
                    company_name=company_name,
                    sector=sector,
                    option_type=option_type,
                    strike=strike,
                    expiry=expiry_str,
                    dte=dte,
                    volume=vol,
                    open_interest=oi,
                    vol_oi_ratio=ratio,
                    bid=bid,
                    ask=ask,
                    last_price=last,
                    implied_vol=round(iv, 6) if iv is not None else None,
                    underlying_price=round(underlying_price, 4) if underlying_price else None,
                    premium_est=premium,
                    sentiment=sentiment,
                    scan_date=today.isoformat(),
                    fetched_at=fetched_at,
                    urgency_score=round(urgency, 2),
                    moneyness=moneyness_val,
                    moneyness_label=moneyness_label,
                    otm_score=round(otm_score, 2),
                    delta=opt_delta,
                    gamma=opt_gamma,
                    theta=opt_theta,
                    vega=opt_vega,
                )
            )

    return results


# ── 3-tier cache: public API ────────────────────────────────────────────────


def get_unusual_options_feed(
    sentiment: str = "",
    sort_by: str = "premium",
    ticker: str = "",
    limit: int = 100,
) -> list[dict]:
    """Fetch unusual options activity — L1 → L2 pipeline.

    Args:
        sentiment:  ``"bullish"``, ``"bearish"``, or ``""`` (all).
        sort_by:    ``"premium"`` | ``"ratio"`` | ``"expiry"`` | ``"ticker"``.
        ticker:     Filter to a specific ticker.
        limit:      Max rows.

    Returns list of row dicts.
    """
    cache_key = f"{sentiment}:{sort_by}:{ticker}:{limit}"

    # L1: in-memory
    cached = _feed_cache.get(cache_key)
    if cached is not None:
        return cached

    # L2: Supabase
    data = supabase_cache.get_unusual_options_feed(
        sentiment=sentiment,
        sort_by=sort_by,
        ticker=ticker,
        limit=limit,
    )

    # Enrich with display formatting
    for row in data:
        row["premium_fmt"] = _fmt_premium(row.get("premium_est"))
        row["vol_oi_fmt"] = f"{row.get('vol_oi_ratio', 0):.1f}x"
        row["iv_fmt"] = _fmt_iv(row.get("implied_vol"))
        row["strike_fmt"] = _fmt_strike(row.get("strike"))
        # OI delta formatting
        oi_d = row.get("oi_delta")
        if oi_d is not None:
            sign = "+" if int(oi_d) > 0 else ""
            row["oi_delta_fmt"] = f"{sign}{int(oi_d):,}"
        else:
            row["oi_delta_fmt"] = "—"
        # Moneyness label (already stored, just ensure presence)
        row.setdefault("moneyness_label", "")
        row.setdefault("urgency_score", 1.0)
        # Delta (greek) formatting
        d = row.get("delta")
        row["delta_fmt"] = f"{float(d):.2f}" if d is not None else "—"

    _feed_cache.set(cache_key, data)

    return data


def get_options_heatmap_data() -> list[dict]:
    """Build ECharts treemap data grouped by sector.

    Returns list of sector nodes::

        [
            {
                "name": "Information Technology",
                "children": [
                    {"name": "AAPL", "value": 1, "premium": ..., ...},
                    ...
                ]
            },
            ...
        ]

    Each sector node's color represents net sentiment:
      - More bullish premium → green
      - More bearish premium → red
    """
    global _heatmap_cache

    # L1
    with _lock:
        if _heatmap_cache is not None:
            ts, data = _heatmap_cache
            if time.time() - ts < _HEATMAP_TTL:
                return data

    # L2: fetch sector summary from Supabase
    summary = supabase_cache.get_options_sector_summary()
    if not summary:
        return []

    # Also get the raw feed for per-ticker breakdown in the heatmap
    feed = supabase_cache.get_unusual_options_feed(limit=500)

    # Build treemap: group by sector
    sectors: dict[str, dict] = {}
    for row in feed:
        sector = row.get("sector") or "Other"
        ticker = row.get("ticker", "")
        premium = float(row.get("premium_est") or 0)
        sentiment = row.get("sentiment", "neutral")

        if sector not in sectors:
            sectors[sector] = {"tickers": {}, "bullish_premium": 0, "bearish_premium": 0}

        s = sectors[sector]
        if sentiment == "bullish":
            s["bullish_premium"] += premium
        else:
            s["bearish_premium"] += premium

        if ticker not in s["tickers"]:
            s["tickers"][ticker] = {
                "bullish_premium": 0,
                "bearish_premium": 0,
                "count": 0,
            }
        t = s["tickers"][ticker]
        if sentiment == "bullish":
            t["bullish_premium"] += premium
        else:
            t["bearish_premium"] += premium
        t["count"] += 1

    # Convert to ECharts treemap format
    result = []
    for sector_name in sorted(sectors, key=lambda s: -(sectors[s]["bullish_premium"] + sectors[s]["bearish_premium"])):
        s = sectors[sector_name]
        children = []
        for tkr, data in sorted(s["tickers"].items(), key=lambda x: -(x[1]["bullish_premium"] + x[1]["bearish_premium"])):
            total = data["bullish_premium"] + data["bearish_premium"]
            net = data["bullish_premium"] - data["bearish_premium"]
            color = _sentiment_to_color(net, total)
            children.append({
                "name": tkr,
                "value": max(total, 1),
                "bullish_premium": round(data["bullish_premium"], 2),
                "bearish_premium": round(data["bearish_premium"], 2),
                "contract_count": data["count"],
                "link": f"/stock/{tkr}",
                "itemStyle": {
                    "color": color,
                    "borderColor": "rgba(0,0,0,0.15)",
                    "borderWidth": 1,
                },
            })

        if children:
            result.append({"name": sector_name, "children": children})

    with _lock:
        _heatmap_cache = (time.time(), result)

    return result


def get_ticker_options_activity(ticker: str, limit: int = 20) -> list[dict]:
    """Fetch unusual options for a specific ticker (for stock detail page)."""
    return get_unusual_options_feed(ticker=ticker, sort_by="premium", limit=limit)


def get_options_clusters(limit: int = 25) -> list[dict]:
    """Detect clustered unusual activity — multiple contracts on the same ticker.

    Groups today's unusual contracts by ticker, aggregates premium, determines
    cluster direction, and flags clusters with 3+ contracts as "strong".

    Returns a list of cluster dicts sorted by total premium descending.
    """
    global _cluster_cache

    # L1 cache
    with _lock:
        if _cluster_cache is not None:
            ts, data = _cluster_cache
            if time.time() - ts < _CLUSTER_TTL:
                return data[:limit]

    feed = supabase_cache.get_unusual_options_feed(limit=500)
    clusters = detect_clusters(feed)

    with _lock:
        _cluster_cache = (time.time(), clusters)

    return clusters[:limit]


def detect_clusters(feed: list[dict]) -> list[dict]:
    """Group unusual contracts by ticker into clusters.

    Args:
        feed: List of row dicts from the unusual options feed.

    Returns:
        List of cluster dicts sorted by total_premium descending::

            {
                "ticker": "AAPL",
                "company_name": "Apple Inc.",
                "sector": "Information Technology",
                "contract_count": 4,
                "bullish_count": 3,
                "bearish_count": 1,
                "total_premium": 5_200_000.0,
                "bullish_premium": 4_800_000.0,
                "bearish_premium": 400_000.0,
                "direction": "bullish",
                "strength": "strong",      # "strong" if 3+ contracts
                "max_vol_oi": 42.5,
                "avg_urgency": 1.7,
                "contracts": [...]         # individual contract rows
            }
    """
    if not feed:
        return []

    by_ticker: dict[str, list[dict]] = defaultdict(list)
    for row in feed:
        tk = row.get("ticker", "")
        if tk:
            by_ticker[tk].append(row)

    clusters = []
    for ticker, rows in by_ticker.items():
        if len(rows) < 2:
            continue  # need at least 2 contracts to form a cluster

        bullish = [r for r in rows if r.get("sentiment") == "bullish"]
        bearish = [r for r in rows if r.get("sentiment") == "bearish"]
        bull_prem = sum(float(r.get("premium_est") or 0) for r in bullish)
        bear_prem = sum(float(r.get("premium_est") or 0) for r in bearish)
        total_prem = bull_prem + bear_prem
        max_ratio = max((float(r.get("vol_oi_ratio") or 0) for r in rows), default=0)
        avg_urgency = sum(float(r.get("urgency_score") or 1.0) for r in rows) / len(rows)

        if bull_prem > bear_prem * 1.2:
            direction = "bullish"
        elif bear_prem > bull_prem * 1.2:
            direction = "bearish"
        else:
            direction = "mixed"

        clusters.append({
            "ticker": ticker,
            "company_name": rows[0].get("company_name", ""),
            "sector": rows[0].get("sector", ""),
            "contract_count": len(rows),
            "bullish_count": len(bullish),
            "bearish_count": len(bearish),
            "total_premium": round(total_prem, 2),
            "total_premium_fmt": _fmt_premium(total_prem),
            "bullish_premium": round(bull_prem, 2),
            "bearish_premium": round(bear_prem, 2),
            "direction": direction,
            "strength": "strong" if len(rows) >= 3 else "moderate",
            "max_vol_oi": round(max_ratio, 2),
            "avg_urgency": round(avg_urgency, 2),
            "contracts": rows,
        })

    clusters.sort(key=lambda c: -c["total_premium"])
    return clusters


def invalidate_cache() -> None:
    """Clear all L1 caches — call after a sync cycle completes."""
    global _heatmap_cache, _cluster_cache
    _feed_cache.clear()
    with _lock:
        _heatmap_cache = None
        _cluster_cache = None


# ── Formatting helpers ──────────────────────────────────────────────────────


def _safe_int(val) -> int:
    """Convert to int, returning 0 for NaN/None/invalid."""
    if val is None:
        return 0
    try:
        f = float(val)
        return 0 if math.isnan(f) else int(f)
    except (ValueError, TypeError):
        return 0


def _safe_float(val) -> float | None:
    """Convert to float, returning None for NaN/None/invalid."""
    if val is None:
        return None
    try:
        f = float(val)
        return None if math.isnan(f) else f
    except (ValueError, TypeError):
        return None


def _fmt_premium(val) -> str:
    """Format a premium value like ``$1.2M`` or ``$450K``."""
    if val is None:
        return "—"
    v = float(val)
    if v >= 1_000_000:
        return f"${v / 1_000_000:.1f}M"
    if v >= 1_000:
        return f"${v / 1_000:.0f}K"
    return f"${v:,.0f}"


def _fmt_iv(val) -> str:
    """Format implied volatility as percentage."""
    if val is None:
        return "—"
    return f"{float(val) * 100:.1f}%"


def _fmt_strike(val) -> str:
    """Format strike price."""
    if val is None:
        return "—"
    v = float(val)
    if v == int(v):
        return f"${int(v)}"
    return f"${v:.2f}"


def _sentiment_to_color(net_premium: float, total_premium: float) -> str:
    """Map net sentiment to a green/red color for the heatmap.

    Fully bullish → ``#2e7d32`` (green)
    Fully bearish → ``#c62828`` (red)
    Mixed         → blended
    """
    if total_premium <= 0:
        return "#64748b"  # slate (no data)

    ratio = net_premium / total_premium  # -1.0 to +1.0
    ratio = max(-1.0, min(1.0, ratio))

    if ratio >= 0:
        # Green interpolation: slate → green
        r = int(100 - ratio * 54)       # 100 → 46
        g = int(116 + ratio * 9)        # 116 → 125
        b = int(139 - ratio * 89)       # 139 → 50
    else:
        # Red interpolation: slate → red
        intensity = -ratio
        r = int(100 + intensity * 98)   # 100 → 198
        g = int(116 - intensity * 76)   # 116 → 40
        b = int(139 - intensity * 99)   # 139 → 40

    return f"#{r:02x}{g:02x}{b:02x}"
