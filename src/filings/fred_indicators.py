"""FRED Macro Indicators — key economic data with sparklines.

Fetches curated FRED series for a macro dashboard snapshot:
  - Interest rates (Fed Funds, 10Y, 2Y, yield spread)
  - Inflation (CPI YoY, Core PCE YoY, breakeven inflation)
  - Employment (Unemployment, NFP change, Initial Claims)
  - Consumer (Retail Sales MoM, Michigan Sentiment)
  - Credit (HY Spread, IG Spread)

Each indicator returns: current value, prior value, change,
direction, and 12-month sparkline data.

Caching: L1 in-memory (1 hour TTL).
"""

from __future__ import annotations

import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

import httpx

logger = logging.getLogger(__name__)

_FRED_BASE = "https://api.stlouisfed.org/fred"
_TIMEOUT = 15


def _fred_key() -> str:
    return os.environ.get("FRED_API_KEY", "").strip()


# ── Indicator definitions ────────────────────────────────────────
# Each entry: series_id → metadata for display + API params
# `units`: FRED transform — "lin" (raw), "pch" (MoM%), "pc1" (YoY%)
# `display_unit`: what to show in the UI
# `decimals`: rounding precision
# `invert_direction`: if True, "up" is bad (e.g. unemployment)

INDICATORS: dict[str, dict] = {
    # ── Interest Rates ────────────────────────────────────────────
    "DFF": {
        "name": "Fed Funds Rate",
        "group": "rates",
        "units": "lin",
        "display_unit": "%",
        "decimals": 2,
        "frequency": "d",
        "icon": "🏦",
    },
    "DGS10": {
        "name": "10-Year Treasury",
        "group": "rates",
        "units": "lin",
        "display_unit": "%",
        "decimals": 2,
        "frequency": "d",
        "icon": "📊",
    },
    "DGS2": {
        "name": "2-Year Treasury",
        "group": "rates",
        "units": "lin",
        "display_unit": "%",
        "decimals": 2,
        "frequency": "d",
        "icon": "📈",
    },
    "T10Y2Y": {
        "name": "Yield Curve (10Y-2Y)",
        "group": "rates",
        "units": "lin",
        "display_unit": "%",
        "decimals": 2,
        "frequency": "d",
        "icon": "📉",
        "note": "Negative = inverted (recession signal)",
    },
    # ── Inflation ─────────────────────────────────────────────────
    "CPIAUCSL": {
        "name": "CPI (YoY)",
        "group": "inflation",
        "units": "pc1",
        "display_unit": "%",
        "decimals": 1,
        "frequency": "m",
        "icon": "🔥",
        "invert_direction": True,
    },
    "PCEPILFE": {
        "name": "Core PCE (YoY)",
        "group": "inflation",
        "units": "pc1",
        "display_unit": "%",
        "decimals": 1,
        "frequency": "m",
        "icon": "🎯",
        "note": "Fed's preferred inflation measure",
        "invert_direction": True,
    },
    "T10YIE": {
        "name": "10Y Breakeven Inflation",
        "group": "inflation",
        "units": "lin",
        "display_unit": "%",
        "decimals": 2,
        "frequency": "d",
        "icon": "💨",
        "note": "Market-implied inflation expectations",
    },
    # ── Employment ────────────────────────────────────────────────
    "UNRATE": {
        "name": "Unemployment Rate",
        "group": "employment",
        "units": "lin",
        "display_unit": "%",
        "decimals": 1,
        "frequency": "m",
        "icon": "👷",
        "invert_direction": True,
    },
    "PAYEMS": {
        "name": "Nonfarm Payrolls (MoM Δ)",
        "group": "employment",
        "units": "chg",
        "display_unit": "K",
        "decimals": 0,
        "frequency": "m",
        "icon": "💼",
    },
    "ICSA": {
        "name": "Initial Jobless Claims",
        "group": "employment",
        "units": "lin",
        "display_unit": "K",
        "decimals": 0,
        "frequency": "w",
        "icon": "📋",
        "invert_direction": True,
    },
    # ── Consumer ──────────────────────────────────────────────────
    "RSXFS": {
        "name": "Retail Sales (MoM)",
        "group": "consumer",
        "units": "pch",
        "display_unit": "%",
        "decimals": 1,
        "frequency": "m",
        "icon": "🛒",
    },
    "UMCSENT": {
        "name": "Consumer Sentiment",
        "group": "consumer",
        "units": "lin",
        "display_unit": "",
        "decimals": 1,
        "frequency": "m",
        "icon": "😊",
    },
    # ── Credit / Risk ─────────────────────────────────────────────
    "BAMLH0A0HYM2": {
        "name": "High Yield Spread",
        "group": "credit",
        "units": "lin",
        "display_unit": "%",
        "decimals": 2,
        "frequency": "d",
        "icon": "⚡",
        "invert_direction": True,
        "note": "Widens in risk-off environments",
    },
}

# Group labels for display
GROUP_LABELS = {
    "rates": "Interest Rates",
    "inflation": "Inflation",
    "employment": "Employment",
    "consumer": "Consumer",
    "credit": "Credit & Risk",
}

# ── Cache ────────────────────────────────────────────────────────
_lock = threading.Lock()
_cache: dict | None = None
_cache_ts: float = 0
_CACHE_TTL = 3600  # 1 hour


# ── FRED API helper ─────────────────────────────────────────────

def _fetch_series(
    series_id: str,
    meta: dict,
    obs_count: int = 24,
) -> dict | None:
    """Fetch one FRED series. Returns indicator dict or None."""
    key = _fred_key()
    if not key:
        return None

    try:
        r = httpx.get(
            f"{_FRED_BASE}/series/observations",
            params={
                "series_id": series_id,
                "api_key": key,
                "file_type": "json",
                "units": meta["units"],
                "sort_order": "desc",
                "limit": obs_count,
                "output_type": 1,
            },
            timeout=_TIMEOUT,
        )
        r.raise_for_status()
        data = r.json()
    except Exception:
        logger.warning("FRED fetch failed: %s", series_id, exc_info=True)
        return None

    observations = data.get("observations", [])
    # Parse values, skipping missing (".")
    points: list[dict] = []
    for obs in observations:
        v = obs.get("value", ".")
        if v == ".":
            continue
        try:
            points.append({
                "date": obs["date"],
                "value": round(float(v), meta["decimals"]),
            })
        except (ValueError, KeyError):
            continue

    if not points:
        return None

    current = points[0]
    prior = points[1] if len(points) > 1 else None

    # Change calculation
    change = None
    change_pct = None
    direction = "neutral"
    if prior:
        change = round(current["value"] - prior["value"], meta["decimals"])
        if prior["value"] != 0:
            change_pct = round(
                (current["value"] - prior["value"]) / abs(prior["value"]) * 100,
                2,
            )
        if change > 0:
            direction = "down" if meta.get("invert_direction") else "up"
        elif change < 0:
            direction = "up" if meta.get("invert_direction") else "down"

    # Sparkline data (chronological order, oldest→newest)
    sparkline = [p["value"] for p in reversed(points)]

    # Format current value for display
    val = current["value"]
    unit = meta["display_unit"]
    if unit == "K":
        formatted = f"{val:,.0f}K"
    elif unit == "%":
        formatted = f"{val:.{meta['decimals']}f}%"
    else:
        formatted = f"{val:.{meta['decimals']}f}"

    # Format change
    if change is not None:
        if unit == "%":
            change_fmt = f"{change:+.{meta['decimals']}f}%"
        elif unit == "K":
            change_fmt = f"{change:+,.0f}K"
        else:
            change_fmt = f"{change:+.{meta['decimals']}f}"
    else:
        change_fmt = "—"

    return {
        "series_id": series_id,
        "name": meta["name"],
        "group": meta["group"],
        "icon": meta.get("icon", "📊"),
        "note": meta.get("note"),
        "value": current["value"],
        "value_fmt": formatted,
        "date": current["date"],
        "change": change,
        "change_fmt": change_fmt,
        "change_pct": change_pct,
        "direction": direction,
        "sparkline": sparkline,
        "prior_value": prior["value"] if prior else None,
        "prior_date": prior["date"] if prior else None,
    }


# ── Public API ───────────────────────────────────────────────────

def fetch_indicators() -> dict:
    """Fetch all macro indicators from FRED.

    Returns dict with:
      indicators: list of indicator dicts (grouped)
      groups: ordered list of {key, label, items}
      is_mock: bool
      last_updated: ISO date string
    """
    global _cache, _cache_ts

    now = time.time()
    with _lock:
        if _cache and (now - _cache_ts) < _CACHE_TTL:
            return _cache

    key = _fred_key()
    if not key:
        result = _build_mock_indicators()
        with _lock:
            _cache = result
            _cache_ts = now
        return result

    # Fetch all series in parallel (max 6 concurrent)
    indicators: list[dict] = []
    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = {
            pool.submit(_fetch_series, sid, meta): sid
            for sid, meta in INDICATORS.items()
        }
        for fut, sid in futures.items():
            try:
                result = fut.result()
                if result:
                    indicators.append(result)
            except Exception:
                logger.warning("FRED indicator %s failed", sid, exc_info=True)

    if not indicators:
        logger.warning("All FRED indicators failed, using mock data")
        result = _build_mock_indicators()
        with _lock:
            _cache = result
            _cache_ts = now
        return result

    # Group indicators
    groups = []
    for group_key in GROUP_LABELS:
        items = [i for i in indicators if i["group"] == group_key]
        if items:
            groups.append({
                "key": group_key,
                "label": GROUP_LABELS[group_key],
                "indicators": items,
            })

    result = {
        "indicators": indicators,
        "groups": groups,
        "is_mock": False,
        "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M"),
    }

    with _lock:
        _cache = result
        _cache_ts = now

    logger.info("FRED indicators: %d of %d fetched", len(indicators), len(INDICATORS))
    return result


# ── Mock data ────────────────────────────────────────────────────

def _build_mock_indicators() -> dict:
    """Return realistic mock data for dev/demo when no FRED key."""
    import random
    rng = random.Random(42)

    _MOCK = [
        ("DFF",          "Fed Funds Rate",          "rates",      4.75,  "%",  2, False),
        ("DGS10",        "10-Year Treasury",        "rates",      4.23,  "%",  2, False),
        ("DGS2",         "2-Year Treasury",          "rates",      4.58,  "%",  2, False),
        ("T10Y2Y",       "Yield Curve (10Y-2Y)",    "rates",     -0.35,  "%",  2, False),
        ("CPIAUCSL",     "CPI (YoY)",               "inflation",  3.1,   "%",  1, True),
        ("PCEPILFE",     "Core PCE (YoY)",           "inflation",  2.8,   "%",  1, True),
        ("T10YIE",       "10Y Breakeven Inflation",  "inflation",  2.35,  "%",  2, False),
        ("UNRATE",       "Unemployment Rate",        "employment", 3.9,   "%",  1, True),
        ("PAYEMS",       "Nonfarm Payrolls (MoM Δ)", "employment", 187,   "K",  0, False),
        ("ICSA",         "Initial Jobless Claims",   "employment", 215,   "K",  0, True),
        ("RSXFS",        "Retail Sales (MoM)",       "consumer",   0.4,   "%",  1, False),
        ("UMCSENT",      "Consumer Sentiment",       "consumer",   67.4,  "",   1, False),
        ("BAMLH0A0HYM2", "High Yield Spread",        "credit",     3.45,  "%",  2, True),
    ]

    indicators = []
    for sid, name, group, base_val, unit, dec, invert in _MOCK:
        # Generate sparkline
        sparkline = []
        v = base_val * (1 + rng.uniform(-0.15, 0.05))
        for _ in range(24):
            v += rng.uniform(-0.02, 0.02) * abs(base_val or 1)
            sparkline.append(round(v, dec))
        sparkline[-1] = base_val  # current = base

        change = round(rng.uniform(-0.3, 0.3) * abs(base_val or 1) * 0.1, dec)
        direction = "neutral"
        if change > 0:
            direction = "down" if invert else "up"
        elif change < 0:
            direction = "up" if invert else "down"

        if unit == "K":
            val_fmt = f"{base_val:,.0f}K"
            chg_fmt = f"{change:+,.0f}K"
        elif unit == "%":
            val_fmt = f"{base_val:.{dec}f}%"
            chg_fmt = f"{change:+.{dec}f}%"
        else:
            val_fmt = f"{base_val:.{dec}f}"
            chg_fmt = f"{change:+.{dec}f}"

        meta = INDICATORS.get(sid, {})
        indicators.append({
            "series_id": sid,
            "name": name,
            "group": group,
            "icon": meta.get("icon", "📊"),
            "note": meta.get("note"),
            "value": base_val,
            "value_fmt": val_fmt,
            "date": datetime.now().strftime("%Y-%m-%d"),
            "change": change,
            "change_fmt": chg_fmt,
            "change_pct": None,
            "direction": direction,
            "sparkline": sparkline,
            "prior_value": round(base_val - change, dec),
            "prior_date": (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"),
        })

    groups = []
    for gk in GROUP_LABELS:
        items = [i for i in indicators if i["group"] == gk]
        if items:
            groups.append({"key": gk, "label": GROUP_LABELS[gk], "indicators": items})

    return {
        "indicators": indicators,
        "groups": groups,
        "is_mock": True,
        "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M"),
    }
