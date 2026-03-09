"""Cross-Reference Convergence Engine.

Finds stocks where multiple bullish (or bearish) signals fire simultaneously
by reading from existing cached/synced data sources — zero new API calls.

Signal sources:
  1. Unusual options activity  (today's scan, Supabase)
  2. Insider purchases         (last 7 days, Supabase)
  3. Congress buys             (last 30 days, Supabase)
  4. Short interest             (latest FINRA, Supabase)
  5. Superinvestor adds         (this quarter, 13F cache)
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from datetime import date, timedelta

logger = logging.getLogger(__name__)

# ── Configuration ────────────────────────────────────────────────────────────
_CONVERGENCE_TTL = 300          # 5-minute L1 cache
_INSIDER_LOOKBACK_DAYS = 7
_CONGRESS_LOOKBACK_DAYS = 30
_SHORT_INTEREST_THRESHOLD = 10.0  # % float short to flag
_MIN_SIGNALS = 2                  # minimum signal count for results

# Signal type weights for conviction scoring
_SIGNAL_WEIGHTS = {
    "superinvestor": 1.5,   # 13F data is highest conviction
    "insider":       1.3,   # insiders buying is very informative
    "options":       1.2,   # unusual options = institutional flow
    "congress":      1.0,   # congress trades — signal but noisy
    "short":         0.8,   # short interest is contrarian context
}

# ── L1 in-memory cache ───────────────────────────────────────────────────────
_lock = threading.Lock()
_convergence_cache: tuple[float, list[dict]] | None = None


# ── Data models ──────────────────────────────────────────────────────────────

@dataclass
class SignalDetail:
    """One signal's data for a specific ticker."""

    signal_type: str    # "options", "insider", "congress", "short", "superinvestor"
    direction: str      # "bullish" or "bearish"
    strength: float     # 0.0 – 1.0 normalised conviction
    summary: str        # human-readable one-liner
    detail: dict = field(default_factory=dict)


# ── Signal extractors ────────────────────────────────────────────────────────

def _extract_options_signals() -> dict[str, list[SignalDetail]]:
    """Tickers with unusual options activity from today's scan."""
    from filings import supabase_cache

    feed = supabase_cache.get_unusual_options_feed(limit=500)
    if not feed:
        return {}

    by_ticker: dict[str, list[dict]] = {}
    for row in feed:
        tk = row.get("ticker", "")
        if tk:
            by_ticker.setdefault(tk, []).append(row)

    result: dict[str, list[SignalDetail]] = {}
    for ticker, rows in by_ticker.items():
        bullish = [r for r in rows if r.get("sentiment") == "bullish"]
        bearish = [r for r in rows if r.get("sentiment") == "bearish"]
        total_premium = sum(float(r.get("premium_est") or 0) for r in rows)
        bull_premium = sum(float(r.get("premium_est") or 0) for r in bullish)

        direction = "bullish" if bull_premium > total_premium / 2 else "bearish"
        max_ratio = max((float(r.get("vol_oi_ratio") or 0) for r in rows), default=0)

        # Strength: normalise vol/OI ratio (5× baseline, 50× = max)
        strength = min(max_ratio / 50.0, 1.0)

        if total_premium >= 1e6:
            prem_fmt = f"${total_premium / 1e6:.1f}M premium"
        else:
            prem_fmt = f"${total_premium / 1e3:.0f}K premium"
        summary = f"{len(rows)} contract{'s' if len(rows) != 1 else ''}, {prem_fmt}"

        result[ticker] = [SignalDetail(
            signal_type="options",
            direction=direction,
            strength=strength,
            summary=summary,
            detail={
                "contract_count": len(rows),
                "total_premium": total_premium,
                "max_vol_oi": max_ratio,
                "bullish_count": len(bullish),
                "bearish_count": len(bearish),
                "company_name": rows[0].get("company_name", ""),
                "sector": rows[0].get("sector", ""),
            },
        )]
    return result


def _extract_insider_signals() -> dict[str, list[SignalDetail]]:
    """Tickers with recent insider purchases (last 7 days)."""
    from filings import supabase_cache

    cutoff = (date.today() - timedelta(days=_INSIDER_LOOKBACK_DAYS)).isoformat()
    trades = supabase_cache.get_insider_trades(
        trade_type="Purchase", limit=500, since_date=cutoff,
    )
    if not trades:
        return {}

    by_ticker: dict[str, list[dict]] = {}
    for t in trades:
        tk = t.get("ticker", "")
        if tk:
            by_ticker.setdefault(tk, []).append(t)

    result: dict[str, list[SignalDetail]] = {}
    for ticker, rows in by_ticker.items():
        count = len(rows)
        names = list({r.get("insider_name", "Unknown") for r in rows})
        # Strength: 1 purchase → 0.3, 5+ → 1.0
        strength = min(count / 5.0, 1.0) * 0.7 + 0.3

        summary = f"{count} insider purchase{'s' if count != 1 else ''}"
        if len(names) <= 2:
            summary += f" ({', '.join(names[:2])})"

        result[ticker] = [SignalDetail(
            signal_type="insider",
            direction="bullish",
            strength=min(strength, 1.0),
            summary=summary,
            detail={"purchase_count": count, "insiders": names[:5]},
        )]
    return result


def _extract_congress_signals() -> dict[str, list[SignalDetail]]:
    """Tickers with recent congressional purchases (last 30 days)."""
    from filings import supabase_cache

    trades = supabase_cache.get_congress_recent_trades(limit=500)
    if not trades:
        return {}

    cutoff = (date.today() - timedelta(days=_CONGRESS_LOOKBACK_DAYS)).isoformat()
    by_ticker: dict[str, list[dict]] = {}
    for t in trades:
        trade_dt = t.get("trade_date", "") or t.get("filing_date", "")
        if (trade_dt >= cutoff
                and t.get("ticker")
                and t.get("trade_type", "").lower() in ("buy", "purchase")):
            by_ticker.setdefault(t["ticker"], []).append(t)

    result: dict[str, list[SignalDetail]] = {}
    for ticker, rows in by_ticker.items():
        politicians = list({r.get("politician_name", "") for r in rows})
        count = len(rows)
        strength = min(count / 3.0, 1.0)

        summary = f"{count} congress buy{'s' if count != 1 else ''}"
        if len(politicians) <= 2:
            summary += f" ({', '.join(politicians[:2])})"

        result[ticker] = [SignalDetail(
            signal_type="congress",
            direction="bullish",
            strength=strength,
            summary=summary,
            detail={"trade_count": count, "politicians": politicians[:5]},
        )]
    return result


def _extract_short_interest_signals() -> dict[str, list[SignalDetail]]:
    """Tickers with elevated short interest (≥10 % float)."""
    from filings import supabase_cache

    rows = supabase_cache.get_latest_short_interest_all(limit=200)
    if not rows:
        return {}

    result: dict[str, list[SignalDetail]] = {}
    for row in rows:
        ticker = row.get("ticker", "")
        pct = float(row.get("short_pct_float") or 0)
        if not ticker or pct < _SHORT_INTEREST_THRESHOLD:
            continue

        ratio = float(row.get("short_ratio") or 0)
        # Strength: 10 % → 0.2, 20 % → 0.6, 30 %+ → 1.0
        strength = min((pct - 5) / 25.0, 1.0)

        summary = f"{pct:.1f}% float short"
        if ratio > 0:
            summary += f", {ratio:.1f} days to cover"

        result[ticker] = [SignalDetail(
            signal_type="short",
            direction="bearish",
            strength=strength,
            summary=summary,
            detail={"short_pct_float": pct, "short_ratio": ratio},
        )]
    return result


def _extract_superinvestor_signals(
    cache_data: dict,
    superinvestors_by_cik: dict,
) -> dict[str, list[SignalDetail]]:
    """Tickers being added by multiple superinvestors this quarter."""
    from filings import market_data

    most_added = market_data.build_most_added_table(cache_data, superinvestors_by_cik)
    if not most_added:
        return {}

    result: dict[str, list[SignalDetail]] = {}
    for entry in most_added:
        ticker = entry.get("ticker")
        if not ticker:
            continue
        add_count = entry.get("add_count", 0)
        adders = entry.get("adders", [])
        # Strength: 1 adder → 0.3, 5+ → 1.0
        strength = min(add_count / 5.0, 1.0) * 0.7 + 0.3

        summary = f"{add_count} superinvestor{'s' if add_count != 1 else ''} adding"
        if len(adders) <= 3:
            summary += f" ({', '.join(adders[:3])})"

        result[ticker] = [SignalDetail(
            signal_type="superinvestor",
            direction="bullish",
            strength=min(strength, 1.0),
            summary=summary,
            detail={"add_count": add_count, "adders": adders[:5]},
        )]
    return result


# ── Merge & score ────────────────────────────────────────────────────────────

def build_convergence(
    cache_data: dict,
    superinvestors_by_cik: dict,
    signals_filter: set[str] | None = None,
    min_signals: int = _MIN_SIGNALS,
) -> list[dict]:
    """Build the convergence table — tickers where multiple signals align.

    Args:
        cache_data:            In-memory 13F fund cache (for superinvestor signal).
        superinvestors_by_cik: CIK → fund name map.
        signals_filter:        Set of signal types to include.  ``None`` = all.
        min_signals:           Minimum unique signal types to include a ticker.

    Returns:
        List of result dicts sorted by signal_count desc, conviction desc.
    """
    global _convergence_cache

    # L1 cache check (unfiltered + default min only)
    is_default = signals_filter is None and min_signals == _MIN_SIGNALS
    if is_default:
        with _lock:
            if _convergence_cache is not None:
                ts, data = _convergence_cache
                if time.time() - ts < _CONVERGENCE_TTL:
                    return data

    # ── Gather all signal sets ───────────────────────────────────────────
    all_signals: dict[str, list[SignalDetail]] = {}

    extractors: dict[str, object] = {
        "options":        lambda: _extract_options_signals(),
        "insider":        lambda: _extract_insider_signals(),
        "congress":       lambda: _extract_congress_signals(),
        "short":          lambda: _extract_short_interest_signals(),
        "superinvestor":  lambda: _extract_superinvestor_signals(
            cache_data, superinvestors_by_cik,
        ),
    }

    active = signals_filter or set(extractors.keys())
    for sig_type, extractor in extractors.items():
        if sig_type not in active:
            continue
        try:
            sig_data = extractor()
            for ticker, signals in sig_data.items():
                all_signals.setdefault(ticker, []).extend(signals)
        except Exception:
            logger.warning("Convergence extractor %s failed", sig_type, exc_info=True)

    # ── Build results ────────────────────────────────────────────────────
    results: list[dict] = []
    for ticker, signals in all_signals.items():
        signal_types = {s.signal_type for s in signals}
        if len(signal_types) < min_signals:
            continue

        # Net direction
        bullish_w = sum(
            s.strength * _SIGNAL_WEIGHTS.get(s.signal_type, 1.0)
            for s in signals if s.direction == "bullish"
        )
        bearish_w = sum(
            s.strength * _SIGNAL_WEIGHTS.get(s.signal_type, 1.0)
            for s in signals if s.direction == "bearish"
        )
        if bullish_w > bearish_w * 1.2:
            net_direction = "bullish"
        elif bearish_w > bullish_w * 1.2:
            net_direction = "bearish"
        else:
            net_direction = "mixed"

        # Conviction score (weighted sum)
        conviction = sum(
            s.strength * _SIGNAL_WEIGHTS.get(s.signal_type, 1.0)
            for s in signals
        )

        # Try to pull company_name / sector from options signal detail
        company_name = ""
        sector = ""
        for s in signals:
            if s.signal_type == "options":
                company_name = company_name or s.detail.get("company_name", "")
                sector = sector or s.detail.get("sector", "")

        results.append({
            "ticker": ticker,
            "signal_count": len(signal_types),
            "signals": [
                {
                    "type": s.signal_type,
                    "direction": s.direction,
                    "strength": round(s.strength, 2),
                    "summary": s.summary,
                }
                for s in signals
            ],
            "net_direction": net_direction,
            "conviction_score": round(conviction, 2),
            "company_name": company_name,
            "sector": sector,
            # Pre-computed booleans for badge rendering
            "has_options": "options" in signal_types,
            "has_insider": "insider" in signal_types,
            "has_congress": "congress" in signal_types,
            "has_short": "short" in signal_types,
            "has_superinvestor": "superinvestor" in signal_types,
        })

    results.sort(key=lambda r: (-r["signal_count"], -r["conviction_score"]))

    # L1 cache store (unfiltered only)
    if is_default:
        with _lock:
            _convergence_cache = (time.time(), results)

    return results


def invalidate_cache() -> None:
    """Clear L1 cache — call after any sync cycle completes."""
    global _convergence_cache
    with _lock:
        _convergence_cache = None
