"""Earnings history — quarterly EPS & revenue results.

On-demand fetch from yfinance with Supabase cold-storage caching.
No separate cron worker; data is fetched when a user views the
Earnings tab and cached in the ``earnings_history`` table.

Data flow:
  1. Check in-memory L1 cache (24h TTL)
  2. Check Supabase earnings_history table (L2 cold storage)
  3. If stale/missing, fetch from yfinance, upsert to DB
  4. Return stale data immediately if yfinance fails
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# ── Configuration ────────────────────────────────────────────────
_EARNINGS_TTL = 86_400  # 24 hours — earnings don't change once reported
_FWD_TTL = 3_600  # 1 hour — forward estimates update more often

# ── In-memory L1 caches ─────────────────────────────────────────
_history_cache: dict[str, tuple[float, list[dict]]] = {}
_fwd_cache: dict[str, tuple[float, dict]] = {}
_MAX_CACHE = 500


# ── Public API ───────────────────────────────────────────────────


def get_earnings_data(ticker: str) -> dict:
    """Main entry point: return earnings data for a ticker.

    Returns::

        {
            "history":           list[dict],   # quarterly results, newest first
            "forward_estimates": dict | None,  # EPS + revenue forward estimates
            "streak":            dict,          # consecutive beats count
            "source":            str,           # "fresh" | "cached" | "stale"
        }
    """
    ticker = ticker.upper().strip()

    # ── L1: in-memory history cache ──────────────────────────────
    now = time.time()
    cached = _history_cache.get(ticker)
    if cached and (now - cached[0]) < _EARNINGS_TTL:
        history = cached[1]
        fwd = _get_forward_estimates(ticker)
        return {
            "history": history,
            "forward_estimates": fwd,
            "streak": _compute_streak(history),
            "source": "cached",
        }

    # ── L2: Supabase cold storage ────────────────────────────────
    from filings import supabase_cache

    db_rows = supabase_cache.get_earnings_history(ticker, limit=100)
    db_fresh = False
    if db_rows:
        # Check freshness via updated_at on the most recent row
        latest_updated = db_rows[0].get("updated_at", "")
        if latest_updated:
            try:
                updated_dt = datetime.fromisoformat(
                    latest_updated.replace("Z", "+00:00")
                )
                age = (datetime.now(timezone.utc) - updated_dt).total_seconds()
                db_fresh = age < _EARNINGS_TTL
            except (ValueError, TypeError):
                pass

    if db_fresh:
        _update_l1(ticker, db_rows)
        fwd = _get_forward_estimates(ticker)
        return {
            "history": db_rows,
            "forward_estimates": fwd,
            "streak": _compute_streak(db_rows),
            "source": "cached",
        }

    # ── L3: live yfinance fetch ──────────────────────────────────
    try:
        fresh_rows, fwd = _fetch_from_yfinance(ticker)
    except Exception as exc:
        logger.warning("yfinance earnings fetch failed for %s: %s", ticker, exc)
        fresh_rows, fwd = [], None

    if fresh_rows:
        # Upsert to DB (fire-and-forget style, still synchronous here)
        try:
            supabase_cache.upsert_earnings_history(fresh_rows)
        except Exception as exc:
            logger.warning("upsert_earnings_history(%s) failed: %s", ticker, exc)

        _update_l1(ticker, fresh_rows)
        if fwd:
            _fwd_cache[ticker] = (time.time(), fwd)

        return {
            "history": fresh_rows,
            "forward_estimates": fwd,
            "streak": _compute_streak(fresh_rows),
            "source": "fresh",
        }

    # ── Stale fallback ───────────────────────────────────────────
    if db_rows:
        _update_l1(ticker, db_rows)
        return {
            "history": db_rows,
            "forward_estimates": _get_forward_estimates(ticker),
            "streak": _compute_streak(db_rows),
            "source": "stale",
        }

    return {
        "history": [],
        "forward_estimates": None,
        "streak": {},
        "source": "empty",
    }


# ── Internal helpers ─────────────────────────────────────────────


def _update_l1(ticker: str, rows: list[dict]) -> None:
    """Update L1 in-memory cache, evicting oldest if full."""
    if len(_history_cache) >= _MAX_CACHE:
        oldest = min(_history_cache, key=lambda k: _history_cache[k][0])
        _history_cache.pop(oldest, None)
    _history_cache[ticker] = (time.time(), rows)


def _get_forward_estimates(ticker: str) -> dict | None:
    """Return cached forward estimates, or fetch fresh from yfinance."""
    now = time.time()
    cached = _fwd_cache.get(ticker)
    if cached and (now - cached[0]) < _FWD_TTL:
        return cached[1]

    try:
        import yfinance as yf

        tk = yf.Ticker(ticker)
        fwd = _parse_forward_estimates(tk)
        if fwd:
            _fwd_cache[ticker] = (now, fwd)
        return fwd
    except Exception as exc:
        logger.debug("Forward estimates fetch failed for %s: %s", ticker, exc)
        return cached[1] if cached else None


def _get_fy_end_month(tk) -> int | None:
    """Return fiscal year end month (1-12) from annual financials columns."""
    try:
        cols = tk.financials.columns
        if cols is not None and len(cols) > 0:
            return int(cols[0].month)
    except Exception:
        pass
    return None


def _infer_fiscal_quarter(report_date_str: str, fy_end_month: int) -> str:
    """Infer fiscal quarter label (e.g. 'Q1 FY2026') from report date.

    Earnings are typically released ~45 days after the quarter ends,
    so we back-date to estimate the fiscal period.
    """
    from datetime import datetime, timedelta

    dt = datetime.strptime(report_date_str, "%Y-%m-%d")
    est_qend = dt - timedelta(days=45)

    # Map each quarter-end month → quarter number
    quarter_end_months: dict[int, int] = {}
    for q in range(1, 5):
        months_before = (4 - q) * 3
        m = ((fy_end_month - 1 - months_before) % 12) + 1
        quarter_end_months[m] = q

    month = est_qend.month
    if month not in quarter_end_months:
        return ""

    qnum = quarter_end_months[month]
    # FY year = calendar year when Q4 (fy_end_month) falls
    fy_year = est_qend.year + 1 if month > fy_end_month else est_qend.year
    return f"Q{qnum} FY{fy_year}"


def _fetch_from_yfinance(ticker: str) -> tuple[list[dict], dict | None]:
    """Fetch earnings history + forward estimates from yfinance.

    Uses the default yfinance session (not the curl_cffi one from
    market_data) because get_earnings_dates() is rate-limit-sensitive
    and curl_cffi sessions share fingerprints with the heavier batch
    sync workers.
    """
    import yfinance as yf

    tk = yf.Ticker(ticker)

    fy_end_month = _get_fy_end_month(tk)
    rows = _parse_earnings_dates(ticker, tk, fy_end_month)

    fwd = None
    try:
        fwd = _parse_forward_estimates(tk)
    except Exception as exc:
        logger.debug("Forward estimates failed for %s: %s", ticker, exc)

    return rows, fwd


def _parse_earnings_dates(ticker: str, tk, fy_end_month: int | None = None) -> list[dict]:
    """Parse yf.Ticker.get_earnings_dates() into normalized row dicts."""
    import pandas as pd

    try:
        df = tk.get_earnings_dates(limit=100)
    except Exception as exc:
        logger.warning("get_earnings_dates(%s) failed: %s", ticker, exc)
        return []

    if df is None or df.empty:
        return []

    now_iso = datetime.now(timezone.utc).isoformat()
    rows: list[dict] = []

    for dt_idx, row in df.iterrows():
        report_ts = pd.Timestamp(dt_idx)

        eps_actual = row.get("Reported EPS")
        if pd.isna(eps_actual):
            continue  # skip future/upcoming earnings

        eps_estimate = row.get("EPS Estimate")
        surprise_pct = row.get("Surprise(%)")

        beat_eps = None
        if not pd.isna(eps_actual) and not pd.isna(eps_estimate):
            beat_eps = float(eps_actual) >= float(eps_estimate)

        report_date_str = report_ts.strftime("%Y-%m-%d")
        fiscal_quarter = (
            _infer_fiscal_quarter(report_date_str, fy_end_month)
            if fy_end_month
            else ""
        )

        rows.append({
            "ticker": ticker.upper(),
            "report_date": report_date_str,
            "fiscal_quarter": fiscal_quarter,
            "eps_estimate": round(float(eps_estimate), 4)
            if not pd.isna(eps_estimate)
            else None,
            "eps_actual": round(float(eps_actual), 4)
            if not pd.isna(eps_actual)
            else None,
            "eps_surprise_pct": round(float(surprise_pct), 4)
            if not pd.isna(surprise_pct)
            else None,
            "revenue_estimate": None,
            "revenue_actual": None,
            "revenue_surprise_pct": None,
            "beat_eps": beat_eps,
            "beat_revenue": None,
            "updated_at": now_iso,
        })

    # Deduplicate by report_date (yfinance can return the same date with different timestamps)
    seen: set[str] = set()
    unique_rows = []
    for r in rows:
        if r["report_date"] not in seen:
            seen.add(r["report_date"])
            unique_rows.append(r)
    rows = unique_rows

    # Sort newest first
    rows.sort(key=lambda r: r["report_date"], reverse=True)
    return rows


def _parse_forward_estimates(tk) -> dict | None:
    """Parse forward EPS and revenue estimates from yfinance."""
    import pandas as pd

    result: dict = {}

    # EPS estimates
    try:
        eps_df = tk.get_earnings_estimate()
        if eps_df is not None and not eps_df.empty:
            period_labels = {
                "0q": "Current Qtr",
                "+1q": "Next Qtr",
                "0y": "Current Year",
                "+1y": "Next Year",
            }
            eps_list = []
            for idx, row in eps_df.iterrows():
                label = period_labels.get(str(idx), str(idx))
                eps_list.append({
                    "period": label,
                    "analysts": _safe_int(row.get("numberOfAnalysts")),
                    "avg": _safe_float(row.get("avg")),
                    "low": _safe_float(row.get("low")),
                    "high": _safe_float(row.get("high")),
                    "year_ago": _safe_float(row.get("yearAgoEps")),
                    "growth": _safe_float(row.get("growth")),
                })
            if eps_list:
                result["eps"] = eps_list
    except Exception as exc:
        logger.debug("get_earnings_estimate failed: %s", exc)

    # Revenue estimates
    try:
        rev_df = tk.get_revenue_estimate()
        if rev_df is not None and not rev_df.empty:
            period_labels = {
                "0q": "Current Qtr",
                "+1q": "Next Qtr",
                "0y": "Current Year",
                "+1y": "Next Year",
            }
            rev_list = []
            for idx, row in rev_df.iterrows():
                label = period_labels.get(str(idx), str(idx))
                avg_val = _safe_float(row.get("avg"))
                low_val = _safe_float(row.get("low"))
                high_val = _safe_float(row.get("high"))
                rev_list.append({
                    "period": label,
                    "analysts": _safe_int(row.get("numberOfAnalysts")),
                    "avg": avg_val,
                    "low": low_val,
                    "high": high_val,
                    "avg_fmt": _fmt_revenue(avg_val),
                    "low_fmt": _fmt_revenue(low_val),
                    "high_fmt": _fmt_revenue(high_val),
                    "growth": _safe_float(row.get("growth")),
                })
            if rev_list:
                result["revenue"] = rev_list
    except Exception as exc:
        logger.debug("get_revenue_estimate failed: %s", exc)

    return result if result else None


def _compute_streak(history: list[dict]) -> dict:
    """Compute consecutive EPS beat/miss streak from newest to oldest."""
    if not history:
        return {}

    eps_streak = 0
    direction = None  # True = beat, False = miss

    for row in history:
        beat = row.get("beat_eps")
        if beat is None:
            break
        if direction is None:
            direction = beat
        if beat != direction:
            break
        eps_streak += 1

    if eps_streak == 0:
        return {}

    if direction:
        label = f"{eps_streak} consecutive beat{'s' if eps_streak != 1 else ''}"
        streak_val = eps_streak
    else:
        label = f"{eps_streak} consecutive miss{'es' if eps_streak != 1 else ''}"
        streak_val = -eps_streak

    return {
        "eps_streak": streak_val,
        "eps_streak_label": label,
    }


def _safe_float(val) -> float | None:
    """Convert to float, returning None for NaN/None."""
    import pandas as pd

    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    try:
        return round(float(val), 4)
    except (ValueError, TypeError):
        return None


def _safe_int(val) -> int | None:
    """Convert to int, returning None for NaN/None."""
    import pandas as pd

    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _fmt_revenue(val: float | None) -> str:
    """Format revenue for display: $94.2B, $12.3M, etc."""
    if val is None:
        return ""
    abs_val = abs(val)
    sign = "-" if val < 0 else ""
    if abs_val >= 1e12:
        return f"{sign}${abs_val / 1e12:.1f}T"
    if abs_val >= 1e9:
        return f"{sign}${abs_val / 1e9:.1f}B"
    if abs_val >= 1e6:
        return f"{sign}${abs_val / 1e6:.1f}M"
    if abs_val >= 1e3:
        return f"{sign}${abs_val / 1e3:.0f}K"
    return f"{sign}${abs_val:.0f}"
