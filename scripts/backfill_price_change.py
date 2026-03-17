"""
Backfill stock price reaction (%) into earnings_history.price_change column.

Computes close-to-close % change on earnings report date vs prior trading day.
Uses yfinance (free, no API key needed).

Usage (from project root):
    PYTHONPATH=src uv run python scripts/backfill_price_change.py
    PYTHONPATH=src uv run python scripts/backfill_price_change.py --ticker AAPL
    PYTHONPATH=src uv run python scripts/backfill_price_change.py --dry-run
    PYTHONPATH=src uv run python scripts/backfill_price_change.py --quarter "Q1 2026"
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from dotenv import load_dotenv
load_dotenv()

import httpx
import yfinance as yf

# ── Config ─────────────────────────────────────────────────────────────────
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_SERVICE_KEY = (
    os.environ.get("SUPABASE_SERVICE_KEY", "")
    or os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")
)

if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
    print("ERROR: SUPABASE_URL and SUPABASE_SERVICE_KEY must be set")
    sys.exit(1)

SUPA_HEADERS = {
    "apikey": SUPABASE_SERVICE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal",
}

SUPA_HEADERS_READ = {
    "apikey": SUPABASE_SERVICE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
    "Content-Type": "application/json",
}


# ── Helpers ────────────────────────────────────────────────────────────────

def get_rows_missing_price_change(
    start: str | None = None, end: str | None = None,
    ticker: str | None = None,
) -> list[dict]:
    """Fetch earnings_history rows that need price_change."""
    print("Fetching rows missing price_change...")
    rows: list[dict] = []
    page_size = 1000
    offset = 0

    filters = "&price_change=is.null&eps_actual=not.is.null"
    if ticker:
        filters += f"&ticker=eq.{ticker}"
    if start:
        filters += f"&report_date=gte.{start}"
    if end:
        filters += f"&report_date=lte.{end}"

    while True:
        resp = httpx.get(
            f"{SUPABASE_URL}/rest/v1/earnings_history"
            f"?select=ticker,report_date"
            f"{filters}"
            f"&order=report_date.desc",
            headers={
                **SUPA_HEADERS_READ,
                "Range": f"{offset}-{offset + page_size - 1}",
            },
            timeout=30,
        )
        if resp.status_code == 416:
            break
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break
        rows.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size

    print(f"  Found {len(rows)} rows missing price_change")
    return rows


def compute_price_change(ticker: str, report_date: str) -> float | None:
    """Compute close-to-close % change on earnings date vs prior trading day.

    Uses yfinance to get daily price data in a 10-day window around the report date.
    Returns the % change, or None if data unavailable.
    """
    try:
        dt = datetime.strptime(report_date, "%Y-%m-%d")
        # Fetch a window: 8 trading days before → 3 after
        start = (dt - timedelta(days=12)).strftime("%Y-%m-%d")
        end = (dt + timedelta(days=5)).strftime("%Y-%m-%d")

        tk = yf.Ticker(ticker)
        hist = tk.history(start=start, end=end, auto_adjust=True)

        if hist is None or hist.empty or len(hist) < 2:
            return None

        # Build date → close map
        by_date: dict[str, float] = {}
        for idx, row in hist.iterrows():
            d = idx.strftime("%Y-%m-%d")
            close = row.get("Close")
            if close is not None and close > 0:
                by_date[d] = float(close)

        if not by_date:
            return None

        sorted_dates = sorted(by_date.keys())

        # Find report date (or nearest prior trading day)
        report_idx = None
        for i, d in enumerate(sorted_dates):
            if d == report_date:
                report_idx = i
                break
            if d > report_date:
                report_idx = i - 1 if i > 0 else None
                break
        if report_idx is None:
            report_idx = len(sorted_dates) - 1

        if report_idx < 1:
            return None

        close_before = by_date[sorted_dates[report_idx - 1]]
        close_after = by_date[sorted_dates[report_idx]]
        if close_before > 0:
            return round(((close_after - close_before) / close_before) * 100, 2)

    except Exception as exc:
        pass  # Silently skip — will be logged in main loop

    return None


def update_price_change_batch(updates: list[dict]) -> int:
    """Batch-update price_change in earnings_history."""
    if not updates:
        return 0

    now_iso = datetime.now(timezone.utc).isoformat()
    count = 0

    for upd in updates:
        ticker = upd["ticker"]
        report_date = upd["report_date"]
        patch = {
            "price_change": upd["price_change"],
            "updated_at": now_iso,
        }

        resp = httpx.patch(
            f"{SUPABASE_URL}/rest/v1/earnings_history"
            f"?ticker=eq.{ticker}&report_date=eq.{report_date}",
            headers=SUPA_HEADERS,
            json=patch,
            timeout=15,
        )
        if resp.status_code < 300:
            count += 1

    return count


def quarter_to_dates(quarter_str: str) -> tuple[str, str]:
    """Convert 'Q1 2026' to (start_date, end_date)."""
    parts = quarter_str.strip().split()
    q = int(parts[0].replace("Q", ""))
    year = int(parts[1])
    starts = {1: f"{year}-01-01", 2: f"{year}-04-01", 3: f"{year}-07-01", 4: f"{year}-10-01"}
    ends = {1: f"{year}-03-31", 2: f"{year}-06-30", 3: f"{year}-09-30", 4: f"{year}-12-31"}
    return starts[q], ends[q]


# ── Main ───────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill stock price reactions")
    parser.add_argument("--ticker", help="Process a single ticker only")
    parser.add_argument("--quarter", help="Quarter to process (e.g. 'Q1 2026')")
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of tickers (0=all)")
    args = parser.parse_args()

    print("═══ Price Change Backfill ═══")
    print(f"  Supabase: {SUPABASE_URL[:40]}...")
    print(f"  Dry run: {args.dry_run}")
    print()

    start, end = None, None
    if args.quarter:
        start, end = quarter_to_dates(args.quarter)
        print(f"  Quarter: {args.quarter} ({start} to {end})")

    # Default to Q1 2026 range if no filter specified
    if not args.quarter and not args.ticker:
        start, end = "2025-12-15", "2026-03-31"
        print(f"  Default range: {start} to {end}")

    rows = get_rows_missing_price_change(start=start, end=end, ticker=args.ticker)
    if not rows:
        print("  No rows need price_change data")
        return

    # Group by ticker for efficient yfinance calls
    by_ticker: dict[str, list[str]] = {}
    for row in rows:
        by_ticker.setdefault(row["ticker"], []).append(row["report_date"])

    tickers = sorted(by_ticker.keys())
    if args.limit:
        tickers = tickers[:args.limit]

    print(f"  Processing {len(tickers)} tickers ({sum(len(v) for t, v in by_ticker.items() if t in set(tickers))} rows)...")
    print()

    total_updated = 0
    ok = no_data = errors = 0

    for i, ticker in enumerate(tickers, 1):
        report_dates = by_ticker[ticker]
        updates: list[dict] = []

        for report_date in report_dates:
            try:
                pct = compute_price_change(ticker, report_date)
                if pct is not None:
                    updates.append({
                        "ticker": ticker,
                        "report_date": report_date,
                        "price_change": pct,
                    })
            except Exception:
                errors += 1

        if updates:
            if args.dry_run:
                for u in updates:
                    print(f"    {u['ticker']:>8} {u['report_date']}  → {u['price_change']:+.2f}%")
                ok += 1
            else:
                n = update_price_change_batch(updates)
                total_updated += n
                ok += 1
        else:
            no_data += 1

        if i % 25 == 0 or i == len(tickers):
            if args.dry_run:
                print(f"  [{i:>5}/{len(tickers)}] ok={ok} no_data={no_data} errors={errors}")
            else:
                print(f"  [{i:>5}/{len(tickers)}] ok={ok} no_data={no_data} errors={errors} updated={total_updated}")

        # yfinance is free but be nice — 0.3s between tickers
        time.sleep(0.3)

    print(f"\n═══ Done ═══")
    if args.dry_run:
        print(f"  DRY RUN — no changes written")
    else:
        print(f"  Total rows updated: {total_updated}")
    print(f"  Tickers: {ok} ok, {no_data} no data, {errors} errors")


if __name__ == "__main__":
    main()
