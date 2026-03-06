"""
Backfill earnings history for all tracked tickers.

Fetches from yfinance and upserts into the ``earnings_history`` table.
Uses the short_interest_history ticker universe (~1600 tickers).

Skips tickers already updated within the last 30 days unless --force is passed.

Usage (from project root):
    PYTHONPATH=src .venv/bin/python scripts/backfill_earnings.py
    PYTHONPATH=src .venv/bin/python scripts/backfill_earnings.py --force
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime, timezone

# ── Path setup ────────────────────────────────────────────────────────────────
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from dotenv import load_dotenv
load_dotenv()

import httpx

# ── Config ────────────────────────────────────────────────────────────────────
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_SERVICE_KEY = (
    os.environ.get("SUPABASE_SERVICE_KEY", "")
    or os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")
)
STALE_DAYS = 30          # skip tickers updated within this many days
SLEEP_BETWEEN = 0.6      # seconds between yfinance requests
CHUNK_LOG = 50           # log progress every N tickers

if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
    print("ERROR: SUPABASE_URL and SUPABASE_SERVICE_KEY must be set in .env")
    sys.exit(1)

SUPA_HEADERS = {
    "apikey": SUPABASE_SERVICE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
    "Content-Type": "application/json",
}


# ── Fetch ticker universe ─────────────────────────────────────────────────────

def get_target_tickers() -> list[str]:
    """Return all unique tickers from short_interest_history via pagination."""
    print("Fetching ticker universe from short_interest_history...")
    tickers: set[str] = set()
    page_size = 1000
    offset = 0

    while True:
        resp = httpx.get(
            f"{SUPABASE_URL}/rest/v1/short_interest_history?select=ticker",
            headers={
                **SUPA_HEADERS,
                "Range": f"{offset}-{offset + page_size - 1}",
                "Prefer": "count=none",
            },
            timeout=30,
        )
        if resp.status_code == 416:  # Range Not Satisfiable — past end of table
            break
        resp.raise_for_status()
        rows = resp.json()
        if not rows:
            break
        for r in rows:
            if r.get("ticker"):
                tickers.add(r["ticker"])
        if len(rows) < page_size:
            break
        offset += page_size

    result = sorted(tickers)
    print(f"  Found {len(result)} unique tickers")
    return result


def get_already_fresh(stale_days: int) -> set[str]:
    """Return tickers already in earnings_history updated within stale_days."""
    cutoff = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    # Use a simple approach: fetch all tickers + their max updated_at
    resp = httpx.post(
        f"{SUPABASE_URL}/rest/v1/rpc/get_fresh_earnings_tickers",
        headers=SUPA_HEADERS,
        json={"days_fresh": stale_days},
        timeout=30,
    )
    # If RPC doesn't exist, fall back to fetching from table directly
    if resp.status_code != 200:
        resp2 = httpx.get(
            f"{SUPABASE_URL}/rest/v1/earnings_history"
            f"?select=ticker,updated_at&limit=10000",
            headers=SUPA_HEADERS,
            timeout=30,
        )
        if resp2.status_code != 200:
            return set()
        rows = resp2.json()
        now = datetime.now(timezone.utc)
        fresh = set()
        for r in rows:
            ua = r.get("updated_at", "")
            if not ua:
                continue
            try:
                dt = datetime.fromisoformat(ua.replace("Z", "+00:00"))
                if (now - dt).days < stale_days:
                    fresh.add(r["ticker"])
            except (ValueError, TypeError):
                pass
        return fresh
    return {r["ticker"] for r in resp.json()}


# ── Fetch & store per-ticker ──────────────────────────────────────────────────

def backfill_ticker(ticker: str) -> str:
    """Fetch earnings for one ticker and upsert to DB. Returns status string."""
    from filings.earnings import _fetch_from_yfinance
    from filings.supabase_cache import upsert_earnings_history

    try:
        rows, _fwd = _fetch_from_yfinance(ticker)
    except Exception as exc:
        return f"FETCH_FAIL: {exc}"

    if not rows:
        return "NO_DATA"

    n = upsert_earnings_history(rows)
    return f"OK:{n}"


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill earnings_history table")
    parser.add_argument("--force", action="store_true",
                        help="Re-fetch even if data is recent")
    parser.add_argument("--ticker", help="Backfill a single ticker only")
    args = parser.parse_args()

    if args.ticker:
        targets = [args.ticker.upper()]
    else:
        targets = get_target_tickers()

    if not args.force and not args.ticker:
        fresh = get_already_fresh(STALE_DAYS)
        before = len(targets)
        targets = [t for t in targets if t not in fresh]
        print(f"  Skipping {before - len(targets)} already-fresh tickers "
              f"(updated within {STALE_DAYS} days)")

    print(f"\nBackfilling {len(targets)} tickers...\n")

    ok = fail = no_data = 0
    failed_tickers: list[str] = []
    start = time.time()

    for i, ticker in enumerate(targets, 1):
        status = backfill_ticker(ticker)

        if status.startswith("OK"):
            ok += 1
        elif status == "NO_DATA":
            no_data += 1
        else:
            fail += 1
            failed_tickers.append(f"{ticker}: {status}")

        if i % CHUNK_LOG == 0 or i == len(targets):
            elapsed = time.time() - start
            pct = i / len(targets) * 100
            rate = i / elapsed * 60
            eta = (len(targets) - i) / (i / elapsed) if i < len(targets) else 0
            print(f"  [{i:>4}/{len(targets)}] {pct:5.1f}%  "
                  f"ok={ok} no_data={no_data} fail={fail}  "
                  f"{rate:.0f}/min  ETA {eta/60:.1f}m  last={ticker}")
        else:
            sys.stdout.write(f"\r  [{i:>4}/{len(targets)}] {ticker:<8}  "
                             f"ok={ok} no_data={no_data} fail={fail}")
            sys.stdout.flush()

        time.sleep(SLEEP_BETWEEN)

    elapsed = time.time() - start
    print(f"\n\n── Backfill Complete ─────────────────────────────────")
    print(f"  ✅ Stored:   {ok}")
    print(f"  ⚠️  No data: {no_data}")
    print(f"  ❌ Failed:   {fail}")
    print(f"  ⏱  Time:    {elapsed/60:.1f} minutes")

    if failed_tickers:
        print("\nFailed tickers:")
        for t in failed_tickers[:20]:
            print(f"  {t}")
        if len(failed_tickers) > 20:
            print(f"  ...and {len(failed_tickers) - 20} more")


if __name__ == "__main__":
    main()
