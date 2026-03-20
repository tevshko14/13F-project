"""
Fetch missing ticker logos from Logo.dev's ticker API.

Logo.dev free tier = 500,000 requests/month.
Endpoint: https://img.logo.dev/ticker/{TICKER}?token={KEY}&format=png&size=128

Prioritizes tickers by market cap (highest first) using yfinance.
Stores results in the ``ticker_logos`` table as base64-encoded PNGs.

Usage (from project root):
    PYTHONPATH=src .venv/bin/python scripts/fetch_logos_logodev.py
    PYTHONPATH=src .venv/bin/python scripts/fetch_logos_logodev.py --limit 500
    PYTHONPATH=src .venv/bin/python scripts/fetch_logos_logodev.py --dry-run
"""

from __future__ import annotations

import argparse
import base64
import os
import sys
import time

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
LOGO_DEV_TOKEN = os.environ.get("LOGO_DEV_TOKEN", "")

SLEEP_BETWEEN = 0.15     # seconds between Logo.dev requests (be polite)
CHUNK_SIZE = 50           # rows per Supabase upsert batch
MIN_LOGO_BYTES = 200      # skip tiny fallback monograms

if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
    print("ERROR: SUPABASE_URL and SUPABASE_SERVICE_KEY must be set in .env")
    sys.exit(1)

if not LOGO_DEV_TOKEN:
    print("ERROR: LOGO_DEV_TOKEN must be set in .env")
    print("  Sign up at https://logo.dev → copy your publishable key (pk_...)")
    sys.exit(1)

SUPA_HEADERS = {
    "apikey": SUPABASE_SERVICE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
    "Content-Type": "application/json",
}

LOGO_DEV_URL = "https://img.logo.dev/ticker/{ticker}?token={token}&format=png&size=128"


# ── Step 1: Get tickers that already have logos ──────────────────────────────

def get_existing_logo_tickers() -> set[str]:
    """Return set of tickers that already have logos in ticker_logos."""
    print("Fetching existing logo tickers...")
    tickers: set[str] = set()
    page_size = 1000
    offset = 0

    with httpx.Client(timeout=30) as client:
        while True:
            resp = client.get(
                f"{SUPABASE_URL}/rest/v1/ticker_logos",
                headers=SUPA_HEADERS,
                params={
                    "select": "ticker",
                    "offset": str(offset),
                    "limit": str(page_size),
                },
            )
            resp.raise_for_status()
            rows = resp.json()
            if not rows:
                break
            for row in rows:
                tickers.add(row["ticker"].upper())
            offset += page_size

    print(f"  Found {len(tickers)} tickers with existing logos")
    return tickers


# ── Step 2: Get all tickers from 13f fund cache ─────────────────────────────

def get_all_tracked_tickers() -> list[str]:
    """Return all unique tickers from api_cache (13f fund holdings)."""
    import re
    valid_ticker = re.compile(r"^[A-Z]{1,6}$")

    print("Fetching full ticker universe from api_cache (13f holdings)...")
    tickers: set[str] = set()
    page_size = 1000
    offset = 0

    with httpx.Client(timeout=60) as client:
        while True:
            resp = client.get(
                f"{SUPABASE_URL}/rest/v1/api_cache",
                headers=SUPA_HEADERS,
                params={
                    "select": "response_data",
                    "category": "eq.13f",
                    "offset": str(offset),
                    "limit": str(page_size),
                },
            )
            resp.raise_for_status()
            rows = resp.json()
            if not rows:
                break
            for row in rows:
                holdings = (row.get("response_data") or {}).get("all_holdings", [])
                for h in holdings:
                    t = (h.get("ticker") or "").upper()
                    if t and valid_ticker.match(t):
                        tickers.add(t)
            offset += page_size
            if offset % 5000 == 0:
                print(f"  ... scanned {offset} fund entries, {len(tickers)} unique tickers so far")

    print(f"  Found {len(tickers)} unique tickers in holdings universe")
    return sorted(tickers)


# ── Step 3: Get market caps via yfinance (batch) ────────────────────────────

def get_market_caps(tickers: list[str]) -> dict[str, float]:
    """Fetch market caps for tickers using yfinance. Returns {ticker: market_cap}."""
    print(f"Fetching market caps for {len(tickers)} tickers via yfinance...")
    print("  (This may take a few minutes...)")

    import yfinance as yf

    caps: dict[str, float] = {}
    batch_size = 50
    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i + batch_size]
        batch_str = " ".join(batch)
        try:
            data = yf.Tickers(batch_str)
            for t in batch:
                try:
                    info = data.tickers[t].fast_info
                    mc = getattr(info, "market_cap", None)
                    if mc and mc > 0:
                        caps[t] = float(mc)
                except Exception:
                    pass  # ticker not found or no data
        except Exception as e:
            print(f"  Warning: batch {i//batch_size + 1} failed: {e}")

        if i % (batch_size * 5) == 0 and i > 0:
            print(f"  ... processed {i}/{len(tickers)} tickers, {len(caps)} caps found")
        time.sleep(0.3)

    print(f"  Got market caps for {len(caps)} tickers")
    return caps


# ── Step 4: Fetch logos from Logo.dev ────────────────────────────────────────

def fetch_logos(
    tickers: list[str],
    *,
    limit: int = 0,
    dry_run: bool = False,
) -> list[dict]:
    """Fetch logos from Logo.dev ticker API. Returns list of row dicts for upsert."""
    if limit > 0:
        tickers = tickers[:limit]

    print(f"\nFetching logos for {len(tickers)} tickers from Logo.dev...")
    if dry_run:
        print("  (DRY RUN — no actual requests)")
        for t in tickers[:20]:
            print(f"    Would fetch: {t}")
        if len(tickers) > 20:
            print(f"    ... and {len(tickers) - 20} more")
        return []

    results: list[dict] = []
    skipped = 0
    failed = 0

    with httpx.Client(timeout=15, follow_redirects=True) as client:
        for i, ticker in enumerate(tickers):
            url = LOGO_DEV_URL.format(ticker=ticker, token=LOGO_DEV_TOKEN)
            try:
                resp = client.get(url)
                if resp.status_code == 200 and len(resp.content) > MIN_LOGO_BYTES:
                    b64 = base64.b64encode(resp.content).decode("ascii")
                    ct = resp.headers.get("content-type", "image/png")
                    results.append({
                        "ticker": ticker,
                        "logo_b64": b64,
                        "content_type": ct,
                        "logo_domain": "logo.dev",
                    })
                elif resp.status_code == 200:
                    skipped += 1  # too small (monogram fallback)
                else:
                    failed += 1
                    if failed <= 5:
                        print(f"  ✗ {ticker}: HTTP {resp.status_code}")
            except Exception as e:
                failed += 1
                if failed <= 5:
                    print(f"  ✗ {ticker}: {e}")

            if (i + 1) % 100 == 0:
                print(f"  ... {i + 1}/{len(tickers)} — {len(results)} logos, {skipped} skipped, {failed} failed")

            time.sleep(SLEEP_BETWEEN)

    print(f"\nResults: {len(results)} logos fetched, {skipped} skipped (monogram), {failed} failed")
    return results


# ── Step 5: Upsert to Supabase ──────────────────────────────────────────────

def upsert_logos(rows: list[dict]) -> None:
    """Insert logo rows into ticker_logos (skip conflicts)."""
    if not rows:
        print("No rows to insert.")
        return

    print(f"Upserting {len(rows)} logos to Supabase...")
    with httpx.Client(timeout=30) as client:
        for i in range(0, len(rows), CHUNK_SIZE):
            chunk = rows[i:i + CHUNK_SIZE]
            resp = client.post(
                f"{SUPABASE_URL}/rest/v1/ticker_logos",
                headers={
                    **SUPA_HEADERS,
                    "Prefer": "resolution=merge-duplicates",
                },
                json=chunk,
            )
            if resp.status_code in (200, 201):
                print(f"  ✓ Batch {i // CHUNK_SIZE + 1}: {len(chunk)} rows")
            else:
                print(f"  ✗ Batch {i // CHUNK_SIZE + 1} failed: {resp.status_code} — {resp.text[:200]}")

    print("Done!")


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Fetch missing logos from Logo.dev")
    parser.add_argument("--limit", type=int, default=0, help="Max tickers to fetch (0 = all)")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be fetched")
    parser.add_argument("--skip-mcap", action="store_true", help="Skip market cap sorting (alphabetical)")
    args = parser.parse_args()

    # 1. Get existing logos
    existing = get_existing_logo_tickers()

    # 2. Get full ticker universe
    all_tickers = get_all_tracked_tickers()

    # 3. Filter to missing only
    missing = [t for t in all_tickers if t not in existing]
    print(f"\n{len(missing)} tickers missing logos")

    if not missing:
        print("All tickers have logos! Nothing to do.")
        return

    # 4. Sort by market cap (highest first) unless --skip-mcap
    if not args.skip_mcap and not args.dry_run:
        caps = get_market_caps(missing)
        # Sort: tickers with known market cap first (desc), then unknown tickers alphabetically
        missing_with_cap = [(t, caps.get(t, 0)) for t in missing]
        missing_with_cap.sort(key=lambda x: (-x[1], x[0]))
        missing = [t for t, _ in missing_with_cap]

        # Show top 20
        print("\nTop 20 by market cap:")
        for t, mc in missing_with_cap[:20]:
            if mc > 0:
                print(f"  {t:8s}  ${mc/1e9:,.1f}B")
            else:
                print(f"  {t:8s}  (no data)")

    # 5. Fetch from Logo.dev
    rows = fetch_logos(missing, limit=args.limit, dry_run=args.dry_run)

    # 6. Upsert to Supabase
    if rows:
        upsert_logos(rows)
        print(f"\n✅ {len(rows)} new logos added to ticker_logos")


if __name__ == "__main__":
    main()
