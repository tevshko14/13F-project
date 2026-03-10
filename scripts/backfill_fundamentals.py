"""
Backfill full historical financial data (SEC XBRL) into cold storage.

Fetches ALL historical periods (10-K annual, 10-Q quarterly) for each ticker
from the SEC EDGAR CompanyFacts API, stores the structured JSON in Supabase
Storage, and tracks progress in the fundamentals_backfill_meta table.

Incremental: skips tickers already marked 'ok' in the metadata table.
Resume-safe: can be killed and re-run; it picks up where it left off.

Priority tiers:
  --tier sp500     S&P 500 only (~500 tickers, ~4-5 min)
  --tier nasdaq    All NASDAQ-listed (~3000-4000, ~25-35 min)
  --tier all       All exchanges (~6000-8000, ~60-90 min)

Usage (from project root):
    PYTHONPATH=src .venv/bin/python scripts/backfill_fundamentals.py --tier sp500
    PYTHONPATH=src .venv/bin/python scripts/backfill_fundamentals.py --tier all --force
    PYTHONPATH=src .venv/bin/python scripts/backfill_fundamentals.py --tickers AAPL,MSFT,GOOG
    PYTHONPATH=src .venv/bin/python scripts/backfill_fundamentals.py --report --tier sp500

Environment:
    SUPABASE_URL, SUPABASE_SERVICE_KEY must be set.
"""

from __future__ import annotations

import argparse
import gc
import json
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

CHUNK_LOG = 50           # log progress every N tickers

if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
    print("ERROR: SUPABASE_URL and SUPABASE_SERVICE_KEY must be set in .env")
    sys.exit(1)

SUPA_HEADERS = {
    "apikey": SUPABASE_SERVICE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal",
}


# ═════════════════════════════════════════════════════════════════════════════
# Ticker Universe
# ═════════════════════════════════════════════════════════════════════════════

def get_target_tickers(tier: str) -> list[str]:
    """Return sorted list of tickers for the chosen tier."""
    from filings.market_data import get_sp500_constituents, get_all_listed_tickers

    if tier == "sp500":
        constituents = get_sp500_constituents()
        tickers = [c["ticker"] for c in constituents if c.get("ticker")]
        print(f"  S&P 500 universe: {len(tickers)} tickers")
        return sorted(set(tickers))

    all_listed = get_all_listed_tickers()
    if tier == "nasdaq":
        tickers = [s["ticker"] for s in all_listed
                    if s.get("exchange") == "NASDAQ" and s.get("ticker")]
        print(f"  NASDAQ universe: {len(tickers)} tickers")
    else:  # "all"
        tickers = [s["ticker"] for s in all_listed if s.get("ticker")]
        print(f"  All-exchange universe: {len(tickers)} tickers")

    return sorted(set(tickers))


# ═════════════════════════════════════════════════════════════════════════════
# Metadata Table CRUD
# ═════════════════════════════════════════════════════════════════════════════

def get_already_done() -> set[str]:
    """Return tickers with status='ok' from fundamentals_backfill_meta."""
    done: set[str] = set()
    page_size = 1000
    offset = 0

    while True:
        resp = httpx.get(
            f"{SUPABASE_URL}/rest/v1/fundamentals_backfill_meta"
            f"?select=ticker&status=eq.ok",
            headers={
                **SUPA_HEADERS,
                "Range": f"{offset}-{offset + page_size - 1}",
                "Prefer": "count=none",
            },
            timeout=30,
        )
        if resp.status_code == 416:
            break
        resp.raise_for_status()
        rows = resp.json()
        if not rows:
            break
        for r in rows:
            done.add(r["ticker"])
        if len(rows) < page_size:
            break
        offset += page_size

    return done


def get_all_meta() -> list[dict]:
    """Fetch all rows from fundamentals_backfill_meta."""
    rows: list[dict] = []
    page_size = 1000
    offset = 0

    while True:
        resp = httpx.get(
            f"{SUPABASE_URL}/rest/v1/fundamentals_backfill_meta"
            f"?select=*&order=ticker.asc",
            headers={
                **SUPA_HEADERS,
                "Range": f"{offset}-{offset + page_size - 1}",
                "Prefer": "count=none",
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

    return rows


def upsert_meta(
    ticker: str,
    cik: str | None,
    annual_periods: int,
    quarterly_periods: int,
    cold_path: str,
    status: str,
    error_msg: str | None = None,
) -> None:
    """Upsert a row into fundamentals_backfill_meta."""
    now = datetime.now(timezone.utc).isoformat()
    row = {
        "ticker": ticker,
        "cik": cik,
        "annual_periods": annual_periods,
        "quarterly_periods": quarterly_periods,
        "cold_storage_path": cold_path,
        "status": status,
        "error_message": error_msg,
        "backfilled_at": now if status == "ok" else None,
        "updated_at": now,
    }
    resp = httpx.post(
        f"{SUPABASE_URL}/rest/v1/fundamentals_backfill_meta",
        headers={
            **SUPA_HEADERS,
            "Prefer": "resolution=merge-duplicates,return=minimal",
        },
        json=row,
        timeout=30,
    )
    resp.raise_for_status()


# ═════════════════════════════════════════════════════════════════════════════
# Per-Ticker Backfill
# ═════════════════════════════════════════════════════════════════════════════

def _merge_statements(existing: dict | None, new_data: dict) -> dict:
    """Merge new financial data into existing cold storage data.

    - Keeps ALL existing periods (never deletes old data)
    - Adds any new periods from new_data that don't already exist
    - Updates values for existing periods only if they were previously None
    - Returns the merged data
    """
    if not existing:
        return new_data

    merged = dict(new_data)  # Start with new data as base

    for freq in ("annual", "quarterly"):
        existing_freq = existing.get(freq)
        new_freq = new_data.get(freq)

        if not existing_freq:
            continue  # Nothing to preserve; keep new_data as-is
        if not new_freq:
            # New fetch returned nothing, but we had old data — keep it
            merged[freq] = existing_freq
            continue

        # Merge periods: union of both sets, preserving existing values
        for stmt_key in ("income", "balance", "cashflow", "ratios"):
            existing_stmt = existing_freq.get(stmt_key, {})
            new_stmt = new_freq.get(stmt_key, {})

            if not existing_stmt:
                continue

            ex_periods = set(existing_stmt.get("periods", []))
            new_periods = set(new_stmt.get("periods", []))
            all_periods = sorted(ex_periods | new_periods, reverse=True)

            # Merge row values
            merged_rows = []
            ex_rows_by_label = {r["label"]: r for r in existing_stmt.get("rows", [])}
            new_rows_by_label = {r["label"]: r for r in new_stmt.get("rows", [])}

            # Process all labels from both sources
            all_labels = list(dict.fromkeys(
                [r["label"] for r in new_stmt.get("rows", [])] +
                [r["label"] for r in existing_stmt.get("rows", [])]
            ))

            for label in all_labels:
                ex_row = ex_rows_by_label.get(label, {})
                new_row = new_rows_by_label.get(label, {})
                base_row = new_row if new_row else ex_row

                # Merge values: existing values take priority (never overwrite)
                merged_values = {}
                ex_vals = ex_row.get("values", {})
                new_vals = new_row.get("values", {})
                for p in all_periods:
                    # Keep existing value if non-None, otherwise use new
                    ex_v = ex_vals.get(p)
                    new_v = new_vals.get(p)
                    merged_values[p] = ex_v if ex_v is not None else new_v

                merged_rows.append({
                    **{k: v for k, v in base_row.items() if k != "values"},
                    "values": merged_values,
                })

            merged[freq][stmt_key] = {
                "periods": all_periods,
                "rows": merged_rows,
            }

    return merged


def backfill_one(ticker: str, dry_run: bool = False) -> tuple[str, dict]:
    """Fetch full history for one ticker and store in cold storage.

    Merge-only: new data is merged with existing cold storage data.
    Existing period values are NEVER overwritten — only gaps are filled.

    Returns (status, info_dict):
      - ("ok",      {"annual": N, "quarterly": M, "cik": "..."})
      - ("no_data",  {})
      - ("error",   {"message": "..."})
    """
    from filings.fundamentals import fetch_raw_xbrl, build_full_statements, _resolve_cik
    from filings import cold_storage

    # Resolve CIK first (for metadata tracking)
    cik = _resolve_cik(ticker)

    # Fetch trimmed XBRL from SEC
    xbrl = fetch_raw_xbrl(ticker)
    if xbrl is None:
        upsert_meta(ticker, cik, 0, 0, "", "no_data")
        return ("no_data", {})

    # Build all statements with full history
    data = build_full_statements(xbrl, ticker=ticker)
    if not data or (data.get("annual") is None and data.get("quarterly") is None):
        upsert_meta(ticker, cik, 0, 0, "", "no_data")
        return ("no_data", {})

    # Add metadata to the stored blob
    data["cik"] = cik
    data["backfilled_at"] = datetime.now(timezone.utc).isoformat()

    # Upload to cold storage (merge-only — never delete existing data)
    cold_path = f"fundamentals/{ticker.upper()}/full_history.json"

    if not dry_run:
        # Load existing data from cold storage and merge
        existing = cold_storage.download_json(cold_path)
        merged = _merge_statements(existing, data)

        # Count periods from merged result (includes old + new)
        annual_n = len(merged.get("annual", {}).get("income", {}).get("periods", [])) if merged.get("annual") else 0
        quarterly_n = len(merged.get("quarterly", {}).get("income", {}).get("periods", [])) if merged.get("quarterly") else 0

        ok = cold_storage.upload_json(cold_path, merged)
        if not ok:
            upsert_meta(ticker, cik, annual_n, quarterly_n, cold_path, "error",
                         "Cold storage upload failed")
            return ("error", {"message": "Cold storage upload failed"})

        # Track in metadata table
        upsert_meta(ticker, cik, annual_n, quarterly_n, cold_path, "ok")
    else:
        annual_n = len(data.get("annual", {}).get("income", {}).get("periods", [])) if data.get("annual") else 0
        quarterly_n = len(data.get("quarterly", {}).get("income", {}).get("periods", [])) if data.get("quarterly") else 0

    return ("ok", {"annual": annual_n, "quarterly": quarterly_n, "cik": cik})


# ═════════════════════════════════════════════════════════════════════════════
# Gap Analysis
# ═════════════════════════════════════════════════════════════════════════════

def report_gaps(tier: str) -> None:
    """Print a coverage report for the given tier."""
    targets = get_target_tickers(tier)
    target_set = set(targets)

    meta_rows = get_all_meta()
    meta_by_ticker = {r["ticker"]: r for r in meta_rows}

    ok = [t for t in targets if meta_by_ticker.get(t, {}).get("status") == "ok"]
    no_data = [t for t in targets if meta_by_ticker.get(t, {}).get("status") == "no_data"]
    errors = [t for t in targets if meta_by_ticker.get(t, {}).get("status") == "error"]
    not_attempted = [t for t in targets if t not in meta_by_ticker]

    total = len(targets)
    pct = lambda n: f"{n/total*100:5.1f}%" if total else "  0.0%"

    print(f"\n{'='*60}")
    print(f"  Fundamentals Backfill Gap Report ({tier})")
    print(f"{'='*60}")
    print(f"  Target tickers:     {total}")
    print(f"  Status 'ok':        {len(ok):>5}  ({pct(len(ok))})")
    print(f"  Status 'no_data':   {len(no_data):>5}  ({pct(len(no_data))})")
    print(f"  Status 'error':     {len(errors):>5}  ({pct(len(errors))})")
    print(f"  Not attempted:      {len(not_attempted):>5}  ({pct(len(not_attempted))})")

    # Period coverage for 'ok' tickers
    if ok:
        annual_counts = [meta_by_ticker[t].get("annual_periods", 0) for t in ok]
        print(f"\n  Annual period coverage (ok tickers):")
        print(f"    20+ periods:   {sum(1 for c in annual_counts if c >= 20):>5}")
        print(f"    10-19 periods: {sum(1 for c in annual_counts if 10 <= c < 20):>5}")
        print(f"    5-9 periods:   {sum(1 for c in annual_counts if 5 <= c < 10):>5}")
        print(f"    <5 periods:    {sum(1 for c in annual_counts if c < 5):>5}")

    if errors:
        print(f"\n  Tickers with errors (first 20):")
        for t in errors[:20]:
            msg = meta_by_ticker[t].get("error_message", "unknown")
            print(f"    {t}: {msg}")
        if len(errors) > 20:
            print(f"    ...and {len(errors) - 20} more")

    if not_attempted:
        shown = not_attempted[:30]
        print(f"\n  Not attempted (first 30): {', '.join(shown)}")
        if len(not_attempted) > 30:
            print(f"    ...and {len(not_attempted) - 30} more")

    print()


# ═════════════════════════════════════════════════════════════════════════════
# Main
# ═════════════════════════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill full historical financial data to cold storage"
    )
    parser.add_argument(
        "--tier", choices=["sp500", "nasdaq", "all"], default="sp500",
        help="Ticker universe tier (default: sp500)"
    )
    parser.add_argument(
        "--tickers", type=str, default="",
        help="Comma-separated tickers to backfill (overrides --tier)"
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Re-fetch even if already marked ok"
    )
    parser.add_argument(
        "--max", type=int, default=0, dest="max_tickers",
        help="Cap at N tickers (for testing)"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Fetch from SEC but don't upload to cold storage"
    )
    parser.add_argument(
        "--report", action="store_true",
        help="Print gap analysis and exit (no fetching)"
    )
    args = parser.parse_args()

    # Report mode
    if args.report:
        report_gaps(args.tier)
        return

    # Build target list
    if args.tickers:
        targets = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
        print(f"  Explicit tickers: {len(targets)}")
    else:
        targets = get_target_tickers(args.tier)

    # Skip already-done tickers (unless --force)
    if not args.force:
        done = get_already_done()
        before = len(targets)
        targets = [t for t in targets if t not in done]
        skipped = before - len(targets)
        if skipped:
            print(f"  Skipping {skipped} already-ok tickers (use --force to re-fetch)")

    # Apply --max cap
    if args.max_tickers > 0:
        targets = targets[:args.max_tickers]
        print(f"  Capped to {args.max_tickers} tickers (--max)")

    if not targets:
        print("\n  Nothing to do — all tickers already backfilled!")
        print("  Use --force to re-fetch, or --report for gap analysis.")
        return

    # Check cold storage availability
    from filings import cold_storage
    if not args.dry_run and not cold_storage.is_available():
        print("ERROR: Cold storage (Supabase Storage) is not available.")
        print("  Check SUPABASE_URL and SUPABASE_SERVICE_KEY.")
        sys.exit(1)

    print(f"\n  Backfilling {len(targets)} tickers "
          f"(~{len(targets) * 0.5 / 60:.1f} min estimated)...\n")

    ok = fail = no_data = 0
    failed_tickers: list[str] = []
    start = time.time()

    for i, ticker in enumerate(targets, 1):
        try:
            status, meta = backfill_one(ticker, dry_run=args.dry_run)
        except Exception as exc:
            status = "error"
            meta = {"message": str(exc)}
            fail += 1
            failed_tickers.append(f"{ticker}: {exc}")
            try:
                upsert_meta(ticker, None, 0, 0, "", "error", str(exc)[:500])
            except Exception:
                pass

        if status == "ok":
            ok += 1
        elif status == "no_data":
            no_data += 1
        elif status == "error" and f"{ticker}:" not in "\n".join(failed_tickers):
            fail += 1
            failed_tickers.append(f"{ticker}: {meta.get('message', 'unknown')}")

        # Progress logging
        if i % CHUNK_LOG == 0 or i == len(targets):
            elapsed = time.time() - start
            pct = i / len(targets) * 100
            rate = i / elapsed * 60 if elapsed > 0 else 0
            eta = (len(targets) - i) / (i / elapsed) if i < len(targets) and elapsed > 0 else 0
            annual = meta.get("annual", "") if status == "ok" else ""
            quarterly = meta.get("quarterly", "") if status == "ok" else ""
            detail = f"({annual}a, {quarterly}q)" if annual else ""
            print(f"  [{i:>5}/{len(targets)}] {pct:5.1f}%  "
                  f"ok={ok} no_data={no_data} fail={fail}  "
                  f"{rate:.0f}/min  ETA {eta/60:.1f}m  last={ticker} {detail}")
        else:
            sys.stdout.write(f"\r  [{i:>5}/{len(targets)}] {ticker:<8}  "
                             f"ok={ok} no_data={no_data} fail={fail}")
            sys.stdout.flush()

        # Memory cleanup — free the large XBRL dict
        gc.collect()

    elapsed = time.time() - start
    dry_tag = " (DRY RUN)" if args.dry_run else ""
    print(f"\n\n── Backfill Complete{dry_tag} ─────────────────────────────────")
    print(f"  ✅ Stored:   {ok}")
    print(f"  ⚠️  No data: {no_data}")
    print(f"  ❌ Failed:   {fail}")
    print(f"  ⏱  Time:    {elapsed/60:.1f} minutes")
    print(f"  📦 Cold path: fundamentals/{{ticker}}/full_history.json")

    if failed_tickers:
        print(f"\n  Failed tickers (first 20):")
        for t in failed_tickers[:20]:
            print(f"    {t}")
        if len(failed_tickers) > 20:
            print(f"    ...and {len(failed_tickers) - 20} more")


if __name__ == "__main__":
    main()
