#!/usr/bin/env python3
"""One-time historical load of Capitol Trades data into Supabase.

Scrapes all ~35K congressional trades from Capitol Trades (3 years of
STOCK Act disclosures), derives politician profiles, and populates the
``congress_members`` and ``congress_trades`` Supabase tables.

⚠️  COLD ARCHIVE — DATA PROTECTION ⚠️

    ``congress_trades`` is a WRITE-ONCE cold archive:
      - Application layer: ``ignore_duplicates=True`` → ON CONFLICT DO NOTHING
      - Database layer:     BEFORE DELETE / BEFORE UPDATE triggers raise exceptions
      - Script layer:       Refuses to run if tables already have data (unless
                            CONGRESS_LOAD_ALLOW_RERUN=1 is set)

    Re-running this script on a populated table is safe (duplicates are ignored)
    but unnecessary.  The env-var gate prevents accidental re-runs.

Usage:
    uv run python scripts/load_congress_history.py [--max-pages N] [--dry-run]

The full scrape takes ~12 minutes (~350 pages at 2s/page).  Use --max-pages
for testing.  Use --dry-run to scrape without writing to Supabase.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time

# Ensure the src directory is on the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from filings import congress_trading, supabase_cache


def _setup_logging() -> None:
    fmt = "%(asctime)s %(levelname)-8s %(name)s -- %(message)s"
    logging.basicConfig(level=logging.INFO, format=fmt, force=True)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="Load Capitol Trades history into Supabase")
    parser.add_argument(
        "--max-pages", type=int, default=0,
        help="Max pages to scrape (0 = all, default: 0)",
    )
    parser.add_argument(
        "--page-size", type=int, default=100,
        help="Trades per page (max 100, default: 100)",
    )
    parser.add_argument(
        "--delay", type=float, default=2.0,
        help="Seconds between requests (default: 2.0)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Scrape only — don't write to Supabase",
    )
    args = parser.parse_args()

    _setup_logging()

    logger.info("=== Capitol Trades Historical Load ===")
    logger.info(
        "Settings: max_pages=%s, page_size=%d, delay=%.1fs, dry_run=%s",
        args.max_pages or "ALL", args.page_size, args.delay, args.dry_run,
    )

    # ── Phase 1: Scrape ──
    t0 = time.time()
    trades = congress_trading.scrape_capitol_trades(
        max_pages=args.max_pages,
        page_size=args.page_size,
        delay=args.delay,
    )
    scrape_time = time.time() - t0
    logger.info(
        "Scrape complete: %d trades in %.1f seconds (%.1f trades/sec)",
        len(trades), scrape_time, len(trades) / scrape_time if scrape_time > 0 else 0,
    )

    if not trades:
        logger.error("No trades scraped — aborting")
        sys.exit(1)

    # ── Phase 2: Extract members ──
    members = congress_trading.extract_members_from_trades(trades)
    logger.info("Extracted %d unique congress members", len(members))

    # Show summary
    parties = {}
    chambers = {}
    for m in members:
        parties[m.party] = parties.get(m.party, 0) + 1
        chambers[m.chamber] = chambers.get(m.chamber, 0) + 1
    logger.info("By party: %s", parties)
    logger.info("By chamber: %s", chambers)
    logger.info("Trade date range: %s to %s",
                min(t.trade_date for t in trades if t.trade_date),
                max(t.trade_date for t in trades if t.trade_date))

    if args.dry_run:
        logger.info("DRY RUN — skipping Supabase writes")
        logger.info("Top 10 members by trade count:")
        for m in members[:10]:
            logger.info(
                "  %s (%s-%s) %s — %d trades (%s to %s)",
                m.full_name, m.party[:1], m.chamber, m.state_abbr,
                m.total_trades, m.first_trade_date, m.last_trade_date,
            )
        return

    # ── Phase 3: Check Supabase connection ──
    if not supabase_cache.is_available():
        logger.error("Supabase not available — check SUPABASE_URL and SUPABASE_SERVICE_KEY env vars")
        sys.exit(1)

    # ── Safety check: warn if cold tables already have data ──
    existing_trades = supabase_cache.get_congress_recent_trades(limit=1)
    existing_members = supabase_cache.get_all_congress_members()
    n_existing_members = len(existing_members) if existing_members else 0
    n_existing_trades = 1 if existing_trades else 0  # proxy check

    if n_existing_trades > 0 or n_existing_members > 0:
        logger.warning(
            "⚠️  COLD TABLES ALREADY CONTAIN DATA  ⚠️\n"
            "    congress_members: %d rows\n"
            "    congress_trades:  %s\n"
            "  This is a write-once cold archive — existing rows will NOT be\n"
            "  overwritten (ON CONFLICT DO NOTHING).  Only genuinely new trade_ids\n"
            "  will be inserted.  Re-running is safe but unnecessary if data is\n"
            "  already loaded.",
            n_existing_members,
            "has data" if n_existing_trades else "empty",
        )
        if not os.environ.get("CONGRESS_LOAD_ALLOW_RERUN"):
            logger.info(
                "To proceed anyway, set CONGRESS_LOAD_ALLOW_RERUN=1\n"
                "  Example: CONGRESS_LOAD_ALLOW_RERUN=1 uv run python scripts/load_congress_history.py"
            )
            sys.exit(0)
        logger.info("CONGRESS_LOAD_ALLOW_RERUN=1 set — proceeding with load")

    # ── Phase 4: Upsert members first (FK constraint) ──
    logger.info("Upserting %d congress members...", len(members))
    member_rows = [m.to_db_row() for m in members]
    member_count = supabase_cache.upsert_congress_members(member_rows)
    logger.info("Members upserted: %d", member_count)

    # ── Phase 5: Insert trades ──
    logger.info("Inserting %d congress trades...", len(trades))
    trade_rows = [t.to_db_row() for t in trades]
    trade_count = supabase_cache.upsert_congress_trades(trade_rows)
    logger.info("Trades inserted: %d", trade_count)

    # ── Summary ──
    total_time = time.time() - t0
    logger.info("=== Load Complete ===")
    logger.info("  Members: %d", member_count)
    logger.info("  Trades:  %d", trade_count)
    logger.info("  Total time: %.1f seconds", total_time)


if __name__ == "__main__":
    main()
