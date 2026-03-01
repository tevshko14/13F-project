#!/usr/bin/env python3
"""Daily incremental sync of Capitol Trades data into Supabase.

Designed to run as a Railway Cron Service every 24 hours.  Scrapes the
newest pages from Capitol Trades until it hits trades we already have,
upserts any new members/trades, and logs every run to the
``congress_sync_log`` table for monitoring.

The script is fully idempotent:
  - ``congress_trades`` uses ON CONFLICT DO NOTHING (write-once cold archive)
  - ``congress_members`` uses ON CONFLICT DO UPDATE (refresh metadata)
  - ``congress_sync_log`` is append-only

Usage:
    uv run python scripts/sync_congress_trades.py [--max-pages N] [--dry-run]

Environment:
    SUPABASE_URL, SUPABASE_SERVICE_KEY must be set.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import datetime, timezone

# Ensure the src directory is on the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from filings import congress_trading, supabase_cache


def _setup_logging() -> None:
    fmt = "%(asctime)s %(levelname)-8s %(name)s -- %(message)s"
    logging.basicConfig(level=logging.INFO, format=fmt, force=True)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


logger = logging.getLogger(__name__)


# ── Sync log helpers ────────────────────────────────────────────────


def _create_sync_log() -> int | None:
    """Insert a new 'running' row into congress_sync_log and return its id."""
    client = supabase_cache._get_client()
    if client is None:
        return None
    try:
        resp = (
            client.table("congress_sync_log")
            .insert({"status": "running"})
            .execute()
        )
        rows = resp.data or []
        return rows[0]["id"] if rows else None
    except Exception as exc:
        logger.warning("Failed to create sync log: %s", exc)
        return None


def _update_sync_log(
    log_id: int,
    *,
    status: str,
    pages_scraped: int = 0,
    new_trades: int = 0,
    new_members: int = 0,
    error_message: str | None = None,
    duration_secs: float = 0.0,
) -> None:
    """Update an existing sync log row with final results."""
    client = supabase_cache._get_client()
    if client is None:
        return
    try:
        client.table("congress_sync_log").update({
            "status": status,
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "pages_scraped": pages_scraped,
            "new_trades": new_trades,
            "new_members": new_members,
            "error_message": error_message,
            "duration_secs": round(duration_secs, 2),
        }).eq("id", log_id).execute()
    except Exception as exc:
        logger.warning("Failed to update sync log %d: %s", log_id, exc)


# ── Incremental scrape (stop on known trades) ──────────────────────


def _get_known_trade_ids(limit: int = 500) -> set[str]:
    """Fetch the most recent trade_ids from Supabase for dedup.

    We only need the last few hundred — the sync scrapes newest-first
    and stops when it hits a known trade.
    """
    client = supabase_cache._get_client()
    if client is None:
        return set()
    try:
        resp = (
            client.table("congress_trades")
            .select("trade_id")
            .order("filing_date", desc=True)
            .limit(limit)
            .execute()
        )
        return {r["trade_id"] for r in (resp.data or [])}
    except Exception as exc:
        logger.warning("Failed to fetch known trade IDs: %s", exc)
        return set()


def incremental_scrape(
    max_pages: int = 20,
    page_size: int = 100,
    delay: float = 2.0,
) -> tuple[list, int]:
    """Scrape Capitol Trades newest-first, stopping when we hit known trades.

    Returns (new_trades, pages_scraped).

    The stop heuristic: if an entire page yields zero new trade_ids we
    assume we've caught up.  This is conservative — a single page with
    all known trades means everything before is also known.
    """
    import httpx

    known_ids = _get_known_trade_ids(limit=1000)
    logger.info("Loaded %d known trade IDs for dedup", len(known_ids))

    all_new: list = []
    seen_ids: set[str] = set()
    pages_scraped = 0

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }

    with httpx.Client(headers=headers, timeout=30.0, follow_redirects=True) as client:
        page = 1
        while page <= max_pages:
            url = f"https://www.capitoltrades.com/trades?page={page}&pageSize={page_size}"
            logger.info("Scraping page %d: %s", page, url)

            try:
                resp = client.get(url)
                resp.raise_for_status()
            except httpx.HTTPError as exc:
                logger.error("HTTP error on page %d: %s", page, exc)
                break

            trade_objs = congress_trading._extract_next_data(resp.text)
            pages_scraped += 1

            if not trade_objs:
                logger.info("No trades found on page %d — end of data", page)
                break

            new_on_page = 0
            for obj in trade_objs:
                trade = congress_trading._parse_trade_json(obj)
                if trade is None:
                    continue
                if trade.trade_id in seen_ids:
                    continue
                seen_ids.add(trade.trade_id)

                if trade.trade_id in known_ids:
                    # Already in DB — skip but don't stop yet (might be
                    # interleaved with new filings on the same page)
                    continue

                all_new.append(trade)
                new_on_page += 1

            logger.info(
                "Page %d: %d new trades (%d total new so far)",
                page, new_on_page, len(all_new),
            )

            # Stop heuristic: entire page was already known
            if new_on_page == 0:
                logger.info("Page %d had no new trades — caught up, stopping", page)
                break

            page += 1
            if delay > 0:
                time.sleep(delay)

    logger.info(
        "Incremental scrape complete: %d new trades across %d pages",
        len(all_new), pages_scraped,
    )
    return all_new, pages_scraped


# ── Main ────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Daily incremental sync of Capitol Trades → Supabase"
    )
    parser.add_argument(
        "--max-pages", type=int, default=20,
        help="Max pages to scrape before stopping (default: 20)",
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

    logger.info("=== Congress Trades Daily Sync ===")
    logger.info("Settings: max_pages=%d, delay=%.1fs, dry_run=%s",
                args.max_pages, args.delay, args.dry_run)

    # ── Check Supabase connection ──
    if not supabase_cache.is_available():
        logger.error("Supabase not available — check env vars")
        sys.exit(1)

    # ── Create sync log entry ──
    log_id = None
    if not args.dry_run:
        log_id = _create_sync_log()
        if log_id:
            logger.info("Sync log created: id=%d", log_id)

    t0 = time.time()
    error_msg = None

    try:
        # ── Phase 1: Incremental scrape ──
        new_trades, pages_scraped = incremental_scrape(
            max_pages=args.max_pages,
            delay=args.delay,
        )

        if not new_trades:
            logger.info("No new trades found — nothing to do")
            duration = time.time() - t0
            if log_id:
                _update_sync_log(
                    log_id, status="success",
                    pages_scraped=pages_scraped,
                    new_trades=0, new_members=0,
                    duration_secs=duration,
                )
            return

        if args.dry_run:
            logger.info("DRY RUN — %d new trades would be inserted", len(new_trades))
            for t in new_trades[:10]:
                logger.info(
                    "  %s %s %s %s %s",
                    t.filing_date, t.politician_name, t.trade_type,
                    t.ticker or t.asset_name, t.amount_display,
                )
            return

        # ── Phase 2: Extract & upsert new members ──
        members = congress_trading.extract_members_from_trades(new_trades)
        logger.info("Extracted %d members from new trades", len(members))

        member_rows = [m.to_db_row() for m in members]
        member_count = supabase_cache.upsert_congress_members(member_rows)
        logger.info("Members upserted: %d", member_count)

        # ── Phase 3: Insert new trades ──
        trade_rows = [t.to_db_row() for t in new_trades]
        trade_count = supabase_cache.upsert_congress_trades(trade_rows)
        logger.info("Trades inserted: %d", trade_count)

        # ── Summary ──
        duration = time.time() - t0
        logger.info("=== Sync Complete ===")
        logger.info("  Pages scraped: %d", pages_scraped)
        logger.info("  New members: %d", member_count)
        logger.info("  New trades: %d", trade_count)
        logger.info("  Duration: %.1f seconds", duration)

        if log_id:
            _update_sync_log(
                log_id, status="success",
                pages_scraped=pages_scraped,
                new_trades=trade_count,
                new_members=member_count,
                duration_secs=duration,
            )

    except Exception as exc:
        duration = time.time() - t0
        error_msg = str(exc)[:500]
        logger.exception("Sync failed: %s", exc)
        if log_id:
            _update_sync_log(
                log_id, status="error",
                pages_scraped=0,
                error_message=error_msg,
                duration_secs=duration,
            )
        sys.exit(1)


if __name__ == "__main__":
    main()
