"""Congress trades sync worker — scrapes Capitol Trades into Supabase.

Designed to run as a Railway Cron Job (daily at 06:00 UTC by default).
Scrapes Capitol Trades' paginated /trades listing, upserts new trades
into ``congress_trades`` (cold, write-once via ON CONFLICT DO NOTHING),
and refreshes ``congress_members`` with derived metadata (last_trade_date,
total_trades).  Appends a summary row to ``congress_sync_log`` for ops
health monitoring.

Usage:
    uv run filings-congress-sync

Why this exists: the v2 redesign work landed without a congress sync
entry point — the ``congress_trading`` module had the scraper function
(``scrape_capitol_trades``) but nothing called it as a job.  The
Railway "Congress Trades Chron Job" was configured but had no script
to run, so the table sat unchanged since the initial population
(2026-03-01).  This module restores the daily refresh.
"""

import logging
import time

from filings import congress_trading, supabase_cache
from filings.log_config import setup_worker_logging

logger = logging.getLogger(__name__)


# Daily run scrapes the recent window.  Capitol Trades lists newest-first
# and ``scrape_capitol_trades`` stops paginating once a whole page falls
# below ``min_trade_date`` — so a 60-day cutoff covers any reasonable
# disclosure lag (STOCK Act requires filing within 45 days) without
# rescraping the entire 4-year archive on every cycle.
_MIN_TRADE_DAYS = 60


def _format_min_trade_date() -> str:
    """Return today minus ``_MIN_TRADE_DAYS`` as ``YYYY-MM-DD``."""
    from datetime import datetime, timedelta, timezone
    cutoff = datetime.now(timezone.utc) - timedelta(days=_MIN_TRADE_DAYS)
    return cutoff.strftime("%Y-%m-%d")


def sync_congress_trades() -> dict:
    """Scrape Capitol Trades and upsert into Supabase.

    Returns a summary dict ``{"scraped": N, "new_trades": N,
    "new_members": N, "status": ...}`` for logging.
    """
    started = time.time()
    min_date = _format_min_trade_date()
    logger.info(
        "=== Congress sync starting (scraping trades since %s) ===", min_date,
    )

    pages_scraped = 0
    new_trades_count = 0
    new_members_count = 0
    error_message: str | None = None

    try:
        trades = congress_trading.scrape_capitol_trades(min_trade_date=min_date)
        pages_scraped = max(1, (len(trades) + 99) // 100)  # rough estimate
        logger.info("Scraped %d trades across ~%d pages", len(trades), pages_scraped)

        if not trades:
            logger.warning("No trades scraped — Capitol Trades may have changed format")
            error_message = "No trades returned from scrape"
            duration = time.time() - started
            supabase_cache.insert_congress_sync_log(
                status="failed", pages_scraped=0,
                new_trades=0, new_members=0,
                duration_secs=duration, error_message=error_message,
            )
            return {
                "scraped": 0, "new_trades": 0, "new_members": 0,
                "status": "failed", "error": error_message,
            }

        # Upsert trades — ON CONFLICT (trade_id) DO NOTHING means
        # only genuinely new rows are inserted.  The returned count
        # is the number of rows the upsert call processed (we don't
        # have a precise "newly-inserted" count from PostgREST without
        # a RETURNING-style query; use the call count as a proxy and
        # surface the rough new-vs-total in the log).
        trade_rows = [t.to_db_row() for t in trades if t.trade_id]
        new_trades_count = supabase_cache.upsert_congress_trades(trade_rows)
        logger.info(
            "Upserted %d trade rows (existing rows skipped by ON CONFLICT DO NOTHING)",
            new_trades_count,
        )

        # Derive member metadata from the scraped trades and refresh
        # ``congress_members`` (ON CONFLICT updates last_trade_date,
        # total_trades) — keeps the politician-profile views fresh
        # without a separate sync.
        members = congress_trading.extract_members_from_trades(trades)
        if members:
            member_rows = [m.to_db_row() for m in members]
            new_members_count = supabase_cache.upsert_congress_members(member_rows)
            logger.info("Refreshed %d congress_members rows", new_members_count)

        status = "success" if new_trades_count > 0 else "partial"

    except Exception as exc:
        logger.exception("Congress sync failed")
        error_message = f"{type(exc).__name__}: {exc}"
        status = "failed"

    duration = time.time() - started
    supabase_cache.insert_congress_sync_log(
        status=status,
        pages_scraped=pages_scraped,
        new_trades=new_trades_count,
        new_members=new_members_count,
        duration_secs=duration,
        error_message=error_message,
    )

    summary = {
        "scraped":      pages_scraped,
        "new_trades":   new_trades_count,
        "new_members":  new_members_count,
        "status":       status,
        "duration_secs": round(duration, 1),
    }
    if error_message:
        summary["error"] = error_message
    return summary


def main() -> None:
    """Entry point for ``uv run filings-congress-sync``."""
    setup_worker_logging()
    logger.info("=== PaperPanda Congress Trading Sync starting ===")
    start = time.time()

    result = sync_congress_trades()

    elapsed = round(time.time() - start)
    logger.info("=== Congress sync finished in %ds: %s ===", elapsed, result)


if __name__ == "__main__":
    main()
