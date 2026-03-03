"""Insider trading sync worker -- scrapes OpenInsider into Supabase.

Designed to run as a Railway Cron Job every 30 minutes.
Scrapes the global screener (all / purchases / sales), upserts
into the dedicated ``insider_trades`` table (hot, 30-day retention),
and bridges purchases into ``insider_purchases_history`` (cold,
permanent, delete-protected) so insights stay up to date automatically.

Usage:
    uv run filings-insider-sync
"""

import logging
import os
import time
import uuid

import httpx

from filings import insider_trading, notifications, supabase_cache

# ── Logging ──────────────────────────────────────────────────────────


def _setup_logging() -> None:
    log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
    if os.environ.get("RAILWAY_ENVIRONMENT"):
        fmt = '{"time":"%(asctime)s","level":"%(levelname)s","name":"%(name)s","msg":"%(message)s"}'
    else:
        fmt = "%(asctime)s %(levelname)-8s %(name)s -- %(message)s"
    logging.basicConfig(level=log_level, format=fmt, force=True)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


logger = logging.getLogger(__name__)

# Delay between OpenInsider requests (seconds) -- be polite
_OI_DELAY = 3.0

# Max insider trade notifications per sync cycle (prevent flood)
_INSIDER_NOTIF_MAX = 10


# ── Scrape logic ─────────────────────────────────────────────────────


def _scrape_global_trades() -> list[insider_trading.InsiderTrade]:
    """Scrape all three trade type pages from OpenInsider global screener.

    Deduplicates by ``sec_url`` across the three requests.
    Returns a combined list of unique trades.
    """
    all_trades: list[insider_trading.InsiderTrade] = []
    seen_urls: set[str] = set()

    filters = [
        ("", "all"),
        ("p", "purchases"),
        ("s", "sales"),
    ]

    for trade_type, label in filters:
        url = f"{insider_trading._OI_BASE}/screener"
        params: dict[str, str] = {
            "s": "",
            "o": "",
            "pl": "",
            "ph": "",
            "st": "0",
            "tc": "1",
            "t": trade_type,
            "vf": "",
            "o2d": "2",
            "sortcol": "0",
            "cnt": "100",
            "page": "1",
        }
        try:
            resp = httpx.get(
                url,
                params=params,
                headers=insider_trading._HEADERS,
                timeout=20,
                follow_redirects=True,
            )
            resp.raise_for_status()
            trades = insider_trading._parse_table(resp.text, has_company_col=True)
            new_count = 0
            for t in trades:
                if t.sec_url and t.sec_url not in seen_urls:
                    seen_urls.add(t.sec_url)
                    all_trades.append(t)
                    new_count += 1
            logger.info(
                "Scraped %d %s trades (%d new unique)",
                len(trades),
                label,
                new_count,
            )
        except Exception:
            logger.exception("Failed to scrape OpenInsider %s", label)

        # Rate limit between requests
        time.sleep(_OI_DELAY)

    return all_trades


# ── Sync logic ───────────────────────────────────────────────────────


def sync_insider_trades() -> dict:
    """Main sync: scrape OpenInsider, upsert to Supabase ``insider_trades``.

    Returns a summary dict.
    """
    run_id = f"insider-sync-{uuid.uuid4().hex[:8]}"
    supabase_cache.create_sync_log(run_id)

    trades = _scrape_global_trades()
    logger.info("Total unique trades scraped: %d", len(trades))

    if not trades:
        supabase_cache.complete_sync_log(run_id, 0, 1, 0, ["No trades scraped"])
        return {"scraped": 0, "upserted": 0, "errors": 1}

    # Identify genuinely new trades before upserting (for notifications)
    existing_urls = supabase_cache.get_existing_insider_urls()
    new_trades = [t for t in trades if t.sec_url and t.sec_url not in existing_urls]
    logger.info("New trades (not in Supabase): %d", len(new_trades))

    # Build row dicts and upsert to hot table
    rows = [t.to_db_row() for t in trades if t.sec_url]
    upserted = supabase_cache.upsert_insider_trades(rows)

    # Bridge: populate cold history tables.
    # ON CONFLICT DO NOTHING means existing rows are harmlessly skipped.
    trades_with_url = [t for t in trades if t.sec_url]

    # 1. All-types cold table (insider_trades_history) — full archive
    if trades_with_url:
        full_history_rows = [t.to_full_history_row() for t in trades_with_url]
        full_upserted = supabase_cache.upsert_history_trades(full_history_rows)
        logger.info(
            "Cold table (all types): %d trades → %d upserted",
            len(full_history_rows), full_upserted,
        )

    # 2. Purchases-only cold table (insider_purchases_history) — for insights
    purchase_trades = [t for t in trades_with_url if "Purchase" in t.trade_type]
    if purchase_trades:
        history_rows = [t.to_history_row() for t in purchase_trades]
        history_upserted = supabase_cache.upsert_history_purchases(history_rows)
        logger.info(
            "Cold table (purchases): %d → %d upserted",
            len(history_rows), history_upserted,
        )

    errors: list[str] = []
    if upserted == 0:
        errors.append("Upsert returned 0 rows")

    supabase_cache.complete_sync_log(
        run_id,
        funds_updated=upserted,
        funds_failed=0 if upserted else 1,
        funds_skipped=len(trades) - upserted,
        errors=errors,
    )

    logger.info(
        "Insider sync complete: %d scraped, %d upserted",
        len(trades),
        upserted,
    )

    # Emit notifications for notable new trades
    if new_trades:
        _emit_insider_notifications(new_trades)

    # Run retention cleanup after successful sync
    try:
        cleanup = supabase_cache.run_retention_cleanup()
        logger.info("Post-sync retention cleanup: %s", cleanup)
    except Exception:
        logger.debug("Retention cleanup failed (non-fatal)")

    return {"scraped": len(trades), "upserted": upserted, "errors": len(errors)}


# ── Notifications ────────────────────────────────────────────────────


def _emit_insider_notifications(
    new_trades: list[insider_trading.InsiderTrade],
) -> None:
    """Create notifications for notable new insider trades.

    Only trades meeting the value thresholds in
    :func:`notifications.create_insider_trade_notification` generate
    notifications.  Capped at ``_INSIDER_NOTIF_MAX`` per sync cycle.
    """
    notif_rows: list[dict] = []
    for trade in new_trades:
        row = trade.to_db_row()
        notif = notifications.create_insider_trade_notification(row)
        if notif:
            notif_rows.append(notif)

    # Cap to prevent notification flood
    notif_rows = notif_rows[:_INSIDER_NOTIF_MAX]

    if notif_rows:
        n = supabase_cache.upsert_notifications(notif_rows)
        logger.info("Created %d insider trade notifications", n)
    else:
        logger.info("No notable insider trades for notifications")


# ── Entry point ──────────────────────────────────────────────────────


def main() -> None:
    """Entry point for ``uv run filings-insider-sync``."""
    _setup_logging()
    logger.info("=== PaperPanda Insider Trading Sync starting ===")
    start = time.time()

    result = sync_insider_trades()

    elapsed = round(time.time() - start)
    logger.info("=== Insider sync finished in %ds: %s ===", elapsed, result)


if __name__ == "__main__":
    main()
