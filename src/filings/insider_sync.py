"""Insider trading sync worker -- scrapes OpenInsider into Supabase.

Designed to run as a Railway Cron Job every 30 minutes.
Scrapes the global screener (all / purchases / sales), upserts
into the dedicated ``insider_trades`` table, and logs to ``sync_logs``.

Usage:
    uv run filings-insider-sync
"""

import logging
import os
import time
import uuid

import httpx

from filings import insider_trading, supabase_cache

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
            "s": "", "o": "", "pl": "", "ph": "",
            "st": "0", "tc": "1",
            "t": trade_type,
            "vf": "", "o2d": "2", "sortcol": "0",
            "cnt": "100", "page": "1",
        }
        try:
            resp = httpx.get(
                url, params=params,
                headers=insider_trading._HEADERS,
                timeout=20, follow_redirects=True,
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
                len(trades), label, new_count,
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

    # Build row dicts and upsert
    rows = [t.to_db_row() for t in trades if t.sec_url]
    upserted = supabase_cache.upsert_insider_trades(rows)

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
        len(trades), upserted,
    )
    return {"scraped": len(trades), "upserted": upserted, "errors": len(errors)}


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
