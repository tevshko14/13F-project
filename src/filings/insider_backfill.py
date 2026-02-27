"""Historical backfill of open market insider purchases from OpenInsider.

Two modes:
  1. Global screener (default): paginate the screener with built-in date
     range filters (e.g. last 2 years).  Good for initial bulk load.
  2. Per-ticker (--per-ticker): scrape each ticker's dedicated page on
     OpenInsider (e.g. openinsider.com/AAPL).  This returns the FULL
     history for each ticker and is the only way to get deep historical
     data needed for forward-return analysis.

Usage:
    uv run filings-insider-backfill                 # global: last 2 years
    uv run filings-insider-backfill --days 365      # global: last 1 year
    uv run filings-insider-backfill --per-ticker    # per-ticker: all history
"""

from __future__ import annotations

import argparse
import logging
import time

import httpx

from filings import supabase_cache
from filings.insider_trading import (
    InsiderTrade,
    _parse_table,
    _HEADERS,
)

logger = logging.getLogger(__name__)

_OI_BASE = "http://openinsider.com"
_OI_SCREENER = f"{_OI_BASE}/screener"

# OpenInsider built-in trade-date options (td param):
#   0 = All dates, 1 = Today, 3 = Last 3 days, 7 = Last 1 week,
#   14 = Last 2 weeks, 30 = Last 1 month, 60 = Last 2 months,
#   90 = Last 3 months, 180 = Last 6 months, 365 = Last 1 year,
#   730 = Last 2 years, 1461 = Last 4 years
_VALID_TD = {0, 1, 3, 7, 14, 30, 60, 90, 180, 365, 730, 1461}


# ── Global screener scraping ─────────────────────────────────────


def _scrape_page(td: int, page: int = 1) -> list[InsiderTrade]:
    """Scrape open market purchases from OpenInsider for a date range.

    Args:
        td: Trade date filter (e.g. 730 = last 2 years).
        page: Page number (1-indexed).

    Returns list of InsiderTrade objects (purchases only).
    """
    params: dict[str, str] = {
        "s": "",           # ticker (empty = all)
        "o": "",           # owner type
        "pl": "",          # price low
        "ph": "",          # price high
        "st": "1",         # show only open market trades
        "tc": "1",         # confirmed trades only
        "t": "p",          # trade type: purchases only
        "td": str(td),     # trade date range
        "vl": "",          # value low
        "vh": "",          # value high
        "ocl": "",         # ownership change low
        "och": "",         # ownership change high
        "cnt": "100",      # max per page
        "page": str(page),
        "sortcol": "0",
        "o2d": "2",        # order by trade date desc
        "vf": "",
    }
    try:
        resp = httpx.get(
            _OI_SCREENER,
            params=params,
            headers=_HEADERS,
            timeout=30,
            follow_redirects=True,
        )
        resp.raise_for_status()
        trades = _parse_table(resp.text, has_company_col=True)
        # Double-check: only keep "Purchase" types
        return [t for t in trades if "Purchase" in t.trade_type]
    except Exception:
        logger.exception("Scrape failed (td=%d, page=%d)", td, page)
        return []


def backfill_insider_purchases(
    days: int = 730,
    delay: float = 2.5,
    max_pages: int = 200,
) -> dict:
    """Scrape historical open market purchases from the global screener.

    Uses OpenInsider's built-in date range filters and paginates
    through all results.

    Args:
        days: Trade date lookback (730 = 2 years, 365 = 1 year, etc.).
              Must be one of OpenInsider's built-in values.
        delay: Seconds to wait between HTTP requests.
        max_pages: Safety cap on pagination.

    Returns dict with stats: {"pages": N, "scraped": N, "upserted": N}.
    """
    # Map days to nearest valid td value
    td = min(_VALID_TD, key=lambda x: abs(x - days) if x > 0 else float("inf"))
    if td == 0:
        td = 730  # fallback to 2 years

    logger.info(
        "Starting global backfill: td=%d (%d days of open market purchases)",
        td, td,
    )

    total_scraped = 0
    total_upserted = 0
    pages_processed = 0

    for page_num in range(1, max_pages + 1):
        trades = _scrape_page(td, page=page_num)

        if not trades:
            logger.info("Page %d: empty — stopping", page_num)
            break

        rows = [t.to_db_row() for t in trades if t.sec_url]
        if rows:
            upserted = supabase_cache.upsert_insider_trades(rows)
            total_upserted += upserted
            logger.info(
                "Page %d: %d purchases scraped, %d upserted",
                page_num, len(trades), upserted,
            )

        total_scraped += len(trades)
        pages_processed += 1

        # If we got fewer than a full page, we've reached the end
        if len(trades) < 100:
            logger.info("Partial page (%d trades) — end of results", len(trades))
            break

        time.sleep(delay)

    result = {
        "pages": pages_processed,
        "scraped": total_scraped,
        "upserted": total_upserted,
    }
    logger.info("Global backfill complete: %s", result)
    return result


# ── Per-ticker scraping ───────────────────────────────────────────


def _scrape_ticker_page(ticker: str) -> list[InsiderTrade]:
    """Scrape ALL insider trades for a single ticker from OpenInsider.

    Uses the direct ticker page (e.g. openinsider.com/AAPL) which
    returns the full history.  The table has NO company column, so
    we parse with has_company_col=False.

    Returns ALL trade types (purchases, sales, etc.) — caller filters.
    """
    url = f"{_OI_BASE}/{ticker.upper()}"
    try:
        resp = httpx.get(
            url,
            headers=_HEADERS,
            timeout=30,
            follow_redirects=True,
        )
        resp.raise_for_status()
        return _parse_table(resp.text, has_company_col=False)
    except Exception:
        logger.exception("Per-ticker scrape failed for %s", ticker)
        return []


def backfill_per_ticker(
    tickers: list[str] | None = None,
    delay: float = 2.0,
    purchases_only: bool = False,
) -> dict:
    """Scrape full history for each ticker from OpenInsider.

    If tickers is None, fetches the list of all unique tickers from
    the insider_trades table in Supabase.

    Args:
        tickers: List of tickers to scrape.  None = all from DB.
        delay: Seconds to wait between HTTP requests.
        purchases_only: If True, only keep purchases.  If False,
            upsert all trade types (useful for enriching the DB).

    Returns dict with stats.
    """
    if tickers is None:
        tickers = supabase_cache.get_distinct_insider_tickers()
        if not tickers:
            logger.error("No tickers found in DB — run global backfill first")
            return {"tickers": 0, "scraped": 0, "upserted": 0, "errors": 0}

    total_tickers = len(tickers)
    total_scraped = 0
    total_upserted = 0
    errors = 0

    logger.info("Starting per-ticker backfill for %d tickers", total_tickers)

    for i, ticker in enumerate(tickers, 1):
        try:
            trades = _scrape_ticker_page(ticker)

            if not trades:
                logger.info(
                    "[%d/%d] %s: no trades found", i, total_tickers, ticker
                )
                time.sleep(delay)
                continue

            if purchases_only:
                trades = [t for t in trades if "Purchase" in t.trade_type]

            rows = [t.to_db_row() for t in trades if t.sec_url]
            if rows:
                upserted = supabase_cache.upsert_insider_trades(rows)
                total_upserted += upserted
                # Count purchases specifically for logging
                purchases = sum(1 for t in trades if "Purchase" in t.trade_type)
                logger.info(
                    "[%d/%d] %s: %d trades (%d purchases), %d upserted",
                    i, total_tickers, ticker, len(trades), purchases, upserted,
                )
            else:
                logger.info(
                    "[%d/%d] %s: %d trades but none with SEC URL",
                    i, total_tickers, ticker, len(trades),
                )

            total_scraped += len(trades)

        except Exception:
            logger.exception("[%d/%d] %s: unexpected error", i, total_tickers, ticker)
            errors += 1

        time.sleep(delay)

    result = {
        "tickers": total_tickers,
        "scraped": total_scraped,
        "upserted": total_upserted,
        "errors": errors,
    }
    logger.info("Per-ticker backfill complete: %s", result)
    return result


# ── CLI entry point ──────────────────────────────────────────────


def main() -> None:
    # Load .env for local development (CLI scripts don't get it from web.py)
    import os
    from pathlib import Path
    if not os.environ.get("RAILWAY_ENVIRONMENT"):
        env_file = Path(__file__).resolve().parents[2] / ".env"
        if env_file.exists():
            from dotenv import load_dotenv
            load_dotenv(env_file)

    parser = argparse.ArgumentParser(
        description="Backfill historical insider purchases from OpenInsider"
    )
    parser.add_argument(
        "--per-ticker", action="store_true",
        help="Scrape each ticker's dedicated page for full history "
             "(slower but gets deep historical data)",
    )
    parser.add_argument(
        "--days", type=int, default=730,
        help="(Global mode) Lookback period in days (default: 730 = 2 years). "
             "Valid: 30, 60, 90, 180, 365, 730, 1461",
    )
    parser.add_argument(
        "--delay", type=float, default=2.0,
        help="Delay between HTTP requests in seconds (default: 2.0)",
    )
    parser.add_argument(
        "--max-pages", type=int, default=200,
        help="(Global mode) Safety cap on number of pages (default: 200)",
    )
    parser.add_argument(
        "--all-types", action="store_true",
        help="(Per-ticker mode) Upsert all trade types, not just purchases",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    if args.per_ticker:
        result = backfill_per_ticker(
            delay=args.delay,
            purchases_only=not args.all_types,
        )
        print(f"\n{'='*55}")
        print("Per-Ticker Backfill Summary")
        print(f"{'='*55}")
        print(f"Tickers processed: {result['tickers']}")
        print(f"Trades scraped:    {result['scraped']}")
        print(f"Rows upserted:     {result['upserted']}")
        print(f"Errors:            {result['errors']}")
        print(f"{'='*55}")
    else:
        result = backfill_insider_purchases(
            days=args.days,
            delay=args.delay,
            max_pages=args.max_pages,
        )
        print(f"\n{'='*55}")
        print("Global Backfill Summary")
        print(f"{'='*55}")
        print(f"Pages processed:  {result['pages']}")
        print(f"Trades scraped:   {result['scraped']}")
        print(f"Rows upserted:    {result['upserted']}")
        print(f"{'='*55}")


if __name__ == "__main__":
    main()
