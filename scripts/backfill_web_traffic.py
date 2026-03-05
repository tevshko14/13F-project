#!/usr/bin/env python3
"""Backfill historical web traffic data into Supabase cold storage.

Backfills two sources that have historical data available:

1. **Wikipedia Page Views** — daily page views back to July 2015.
   The Wikimedia API allows requesting up to ~1 year per call, so we
   chunk by year and work backwards from today.

2. **Tranco List** — daily top-1M domain rankings. Historical lists are
   available at https://tranco-list.eu/download/<list_id>/full by date.
   We use the dated download endpoint to fetch past snapshots.

Cloudflare Radar has no historical API, so it is collect-going-forward only.

Usage:
    uv run python scripts/backfill_web_traffic.py [--source wikipedia|tranco|all]
        [--years N] [--limit N] [--dry-run]

Examples:
    # Backfill 5 years of Wikipedia data for all relevant tickers
    uv run python scripts/backfill_web_traffic.py --source wikipedia --years 5

    # Dry run — show what would be inserted
    uv run python scripts/backfill_web_traffic.py --dry-run

    # Backfill just 2 tickers for testing
    uv run python scripts/backfill_web_traffic.py --source wikipedia --limit 2

Environment:
    SUPABASE_URL, SUPABASE_SERVICE_KEY must be set.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone

import httpx

# Ensure the src directory is on the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from filings import supabase_cache, web_traffic

# ── Logging ──────────────────────────────────────────────────────────


def _setup_logging() -> None:
    fmt = "%(asctime)s %(levelname)-8s %(name)s -- %(message)s"
    logging.basicConfig(level=logging.INFO, format=fmt, force=True)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


logger = logging.getLogger(__name__)

# Polite delay between API calls (seconds)
_FETCH_DELAY = 1.0

# Wikipedia API limits: max ~1 year of daily data per request
_WIKI_CHUNK_DAYS = 365
_WIKI_API = "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article"


# ── Upsert helper ───────────────────────────────────────────────────


def _upsert_snapshot(
    ticker: str, source: str, data: dict, snapshot_date: str, *, dry_run: bool = False
) -> bool:
    """Upsert a single snapshot row. Returns True on success."""
    if dry_run:
        return True

    client = supabase_cache._get_client()
    if client is None:
        logger.error("No Supabase client — cannot store snapshot")
        return False

    try:
        client.table("web_traffic_history").upsert(
            {
                "ticker": ticker,
                "snapshot_date": snapshot_date,
                "source": source,
                "data": json.dumps(data) if isinstance(data, dict) else data,
            },
            on_conflict="ticker,snapshot_date,source",
        ).execute()
        return True
    except Exception as e:
        logger.warning("Upsert failed for %s/%s/%s: %s", ticker, source, snapshot_date, e)
        return False


# ── Check what's already backfilled ─────────────────────────────────


def _get_existing_dates(ticker: str, source: str) -> set[str]:
    """Fetch dates already stored for a ticker+source. Returns set of 'YYYY-MM-DD'."""
    client = supabase_cache._get_client()
    if client is None:
        return set()

    try:
        resp = (
            client.table("web_traffic_history")
            .select("snapshot_date")
            .eq("ticker", ticker)
            .eq("source", source)
            .execute()
        )
        return {row["snapshot_date"] for row in (resp.data or [])}
    except Exception as e:
        logger.warning("Failed to check existing dates for %s/%s: %s", ticker, source, e)
        return set()


# ═════════════════════════════════════════════════════════════════════
# Wikipedia backfill
# ═════════════════════════════════════════════════════════════════════


def _fetch_wiki_chunk(article: str, start: datetime, end: datetime) -> list[dict]:
    """Fetch one chunk of Wikipedia daily page views.

    Returns list of {"date": "YYYY-MM-DD", "views": int}.
    """
    start_str = start.strftime("%Y%m%d00")
    end_str = end.strftime("%Y%m%d00")

    url = f"{_WIKI_API}/en.wikipedia/all-access/all-agents/{article}/daily/{start_str}/{end_str}"

    try:
        resp = httpx.get(url, timeout=15, headers={
            "User-Agent": "PaperPanda/1.0 (https://paperpanda.io; mailto:contact@paperpanda.io) httpx/0.27",
            "Accept": "application/json",
        })
        if resp.status_code != 200:
            logger.warning("Wikipedia API returned %d for %s (%s to %s)",
                           resp.status_code, article, start_str, end_str)
            return []

        raw = resp.json()
        items = raw.get("items", [])
        return [
            {
                "date": f"{it['timestamp'][:4]}-{it['timestamp'][4:6]}-{it['timestamp'][6:8]}",
                "views": it["views"],
            }
            for it in items
        ]
    except Exception as e:
        logger.warning("Wikipedia fetch failed for %s: %s", article, e)
        return []


def backfill_wikipedia(ticker: str, years: int = 5, *, dry_run: bool = False) -> int:
    """Backfill Wikipedia page views for one ticker.

    Fetches data in yearly chunks, working backwards from today.
    Stores one row per day in web_traffic_history with source='wikipedia'.

    Returns number of new snapshots inserted.
    """
    article = web_traffic._TICKER_TO_WIKI.get(ticker.upper())
    if not article:
        logger.debug("No Wikipedia article mapped for %s", ticker)
        return 0

    # Check what dates we already have
    existing = _get_existing_dates(ticker, "wikipedia")
    logger.info("  %s: %d existing Wikipedia snapshots", ticker, len(existing))

    inserted = 0
    now = datetime.now(timezone.utc)

    for year_offset in range(years):
        chunk_end = now - timedelta(days=year_offset * _WIKI_CHUNK_DAYS)
        chunk_start = chunk_end - timedelta(days=_WIKI_CHUNK_DAYS)

        # Don't go before Wikipedia pageview API start (July 2015)
        if chunk_start < datetime(2015, 7, 1, tzinfo=timezone.utc):
            chunk_start = datetime(2015, 7, 1, tzinfo=timezone.utc)

        if chunk_start >= chunk_end:
            break

        logger.info("  %s: Fetching %s to %s",
                     ticker, chunk_start.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d"))

        daily_views = _fetch_wiki_chunk(article, chunk_start, chunk_end)
        if not daily_views:
            time.sleep(_FETCH_DELAY)
            continue

        for day in daily_views:
            date_str = day["date"]
            if date_str in existing:
                continue  # Already have this day

            snapshot = {
                "source": "wikipedia",
                "article": article,
                "views": day["views"],
                "fetched_at": now.isoformat(),
            }

            ok = _upsert_snapshot(ticker, "wikipedia", snapshot, date_str, dry_run=dry_run)
            if ok:
                inserted += 1
                existing.add(date_str)

        time.sleep(_FETCH_DELAY)

    return inserted


# ═════════════════════════════════════════════════════════════════════
# Tranco historical backfill
# ═════════════════════════════════════════════════════════════════════
#
# Tranco publishes a new list daily. Historical lists can be fetched at:
#   https://tranco-list.eu/top-1m-incl-subdomains.csv.zip (latest)
#   https://tranco-list.eu/download/<list_id>/full (specific list)
#
# The list ID lookup: https://tranco-list.eu/daily_list?date=YYYY-MM-DD
# returns JSON with {"list_id": "XXXXX"} which we can then download.

_TRANCO_DATE_API = "https://tranco-list.eu/daily_list"


def _fetch_tranco_for_date(date_str: str, domains: set[str]) -> dict[str, int]:
    """Fetch Tranco list for a specific date. Returns {domain: rank} for requested domains only."""
    import csv
    import io
    import zipfile

    # Step 1: Get the list ID for this date
    try:
        resp = httpx.get(
            _TRANCO_DATE_API,
            params={"date": date_str},
            timeout=10,
            follow_redirects=True,
        )
        if resp.status_code != 200:
            logger.debug("Tranco date lookup returned %d for %s", resp.status_code, date_str)
            return {}

        list_data = resp.json()
        list_id = list_data.get("list_id")
        if not list_id:
            return {}
    except Exception as e:
        logger.debug("Tranco date lookup failed for %s: %s", date_str, e)
        return {}

    # Step 2: Download that list
    try:
        dl_url = f"https://tranco-list.eu/download/{list_id}/full"
        resp = httpx.get(dl_url, timeout=30, follow_redirects=True)
        if resp.status_code != 200:
            return {}

        # Parse CSV (may be zipped or plain)
        content = resp.content
        ranks: dict[str, int] = {}

        if content[:2] == b"PK":  # ZIP file
            z = zipfile.ZipFile(io.BytesIO(content))
            csv_name = z.namelist()[0]
            with z.open(csv_name) as f:
                reader = csv.reader(io.TextIOWrapper(f, encoding="utf-8"))
                for row in reader:
                    if len(row) >= 2 and row[1].strip() in domains:
                        try:
                            ranks[row[1].strip()] = int(row[0])
                        except (ValueError, IndexError):
                            continue
        else:
            reader = csv.reader(io.StringIO(content.decode("utf-8")))
            for row in reader:
                if len(row) >= 2 and row[1].strip() in domains:
                    try:
                        ranks[row[1].strip()] = int(row[0])
                    except (ValueError, IndexError):
                        continue

        return ranks

    except Exception as e:
        logger.debug("Tranco download failed for list %s: %s", list_id, e)
        return {}


def backfill_tranco(
    tickers: list[str],
    years: int = 2,
    sample_interval: int = 7,
    *,
    dry_run: bool = False,
) -> int:
    """Backfill Tranco rankings for all tickers at once.

    Since Tranco requires downloading the full 1M-domain CSV per date,
    we batch all tickers together and sample every `sample_interval` days
    (default: weekly) to be efficient.

    Returns total number of new snapshots inserted.
    """
    # Build domain set
    domains: set[str] = set()
    ticker_to_domain: dict[str, str] = {}
    for t in tickers:
        domain = web_traffic._get_domain_for_ticker(t)
        if domain:
            domains.add(domain)
            ticker_to_domain[t] = domain
            # Also add www variant
            domains.add(f"www.{domain}")

    if not domains:
        return 0

    inserted = 0
    now = datetime.now(timezone.utc)
    total_days = years * 365

    # Sample dates (every N days going backwards)
    dates = []
    for day_offset in range(0, total_days, sample_interval):
        d = now - timedelta(days=day_offset)
        dates.append(d.strftime("%Y-%m-%d"))

    logger.info("Tranco backfill: %d dates to check for %d domains", len(dates), len(ticker_to_domain))

    for date_str in dates:
        logger.info("  Tranco: fetching list for %s", date_str)

        ranks = _fetch_tranco_for_date(date_str, domains)
        if not ranks:
            time.sleep(_FETCH_DELAY)
            continue

        for ticker, domain in ticker_to_domain.items():
            rank = ranks.get(domain) or ranks.get(f"www.{domain}")
            if rank is None:
                continue

            snapshot = {
                "source": "tranco",
                "domain": domain,
                "rank": rank,
                "total_domains": 1_000_000,
                "percentile": round((1 - rank / 1_000_000) * 100, 2),
                "fetched_at": now.isoformat(),
            }

            ok = _upsert_snapshot(ticker, "tranco", snapshot, date_str, dry_run=dry_run)
            if ok:
                inserted += 1

        # Tranco is generous but let's be polite
        time.sleep(_FETCH_DELAY * 2)

    return inserted


# ═════════════════════════════════════════════════════════════════════
# Main
# ═════════════════════════════════════════════════════════════════════


def main() -> None:
    _setup_logging()

    parser = argparse.ArgumentParser(description="Backfill historical web traffic data")
    parser.add_argument(
        "--source",
        choices=["wikipedia", "tranco", "all"],
        default="all",
        help="Which source to backfill (default: all)",
    )
    parser.add_argument(
        "--years",
        type=int,
        default=5,
        help="How many years to backfill (default: 5)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Limit to first N tickers (0 = all, default: 0)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Log actions without writing to Supabase",
    )
    args = parser.parse_args()

    # Get all relevant tickers
    all_tickers = sorted(web_traffic._TICKER_TO_DOMAIN.keys())
    relevant = [
        t for t in all_tickers
        if web_traffic.is_web_traffic_relevant(t)[0]
    ]

    if args.limit > 0:
        relevant = relevant[:args.limit]

    logger.info("=== Web traffic backfill starting ===")
    logger.info("Source: %s | Years: %d | Tickers: %d | Dry run: %s",
                args.source, args.years, len(relevant), args.dry_run)

    t0 = time.time()
    wiki_total = 0
    tranco_total = 0

    # Wikipedia backfill (per-ticker, chunked by year)
    if args.source in ("wikipedia", "all"):
        logger.info("--- Wikipedia backfill ---")
        for ticker in relevant:
            article = web_traffic._TICKER_TO_WIKI.get(ticker)
            if not article:
                logger.info("  %s: no Wikipedia article mapped, skipping", ticker)
                continue

            count = backfill_wikipedia(ticker, years=args.years, dry_run=args.dry_run)
            wiki_total += count
            logger.info("  %s: %d new Wikipedia snapshots", ticker, count)

        logger.info("Wikipedia backfill complete: %d total new snapshots", wiki_total)

    # Tranco backfill (batch all tickers per date)
    if args.source in ("tranco", "all"):
        logger.info("--- Tranco backfill ---")
        tranco_total = backfill_tranco(
            relevant,
            years=args.years,
            sample_interval=7,  # weekly snapshots
            dry_run=args.dry_run,
        )
        logger.info("Tranco backfill complete: %d total new snapshots", tranco_total)

    elapsed = time.time() - t0
    logger.info(
        "=== Backfill complete in %.1fs: Wikipedia %d new, Tranco %d new ===",
        elapsed, wiki_total, tranco_total,
    )


if __name__ == "__main__":
    main()
