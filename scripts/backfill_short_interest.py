#!/usr/bin/env python3
"""Backfill historical short interest data from NASDAQ + yfinance.

Uses NASDAQ's free short interest API to fetch ~23 bi-monthly FINRA
reports per ticker (~1 year of history).  Enriches each row with
float data from yfinance so we can calculate short % of float.

The script pulls all tickers from the superinvestor universe (~500+),
fetches their full short interest history, and upserts into the
``short_interest_history`` Supabase table.

Usage:
    uv run python scripts/backfill_short_interest.py
    uv run python scripts/backfill_short_interest.py --max-tickers 10
    uv run python scripts/backfill_short_interest.py --tickers AAPL,GME,TSLA
    uv run python scripts/backfill_short_interest.py --dry-run

Environment:
    SUPABASE_URL, SUPABASE_SERVICE_KEY must be set.

Note: NASDAQ rate-limits. The script enforces 2s delays between requests.
A full backfill of ~500 tickers takes ~25-35 minutes.
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone

import httpx
from dotenv import load_dotenv

load_dotenv()

# Ensure the src directory is on the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from filings import cache, client, supabase_cache
from filings.superinvestors import SUPERINVESTORS_BY_CIK

# ── Logging ──────────────────────────────────────────────────────────


def _setup_logging() -> None:
    fmt = "%(asctime)s %(levelname)-8s %(name)s -- %(message)s"
    logging.basicConfig(level=logging.INFO, format=fmt, force=True)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("yfinance").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("peewee").setLevel(logging.WARNING)


logger = logging.getLogger(__name__)

# ── Configuration ────────────────────────────────────────────────────

_NASDAQ_SI_URL = "https://api.nasdaq.com/api/quote/{ticker}/short-interest"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
}

_REQUEST_DELAY = 2.0  # seconds between NASDAQ requests
_YF_DELAY = 0.5  # seconds between yfinance calls
_MAX_RETRIES = 3
_RETRY_BACKOFF = 5.0
_FLUSH_EVERY = 500  # rows


# ── NASDAQ fetch ─────────────────────────────────────────────────────


def _parse_int(s: str | int | float | None) -> int:
    """Parse a NASDAQ-formatted number like '133,373,267' → 133373267."""
    if s is None:
        return 0
    if isinstance(s, (int, float)):
        return int(s)
    return int(re.sub(r"[,\s]", "", str(s)) or 0)


def _parse_date(s: str) -> str:
    """Convert NASDAQ date '02/13/2026' → ISO '2026-02-13'."""
    try:
        dt = datetime.strptime(s.strip(), "%m/%d/%Y")
        return dt.strftime("%Y-%m-%d")
    except (ValueError, AttributeError):
        return ""


def fetch_nasdaq_short_interest(
    http: httpx.Client, ticker: str
) -> list[dict]:
    """Fetch historical short interest from NASDAQ's API.

    Returns a list of dicts with keys matching ``short_interest_history``
    columns, sorted by report_date ascending.
    """
    url = _NASDAQ_SI_URL.format(ticker=ticker.upper())

    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            resp = http.get(url, params={"assetClass": "stocks"})

            if resp.status_code == 429:
                wait = _RETRY_BACKOFF * attempt
                logger.warning(
                    "%s: NASDAQ rate limited (429), waiting %.0fs...",
                    ticker, wait,
                )
                time.sleep(wait)
                continue

            if resp.status_code != 200:
                logger.debug(
                    "%s: NASDAQ HTTP %d", ticker, resp.status_code
                )
                return []

            data = resp.json()
            table = (
                (data.get("data") or {})
                .get("shortInterestTable") or {}
            )
            raw_rows = table.get("rows") or []

            if not raw_rows:
                return []

            rows: list[dict] = []
            for r in raw_rows:
                report_date = _parse_date(r.get("settlementDate", ""))
                shares_short = _parse_int(r.get("interest"))
                if not report_date or not shares_short:
                    continue

                rows.append({
                    "ticker": ticker.upper(),
                    "report_date": report_date,
                    "shares_short": shares_short,
                    "short_ratio": (
                        round(float(r["daysToCover"]), 2)
                        if r.get("daysToCover")
                        else 0
                    ),
                })

            # Sort oldest → newest
            rows.sort(key=lambda x: x["report_date"])

            # Backfill shares_short_prior from adjacent rows
            for i, row in enumerate(rows):
                if i > 0:
                    row["shares_short_prior"] = rows[i - 1]["shares_short"]

            return rows

        except httpx.HTTPError as exc:
            logger.debug(
                "%s: NASDAQ HTTP error (attempt %d): %s",
                ticker, attempt, exc,
            )
            if attempt < _MAX_RETRIES:
                time.sleep(_RETRY_BACKOFF)
        except Exception as exc:
            logger.debug("%s: unexpected error: %s", ticker, exc)
            break

    return []


# ── yfinance fallback (for NYSE-listed stocks) ──────────────────────


def fetch_yfinance_short_interest(ticker: str) -> tuple[list[dict], int]:
    """Fallback: get current + prior month from yfinance .info.

    NASDAQ API only covers NASDAQ-listed stocks.  For NYSE/AMEX tickers
    we fall back to yfinance, which always returns at least 2 data points.

    Returns (rows, float_shares).
    """
    try:
        import yfinance as yf
        from datetime import date as _date

        info = yf.Ticker(ticker).info or {}
    except Exception as exc:
        logger.debug("%s: yfinance .info failed: %s", ticker, exc)
        return [], 0

    shares_short = info.get("sharesShort")
    report_epoch = info.get("dateShortInterest")
    if not shares_short or not report_epoch:
        return [], 0

    float_shares = info.get("floatShares") or 0

    rows: list[dict] = []

    # Current report
    row: dict = {
        "ticker": ticker.upper(),
        "report_date": _date.fromtimestamp(report_epoch).isoformat(),
        "shares_short": shares_short,
        "shares_short_prior": info.get("sharesShortPriorMonth") or 0,
        "short_pct_float": info.get("shortPercentOfFloat") or 0,
        "short_ratio": info.get("shortRatio") or 0,
        "float_shares": float_shares,
        "shares_outstanding": info.get("sharesOutstanding") or 0,
    }
    rows.append(row)

    # Prior month report (backfill)
    prior_epoch = info.get("sharesShortPreviousMonthDate")
    prior_shares = info.get("sharesShortPriorMonth")
    if prior_epoch and prior_shares:
        try:
            prior_row: dict = {
                "ticker": ticker.upper(),
                "report_date": _date.fromtimestamp(prior_epoch).isoformat(),
                "shares_short": prior_shares,
                "float_shares": float_shares,
                "shares_outstanding": info.get("sharesOutstanding") or 0,
            }
            if float_shares:
                prior_row["short_pct_float"] = round(
                    prior_shares / float_shares, 6
                )
            rows.append(prior_row)
        except Exception:
            pass

    rows.sort(key=lambda x: x["report_date"])
    return rows, float_shares


# ── yfinance enrichment ──────────────────────────────────────────────


def fetch_float_shares(ticker: str) -> int:
    """Get current float shares from yfinance for % of float calculation."""
    try:
        import yfinance as yf
        info = yf.Ticker(ticker).info or {}
        return info.get("floatShares") or 0
    except Exception:
        return 0


def enrich_with_float(rows: list[dict], float_shares: int) -> None:
    """Add short_pct_float to each row using float_shares."""
    if not float_shares:
        return
    for row in rows:
        si = row.get("shares_short", 0)
        if si:
            row["short_pct_float"] = round(si / float_shares, 6)
            row["float_shares"] = float_shares


# ── Ticker universe ──────────────────────────────────────────────────

# Valid ticker: 1-5 uppercase letters, optionally with dots or hyphens
# (e.g. BRK-B, BF.B). Filters out CUSIPs like "05990K" or "20786W".
_TICKER_RE = re.compile(r"^[A-Z]{1,5}([.\-][A-Z]{1,2})?$")


def _is_valid_ticker(ticker: str) -> bool:
    """Return True if *ticker* looks like a real stock symbol."""
    return bool(_TICKER_RE.match(ticker))


def get_ticker_universe(min_holders: int = 1) -> list[str]:
    """Get sorted list of tickers from superinvestor 13F holdings.

    *min_holders* filters to tickers held by at least N superinvestors.
    Set to 2+ to focus on high-conviction names and reduce noise.
    """
    logger.info("Loading 13F cache to discover ticker universe...")
    fund_cache = cache.load_cache_from_supabase() or cache.load_cache()
    if not fund_cache:
        logger.error("No cache data found — cannot discover tickers")
        return []

    ticker_map = client.build_ticker_ownership_map(
        fund_cache, SUPERINVESTORS_BY_CIK
    )
    all_tickers = sorted(ticker_map.keys())

    # Filter to valid stock symbols (skip CUSIPs, warrants, etc.)
    valid = [t for t in all_tickers if _is_valid_ticker(t)]

    # Filter by minimum holder count
    if min_holders > 1:
        tickers = [t for t in valid if len(ticker_map.get(t, [])) >= min_holders]
        logger.info(
            "Discovered %d tickers total, %d with %d+ superinvestor holders "
            "(%d filtered as non-standard symbols)",
            len(all_tickers),
            len(tickers),
            min_holders,
            len(all_tickers) - len(valid),
        )
    else:
        tickers = valid
        logger.info(
            "Discovered %d unique tickers (%d filtered as non-standard)",
            len(tickers),
            len(all_tickers) - len(valid),
        )

    return tickers


# ── Main backfill ────────────────────────────────────────────────────


def backfill(
    tickers: list[str],
    *,
    dry_run: bool = False,
    skip_float: bool = False,
) -> dict:
    """Fetch and upsert historical short interest for all *tickers*.

    1. Fetch ~23 FINRA reports per ticker from NASDAQ API (~1 year)
    2. Enrich each row with float_shares from yfinance → calc % of float
    3. Upsert into short_interest_history

    Returns summary dict with counts.
    """
    total_rows = 0
    tickers_with_data = 0
    tickers_empty = 0
    tickers_nasdaq = 0
    tickers_yf_fallback = 0
    all_rows: list[dict] = []

    with httpx.Client(
        headers=_HEADERS, timeout=20.0, follow_redirects=True
    ) as http:
        for i, ticker in enumerate(tickers):
            logger.info(
                "[%d/%d] %s — fetching short interest...",
                i + 1, len(tickers), ticker,
            )

            # 1. Try NASDAQ API first (~23 data points for NASDAQ-listed)
            rows = fetch_nasdaq_short_interest(http, ticker)
            source = "NASDAQ"

            if rows:
                tickers_nasdaq += 1
                # Enrich with float data from yfinance
                if not skip_float:
                    float_shares = fetch_float_shares(ticker)
                    if float_shares:
                        enrich_with_float(rows, float_shares)
                    time.sleep(_YF_DELAY)
            else:
                # 2. Fallback to yfinance for NYSE/AMEX tickers (2 data points)
                rows, _ = fetch_yfinance_short_interest(ticker)
                source = "yfinance"
                if rows:
                    tickers_yf_fallback += 1
                time.sleep(_YF_DELAY)

            if rows:
                tickers_with_data += 1
                total_rows += len(rows)
                all_rows.extend(rows)

                logger.info(
                    "  %s: %d pts via %s (%s → %s)",
                    ticker,
                    len(rows),
                    source,
                    rows[0]["report_date"],
                    rows[-1]["report_date"],
                )

                # 3. Flush periodically
                if len(all_rows) >= _FLUSH_EVERY:
                    if not dry_run:
                        upserted = supabase_cache.upsert_short_interest_rows(
                            all_rows
                        )
                        logger.info("  Flushed %d rows to Supabase", upserted)
                    else:
                        logger.info(
                            "  [DRY RUN] Would flush %d rows", len(all_rows)
                        )
                    all_rows = []
            else:
                tickers_empty += 1
                logger.info("  %s: no data from either source", ticker)

            # Progress checkpoint
            if (i + 1) % 50 == 0:
                logger.info(
                    "--- Progress: %d/%d tickers, %d rows so far ---",
                    i + 1, len(tickers), total_rows,
                )

            # Rate limit
            if i + 1 < len(tickers):
                time.sleep(_REQUEST_DELAY)

    # Flush remaining
    if all_rows:
        if not dry_run:
            upserted = supabase_cache.upsert_short_interest_rows(all_rows)
            logger.info("Final flush: %d rows to Supabase", upserted)
        else:
            logger.info(
                "[DRY RUN] Would flush %d remaining rows", len(all_rows)
            )

    return {
        "tickers_processed": len(tickers),
        "tickers_with_data": tickers_with_data,
        "tickers_nasdaq": tickers_nasdaq,
        "tickers_yf_fallback": tickers_yf_fallback,
        "tickers_empty": tickers_empty,
        "total_rows": total_rows,
    }


# ── CLI ──────────────────────────────────────────────────────────────


def main() -> None:
    _setup_logging()

    parser = argparse.ArgumentParser(
        description="Backfill historical short interest from NASDAQ + yfinance",
    )
    parser.add_argument(
        "--tickers",
        type=str,
        default="",
        help="Comma-separated tickers (default: all superinvestor tickers)",
    )
    parser.add_argument(
        "--max-tickers",
        type=int,
        default=0,
        help="Limit to first N tickers (0 = all)",
    )
    parser.add_argument(
        "--min-holders",
        type=int,
        default=2,
        help="Min superinvestor holders (default: 2, reduces noise)",
    )
    parser.add_argument(
        "--skip-float",
        action="store_true",
        help="Skip yfinance float enrichment (faster, no %% of float)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch data but don't write to Supabase",
    )
    args = parser.parse_args()

    logger.info("=== Short Interest Historical Backfill starting ===")

    # Determine ticker list
    if args.tickers:
        tickers = [
            t.strip().upper()
            for t in args.tickers.split(",")
            if t.strip()
        ]
        logger.info("Using %d manually specified tickers", len(tickers))
    else:
        tickers = get_ticker_universe(min_holders=args.min_holders)
        if not tickers:
            logger.error("No tickers found. Is Supabase configured?")
            return

    if args.max_tickers > 0:
        tickers = tickers[: args.max_tickers]
        logger.info("Capped to first %d tickers", args.max_tickers)

    logger.info(
        "Config: %d tickers, skip_float=%s, dry_run=%s",
        len(tickers), args.skip_float, args.dry_run,
    )

    t0 = time.time()
    result = backfill(
        tickers,
        dry_run=args.dry_run,
        skip_float=args.skip_float,
    )
    elapsed = time.time() - t0

    logger.info(
        "=== Backfill complete in %.1fs ===\n"
        "  Tickers processed:   %d\n"
        "  Tickers with data:   %d\n"
        "    via NASDAQ (~23pt): %d\n"
        "    via yfinance (2pt): %d\n"
        "  Tickers empty:       %d\n"
        "  Total rows:          %d",
        elapsed,
        result["tickers_processed"],
        result["tickers_with_data"],
        result["tickers_nasdaq"],
        result["tickers_yf_fallback"],
        result["tickers_empty"],
        result["total_rows"],
    )


if __name__ == "__main__":
    main()
