"""Unusual Options Activity sync worker — scans yfinance for vol ≥ 5× OI.

Designed to run as a Railway Cron Job every 30 minutes during US market hours.
Fetches options chains for the S&P 500 + superinvestor top holdings, detects
unusual volume, and upserts results into ``unusual_options_activity``.

Usage:
    uv run filings-options-sync
"""

from __future__ import annotations

import logging
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo

from filings import convergence, notifications, supabase_cache, unusual_options

# ── Logging ──────────────────────────────────────────────────────────────────


def _setup_logging() -> None:
    log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
    if os.environ.get("RAILWAY_ENVIRONMENT"):
        fmt = '{"time":"%(asctime)s","level":"%(levelname)s","name":"%(name)s","msg":"%(message)s"}'
    else:
        fmt = "%(asctime)s %(levelname)-8s %(name)s -- %(message)s"
    logging.basicConfig(level=log_level, format=fmt, force=True)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("yfinance").setLevel(logging.WARNING)
    logging.getLogger("peewee").setLevel(logging.WARNING)


logger = logging.getLogger(__name__)

# ── Configuration ────────────────────────────────────────────────────────────

_BATCH_SIZE = 5             # concurrent yfinance calls per batch
_BATCH_DELAY = 2.0          # seconds between batches (Yahoo rate limit)
_MAX_EXPIRIES = 2           # only check nearest 2 expiration dates
_NOTIF_MAX = 10             # max notifications per sync cycle
_ET = ZoneInfo("America/New_York")

# Persistent thread pool
_pool = ThreadPoolExecutor(max_workers=_BATCH_SIZE, thread_name_prefix="options-sync")


# ── Market hours guard ───────────────────────────────────────────────────────


def _is_market_hours() -> bool:
    """Return True if US equity market is open (or within 30 min of close).

    Skips weekends.  Does not account for holidays — a few wasted scans
    on holidays is acceptable; they'll simply return zero unusual contracts.
    """
    now_et = datetime.now(_ET)
    # Skip weekends (Monday=0, Sunday=6)
    if now_et.weekday() >= 5:
        return False
    # Market open 9:30 AM – 4:30 PM ET (30 min buffer for late prints)
    market_open = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close = now_et.replace(hour=16, minute=30, second=0, microsecond=0)
    return market_open <= now_et <= market_close


# ── Ticker universe ─────────────────────────────────────────────────────────


def _build_ticker_watchlist() -> list[dict]:
    """Build the ticker universe: S&P 500 + superinvestor overlay.

    Returns list of ``{"ticker", "name", "sector"}`` dicts.
    """
    from filings import market_data

    # Primary: S&P 500 constituents (includes sector)
    constituents = market_data.get_sp500_constituents()
    sp_tickers = {c["ticker"] for c in constituents}

    logger.info("S&P 500 constituents: %d tickers", len(constituents))

    # Overlay: superinvestor top holdings not in S&P 500
    overlay: list[dict] = []
    try:
        from filings import client as fund_client

        ownership = fund_client.build_ticker_ownership_map()
        # Sort by number of superinvestor holders, take top 50
        sorted_tickers = sorted(
            ownership.items(), key=lambda x: -len(x[1])
        )[:50]
        for tkr, _holders in sorted_tickers:
            if tkr.upper() not in sp_tickers:
                overlay.append({
                    "ticker": tkr.upper(),
                    "name": tkr.upper(),
                    "sector": "",  # enriched later via yfinance
                })
    except Exception:
        logger.debug("Superinvestor overlay unavailable — using S&P 500 only")

    if overlay:
        logger.info("Superinvestor overlay: %d extra tickers", len(overlay))

    return constituents + overlay


# ── Single-ticker fetch ─────────────────────────────────────────────────────


def _fetch_one(
    ticker: str,
    company_name: str,
    sector: str,
) -> list[unusual_options.UnusualOption]:
    """Fetch options chains for a single ticker, detect unusual contracts.

    Tries Tradier first (if configured) for chains with greeks,
    falls back to yfinance.  Uses Tiingo → Tradier → yfinance cascade
    for the underlying stock price.

    Returns list of UnusualOption objects (may be empty).
    """
    # Try Tradier first (better data quality: greeks, reliable)
    try:
        from filings import tradier

        if tradier.has_tradier_key():
            result = _fetch_one_tradier(ticker, company_name, sector)
            if result is not None:
                return result
    except Exception:
        pass  # fall through to yfinance

    return _fetch_one_yfinance(ticker, company_name, sector)


def _fetch_one_tradier(
    ticker: str,
    company_name: str,
    sector: str,
) -> list[unusual_options.UnusualOption] | None:
    """Fetch chains from Tradier with greeks.

    Returns list of UnusualOption, or None to signal "fall back to yfinance".
    """
    from filings import tradier

    expirations = tradier.get_expirations(ticker)
    if not expirations:
        return None  # signal: try yfinance instead

    # Only check nearest N expiries
    expirations = expirations[:_MAX_EXPIRIES]

    # Get underlying price: Tiingo → Tradier → None
    underlying = _get_underlying_price(ticker)

    now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
    today = date.today()
    all_unusual: list[unusual_options.UnusualOption] = []

    for exp_str in expirations:
        chain_data = tradier.get_option_chain(ticker, exp_str, greeks=True)
        if not chain_data:
            continue

        dfs = tradier.chain_to_dataframes(chain_data)
        if dfs is None:
            continue

        calls_df, puts_df = dfs
        exp_date = date.fromisoformat(exp_str)
        dte = (exp_date - today).days

        unusual = unusual_options.detect_unusual(
            ticker=ticker,
            company_name=company_name,
            sector=sector,
            chain_calls=calls_df,
            chain_puts=puts_df,
            underlying_price=underlying,
            fetched_at=now_iso,
        )

        # Patch expiry and DTE
        for u in unusual:
            u.expiry = exp_str
            u.dte = max(dte, 0)

        all_unusual.extend(unusual)

    return all_unusual


def _fetch_one_yfinance(
    ticker: str,
    company_name: str,
    sector: str,
) -> list[unusual_options.UnusualOption]:
    """Original yfinance-based chain fetch (fallback)."""
    import yfinance as yf

    try:
        t = yf.Ticker(ticker)
        expirations = t.options
    except Exception:
        return []

    if not expirations:
        return []

    # Only check nearest N expiries
    expirations = expirations[:_MAX_EXPIRIES]

    # Get underlying price
    underlying = _get_underlying_price(ticker)
    if underlying is None:
        try:
            info = t.info or {}
            underlying = info.get("currentPrice") or info.get("regularMarketPrice")
        except Exception:
            underlying = None

    # Enrich sector from yfinance if missing
    if not sector:
        try:
            sector = (t.info or {}).get("sector", "Other") or "Other"
        except Exception:
            sector = "Other"

    now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
    today = date.today()
    all_unusual: list[unusual_options.UnusualOption] = []

    for exp_str in expirations:
        try:
            chain = t.option_chain(exp_str)
        except Exception:
            continue

        # yfinance doesn't put expiry date per row — we add it
        exp_date = date.fromisoformat(exp_str)
        dte = (exp_date - today).days

        calls_df = chain.calls
        puts_df = chain.puts

        unusual = unusual_options.detect_unusual(
            ticker=ticker,
            company_name=company_name,
            sector=sector,
            chain_calls=calls_df,
            chain_puts=puts_df,
            underlying_price=underlying,
            fetched_at=now_iso,
        )

        # Patch expiry and DTE (not available per-row from yfinance)
        for u in unusual:
            u.expiry = exp_str
            u.dte = max(dte, 0)

        all_unusual.extend(unusual)

    return all_unusual


def _get_underlying_price(ticker: str) -> float | None:
    """Get underlying stock price: Tiingo → Tradier → None.

    Shared by both Tradier and yfinance fetch paths.
    """
    # Try Tiingo first (most reliable for real-time prices)
    try:
        from filings import tiingo

        if tiingo.has_tiingo_key():
            price = tiingo.get_price(ticker)
            if price:
                return price
    except Exception:
        pass

    # Try Tradier quote
    try:
        from filings import tradier

        if tradier.has_tradier_key():
            quote = tradier.get_quote(ticker)
            if quote and quote.get("last"):
                return float(quote["last"])
    except Exception:
        pass

    return None


# ── Batch orchestration ─────────────────────────────────────────────────────


def sync_unusual_options() -> dict:
    """Main sync: build watchlist → fetch chains → detect → upsert.

    Returns summary dict with counts.
    """
    run_id = f"options-sync-{uuid.uuid4().hex[:8]}"
    supabase_cache.create_sync_log(run_id)

    watchlist = _build_ticker_watchlist()
    logger.info("Ticker watchlist: %d tickers", len(watchlist))

    if not watchlist:
        supabase_cache.complete_sync_log(run_id, 0, 1, 0, ["Empty watchlist"])
        return {"watchlist": 0, "unusual": 0, "upserted": 0, "errors": 1}

    # Batch fetch with ThreadPoolExecutor
    all_unusual: list[unusual_options.UnusualOption] = []
    failed = 0
    total_batches = (len(watchlist) + _BATCH_SIZE - 1) // _BATCH_SIZE

    for batch_idx in range(0, len(watchlist), _BATCH_SIZE):
        batch = watchlist[batch_idx : batch_idx + _BATCH_SIZE]
        batch_num = batch_idx // _BATCH_SIZE + 1

        futures = {
            _pool.submit(
                _fetch_one,
                item["ticker"],
                item.get("name", ""),
                item.get("sector", ""),
            ): item["ticker"]
            for item in batch
        }

        for future in as_completed(futures):
            ticker = futures[future]
            try:
                results = future.result()
                if results:
                    all_unusual.extend(results)
            except Exception as exc:
                logger.debug("Fetch error for %s: %s", ticker, exc)
                failed += 1

        # Progress log every 20 batches
        if batch_num % 20 == 0 or batch_num == total_batches:
            logger.info(
                "Progress: batch %d/%d — %d unusual found, %d failed",
                batch_num,
                total_batches,
                len(all_unusual),
                failed,
            )

        # Rate limit between batches
        if batch_idx + _BATCH_SIZE < len(watchlist):
            time.sleep(_BATCH_DELAY)

    logger.info(
        "Scan complete: %d tickers, %d unusual contracts, %d failed",
        len(watchlist),
        len(all_unusual),
        failed,
    )

    if not all_unusual:
        supabase_cache.complete_sync_log(
            run_id, 0, 0, len(watchlist),
            ["No unusual options found" if not failed else f"{failed} tickers failed"],
        )
        return {"watchlist": len(watchlist), "unusual": 0, "upserted": 0, "failed": failed}

    # ── Phase 1B: OI delta computation ──────────────────────────────────
    today_str = date.today().isoformat()
    contract_symbols = [u.contract_symbol for u in all_unusual if u.contract_symbol]
    prev_oi = supabase_cache.get_previous_oi_snapshots(contract_symbols, today_str)

    for u in all_unusual:
        if u.contract_symbol in prev_oi:
            oi_prev = prev_oi[u.contract_symbol]
            u.oi_prev = oi_prev
            u.oi_delta = u.open_interest - oi_prev
            if oi_prev > 0:
                u.oi_delta_pct = round(
                    ((u.open_interest - oi_prev) / oi_prev) * 100, 1
                )
            # New positioning: OI grew by ≥50 % and volume is high
            u.is_new_positioning = (
                u.oi_delta_pct is not None
                and u.oi_delta_pct >= 50.0
                and u.volume >= 500
            )

    # Snapshot today's OI for tomorrow's delta computation
    oi_snapshot_rows = [
        {
            "contract_symbol": u.contract_symbol,
            "scan_date": today_str,
            "open_interest": u.open_interest,
        }
        for u in all_unusual
        if u.contract_symbol
    ]
    if oi_snapshot_rows:
        snapped = supabase_cache.upsert_oi_snapshots(oi_snapshot_rows)
        logger.info("Snapshotted %d OI records for delta tracking", snapped)

    # Upsert to Supabase
    rows = [u.to_db_row() for u in all_unusual]
    upserted = supabase_cache.upsert_unusual_options(rows)
    logger.info("Upserted %d unusual options to Supabase", upserted)

    # Invalidate L1 caches so the next web request gets fresh data
    unusual_options.invalidate_cache()
    convergence.invalidate_cache()

    # Emit notifications for the most notable trades
    _emit_options_notifications(all_unusual)

    # Run retention cleanup
    try:
        cleanup = supabase_cache.run_retention_cleanup()
        logger.info("Post-sync retention cleanup: %s", cleanup)
    except Exception:
        logger.debug("Retention cleanup failed (non-fatal)")

    errors: list[str] = []
    if failed:
        errors.append(f"{failed} tickers failed to fetch")

    supabase_cache.complete_sync_log(
        run_id,
        funds_updated=upserted,
        funds_failed=failed,
        funds_skipped=len(watchlist) - len(set(u.ticker for u in all_unusual)),
        errors=errors,
    )

    return {
        "watchlist": len(watchlist),
        "unusual": len(all_unusual),
        "upserted": upserted,
        "failed": failed,
    }


# ── Notifications ────────────────────────────────────────────────────────────


def _emit_options_notifications(
    unusual: list[unusual_options.UnusualOption],
) -> None:
    """Create notifications for the most notable unusual options.

    Criteria: premium ≥ $500K or vol/OI ratio ≥ 20×.
    Capped at ``_NOTIF_MAX`` per cycle.
    """
    notif_rows: list[dict] = []
    for u in unusual:
        notif = notifications.create_unusual_options_notification(u.to_db_row())
        if notif:
            notif_rows.append(notif)

    notif_rows = notif_rows[:_NOTIF_MAX]

    if notif_rows:
        n = supabase_cache.upsert_notifications(notif_rows)
        logger.info("Created %d unusual options notifications", n)
    else:
        logger.info("No notable unusual options for notifications")


# ── Entry point ──────────────────────────────────────────────────────────────


def main() -> None:
    """Entry point for ``uv run filings-options-sync``."""
    _setup_logging()
    logger.info("=== PaperPanda Unusual Options Sync starting ===")

    if not _is_market_hours():
        logger.info("Outside US market hours — skipping scan")
        return

    start = time.time()
    result = sync_unusual_options()
    elapsed = round(time.time() - start)

    logger.info("=== Options sync finished in %ds: %s ===", elapsed, result)


if __name__ == "__main__":
    main()
