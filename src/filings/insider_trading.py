"""Insider trading screener -- Supabase-first with OpenInsider fallback.

Two modes:
  1. Global screener: latest insider buys/sells across all stocks
  2. Per-ticker: insider trades for a specific company

Data flow:
  - L1: in-memory dict with 5-10 min TTL (fast path, sub-ms)
  - L2: Supabase ``insider_trades`` table (dedicated, typed columns)
  - L3 (fallback): scrape OpenInsider directly (empty DB or outage)

The ``insider_sync`` cron worker populates the Supabase table every
30 minutes.  OpenInsider aggregates SEC Form 4 filings into clean
HTML tables, so we avoid parsing raw XML.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone

import httpx
from bs4 import BeautifulSoup

from filings import supabase_cache

logger = logging.getLogger(__name__)

# ── Numeric parsing helpers ──────────────────────────────────────────


def _parse_price(val: str) -> float | None:
    """``'$150.50'`` -> ``150.50``, ``''`` -> ``None``."""
    if not val:
        return None
    cleaned = val.replace("$", "").replace(",", "").strip()
    try:
        return float(cleaned)
    except ValueError:
        return None


def _parse_qty(val: str) -> int | None:
    """``'+75,000'`` -> ``75000``, ``'-3,752'`` -> ``-3752``."""
    if not val:
        return None
    cleaned = val.replace(",", "").replace("+", "").strip()
    try:
        return int(cleaned)
    except ValueError:
        return None


def _parse_owned(val: str) -> int | None:
    """``'1,500,000'`` -> ``1500000``, ``''`` -> ``None``."""
    if not val:
        return None
    cleaned = val.replace(",", "").replace("+", "").strip()
    try:
        return int(cleaned)
    except ValueError:
        return None


def _parse_delta_own(val: str) -> float | None:
    """``'+13%'`` -> ``13.0``, ``'New'`` -> ``None``."""
    if not val:
        return None
    cleaned = val.replace("%", "").replace("+", "").strip()
    if cleaned.lower() in ("new", ""):
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


# ── Data model ───────────────────────────────────────────────────────


@dataclass
class InsiderTrade:
    """A single insider trading transaction."""

    filing_date: str
    trade_date: str
    ticker: str
    company_name: str
    insider_name: str
    title: str  # CEO, CFO, Director, 10%, etc.
    trade_type: str  # Purchase, Sale, Sale+OE, etc.
    price: str  # keep as formatted string for display
    qty: str  # e.g. "+75,000" or "-3,752"
    owned: str  # shares owned after
    delta_own: str  # e.g. "+13%", "-4%"
    value: str  # e.g. "+$150,000"
    sec_url: str  # link to SEC Form 4

    def to_dict(self) -> dict:
        return asdict(self)

    def to_db_row(self) -> dict:
        """Convert to a dict suitable for Supabase upsert into ``insider_trades``."""
        return {
            "sec_url": self.sec_url,
            "filing_date": self.filing_date,
            "trade_date": self.trade_date,
            "ticker": self.ticker.upper(),
            "company_name": self.company_name,
            "insider_name": self.insider_name,
            "title": self.title,
            "trade_type": self.trade_type,
            "price": _parse_price(self.price),
            "qty": _parse_qty(self.qty),
            "owned": _parse_owned(self.owned),
            "delta_own_pct": _parse_delta_own(self.delta_own),
            "value": parse_dollar_value(self.value),
            "price_fmt": self.price,
            "qty_fmt": self.qty,
            "owned_fmt": self.owned,
            "delta_own_fmt": self.delta_own,
            "value_fmt": self.value,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }

    def to_history_row(self) -> dict:
        """Convert to a lean dict for ``insider_purchases_history`` (cold table).

        Excludes: trade_type (always Purchase), owned, delta_own_pct,
        all ``_fmt`` display columns, and updated_at (write-once table).
        """
        return {
            "sec_url": self.sec_url,
            "filing_date": self.filing_date,
            "trade_date": self.trade_date,
            "ticker": self.ticker.upper(),
            "company_name": self.company_name,
            "insider_name": self.insider_name,
            "title": self.title,
            "price": _parse_price(self.price),
            "qty": _parse_qty(self.qty),
            "value": parse_dollar_value(self.value),
        }

    @classmethod
    def from_db_row(cls, row: dict) -> InsiderTrade:
        """Reconstruct an ``InsiderTrade`` from a Supabase row dict."""
        return cls(
            filing_date=str(row.get("filing_date", "")),
            trade_date=str(row.get("trade_date", "")),
            ticker=row.get("ticker", ""),
            company_name=row.get("company_name", ""),
            insider_name=row.get("insider_name", ""),
            title=row.get("title", ""),
            trade_type=row.get("trade_type", ""),
            price=row.get("price_fmt", ""),
            qty=row.get("qty_fmt", ""),
            owned=row.get("owned_fmt", ""),
            delta_own=row.get("delta_own_fmt", ""),
            value=row.get("value_fmt", ""),
            sec_url=row.get("sec_url", ""),
        )

    @classmethod
    def from_history_row(cls, row: dict) -> InsiderTrade:
        """Reconstruct from ``insider_purchases_history`` (cold table).

        The cold table stores raw numeric values instead of ``_fmt``
        display strings, so we format them here for display.
        """
        price = row.get("price")
        qty = row.get("qty")
        value = row.get("value")
        return cls(
            filing_date=str(row.get("filing_date", "")),
            trade_date=str(row.get("trade_date", "")),
            ticker=row.get("ticker", ""),
            company_name=row.get("company_name", ""),
            insider_name=row.get("insider_name", ""),
            title=row.get("title", ""),
            trade_type="Purchase",
            price=f"${price:,.2f}" if price is not None else "",
            qty=f"{qty:,}" if qty is not None else "",
            owned="",
            delta_own="",
            value=f"${value:,.0f}" if value is not None else "",
            sec_url=row.get("sec_url", ""),
        )


# ── Helpers ──────────────────────────────────────────────────────────

_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; 13f-insider-screener/1.0)"}
_OI_BASE = "http://openinsider.com"

import re as _re

_SAFE_TICKER_RE = _re.compile(r"^[A-Za-z][A-Za-z0-9.]{0,11}$")

_lock = threading.Lock()
_cache: dict[str, tuple[float, list[InsiderTrade]]] = {}
_GLOBAL_TTL = 300  # 5 min for global screener
_TICKER_TTL = 600  # 10 min for per-ticker
_MAX_CACHE = 50  # ~150 MB worst-case at 300; keep at 50 to stay well under Railway RAM


def _evict_oldest() -> None:
    """Remove the oldest entry when the cache exceeds _MAX_CACHE."""
    while len(_cache) > _MAX_CACHE:
        oldest = min(_cache, key=lambda k: _cache[k][0])
        _cache.pop(oldest, None)


def _get_cached(key: str, ttl: int) -> list[InsiderTrade] | None:
    with _lock:
        if key in _cache:
            ts, data = _cache[key]
            if time.time() - ts < ttl:
                return data
    return None


def _get_cached_with_stale(
    key: str, ttl: int
) -> tuple[list[InsiderTrade] | None, bool]:
    """Return ``(data, is_fresh)`` — stale data is returned instead of ``None``.

    Unlike ``_get_cached`` which returns ``None`` when TTL expires, this
    always returns the last cached value (even if stale) so users never
    see an empty / error state while upstream sources are refreshing.
    """
    with _lock:
        if key in _cache:
            ts, data = _cache[key]
            is_fresh = (time.time() - ts) < ttl
            return data, is_fresh
    return None, False


def _set_cached(key: str, data: list[InsiderTrade]) -> None:
    with _lock:
        _cache[key] = (time.time(), data)
        _evict_oldest()


def _parse_table(html: str, *, has_company_col: bool) -> list[InsiderTrade]:
    """Parse an OpenInsider HTML table into InsiderTrade objects."""
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", class_="tinytable")
    if not table:
        return []

    trades: list[InsiderTrade] = []
    for row in table.find_all("tr")[1:]:  # skip header
        cells = row.find_all("td")
        texts = [td.get_text(strip=True) for td in cells]

        if has_company_col and len(texts) < 17:
            continue
        if not has_company_col and len(texts) < 16:
            continue

        # Extract SEC filing link (check first 2 cells -- cell 0 may be
        # empty; the link is typically in cell 1, the filing date cell)
        sec_url = ""
        for cell_idx in range(min(2, len(cells))):
            for a_tag in cells[cell_idx].find_all("a"):
                href = a_tag.get("href", "")
                if "sec.gov" in href:
                    sec_url = href
                    break
                if href.startswith("/") and "edgar" in href.lower():
                    sec_url = f"https://www.sec.gov{href}"
                    break
            if sec_url:
                break

        if has_company_col:
            # Global: X Filing Trade Ticker Company Insider Title Type Price Qty Owned dOwn Value 1d 1w 1m 6m
            trade = InsiderTrade(
                filing_date=texts[1].split(" ")[0] if " " in texts[1] else texts[1],
                trade_date=texts[2],
                ticker=texts[3],
                company_name=texts[4],
                insider_name=texts[5],
                title=texts[6],
                trade_type=_clean_trade_type(texts[7]),
                price=texts[8],
                qty=texts[9],
                owned=texts[10],
                delta_own=texts[11],
                value=texts[12],
                sec_url=sec_url,
            )
        else:
            # Per-ticker: X Filing Trade Ticker Insider Title Type Price Qty Owned dOwn Value 1d 1w 1m 6m
            trade = InsiderTrade(
                filing_date=texts[1].split(" ")[0] if " " in texts[1] else texts[1],
                trade_date=texts[2],
                ticker=texts[3],
                company_name="",
                insider_name=texts[4],
                title=texts[5],
                trade_type=_clean_trade_type(texts[6]),
                price=texts[7],
                qty=texts[8],
                owned=texts[9],
                delta_own=texts[10],
                value=texts[11],
                sec_url=sec_url,
            )

        trades.append(trade)

    return trades


def _clean_trade_type(raw: str) -> str:
    """'P - Purchase' → 'Purchase', 'S - Sale' → 'Sale'."""
    if " - " in raw:
        return raw.split(" - ", 1)[1]
    return raw


def parse_dollar_value(val: str) -> float:
    """Parse formatted value string to float.

    '+$150,000' -> 150000.0
    '-$1,234,567' -> -1234567.0
    '$42' -> 42.0
    '' or invalid -> 0.0
    """
    if not val:
        return 0.0
    cleaned = val.replace("$", "").replace(",", "").replace("+", "").strip()
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def aggregate_top_tickers(
    trades: list[InsiderTrade],
    limit: int = 10,
    mixed: bool = True,
) -> list[dict]:
    """Aggregate trades by ticker, return top N for the chart.

    Args:
        mixed: When ``True`` (Latest tab), selects top N/2 by most
            positive net flow and top N/2 by most negative net flow,
            sorted ascending from most bearish (left) to most bullish
            (right).  When ``False`` (Purchases / Sales tab), simply
            picks the top N by total dollar volume.

    Returns list of dicts for JSON serialization, each containing:
    ticker, company_name, buy_value, sell_value, net_flow, trade_count,
    insider_details [{name, buy_value, sell_value, count}], date_range.
    """
    from collections import defaultdict

    ticker_data: dict[str, dict] = {}

    for t in trades:
        if not t.ticker:
            continue
        key = t.ticker.upper()
        abs_val = abs(parse_dollar_value(t.value))
        is_buy = "Purchase" in t.trade_type

        if key not in ticker_data:
            ticker_data[key] = {
                "ticker": t.ticker.upper(),
                "company_name": t.company_name,
                "buy_value": 0.0,
                "sell_value": 0.0,
                "trade_count": 0,
                "insiders": defaultdict(
                    lambda: {"buy_value": 0.0, "sell_value": 0.0, "count": 0}
                ),
                "dates": [],
            }

        entry = ticker_data[key]
        if is_buy:
            entry["buy_value"] += abs_val
        else:
            entry["sell_value"] += abs_val
        entry["trade_count"] += 1
        if t.trade_date:
            entry["dates"].append(t.trade_date)

        insider = entry["insiders"][t.insider_name]
        if is_buy:
            insider["buy_value"] += abs_val
        else:
            insider["sell_value"] += abs_val
        insider["count"] += 1

    all_tickers = list(ticker_data.values())

    if mixed:
        # ── Latest tab: top N/2 by most positive + N/2 by most negative ──
        # Uses *net dollar flow* (buys - sells) so the chart naturally
        # shows a diverging layout from bearish (left) to bullish (right).
        half = max(limit // 2, 1)

        for d in all_tickers:
            d["_net"] = d["buy_value"] - d["sell_value"]

        net_negative = sorted(
            [d for d in all_tickers if d["_net"] < 0],
            key=lambda d: d["_net"],  # most negative first
        )
        net_positive = sorted(
            [d for d in all_tickers if d["_net"] > 0],
            key=lambda d: d["_net"],
            reverse=True,  # most positive first
        )

        top_sells = net_negative[:half]
        top_buys = net_positive[:half]

        # Backfill if one side has fewer than half
        if len(top_buys) < half:
            top_sells = net_negative[: limit - len(top_buys)]
        elif len(top_sells) < half:
            top_buys = net_positive[: limit - len(top_sells)]

        # Sort ascending by net flow: most bearish (left) → most bullish (right)
        sorted_tickers = sorted(top_sells + top_buys, key=lambda d: d["_net"])

        # Clean up temp key
        for d in all_tickers:
            d.pop("_net", None)
    else:
        # ── Purchases / Sales tab: simple top N by total dollar volume ──
        sorted_tickers = sorted(
            all_tickers,
            key=lambda d: d["buy_value"] + d["sell_value"],
            reverse=True,
        )[:limit]

    result = []
    for d in sorted_tickers:
        dates = sorted(d["dates"])
        date_range = (
            f"{dates[0]} to {dates[-1]}"
            if len(dates) > 1
            else (dates[0] if dates else "")
        )

        insider_details = [
            {
                "name": name,
                "buy_value": round(info["buy_value"], 2),
                "sell_value": round(info["sell_value"], 2),
                "count": info["count"],
            }
            for name, info in sorted(
                d["insiders"].items(),
                key=lambda x: x[1]["buy_value"] + x[1]["sell_value"],
                reverse=True,
            )
        ]

        result.append(
            {
                "ticker": d["ticker"],
                "company_name": d["company_name"],
                "buy_value": round(d["buy_value"], 2),
                "sell_value": round(d["sell_value"], 2),
                "net_flow": round(d["buy_value"] - d["sell_value"], 2),
                "trade_count": d["trade_count"],
                "insider_details": insider_details,
                "date_range": date_range,
            }
        )

    return result


# ── OpenInsider scrape helpers (fallback only) ───────────────────────


def _scrape_openinsider_global(
    trade_type: str = "",
    count: int = 100,
) -> list[InsiderTrade]:
    """Direct scrape of OpenInsider global screener (fallback path)."""
    url = f"{_OI_BASE}/screener"
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
        "cnt": str(min(count, 100)),
        "page": "1",
    }
    try:
        resp = httpx.get(
            url, params=params, headers=_HEADERS, timeout=15, follow_redirects=True
        )
        resp.raise_for_status()
        return _parse_table(resp.text, has_company_col=True)
    except Exception:
        logger.exception("Fallback scrape of OpenInsider global failed")
        return []


def _scrape_openinsider_ticker(ticker: str, max_pages: int = 3) -> list[InsiderTrade]:
    """Scrape OpenInsider screener for a specific ticker (all-time history).

    Uses the screener endpoint with ``fd=0`` (all filing dates) and
    ``td=0`` (all trade dates) to fetch the full history, not just the
    ~40 most recent trades shown on the default per-ticker page.
    Paginates up to *max_pages* (100 trades each) for a max of 300 trades.
    """
    if not _SAFE_TICKER_RE.match(ticker):
        logger.warning("Invalid ticker rejected by scraper: %s", ticker)
        return []

    all_trades: list[InsiderTrade] = []
    seen_urls: set[str] = set()

    for page in range(1, max_pages + 1):
        try:
            resp = httpx.get(
                f"{_OI_BASE}/screener",
                params={
                    "s": ticker,
                    "o": "",
                    "pl": "",
                    "ph": "",
                    "st": "0",
                    "tc": "1",
                    "fd": "0",   # all filing dates
                    "td": "0",   # all trade dates
                    "t": "",     # all trade types
                    "vf": "",
                    "o2d": "",
                    "sortcol": "0",
                    "cnt": "100",
                    "page": str(page),
                },
                headers=_HEADERS,
                timeout=20,
                follow_redirects=True,
            )
            resp.raise_for_status()
            trades = _parse_table(resp.text, has_company_col=False)
            if not trades:
                break  # no more pages
            for t in trades:
                if t.sec_url and t.sec_url not in seen_urls:
                    seen_urls.add(t.sec_url)
                    all_trades.append(t)
            if len(trades) < 100:
                break  # last page
        except Exception:
            logger.exception("OpenInsider screener scrape for %s page %d failed", ticker, page)
            break
        if page < max_pages:
            time.sleep(2)  # rate-limit between pages

    logger.info("Scraped %d trades for %s from OpenInsider screener (%d pages)", len(all_trades), ticker, page)
    return all_trades


def _scrape_and_backfill_ticker(ticker: str) -> list[InsiderTrade]:
    """Scrape per-ticker page and backfill results into Supabase."""
    trades = _scrape_openinsider_ticker(ticker)
    if trades:
        try:
            rows = [t.to_db_row() for t in trades if t.sec_url]
            if rows:
                upserted = supabase_cache.upsert_insider_trades(rows)
                logger.info("Backfilled %d trades for %s", upserted, ticker)
        except Exception:
            logger.debug("Backfill upsert failed for %s", ticker)
    return trades


# ── Public API ───────────────────────────────────────────────────────


def get_latest_insider_trades(
    trade_type: str = "",  # "p"=purchases, "s"=sales, ""=all
    count: int = 100,
) -> list[InsiderTrade]:
    """Fetch latest insider trades -- Supabase-first with scrape fallback.

    Data flow:
      1. L1 in-memory cache (5 min TTL) -- sub-ms
      2. Query ``insider_trades`` table in Supabase
      3. Fallback: scrape OpenInsider directly (empty DB or outage)
      4. Last resort: return stale L1 data (never show empty/error)

    Args:
        trade_type: ``"p"`` for purchases, ``"s"`` for sales, ``""`` for all.
        count: Number of trades to fetch (max 100).
    """
    cache_key = f"global:{trade_type or 'all'}:{count}"

    # ── L1: in-memory fast path (fresh hit) ──
    stale_data, is_fresh = _get_cached_with_stale(cache_key, _GLOBAL_TTL)
    if stale_data is not None and is_fresh:
        return stale_data

    # ── L2: Supabase dedicated table ──
    db_filter = {"p": "Purchase", "s": "Sale"}.get(trade_type, "")
    rows = supabase_cache.get_insider_trades(trade_type=db_filter, limit=count)

    if rows:
        trades = [InsiderTrade.from_db_row(r) for r in rows]
        _set_cached(cache_key, trades)
        return trades

    # ── L3: Fallback -- scrape OpenInsider directly + backfill to Supabase ──
    logger.warning("Supabase insider_trades empty/unavailable -- scraping OpenInsider")
    trades = _scrape_openinsider_global(trade_type, count)
    if trades:
        _set_cached(cache_key, trades)
        # Backfill scraped trades into Supabase so they persist across restarts
        try:
            rows = [t.to_db_row() for t in trades if t.sec_url]
            if rows:
                upserted = supabase_cache.upsert_insider_trades(rows)
                logger.info("Backfilled %d global insider trades to Supabase", upserted)
        except Exception:
            logger.debug("Global insider backfill upsert failed")
        return trades

    # ── L4: Stale L1 data -- never show empty/error to users ──
    if stale_data:
        logger.info(
            "Returning stale L1 insider data for key=%s (%d trades)",
            cache_key,
            len(stale_data),
        )
        return stale_data

    return []


def get_insider_chart_data(limit: int = 10, trade_type: str = "") -> list[dict]:
    """Fetch aggregated chart data for the insider momentum chart.

    Args:
        limit: Number of tickers to include in the chart (default 10).
        trade_type: ``""`` for Latest (mixed buys+sells), ``"p"`` for
            Purchases only, ``"s"`` for Sales only.

    When ``trade_type`` is ``""`` (Latest tab), fetches buys and sells
    separately from Supabase and uses ``mixed=True`` aggregation so both
    sides are represented.  When filtered to Purchases or Sales, uses
    the standard table query with ``mixed=False`` (simple top-N sort).
    """
    is_mixed = trade_type == ""
    cache_key = f"chart:insider_momentum:{trade_type or 'all'}"

    # ── L1: in-memory fast path ──
    stale_data, is_fresh = _get_cached_with_stale(cache_key, _GLOBAL_TTL)
    if stale_data is not None and is_fresh:
        return aggregate_top_tickers(stale_data, limit=limit, mixed=is_mixed)

    if is_mixed:
        # ── L2: Supabase — separate buy + sell queries (30 days) ──
        rows = supabase_cache.get_insider_trades_for_chart(days=30, limit_per_type=250)
        if rows:
            trades = [InsiderTrade.from_db_row(r) for r in rows]
            logger.info(
                "Chart L2 hit: %d trades (%d buys, %d sells)",
                len(trades),
                sum(1 for t in trades if "Purchase" in t.trade_type),
                sum(1 for t in trades if "Sale" in t.trade_type),
            )
            _set_cached(cache_key, trades)
            return aggregate_top_tickers(trades, limit=limit, mixed=True)

        # ── L3: Supabase chart query failed -- fetch buys + sells separately ──
        logger.warning("Chart L2 returned None; falling back to separate queries")
        buy_trades = get_latest_insider_trades(trade_type="p", count=200)
        sell_trades = get_latest_insider_trades(trade_type="s", count=200)
        trades = (buy_trades or []) + (sell_trades or [])
        if trades:
            _set_cached(cache_key, trades)
            return aggregate_top_tickers(trades, limit=limit, mixed=True)
    else:
        # ── L2: Supabase — single-type query (more rows for richer chart) ──
        trades = get_latest_insider_trades(trade_type=trade_type, count=200)
        if trades:
            _set_cached(cache_key, trades)
            return aggregate_top_tickers(trades, limit=limit, mixed=False)

        # ── L3: Fall back to smaller fetch ──
        trades = get_latest_insider_trades(trade_type=trade_type, count=100)
        if trades:
            return aggregate_top_tickers(trades, limit=limit, mixed=False)

    # ── L4: Stale data — never show empty chart ──
    if stale_data:
        return aggregate_top_tickers(stale_data, limit=limit, mixed=is_mixed)

    return []


def _fetch_historical_from_efts(
    ticker: str, max_results: int = 50
) -> list[InsiderTrade]:
    """Fetch historical Form 4 filings from SEC EDGAR full-text search.

    Queries the EFTS endpoint for Form 4 filings mentioning the ticker.
    Returns InsiderTrade objects with filing metadata (filing_date,
    insider_name, sec_url).  Trade detail columns (price, qty, value)
    are set to empty strings since EFTS only returns filing metadata.

    Falls back to empty list on any error.
    """
    if not _SAFE_TICKER_RE.match(ticker):
        logger.warning("Invalid ticker rejected by EFTS query: %s", ticker)
        return []

    import os
    import re

    identity = os.environ.get("SEC_IDENTITY", "13f-tool-user user@example.com")
    headers = {"User-Agent": identity}
    url = "https://efts.sec.gov/LATEST/search-index"
    params = {
        "q": f'"{ticker.upper()}"',
        "forms": "4",
        "from": "0",
        "size": str(min(max_results, 100)),
    }

    try:
        resp = httpx.get(url, params=params, headers=headers, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        logger.debug("EFTS Form 4 query for %s failed: %s", ticker, exc)
        return []

    trades: list[InsiderTrade] = []
    seen_urls: set[str] = set()

    for hit in data.get("hits", {}).get("hits", []):
        source = hit.get("_source", {})
        file_date = source.get("file_date", "")
        # display_names: ["INSIDER NAME  (CIK 0001234567)"]
        display_names = source.get("display_names", [])
        insider_name = ""
        for dn in display_names:
            match = re.match(r"^(.+?)\s+\(CIK\s+\d+\)", dn)
            if match:
                insider_name = match.group(1).strip()
                break
        if not insider_name and display_names:
            insider_name = display_names[0]

        # Build SEC URL from accession number
        # EFTS results have root_form, form_type, etc.
        sec_url = ""
        # Try to build URL from the filing's accession number if available
        accession = hit.get("_id", "")
        if accession:
            # accession format: "0001234567-25-000001"
            acc_clean = accession.replace("-", "")
            # CIK from display_names
            cik_match = re.search(r"\(CIK\s+(\d+)\)", " ".join(display_names))
            if cik_match:
                cik = cik_match.group(1)
                sec_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_clean}/{accession}-index.htm"

        if sec_url in seen_urls:
            continue
        if sec_url:
            seen_urls.add(sec_url)

        trades.append(
            InsiderTrade(
                filing_date=file_date,
                trade_date=file_date,  # Best approximation; EFTS lacks exact trade date
                ticker=ticker.upper(),
                company_name="",
                insider_name=insider_name,
                title="",
                trade_type="Form 4",
                price="",
                qty="",
                owned="",
                delta_own="",
                value="",
                sec_url=sec_url,
            )
        )

    return trades


def get_ticker_insider_trades(ticker: str) -> list[InsiderTrade]:
    """Fetch insider trades for a specific ticker -- Supabase + OpenInsider.

    Data flow:
      1. L1 in-memory cache (10 min TTL)
      2. Query ``insider_trades`` table by ticker (hot 30-day window)
      3. Scrape OpenInsider per-ticker page for full history (all trade
         types with real prices/qty/values)
      4. Merge historical purchases from ``insider_purchases_history`` (cold)
         to fill any gaps OpenInsider might miss
      5. Fallback: scrape + backfill to Supabase
      6. Last resort: return stale L1 data (never show empty/error)
    """
    key = ticker.upper()
    cache_key = f"ticker:{key}"

    # ── L1: in-memory fast path (fresh hit) ──
    stale_data, is_fresh = _get_cached_with_stale(cache_key, _TICKER_TTL)
    if stale_data is not None and is_fresh:
        return stale_data

    # ── L2: Supabase dedicated table (hot 30-day window) ──
    rows = supabase_cache.get_insider_trades_by_ticker(key)

    if rows:
        hot_trades = [InsiderTrade.from_db_row(r) for r in rows]
        seen_urls = {t.sec_url for t in hot_trades if t.sec_url}

        # ── L2.5: Scrape OpenInsider for full history (all trade types) ──
        # Returns real prices/qty/values — much richer than SEC EFTS metadata.
        try:
            oi_trades = _scrape_openinsider_ticker(key)
            if oi_trades:
                oi_unique = [
                    t for t in oi_trades
                    if t.sec_url and t.sec_url not in seen_urls
                ]
                seen_urls.update(t.sec_url for t in oi_unique if t.sec_url)
            else:
                oi_unique = []
        except Exception:
            logger.debug("OpenInsider scrape failed for %s", key)
            oi_unique = []

        # ── L2.6: Fill gaps from cold table (purchases only) ──
        # In case OpenInsider is down or missing older data, the cold table
        # has a permanent copy of all historical purchases.
        try:
            history_rows = supabase_cache.get_history_purchases_by_ticker(key)
            if history_rows:
                cold_purchases = [
                    InsiderTrade.from_history_row(r)
                    for r in history_rows
                    if r.get("sec_url") and r["sec_url"] not in seen_urls
                ]
            else:
                cold_purchases = []
        except Exception:
            logger.debug("Cold table supplement failed for %s", key)
            cold_purchases = []

        combined = hot_trades + oi_unique + cold_purchases
        _set_cached(cache_key, combined)
        return combined

    # ── L3: Fallback -- scrape + backfill ──
    logger.warning("No Supabase data for %s -- scraping OpenInsider", key)
    trades = _scrape_and_backfill_ticker(key)
    if trades:
        _set_cached(cache_key, trades)
        return trades

    # ── L4: Stale L1 data -- never show empty/error to users ──
    if stale_data:
        logger.info(
            "Returning stale L1 insider data for ticker=%s (%d trades)",
            key,
            len(stale_data),
        )
        return stale_data

    return []


# ── Per-ticker display helpers ──────────────────────────────────────


def _quarter_key(date_str: str) -> str:
    """``'2025-11-15'`` → ``'2025-Q4'``.  Returns ``'????-Q?'`` on bad input."""
    try:
        parts = date_str.split("-")
        year = parts[0]
        month = int(parts[1])
        q = (month - 1) // 3 + 1
        return f"{year}-Q{q}"
    except Exception:
        return "????-Q?"


def _quarter_label(key: str) -> str:
    """``'2025-Q4'`` → ``'Q4 2025'``."""
    try:
        year, qpart = key.split("-")
        return f"{qpart} {year}"
    except Exception:
        return key


def _format_compact_value(val: float) -> str:
    """``1234567.0`` → ``'$1.2M'``, ``45000.0`` → ``'$45K'``."""
    if val >= 1_000_000:
        return f"${val / 1_000_000:.1f}M"
    if val >= 1_000:
        return f"${val / 1_000:.0f}K"
    if val > 0:
        return f"${val:,.0f}"
    return "$0"


# ── Title resolution (fix "See Remarks") ────────────────────────────

# LRU-style cache with max 500 entries to prevent unbounded memory growth.
# Keyed by (insider_name, ticker) → abbreviated title string.
_TITLE_CACHE_MAX = 500
_title_cache: dict[tuple[str, str], str] = {}

# Longest-first so "Chief Information Security Officer" matches before
# "Chief Information Officer".
_TITLE_ABBREVS: list[tuple[str, str]] = [
    ("Chief Executive Officer", "CEO"),
    ("Chief Financial Officer", "CFO"),
    ("Chief Technology Officer", "CTO"),
    ("Chief Operating Officer", "COO"),
    ("Chief Revenue Officer", "CRO"),
    ("Chief Legal Officer", "CLO"),
    ("Chief Accounting Officer", "CAO"),
    ("Chief Information Security Officer", "CISO"),
    ("Chief Information Officer", "CIO"),
    ("Chief Marketing Officer", "CMO"),
    ("Chief Product Officer", "CPO"),
    ("Chief Business Officer", "CBO"),
    ("Chief People Officer", "CPO"),
    ("Chief Strategy Officer", "CSO"),
    ("Chief Administrative Officer", "CAO"),
    ("Executive Vice President", "EVP"),
    ("Senior Vice President", "SVP"),
    ("Vice President", "VP"),
    ("General Counsel", "GC"),
    ("Treasurer", "Treas."),
    ("Secretary", "Sec."),
    ("President", "Pres."),
]


def _shorten_title(title: str) -> str:
    """Abbreviate verbose officer titles for compact display.

    ``'Chief Executive Officer'`` → ``'CEO'``
    ``'Chief Revenue Officer and Chief Legal Officer'`` → ``'CRO, CLO'``
    """
    result = title
    for long_form, short_form in _TITLE_ABBREVS:
        result = result.replace(long_form, short_form)
    # Normalise connectors: "CEO and CFO" → "CEO, CFO"
    result = result.replace(" and ", ", ")
    return result.strip()


def _resolve_see_remarks_titles(trades: list[InsiderTrade]) -> None:
    """Resolve *See Remarks* titles via SEC Form 4 XML.

    Many filers put ``See Remarks`` in the officer-title field and write
    the real title (e.g. *Chief Executive Officer*) in the ``<remarks>``
    element of the XML filing.  This function fetches the raw XML for
    one filing per unique insider, extracts the real title, abbreviates
    it, and updates **all** matching trades in-place.

    Results are cached in ``_title_cache`` so repeat calls for the same
    ticker are effectively free.
    """
    if not trades:
        return

    # ── Phase 1: apply cached resolutions & collect unknowns ──
    needs_fetch: dict[str, str] = {}  # insider_name → sec_url
    ticker = ""
    for t in trades:
        if not ticker and t.ticker:
            ticker = t.ticker.upper()
        if "see" not in t.title.lower() or "remark" not in t.title.lower():
            continue
        cache_key = (t.insider_name, ticker)
        if cache_key in _title_cache:
            t.title = _title_cache[cache_key]
        elif t.insider_name not in needs_fetch and t.sec_url:
            needs_fetch[t.insider_name] = t.sec_url

    if not needs_fetch:
        return

    # ── Phase 2: fetch SEC XML and extract real titles ──
    sec_headers = {"User-Agent": "13f-tool user@example.com"}
    resolved: dict[str, str] = {}

    for name, sec_url in list(needs_fetch.items())[:10]:
        # Strip the XSLT rendering prefix to get raw XML
        raw_url = sec_url.replace("/xslF345X03/", "/")
        try:
            resp = httpx.get(
                raw_url, headers=sec_headers, timeout=10, follow_redirects=True,
            )
            if resp.status_code != 200:
                continue
            xml = resp.text

            # Try extracting title from <remarks> first
            real_title = ""
            remarks_match = _re.search(
                r"<remarks>(.*?)</remarks>", xml,
                _re.DOTALL | _re.IGNORECASE,
            )
            if remarks_match:
                remarks = remarks_match.group(1).strip()
                # Common pattern: "Officer title: Chief Executive Officer."
                title_match = _re.search(
                    r"[Oo]fficer\s+[Tt]itle:\s*(.+?)\.",
                    remarks,
                )
                if title_match:
                    real_title = _shorten_title(title_match.group(1).strip())

            # Fallback: infer from XML relationship flags
            if not real_title:
                is_dir = _re.search(r"<isDirector>1</isDirector>", xml)
                is_off = _re.search(r"<isOfficer>1</isOfficer>", xml)
                is_ten = _re.search(r"<isTenPercentOwner>1</isTenPercentOwner>", xml)
                # Also check <officerTitle> directly (some use it properly)
                ot_match = _re.search(
                    r"<officerTitle>(.*?)</officerTitle>", xml, _re.IGNORECASE,
                )
                ot_val = ot_match.group(1).strip() if ot_match else ""
                if ot_val and "see" not in ot_val.lower():
                    real_title = _shorten_title(ot_val)
                elif is_dir:
                    real_title = "Dir"
                elif is_ten:
                    real_title = "10% Owner"
                elif is_off:
                    real_title = "Officer"

            if real_title:
                resolved[name] = real_title
        except Exception:
            logger.debug("Title resolution failed for %s", name)
        time.sleep(0.15)  # SEC rate-limiting

    if not resolved:
        return

    # ── Phase 3: apply resolved titles to trades & populate cache ──
    for t in trades:
        if t.insider_name in resolved and "see" in t.title.lower():
            t.title = resolved[t.insider_name]

    for name, title in resolved.items():
        _title_cache[(name, ticker)] = title
    # Evict oldest entries if cache exceeds max size
    while len(_title_cache) > _TITLE_CACHE_MAX:
        _title_cache.pop(next(iter(_title_cache)))

    logger.info(
        "Resolved %d 'See Remarks' titles for %s via SEC Form 4 XML",
        len(resolved), ticker,
    )


def prepare_ticker_display(trades: list[InsiderTrade]) -> dict:
    """Structure trades for the redesigned per-ticker display.

    Returns a dict with four keys:

    - ``insiders``: list of dicts sorted by total value (descending), each with
      ``name``, ``title``, ``buys``, ``sells``, ``total_buy_value``,
      ``total_sell_value``, ``buy_value_fmt``, ``sell_value_fmt``,
      ``last_trade_date``, ``quarterly_breakdown`` (list of per-quarter dicts).
    - ``quarters``: list of dicts sorted newest-first, each with ``label``,
      ``key``, ``trades`` (list of InsiderTrade), ``buy_count``, ``sell_count``,
      ``total_value_fmt``.
    - ``chart``: dict with ``labels`` (quarter labels), ``buy_values`` (list of
      floats), ``sell_values`` (list of floats) — chronological order for
      Chart.js.
    - ``per_insider_chart``: dict mapping insider name (and ``'__all__'``) to
      chart dicts, for the per-insider dropdown filter.
    """
    empty_chart: dict = {"labels": [], "buy_values": [], "sell_values": []}
    if not trades:
        return {"insiders": [], "quarters": [], "chart": empty_chart, "per_insider_chart": {"__all__": empty_chart}}

    # ── Resolve "See Remarks" titles from SEC Form 4 XML ──
    _resolve_see_remarks_titles(trades)

    # ── Group by insider (+ per-insider-per-quarter tracking) ──
    insider_map: dict[str, dict] = {}
    # iqdata[name][quarter_key] = {"buys": 0, "sells": 0, "buy_val": 0, "sell_val": 0,
    #                               "qty_sold": 0, "owned_last": 0}
    iqdata: dict[str, dict[str, dict]] = {}

    for t in trades:
        name = t.insider_name or "Unknown"
        qk = _quarter_key(t.trade_date)

        if name not in insider_map:
            insider_map[name] = {
                "name": name,
                "title": t.title or "",
                "buys": 0,
                "sells": 0,
                "total_buy_value": 0.0,
                "total_sell_value": 0.0,
                "last_trade_date": t.trade_date,
            }
        rec = insider_map[name]
        val = abs(parse_dollar_value(t.value))
        is_buy = "Purchase" in t.trade_type
        is_sell = "Sale" in t.trade_type

        if is_buy:
            rec["buys"] += 1
            rec["total_buy_value"] += val
        elif is_sell:
            rec["sells"] += 1
            rec["total_sell_value"] += val

        # Keep the most recent trade date
        if t.trade_date > rec["last_trade_date"]:
            rec["last_trade_date"] = t.trade_date

        # Track per-insider per-quarter data for tooltip breakdown
        if name not in iqdata:
            iqdata[name] = {}
        if qk not in iqdata[name]:
            iqdata[name][qk] = {
                "buys": 0, "sells": 0,
                "buy_val": 0.0, "sell_val": 0.0,
                "qty_sold": 0, "owned_last": 0,
            }
        iq = iqdata[name][qk]
        if is_buy:
            iq["buys"] += 1
            iq["buy_val"] += val
        elif is_sell:
            iq["sells"] += 1
            iq["sell_val"] += val
            qty_parsed = _parse_qty(t.qty)
            if qty_parsed is not None:
                iq["qty_sold"] += abs(qty_parsed)
        # Track owned-after from the most recent trade in this quarter
        owned_parsed = _parse_owned(t.owned)
        if owned_parsed is not None and owned_parsed > 0:
            iq["owned_last"] = owned_parsed

    insiders = sorted(
        insider_map.values(),
        key=lambda r: r["total_buy_value"] + r["total_sell_value"],
        reverse=True,
    )
    for ins in insiders:
        ins["buy_value_fmt"] = _format_compact_value(ins["total_buy_value"])
        ins["sell_value_fmt"] = _format_compact_value(ins["total_sell_value"])

        # Build quarterly breakdown for hover tooltip
        name = ins["name"]
        breakdown = []
        if name in iqdata:
            for qk in sorted(iqdata[name].keys(), reverse=True):
                iq = iqdata[name][qk]
                # Position % = qty_sold / (owned_after + qty_sold) * 100
                pct_sold = ""
                if iq["qty_sold"] > 0 and iq["owned_last"] > 0:
                    pre_trade = iq["owned_last"] + iq["qty_sold"]
                    pct = round(iq["qty_sold"] / pre_trade * 100, 1)
                    pct_sold = f"{pct}%"
                breakdown.append({
                    "label": _quarter_label(qk),
                    "buys": iq["buys"],
                    "sells": iq["sells"],
                    "buy_value_fmt": _format_compact_value(iq["buy_val"]),
                    "sell_value_fmt": _format_compact_value(iq["sell_val"]),
                    "pct_sold": pct_sold,
                })
        ins["quarterly_breakdown"] = breakdown

    # ── Group by quarter ──
    quarter_map: dict[str, list[InsiderTrade]] = {}
    for t in trades:
        qk = _quarter_key(t.trade_date)
        quarter_map.setdefault(qk, []).append(t)

    # Sort keys newest-first
    sorted_keys = sorted(quarter_map.keys(), reverse=True)

    quarters = []
    for qk in sorted_keys:
        qtrades = quarter_map[qk]
        buy_count = sum(1 for t in qtrades if "Purchase" in t.trade_type)
        sell_count = sum(1 for t in qtrades if "Sale" in t.trade_type)
        total_val = sum(abs(parse_dollar_value(t.value)) for t in qtrades)
        quarters.append({
            "label": _quarter_label(qk),
            "key": qk,
            "trades": qtrades,
            "buy_count": buy_count,
            "sell_count": sell_count,
            "total_value_fmt": _format_compact_value(total_val),
        })

    # ── Chart data (chronological order) ──
    chart_keys = sorted(quarter_map.keys())  # oldest-first for chart
    chart_labels = [_quarter_label(k) for k in chart_keys]
    chart_buy_values = []
    chart_sell_values = []
    for qk in chart_keys:
        qtrades = quarter_map[qk]
        bv = sum(abs(parse_dollar_value(t.value)) for t in qtrades if "Purchase" in t.trade_type)
        sv = sum(abs(parse_dollar_value(t.value)) for t in qtrades if "Sale" in t.trade_type)
        chart_buy_values.append(round(bv, 2))
        chart_sell_values.append(round(sv, 2))

    chart = {
        "labels": chart_labels,
        "buy_values": chart_buy_values,
        "sell_values": chart_sell_values,
    }

    # ── Per-insider chart data (for dropdown filter) ──
    per_insider_chart: dict[str, dict] = {"__all__": chart}
    for ins in insiders:
        name = ins["name"]
        ibv = []
        isv = []
        for qk in chart_keys:
            if name in iqdata and qk in iqdata[name]:
                ibv.append(round(iqdata[name][qk]["buy_val"], 2))
                isv.append(round(iqdata[name][qk]["sell_val"], 2))
            else:
                ibv.append(0)
                isv.append(0)
        per_insider_chart[name] = {
            "labels": chart_labels,
            "buy_values": ibv,
            "sell_values": isv,
        }

    return {"insiders": insiders, "quarters": quarters, "chart": chart, "per_insider_chart": per_insider_chart}
