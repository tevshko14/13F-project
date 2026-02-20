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
    title: str          # CEO, CFO, Director, 10%, etc.
    trade_type: str     # Purchase, Sale, Sale+OE, etc.
    price: str          # keep as formatted string for display
    qty: str            # e.g. "+75,000" or "-3,752"
    owned: str          # shares owned after
    delta_own: str      # e.g. "+13%", "-4%"
    value: str          # e.g. "+$150,000"
    sec_url: str        # link to SEC Form 4

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


# ── Helpers ──────────────────────────────────────────────────────────

_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; 13f-insider-screener/1.0)"}
_OI_BASE = "http://openinsider.com"

_lock = threading.Lock()
_cache: dict[str, tuple[float, list[InsiderTrade]]] = {}
_GLOBAL_TTL = 300       # 5 min for global screener
_TICKER_TTL = 600       # 10 min for per-ticker
_MAX_CACHE = 300


def _evict_oldest() -> None:
    if len(_cache) <= _MAX_CACHE:
        return
    oldest = min(_cache, key=lambda k: _cache[k][0])
    _cache.pop(oldest, None)


def _get_cached(key: str, ttl: int) -> list[InsiderTrade] | None:
    with _lock:
        if key in _cache:
            ts, data = _cache[key]
            if time.time() - ts < ttl:
                return data
    return None


def _get_cached_with_stale(key: str, ttl: int) -> tuple[list[InsiderTrade] | None, bool]:
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
) -> list[dict]:
    """Aggregate trades by ticker, return top N by total dollar volume.

    Ranks tickers by ``buy_value + sell_value`` so the chart shows
    the most actively traded tickers with both purchases and sales
    visible on aggregate.

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
                "insiders": defaultdict(lambda: {"buy_value": 0.0, "sell_value": 0.0, "count": 0}),
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

    # Sort by total dollar volume (most actively traded tickers first)
    sorted_tickers = sorted(
        ticker_data.values(),
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

        result.append({
            "ticker": d["ticker"],
            "company_name": d["company_name"],
            "buy_value": round(d["buy_value"], 2),
            "sell_value": round(d["sell_value"], 2),
            "net_flow": round(d["buy_value"] - d["sell_value"], 2),
            "trade_count": d["trade_count"],
            "insider_details": insider_details,
            "date_range": date_range,
        })

    return result


# ── OpenInsider scrape helpers (fallback only) ───────────────────────


def _scrape_openinsider_global(
    trade_type: str = "",
    count: int = 100,
) -> list[InsiderTrade]:
    """Direct scrape of OpenInsider global screener (fallback path)."""
    url = f"{_OI_BASE}/screener"
    params: dict[str, str] = {
        "s": "", "o": "", "pl": "", "ph": "",
        "st": "0", "tc": "1",
        "t": trade_type,
        "vf": "", "o2d": "2", "sortcol": "0",
        "cnt": str(min(count, 100)), "page": "1",
    }
    try:
        resp = httpx.get(url, params=params, headers=_HEADERS,
                         timeout=15, follow_redirects=True)
        resp.raise_for_status()
        return _parse_table(resp.text, has_company_col=True)
    except Exception:
        logger.exception("Fallback scrape of OpenInsider global failed")
        return []


def _scrape_openinsider_ticker(ticker: str) -> list[InsiderTrade]:
    """Direct scrape of OpenInsider per-ticker page (fallback path)."""
    try:
        resp = httpx.get(f"{_OI_BASE}/{ticker}", headers=_HEADERS,
                         timeout=15, follow_redirects=True)
        resp.raise_for_status()
        return _parse_table(resp.text, has_company_col=False)
    except Exception:
        logger.exception("Fallback scrape of OpenInsider for %s failed", ticker)
        return []


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

    # ── L3: Fallback -- scrape OpenInsider directly ──
    logger.warning("Supabase insider_trades empty/unavailable -- scraping OpenInsider")
    trades = _scrape_openinsider_global(trade_type, count)
    if trades:
        _set_cached(cache_key, trades)
        return trades

    # ── L4: Stale L1 data -- never show empty/error to users ──
    if stale_data:
        logger.info("Returning stale L1 insider data for key=%s (%d trades)", cache_key, len(stale_data))
        return stale_data

    return []


def get_ticker_insider_trades(ticker: str) -> list[InsiderTrade]:
    """Fetch insider trades for a specific ticker -- Supabase-first.

    Data flow:
      1. L1 in-memory cache (10 min TTL)
      2. Query ``insider_trades`` table by ticker
      3. Fallback: scrape OpenInsider + backfill to Supabase
      4. Last resort: return stale L1 data (never show empty/error)
    """
    key = ticker.upper()
    cache_key = f"ticker:{key}"

    # ── L1: in-memory fast path (fresh hit) ──
    stale_data, is_fresh = _get_cached_with_stale(cache_key, _TICKER_TTL)
    if stale_data is not None and is_fresh:
        return stale_data

    # ── L2: Supabase dedicated table ──
    rows = supabase_cache.get_insider_trades_by_ticker(key)

    if rows:
        trades = [InsiderTrade.from_db_row(r) for r in rows]
        _set_cached(cache_key, trades)
        return trades

    # ── L3: Fallback -- scrape + backfill ──
    logger.warning("No Supabase data for %s -- scraping OpenInsider", key)
    trades = _scrape_and_backfill_ticker(key)
    if trades:
        _set_cached(cache_key, trades)
        return trades

    # ── L4: Stale L1 data -- never show empty/error to users ──
    if stale_data:
        logger.info("Returning stale L1 insider data for ticker=%s (%d trades)", key, len(stale_data))
        return stale_data

    return []
