"""Insider trading screener -- scrapes OpenInsider for SEC Form 4 data.

Two modes:
  1. Global screener: latest insider buys/sells across all stocks
  2. Per-ticker: insider trades for a specific company

Caching strategy:
  - L1: in-memory dict with 5-10 min TTL (fast path, sub-ms)
  - L2: Supabase via ``fetch_with_cache`` (survives Railway deploys,
        returns stale data on API failure)

OpenInsider aggregates SEC Form 4 filings into clean HTML tables,
so we avoid parsing raw XML.
"""

from __future__ import annotations

import logging
import re
import threading
import time
from dataclasses import dataclass, asdict

import httpx
from bs4 import BeautifulSoup

from filings import supabase_cache

logger = logging.getLogger(__name__)

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

        # Extract SEC filing link from the first cell
        sec_url = ""
        first_a = cells[0].find("a")
        if first_a and first_a.get("href"):
            href = first_a["href"]
            if href.startswith("/"):
                sec_url = f"https://www.sec.gov{href}"
            elif href.startswith("http"):
                sec_url = href

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
    """Aggregate trades by ticker, return top N by total absolute dollar volume.

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

    # Sort by total absolute volume, take top N
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


# ── Public API ───────────────────────────────────────────────────────


def get_latest_insider_trades(
    trade_type: str = "",  # "p"=purchases, "s"=sales, ""=all
    count: int = 100,
) -> list[InsiderTrade]:
    """Fetch latest insider trades from OpenInsider (global screener).

    Uses a two-tier cache:
      - L1: in-memory dict (5 min TTL, sub-ms reads)
      - L2: Supabase via ``fetch_with_cache`` (survives Railway deploys)

    Args:
        trade_type: "p" for purchases only, "s" for sales only, "" for all.
        count: Number of trades to fetch (max 100).
    """
    cache_key = f"global:{trade_type or 'all'}:{count}"

    # ── L1: in-memory fast path ──
    cached = _get_cached(cache_key, _GLOBAL_TTL)
    if cached is not None:
        return cached

    # ── L2: Supabase-backed fetch with stale fallback ──
    supabase_key = f"insider_{cache_key}"

    def _fetch_from_openinsider() -> list[dict] | None:
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
            trades = _parse_table(resp.text, has_company_col=True)
            if trades:
                return [t.to_dict() for t in trades]
        except Exception:
            logger.exception("Failed to fetch OpenInsider global screener")
        return None

    result_data = supabase_cache.fetch_with_cache(
        cache_key=supabase_key,
        category="insider",
        ttl_days=1.0 / 288,  # 5 minutes
        api_fetch_fn=_fetch_from_openinsider,
        symbol="global",
    )

    # Reconstruct InsiderTrade objects from dicts
    trades: list[InsiderTrade] = []
    if result_data and isinstance(result_data, list):
        for d in result_data:
            try:
                trades.append(InsiderTrade(**d))
            except Exception:
                continue

    _set_cached(cache_key, trades)
    return trades


def get_ticker_insider_trades(ticker: str) -> list[InsiderTrade]:
    """Fetch insider trades for a specific ticker from OpenInsider.

    Uses a two-tier cache:
      - L1: in-memory dict (10 min TTL, sub-ms reads)
      - L2: Supabase via ``fetch_with_cache`` (survives Railway deploys)
    """
    key = ticker.upper()
    cache_key = f"ticker:{key}"

    # ── L1: in-memory fast path ──
    cached = _get_cached(cache_key, _TICKER_TTL)
    if cached is not None:
        return cached

    # ── L2: Supabase-backed fetch with stale fallback ──
    supabase_key = f"insider_ticker:{key}"

    def _fetch_ticker() -> list[dict] | None:
        try:
            resp = httpx.get(f"{_OI_BASE}/{key}", headers=_HEADERS,
                             timeout=15, follow_redirects=True)
            resp.raise_for_status()
            trades = _parse_table(resp.text, has_company_col=False)
            if trades:
                return [t.to_dict() for t in trades]
        except Exception:
            logger.exception("Failed to fetch OpenInsider data for %s", key)
        return None

    result_data = supabase_cache.fetch_with_cache(
        cache_key=supabase_key,
        category="insider",
        ttl_days=1.0 / 144,  # 10 minutes
        api_fetch_fn=_fetch_ticker,
        symbol=key,
    )

    # Reconstruct InsiderTrade objects from dicts
    trades: list[InsiderTrade] = []
    if result_data and isinstance(result_data, list):
        for d in result_data:
            try:
                trades.append(InsiderTrade(**d))
            except Exception:
                continue

    _set_cached(cache_key, trades)
    return trades
