"""Insider trading screener — scrapes OpenInsider for SEC Form 4 data.

Two modes:
  1. Global screener: latest insider buys/sells across all stocks
  2. Per-ticker: insider trades for a specific company

Data is cached in memory with short TTLs since insider trades are
time-sensitive.  OpenInsider aggregates SEC Form 4 filings into
clean HTML tables, so we avoid parsing raw XML.
"""

from __future__ import annotations

import logging
import re
import threading
import time
from dataclasses import dataclass, asdict

import httpx
from bs4 import BeautifulSoup

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


# ── Public API ───────────────────────────────────────────────────────


def get_latest_insider_trades(
    trade_type: str = "",  # "p"=purchases, "s"=sales, ""=all
    count: int = 100,
) -> list[InsiderTrade]:
    """Fetch latest insider trades from OpenInsider (global screener).

    Args:
        trade_type: "p" for purchases only, "s" for sales only, "" for all.
        count: Number of trades to fetch (max 100).
    """
    cache_key = f"global:{trade_type or 'all'}:{count}"
    cached = _get_cached(cache_key, _GLOBAL_TTL)
    if cached is not None:
        return cached

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
    except Exception:
        logger.exception("Failed to fetch OpenInsider global screener")
        trades = []

    _set_cached(cache_key, trades)
    return trades


def get_ticker_insider_trades(ticker: str) -> list[InsiderTrade]:
    """Fetch insider trades for a specific ticker from OpenInsider."""
    key = ticker.upper()
    cache_key = f"ticker:{key}"
    cached = _get_cached(cache_key, _TICKER_TTL)
    if cached is not None:
        return cached

    url = f"{_OI_BASE}/{key}"
    try:
        resp = httpx.get(url, headers=_HEADERS, timeout=15,
                         follow_redirects=True)
        resp.raise_for_status()
        trades = _parse_table(resp.text, has_company_col=False)
    except Exception:
        logger.exception("Failed to fetch OpenInsider data for %s", key)
        trades = []

    _set_cached(cache_key, trades)
    return trades
