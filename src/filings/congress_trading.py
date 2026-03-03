"""Congressional stock trading tracker — Capitol Trades scraper + display.

Tracks US Congress (House + Senate) stock trading activity from STOCK Act
disclosures.  Data sourced from Capitol Trades (capitoltrades.com) which
aggregates official financial disclosures.

Architecture (Phase 1 — cold historical data):
  - Scrape Capitol Trades paginated HTML (Next.js SSR with embedded JSON)
  - Store in Supabase ``congress_members`` + ``congress_trades`` (cold, write-once)
  - Display on politician profile pages and stock page "Congress" subtab

Phase 2 (future): hot table with periodic sync worker for recent filings.
"""

from __future__ import annotations

import hashlib
import heapq
import json
import logging
import re
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone

import httpx

logger = logging.getLogger(__name__)

# ── Constants ────────────────────────────────────────────────────────

_CT_BASE = "https://www.capitoltrades.com"
_CT_TRADES_URL = f"{_CT_BASE}/trades"
_CT_PAGE_SIZE = 100  # Capitol Trades supports pageSize param
_CT_DELAY = 2.0  # seconds between requests (polite scraping)
_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# STOCK Act disclosure amount ranges (official tiers)
_AMOUNT_RANGES: list[tuple[int, int, str]] = [
    (1_001, 15_000, "$1,001 - $15,000"),
    (15_001, 50_000, "$15,001 - $50,000"),
    (50_001, 100_000, "$50,001 - $100,000"),
    (100_001, 250_000, "$100,001 - $250,000"),
    (250_001, 500_000, "$250,001 - $500,000"),
    (500_001, 1_000_000, "$500,001 - $1,000,000"),
    (1_000_001, 5_000_000, "$1,000,001 - $5,000,000"),
    (5_000_001, 25_000_000, "$5,000,001 - $25,000,000"),
    (25_000_001, 50_000_000, "$25,000,001 - $50,000,000"),
]

# US state abbreviation → full name mapping
_STATE_ABBR_TO_NAME: dict[str, str] = {
    "al": "Alabama", "ak": "Alaska", "az": "Arizona", "ar": "Arkansas",
    "ca": "California", "co": "Colorado", "ct": "Connecticut", "de": "Delaware",
    "fl": "Florida", "ga": "Georgia", "hi": "Hawaii", "id": "Idaho",
    "il": "Illinois", "in": "Indiana", "ia": "Iowa", "ks": "Kansas",
    "ky": "Kentucky", "la": "Louisiana", "me": "Maine", "md": "Maryland",
    "ma": "Massachusetts", "mi": "Michigan", "mn": "Minnesota", "ms": "Mississippi",
    "mo": "Missouri", "mt": "Montana", "ne": "Nebraska", "nv": "Nevada",
    "nh": "New Hampshire", "nj": "New Jersey", "nm": "New Mexico", "ny": "New York",
    "nc": "North Carolina", "nd": "North Dakota", "oh": "Ohio", "ok": "Oklahoma",
    "or": "Oregon", "pa": "Pennsylvania", "ri": "Rhode Island", "sc": "South Carolina",
    "sd": "South Dakota", "tn": "Tennessee", "tx": "Texas", "ut": "Utah",
    "vt": "Vermont", "va": "Virginia", "wa": "Washington", "wv": "West Virginia",
    "wi": "Wisconsin", "wy": "Wyoming", "dc": "District of Columbia",
    "as": "American Samoa", "gu": "Guam", "mp": "Northern Mariana Islands",
    "pr": "Puerto Rico", "vi": "Virgin Islands",
}


# ── Data Models ──────────────────────────────────────────────────────


@dataclass
class CongressMember:
    """A US congressperson's profile."""

    member_id: str  # bioguide ID from Capitol Trades (e.g., "P000197")
    full_name: str
    first_name: str
    last_name: str
    party: str  # "Democrat", "Republican", "Independent"
    chamber: str  # "House", "Senate"
    state: str  # full name, e.g., "California"
    state_abbr: str  # e.g., "CA"
    district: str  # e.g., "" (derived from state for now)
    is_current: bool
    first_trade_date: str  # YYYY-MM-DD
    last_trade_date: str  # YYYY-MM-DD
    total_trades: int

    def to_db_row(self) -> dict:
        """Convert to a dict suitable for Supabase upsert."""
        return {
            "member_id": self.member_id,
            "full_name": self.full_name,
            "first_name": self.first_name,
            "last_name": self.last_name,
            "party": self.party,
            "chamber": self.chamber,
            "state": self.state,
            "state_abbr": self.state_abbr,
            "district": self.district,
            "is_current": self.is_current,
            "first_trade_date": self.first_trade_date or None,
            "last_trade_date": self.last_trade_date or None,
            "total_trades": self.total_trades,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }


@dataclass
class CongressTrade:
    """A single congressional stock trade (STOCK Act disclosure)."""

    trade_id: str  # deterministic dedup key
    member_id: str  # FK to CongressMember
    politician_name: str
    party: str
    chamber: str
    state: str  # state abbreviation
    ticker: str | None  # NULL for non-stock assets
    asset_name: str
    trade_type: str  # "buy", "sell", "exchange"
    trade_date: str  # YYYY-MM-DD
    filing_date: str  # YYYY-MM-DD (pubDate from Capitol Trades)
    amount_low: int | None
    amount_high: int | None
    amount_display: str  # "$1,001 - $15,000"
    owner: str  # "Self", "Spouse", "Dependent", "Joint", "not-disclosed"
    cap_trades_url: str  # URL to Capitol Trades detail page

    def to_db_row(self) -> dict:
        """Convert to a dict suitable for Supabase insert (write-once)."""
        return {
            "trade_id": self.trade_id,
            "member_id": self.member_id,
            "politician_name": self.politician_name,
            "party": self.party,
            "chamber": self.chamber,
            "state": self.state,
            "ticker": self.ticker if self.ticker else None,
            "asset_name": self.asset_name,
            "trade_type": self.trade_type,
            "trade_date": self.trade_date,
            "filing_date": self.filing_date,
            "amount_low": self.amount_low,
            "amount_high": self.amount_high,
            "amount_display": self.amount_display,
            "owner": self.owner,
            "cap_trades_url": self.cap_trades_url,
        }


# ── Amount Helpers ───────────────────────────────────────────────────


def _value_to_range(value: int | float | None) -> tuple[int | None, int | None, str]:
    """Map Capitol Trades midpoint ``value`` to STOCK Act range.

    Capitol Trades encodes amounts as approximate midpoints of the
    official STOCK Act disclosure tiers.

    Returns (low, high, display_string).
    """
    if value is None or value <= 0:
        return None, None, "N/A"

    for low, high, display in _AMOUNT_RANGES:
        midpoint = (low + high) / 2
        # Check if value is within ~20% of the midpoint (handles rounding)
        if value <= midpoint * 1.3 and value >= low * 0.5:
            return low, high, display

    # Over $50M
    if value > 25_000_000:
        return 50_000_001, None, "Over $50,000,000"

    # Fallback: find closest range
    for low, high, display in _AMOUNT_RANGES:
        if value <= high:
            return low, high, display

    return 50_000_001, None, "Over $50,000,000"


def _format_compact_amount(low: int | None, high: int | None) -> str:
    """Format amount range compactly: ``$1K - $15K``, ``$100K - $250K``."""
    if low is None:
        return "N/A"

    def _fmt(n: int) -> str:
        if n >= 1_000_000:
            return f"${n / 1_000_000:.1f}M".replace(".0M", "M")
        if n >= 1_000:
            return f"${n // 1_000}K"
        return f"${n:,}"

    if high is None:
        return f"Over {_fmt(low)}"
    return f"{_fmt(low)} - {_fmt(high)}"


def _format_net_worth(amount: int) -> str:
    """Format a single net-worth amount: ``$300M``, ``$16M``, ``$1.6M``."""
    if amount >= 1_000_000_000:
        val = amount / 1_000_000_000
        return f"${val:.1f}B".replace(".0B", "B")
    if amount >= 1_000_000:
        val = amount / 1_000_000
        return f"${val:.1f}M".replace(".0M", "M")
    if amount >= 1_000:
        return f"${amount // 1_000}K"
    return f"${amount:,}"


# ── JSON Extraction from Next.js SSR ─────────────────────────────────

# Capitol Trades is a Next.js app that embeds trade data in
# self.__next_f.push() calls within <script> tags.  The data is
# double-escaped JSON inside RSC (React Server Components) streaming
# format: self.__next_f.push([1,"...escaped content..."])
#
# The trade array lives inside a "data":[...] property of a DataTable
# component.  Each trade object has _txId, _politicianId, txType, etc.


def _extract_next_data(html: str) -> list[dict]:
    """Extract trade JSON objects from Next.js RSC streamed HTML.

    Capitol Trades embeds trade data as double-escaped JSON within
    ``self.__next_f.push([1, "..."])`` calls.  The trade array is in
    a ``"data":[{...},{...},...]`` structure within the RSC payload.

    Strategy:
    1. Find script tags containing both ``_txId`` and ``txType``
    2. Unescape the double-escaped JSON (``\\"`` → ``"``)
    3. Extract individual trade objects from the ``"data":[...]`` array
    """
    trades: list[dict] = []

    # Extract all script tag contents
    script_blocks = re.findall(r'<script[^>]*>(.*?)</script>', html, re.DOTALL)

    for block in script_blocks:
        if '"_txId"' not in block and '\\"_txId\\"' not in block:
            continue
        if '"txType"' not in block and '\\"txType\\"' not in block:
            continue

        # The content is double-escaped: \\" represents "
        # Unescape one level to get valid JSON fragments
        unescaped = block.replace('\\"', '"').replace("\\'", "'")

        # Find the "data":[...] array that contains trade objects
        # Pattern: "data":[{...trade...},{...trade...},...]
        data_match = re.search(r'"data"\s*:\s*\[', unescaped)
        if not data_match:
            continue

        # Extract from "data":[ to the matching ]
        start = data_match.end() - 1  # include the [
        depth = 0
        end = start
        for i in range(start, min(len(unescaped), start + 500_000)):
            ch = unescaped[i]
            if ch == '[':
                depth += 1
            elif ch == ']':
                depth -= 1
                if depth == 0:
                    end = i + 1
                    break

        if end <= start:
            continue

        array_str = unescaped[start:end]

        try:
            data_array = json.loads(array_str)
            if isinstance(data_array, list):
                for item in data_array:
                    if isinstance(item, dict) and "_txId" in item:
                        trades.append(item)
        except (json.JSONDecodeError, ValueError):
            # If full array parse fails, try extracting individual objects
            # Find each {...} block that contains _txId
            for obj_match in re.finditer(
                r'\{"_issuerId".*?"value"\s*:\s*\d+\s*\}', array_str
            ):
                try:
                    obj = json.loads(obj_match.group())
                    if "_txId" in obj:
                        trades.append(obj)
                except (json.JSONDecodeError, ValueError):
                    continue

    return trades


def _parse_trade_json(obj: dict, tx_id: int | None = None) -> CongressTrade | None:
    """Parse a single Capitol Trades JSON trade object into a CongressTrade.

    Expected JSON structure (from self.__next_f.push()):
    {
        "_txId": 20003795309,
        "_politicianId": "K000375",
        "_issuerId": 435544,
        "chamber": "house",
        "txDate": "2026-02-04",
        "txType": "buy",
        "value": 8000,
        "pubDate": "2026-02-27T14:05:02Z",
        "reportingGap": 22,
        "owner": "not-disclosed",
        "price": null,
        "politician": {
            "_stateId": "ma",
            "chamber": "house",
            "firstName": "William",
            "lastName": "Keating",
            "nickname": "Bill",
            "party": "democrat",
            "gender": "male",
            "dob": "1952-09-06"
        },
        "issuer": {
            "issuerName": "US TREASURY BILLS",
            "issuerTicker": null,
            "sector": null
        }
    }
    """
    try:
        tx_id = tx_id or obj.get("_txId")
        member_id = obj.get("_politicianId", "")
        if not member_id or not tx_id:
            return None

        pol = obj.get("politician", {})
        issuer = obj.get("issuer", {})

        # Build politician name
        first_name = pol.get("nickname") or pol.get("firstName", "")
        last_name = pol.get("lastName", "")
        full_name = f"{first_name} {last_name}".strip()

        # Party
        raw_party = pol.get("party", "").lower()
        party_map = {"democrat": "Democrat", "republican": "Republican",
                     "independent": "Independent"}
        party = party_map.get(raw_party, raw_party.title() if raw_party else "Unknown")

        # Chamber
        chamber = (obj.get("chamber") or pol.get("chamber", "")).lower()
        chamber = "House" if chamber == "house" else "Senate" if chamber == "senate" else chamber.title()

        # State
        state_abbr = (pol.get("_stateId") or "").upper()

        # Ticker
        ticker = issuer.get("issuerTicker")
        if ticker:
            # Capitol Trades uses "AAPL:US" format — strip the suffix
            ticker = ticker.split(":")[0].upper()

        # Asset name
        asset_name = issuer.get("issuerName", "Unknown Asset")

        # Trade type
        tx_type = (obj.get("txType") or "").lower()
        if tx_type not in ("buy", "sell", "exchange"):
            tx_type = tx_type or "unknown"

        # Dates
        trade_date = obj.get("txDate", "")  # "2026-02-04"
        pub_date = obj.get("pubDate", "")  # "2026-02-27T14:05:02Z"
        filing_date = pub_date[:10] if pub_date else ""  # just the date part

        # Amount range
        value = obj.get("value")
        amount_low, amount_high, amount_display = _value_to_range(value)

        # Owner
        owner = obj.get("owner", "not-disclosed")
        owner_map = {"self": "Self", "spouse": "Spouse", "child": "Dependent",
                     "dependent": "Dependent", "joint": "Joint",
                     "not-disclosed": "Not Disclosed"}
        owner = owner_map.get(owner.lower(), owner.title() if owner else "Not Disclosed")

        # Deterministic trade ID
        trade_id_raw = f"{member_id}_{ticker or 'NOTKR'}_{trade_date}_{tx_type}_{value}_{tx_id}"
        trade_id = hashlib.md5(trade_id_raw.encode()).hexdigest()[:16]

        # Capitol Trades URL
        cap_url = f"{_CT_BASE}/trades/{tx_id}" if tx_id else ""

        return CongressTrade(
            trade_id=trade_id,
            member_id=member_id,
            politician_name=full_name,
            party=party,
            chamber=chamber,
            state=state_abbr,
            ticker=ticker,
            asset_name=asset_name,
            trade_type=tx_type,
            trade_date=trade_date,
            filing_date=filing_date,
            amount_low=amount_low,
            amount_high=amount_high,
            amount_display=amount_display,
            owner=owner,
            cap_trades_url=cap_url,
        )

    except Exception as exc:
        logger.warning("Failed to parse trade JSON: %s — %s", exc, str(obj)[:200])
        return None


# ── Scraper ──────────────────────────────────────────────────────────


def scrape_capitol_trades(
    max_pages: int = 0,
    page_size: int = _CT_PAGE_SIZE,
    delay: float = _CT_DELAY,
    min_trade_date: str = "",
) -> list[CongressTrade]:
    """Scrape all trades from Capitol Trades paginated listing.

    Args:
        max_pages: Maximum pages to scrape (0 = all pages).
        page_size: Trades per page (Capitol Trades supports up to 100).
        delay: Seconds to wait between requests (polite scraping).
        min_trade_date: Optional YYYY-MM-DD cutoff — trades older than
            this are skipped, and scraping stops when an entire page
            falls below the cutoff (Capitol Trades is newest-first).

    Returns:
        List of CongressTrade objects, newest first.
    """
    all_trades: list[CongressTrade] = []
    seen_ids: set[str] = set()
    page = 1

    if min_trade_date:
        logger.info("Date cutoff: trades before %s will be skipped", min_trade_date)

    with httpx.Client(headers=_HEADERS, timeout=30.0, follow_redirects=True) as client:
        while True:
            if max_pages and page > max_pages:
                break

            url = f"{_CT_TRADES_URL}?page={page}&pageSize={page_size}"
            logger.info("Scraping Capitol Trades page %d: %s", page, url)

            try:
                resp = client.get(url)
                resp.raise_for_status()
            except httpx.HTTPError as exc:
                logger.error("HTTP error on page %d: %s", page, exc)
                break

            html = resp.text
            trade_objs = _extract_next_data(html)

            if not trade_objs:
                logger.info("No more trades found on page %d — stopping", page)
                break

            page_count = 0
            skipped_old = 0
            for obj in trade_objs:
                trade = _parse_trade_json(obj)
                if trade is None:
                    continue
                if trade.trade_id in seen_ids:
                    continue
                # Date cutoff: skip trades before min_trade_date
                if min_trade_date and trade.trade_date and trade.trade_date < min_trade_date:
                    skipped_old += 1
                    continue
                seen_ids.add(trade.trade_id)
                all_trades.append(trade)
                page_count += 1

            logger.info(
                "Page %d: parsed %d trades (%d unique total)%s",
                page, page_count, len(all_trades),
                f" (skipped {skipped_old} before cutoff)" if skipped_old else "",
            )

            # If the entire page was before the cutoff, we've gone far enough
            if min_trade_date and page_count == 0 and skipped_old > 0:
                logger.info(
                    "Entire page %d was before %s — stopping", page, min_trade_date
                )
                break

            if page_count == 0 and skipped_old == 0:
                logger.info("No new trades on page %d — stopping", page)
                break

            page += 1
            if delay > 0:
                time.sleep(delay)

    logger.info("Scraping complete: %d total unique trades", len(all_trades))
    return all_trades


# ── Member Extraction ────────────────────────────────────────────────


def extract_members_from_trades(trades: list[CongressTrade]) -> list[CongressMember]:
    """Derive unique CongressMember profiles from scraped trade data.

    Aggregates per-politician: trade count, first/last trade dates.
    """
    members: dict[str, dict] = {}  # member_id -> aggregated data

    for t in trades:
        mid = t.member_id
        if mid not in members:
            # Parse full name parts from politician_name
            parts = t.politician_name.split(None, 1)
            first = parts[0] if parts else ""
            last = parts[1] if len(parts) > 1 else ""

            state_full = _STATE_ABBR_TO_NAME.get(t.state.lower(), t.state)

            members[mid] = {
                "member_id": mid,
                "full_name": t.politician_name,
                "first_name": first,
                "last_name": last,
                "party": t.party,
                "chamber": t.chamber,
                "state": state_full,
                "state_abbr": t.state.upper(),
                "district": "",
                "is_current": True,  # assume current; can refine later
                "trades": [],
            }

        members[mid]["trades"].append(t.trade_date)

    result: list[CongressMember] = []
    for mid, data in members.items():
        dates = sorted(d for d in data["trades"] if d)
        result.append(CongressMember(
            member_id=data["member_id"],
            full_name=data["full_name"],
            first_name=data["first_name"],
            last_name=data["last_name"],
            party=data["party"],
            chamber=data["chamber"],
            state=data["state"],
            state_abbr=data["state_abbr"],
            district=data["district"],
            is_current=data["is_current"],
            first_trade_date=dates[0] if dates else "",
            last_trade_date=dates[-1] if dates else "",
            total_trades=len(data["trades"]),
        ))

    result.sort(key=lambda m: m.total_trades, reverse=True)
    logger.info("Extracted %d unique congress members from %d trades", len(result), len(trades))
    return result


# ── Display Preparation ──────────────────────────────────────────────


def prepare_politician_display(
    trades: list[dict],
    member: dict,
) -> dict:
    """Build display data for a politician profile page.

    Args:
        trades: List of trade dicts from Supabase (congress_trades rows).
        member: Congress member dict from Supabase (congress_members row).

    Returns:
        Dict with keys: member, trades, portfolio, net_worth_low,
        net_worth_high, total_trades, total_buys, total_sells, years_active.
    """
    # Sort trades newest first
    sorted_trades = sorted(trades, key=lambda t: t.get("trade_date", ""), reverse=True)

    total_buys = sum(1 for t in sorted_trades if t.get("trade_type") == "buy")
    total_sells = sum(1 for t in sorted_trades if t.get("trade_type") == "sell")

    # Build portfolio: net position per ticker from buy/sell history
    # Since STOCK Act only gives ranges, we estimate using midpoints
    portfolio: dict[str, dict] = {}  # ticker -> aggregated data
    for t in sorted_trades:
        ticker = t.get("ticker")
        if not ticker:
            continue

        if ticker not in portfolio:
            portfolio[ticker] = {
                "ticker": ticker,
                "asset_name": t.get("asset_name", ticker),
                "buys": 0,
                "sells": 0,
                "buy_value_low": 0,
                "buy_value_high": 0,
                "sell_value_low": 0,
                "sell_value_high": 0,
                "last_action": t.get("trade_type", ""),
                "last_trade_date": t.get("trade_date", ""),
            }

        pos = portfolio[ticker]
        low = t.get("amount_low") or 0
        high = t.get("amount_high") or low

        if t.get("trade_type") == "buy":
            pos["buys"] += 1
            pos["buy_value_low"] += low
            pos["buy_value_high"] += high
        elif t.get("trade_type") == "sell":
            pos["sells"] += 1
            pos["sell_value_low"] += low
            pos["sell_value_high"] += high

    # Compute estimated holdings (buys - sells by value range)
    portfolio_list: list[dict] = []
    net_worth_low = 0
    net_worth_high = 0

    for ticker, pos in portfolio.items():
        est_low = max(0, pos["buy_value_low"] - pos["sell_value_high"])
        est_high = max(0, pos["buy_value_high"] - pos["sell_value_low"])

        if est_high <= 0 and pos["sells"] >= pos["buys"]:
            # Likely fully sold
            continue

        pos["est_value_low"] = est_low
        pos["est_value_high"] = est_high
        pos["est_value_display"] = _format_compact_amount(est_low, est_high) if est_high > 0 else "Sold"
        pos["total_trades"] = pos["buys"] + pos["sells"]
        portfolio_list.append(pos)

        net_worth_low += est_low
        net_worth_high += est_high

    # Sort portfolio by estimated value (high end), descending
    portfolio_list.sort(key=lambda p: p.get("est_value_high", 0), reverse=True)

    # Years active
    first_date = member.get("first_trade_date", "")
    last_date = member.get("last_trade_date", "")
    if first_date and last_date:
        try:
            first_year = int(first_date[:4])
            last_year = int(last_date[:4])
            years_active = f"{first_year} - {last_year}" if first_year != last_year else str(first_year)
        except (ValueError, IndexError):
            years_active = ""
    else:
        years_active = ""

    # Format trades for display
    display_trades = []
    for t in sorted_trades:
        display_trades.append({
            **t,
            "amount_compact": _format_compact_amount(
                t.get("amount_low"), t.get("amount_high")
            ),
        })

    # Use stored net-worth estimate when available; fall back to trade-range calc
    stored_nw = member.get("net_worth_estimate")
    portfolio_display = _format_compact_amount(net_worth_low, net_worth_high)

    if stored_nw and int(stored_nw) > 0:
        nw_display = _format_net_worth(int(stored_nw))
        nw_source = member.get("net_worth_source", "")
        nw_year = member.get("net_worth_year")
    else:
        nw_display = portfolio_display
        nw_source = ""
        nw_year = None

    return {
        "member": member,
        "trades": display_trades,
        "portfolio": portfolio_list,
        "net_worth_low": net_worth_low,
        "net_worth_high": net_worth_high,
        "net_worth_display": nw_display,
        "net_worth_source": nw_source,
        "net_worth_year": nw_year,
        "portfolio_value_display": portfolio_display,
        "total_trades": len(sorted_trades),
        "total_buys": total_buys,
        "total_sells": total_sells,
        "years_active": years_active,
    }


def prepare_stock_congress_display(
    ticker: str,
    trades: list[dict],
) -> dict:
    """Build display data for the stock page Congress subtab.

    Args:
        ticker: Stock ticker symbol.
        trades: List of trade dicts from Supabase for this ticker.

    Returns:
        Dict with keys: ticker, politicians, party_breakdown,
        recent_trades, total_politicians.
    """
    sorted_trades = sorted(trades, key=lambda t: t.get("trade_date", ""), reverse=True)

    # Aggregate per politician
    by_politician: dict[str, dict] = {}
    dem_value = 0
    rep_value = 0

    for t in sorted_trades:
        mid = t.get("member_id", "")
        if mid not in by_politician:
            by_politician[mid] = {
                "member_id": mid,
                "name": t.get("politician_name", ""),
                "party": t.get("party", ""),
                "party_short": t.get("party", "")[:1],  # "D" or "R"
                "chamber": t.get("chamber", ""),
                "state": t.get("state", ""),
                "total_buys": 0,
                "total_sells": 0,
                "buy_value": 0,
                "sell_value": 0,
                "last_action": "",
                "last_trade_date": "",
            }

        pol = by_politician[mid]
        midpoint = 0
        low = t.get("amount_low") or 0
        high = t.get("amount_high") or 0
        if low and high:
            midpoint = (low + high) // 2

        if t.get("trade_type") == "buy":
            pol["total_buys"] += 1
            pol["buy_value"] += midpoint
        elif t.get("trade_type") == "sell":
            pol["total_sells"] += 1
            pol["sell_value"] += midpoint

        if not pol["last_action"]:
            pol["last_action"] = t.get("trade_type", "")
            pol["last_trade_date"] = t.get("trade_date", "")

        # Party aggregation (use midpoints for rough estimates)
        party = t.get("party", "").lower()
        if t.get("trade_type") == "buy":
            if "democrat" in party:
                dem_value += midpoint
            elif "republican" in party:
                rep_value += midpoint

    # Build politician list
    politicians: list[dict] = []
    for pol in by_politician.values():
        net_value = pol["buy_value"] - pol["sell_value"]
        pol["net_value"] = max(0, net_value)
        pol["total_trades"] = pol["total_buys"] + pol["total_sells"]
        pol["est_value_fmt"] = _format_compact_amount(
            max(0, net_value - net_value // 4),
            max(0, net_value + net_value // 4),
        ) if net_value > 0 else "Net Seller"
        politicians.append(pol)

    politicians.sort(key=lambda p: p.get("net_value", 0), reverse=True)

    # Party breakdown
    total_value = dem_value + rep_value
    party_breakdown = {
        "democrat_pct": round(dem_value / total_value * 100) if total_value else 0,
        "republican_pct": round(rep_value / total_value * 100) if total_value else 0,
        "democrat_value": dem_value,
        "republican_value": rep_value,
        "democrat_value_fmt": _format_compact_amount(dem_value, dem_value),
        "republican_value_fmt": _format_compact_amount(rep_value, rep_value),
    }

    # Recent trades (top 20)
    recent_trades = []
    for t in sorted_trades[:20]:
        recent_trades.append({
            **t,
            "amount_compact": _format_compact_amount(
                t.get("amount_low"), t.get("amount_high")
            ),
            "party_short": t.get("party", "")[:1],
        })

    return {
        "ticker": ticker,
        "politicians": politicians,
        "party_breakdown": party_breakdown,
        "recent_trades": recent_trades,
        "total_politicians": len(politicians),
        "total_trades": len(sorted_trades),
    }


# ── Congress Page Display Functions (Phase 2) ───────────────────────────


def prepare_chamber_visualization(
    members: list[dict],
    trades: list[dict],
) -> dict:
    """Build ECharts scatter data for House + Senate dot visualizations.

    Each politician = one dot arranged in a grid. Color by party.
    Tooltip shows: name, party, state, total trades, last trade date,
    top 3 holdings by estimated value.

    Args:
        members: All congress_members rows.
        trades: All trades (with ticker) for top-holdings calculation.

    Returns:
        Dict with ``house`` and ``senate`` keys, each containing ``data``
        (list of dot dicts) and party counts.
    """
    _PARTY_COLORS = {
        "Democrat": "#2563eb",
        "Republican": "#dc2626",
        "Independent": "#6b7280",
    }

    # Pre-compute top 3 holdings per member from trades
    member_holdings: dict[str, dict[str, dict]] = {}  # member_id -> ticker -> agg
    for t in trades:
        mid = t.get("member_id", "")
        ticker = t.get("ticker")
        if not mid or not ticker:
            continue
        if mid not in member_holdings:
            member_holdings[mid] = {}
        if ticker not in member_holdings[mid]:
            member_holdings[mid][ticker] = {
                "ticker": ticker,
                "asset_name": t.get("asset_name", ticker),
                "buy_value": 0,
                "sell_value": 0,
            }
        low = t.get("amount_low") or 0
        high = t.get("amount_high") or 0
        mid_val = (low + high) // 2 if low and high else 0
        if t.get("trade_type") == "buy":
            member_holdings[mid][ticker]["buy_value"] += mid_val
        elif t.get("trade_type") == "sell":
            member_holdings[mid][ticker]["sell_value"] += mid_val

    def _top_holdings(member_id: str) -> list[dict]:
        holdings = member_holdings.get(member_id, {})
        ranked = []
        for h in holdings.values():
            net = h["buy_value"] - h["sell_value"]
            if net > 0:
                ranked.append({
                    "ticker": h["ticker"],
                    "name": h["asset_name"],
                    "net_value": net,
                    "est_value": _format_compact_amount(
                        max(0, net - net // 4), net + net // 4
                    ),
                })
        ranked.sort(key=lambda x: x.get("net_value", 0), reverse=True)
        return ranked[:3]

    result = {}
    for chamber in ("House", "Senate"):
        chamber_members = [
            m for m in members if m.get("chamber") == chamber
        ]
        # Sort: Democrats first, then Republicans, then Independent
        # Within each party, sort by total_trades DESC
        party_order = {"Democrat": 0, "Republican": 1, "Independent": 2}
        chamber_members.sort(
            key=lambda m: (
                party_order.get(m.get("party", ""), 9),
                -(m.get("total_trades") or 0),
            )
        )

        cols = 20 if chamber == "House" else 10
        dots = []
        for i, m in enumerate(chamber_members):
            x = i % cols
            y = i // cols
            party = m.get("party", "Independent")
            dots.append({
                "x": x,
                "y": y,
                "member_id": m.get("member_id", ""),
                "name": m.get("full_name", ""),
                "party": party,
                "party_short": party[:1],
                "state": m.get("state_abbr", ""),
                "chamber": chamber,
                "total_trades": m.get("total_trades", 0),
                "last_trade": m.get("last_trade_date", ""),
                "top_holdings": _top_holdings(m.get("member_id", "")),
                "color": _PARTY_COLORS.get(party, "#6b7280"),
            })

        dem_count = sum(1 for d in dots if d["party"] == "Democrat")
        rep_count = sum(1 for d in dots if d["party"] == "Republican")
        ind_count = sum(1 for d in dots if d["party"] == "Independent")

        key = chamber.lower()
        result[key] = {
            "data": dots,
            "total": len(dots),
            "democrat": dem_count,
            "republican": rep_count,
            "independent": ind_count,
            "rows": (len(dots) // cols) + (1 if len(dots) % cols else 0),
            "cols": cols,
        }

    return result


def prepare_congress_trending(trades: list[dict], top_n: int = 15) -> list[dict]:
    """Most-bought stocks by Congress in last 6 months.

    Same structure as superinvestor 'most_added' for ECharts bar chart.

    Args:
        trades: Recent trades (e.g., last 6 months).
        top_n: Number of top stocks to return.

    Returns:
        List of dicts with ticker, name, add_count, party_breakdown,
        top_traders, link.
    """
    # Count unique buyers per ticker
    ticker_buyers: dict[str, dict] = {}  # ticker -> aggregation

    for t in trades:
        ticker = t.get("ticker")
        if not ticker or t.get("trade_type") != "buy":
            continue

        if ticker not in ticker_buyers:
            ticker_buyers[ticker] = {
                "ticker": ticker,
                "name": t.get("asset_name", ticker),
                "buyers": set(),
                "buyer_names": {},
                "democrat": 0,
                "republican": 0,
            }

        agg = ticker_buyers[ticker]
        mid = t.get("member_id", "")
        if mid not in agg["buyers"]:
            agg["buyers"].add(mid)
            agg["buyer_names"][mid] = t.get("politician_name", "")
            party = t.get("party", "").lower()
            if "democrat" in party:
                agg["democrat"] += 1
            elif "republican" in party:
                agg["republican"] += 1

    # Rank by buyer count
    ranked = sorted(
        ticker_buyers.values(),
        key=lambda x: len(x["buyers"]),
        reverse=True,
    )[:top_n]

    result = []
    for item in ranked:
        # Pick top 5 buyer names
        top_traders = list(item["buyer_names"].values())[:5]
        result.append({
            "ticker": item["ticker"],
            "name": item["name"],
            "add_count": len(item["buyers"]),
            "democrat": item["democrat"],
            "republican": item["republican"],
            "top_traders": top_traders,
            "link": f"/stock/{item['ticker']}",
        })

    return result


def prepare_congress_consensus(
    trades: list[dict],
    members: list[dict],
    top_n: int = 10,
) -> list[dict]:
    """Top stocks by total congressional ownership (# of members holding).

    Horizontal bar chart — same pattern as grand_portfolio consensus_json.

    Args:
        trades: All trades with tickers.
        members: All congress_members for name lookup.
        top_n: Number of top stocks to return.

    Returns:
        List of dicts with ticker, issuer, holders, combined_value,
        top_holders, link.
    """
    member_map = {m["member_id"]: m for m in (members or [])}

    # Aggregate per ticker: unique members who are net buyers
    ticker_agg: dict[str, dict] = {}

    for t in trades:
        ticker = t.get("ticker")
        mid = t.get("member_id", "")
        if not ticker or not mid:
            continue

        if ticker not in ticker_agg:
            ticker_agg[ticker] = {
                "ticker": ticker,
                "issuer": t.get("asset_name", ticker),
                "member_buys": {},   # member_id -> buy_value
                "member_sells": {},  # member_id -> sell_value
            }

        agg = ticker_agg[ticker]
        low = t.get("amount_low") or 0
        high = t.get("amount_high") or 0
        mid_val = (low + high) // 2 if low and high else 0

        if t.get("trade_type") == "buy":
            agg["member_buys"][mid] = agg["member_buys"].get(mid, 0) + mid_val
        elif t.get("trade_type") == "sell":
            agg["member_sells"][mid] = agg["member_sells"].get(mid, 0) + mid_val

    # Compute net holders (members who are net buyers)
    results = []
    for ticker, agg in ticker_agg.items():
        holders = []
        combined_low = 0
        combined_high = 0

        for mid in set(list(agg["member_buys"].keys()) + list(agg["member_sells"].keys())):
            bought = agg["member_buys"].get(mid, 0)
            sold = agg["member_sells"].get(mid, 0)
            net = bought - sold
            if net > 0:
                name = member_map.get(mid, {}).get("full_name", mid)
                holders.append((name, net))
                combined_low += max(0, net - net // 4)
                combined_high += net + net // 4

        if not holders:
            continue

        holders.sort(key=lambda x: x[1], reverse=True)
        top_holder_names = [h[0] for h in holders[:5]]

        results.append({
            "ticker": agg["ticker"],
            "issuer": agg["issuer"],
            "holders": len(holders),
            "combined_value": combined_high,
            "combined_value_fmt": _format_compact_amount(combined_low, combined_high),
            "top_holders": top_holder_names,
            "link": f"/stock/{agg['ticker']}",
        })

    return heapq.nlargest(top_n, results, key=lambda x: x["holders"])


def prepare_congress_momentum(trades: list[dict], top_n: int = 15) -> list[dict]:
    """Recent momentum — stocks with most net buys in last 90 days.

    Vertical bar chart like grand_portfolio momentum_json.

    Args:
        trades: Recent trades (last ~6 months; we filter to 90 days).
        top_n: Number of top stocks to return.

    Returns:
        List of dicts with ticker, name, buys, sells, net, traders, link.
    """
    from datetime import datetime, timedelta, timezone

    cutoff = (datetime.now(timezone.utc) - timedelta(days=90)).strftime("%Y-%m-%d")

    ticker_agg: dict[str, dict] = {}

    for t in trades:
        ticker = t.get("ticker")
        trade_date = t.get("trade_date", "")
        if not ticker or trade_date < cutoff:
            continue

        if ticker not in ticker_agg:
            ticker_agg[ticker] = {
                "ticker": ticker,
                "name": t.get("asset_name", ticker),
                "buys": 0,
                "sells": 0,
                "traders": set(),
                "trader_names": [],
            }

        agg = ticker_agg[ticker]
        if t.get("trade_type") == "buy":
            agg["buys"] += 1
        elif t.get("trade_type") == "sell":
            agg["sells"] += 1

        mid = t.get("member_id", "")
        if mid and mid not in agg["traders"]:
            agg["traders"].add(mid)
            agg["trader_names"].append(t.get("politician_name", ""))

    # Rank by net (buys - sells)
    ranked = []
    for agg in ticker_agg.values():
        net = agg["buys"] - agg["sells"]
        ranked.append({
            "ticker": agg["ticker"],
            "name": agg["name"],
            "buys": agg["buys"],
            "sells": agg["sells"],
            "net": net,
            "traders": agg["trader_names"][:5],
            "link": f"/stock/{agg['ticker']}",
        })

    return heapq.nlargest(top_n, ranked, key=lambda x: x["net"])


def _estimate_midpoint(low: int | None, high: int | None) -> float:
    """Return dollar midpoint for a STOCK Act amount range."""
    if low is None:
        return 0.0
    if high is None:
        return float(low)
    return (low + high) / 2.0


def _filter_activity_trades(
    trades: list[dict],
    timeframe: str = "ALL",
    chamber: str = "all",
    party: str = "all",
) -> list[dict]:
    """Apply timeframe/chamber/party filters to trade list."""
    from datetime import datetime, timedelta, timezone

    filtered = list(trades)

    # Timeframe filter
    if timeframe != "ALL":
        now = datetime.now(timezone.utc).date()
        if timeframe == "1W":
            cutoff = now - timedelta(days=7)
        elif timeframe == "1M":
            cutoff = now - timedelta(days=30)
        elif timeframe == "3M":
            cutoff = now - timedelta(days=90)
        else:
            cutoff = None
        if cutoff:
            cutoff_str = cutoff.strftime("%Y-%m-%d")
            filtered = [
                t for t in filtered if t.get("filing_date", "") >= cutoff_str
            ]

    # Chamber filter
    if chamber and chamber.lower() != "all":
        filtered = [
            t for t in filtered
            if (t.get("chamber") or "").lower() == chamber.lower()
        ]

    # Party filter
    if party and party.lower() != "all":
        filtered = [
            t for t in filtered
            if (t.get("party") or "").lower() == party.lower()
        ]

    return filtered


def prepare_congress_activity_stats(trades: list[dict]) -> dict:
    """Compute summary stats for the activity dashboard header.

    Uses midpoint of STOCK Act amount ranges for dollar flows.

    Returns:
        Dict with value_sentiment, net_dollar_flow, total_buy_value,
        total_sell_value, most_traded_ticker, bipartisan_count,
        total_activities.
    """
    total_buy_value = 0.0
    total_sell_value = 0.0
    total_buys = 0
    total_sells = 0
    ticker_counts: dict[str, int] = {}
    # Track which parties traded each ticker for bipartisan detection
    ticker_parties: dict[str, set] = {}

    for t in trades:
        mid = _estimate_midpoint(t.get("amount_low"), t.get("amount_high"))
        tt = (t.get("trade_type") or "").lower()
        ticker = t.get("ticker")

        if tt in ("buy", "purchase"):
            total_buy_value += mid
            total_buys += 1
        elif tt in ("sell", "sale", "sale (full)", "sale (partial)"):
            total_sell_value += mid
            total_sells += 1

        if ticker:
            ticker_counts[ticker] = ticker_counts.get(ticker, 0) + 1
            if ticker not in ticker_parties:
                ticker_parties[ticker] = set()
            party = (t.get("party") or "")[:1]
            if party:
                ticker_parties[ticker].add(party)

    net_dollar_flow = total_buy_value - total_sell_value

    # Sentiment based on dollar flow
    if net_dollar_flow > 0 and total_buy_value > 0:
        ratio = total_buy_value / max(total_sell_value, 1)
        value_sentiment = "BULLISH" if ratio >= 1.1 else "NEUTRAL"
    elif net_dollar_flow < 0 and total_sell_value > 0:
        ratio = total_sell_value / max(total_buy_value, 1)
        value_sentiment = "BEARISH" if ratio >= 1.1 else "NEUTRAL"
    else:
        value_sentiment = "NEUTRAL"

    # Most traded ticker
    most_traded = ""
    if ticker_counts:
        most_traded = max(ticker_counts, key=ticker_counts.get)

    # Bipartisan: tickers traded by both D and R
    bipartisan_count = sum(
        1 for parties in ticker_parties.values()
        if "D" in parties and "R" in parties
    )

    return {
        "value_sentiment": value_sentiment,
        "net_dollar_flow": net_dollar_flow,
        "total_buy_value": total_buy_value,
        "total_sell_value": total_sell_value,
        "total_buys": total_buys,
        "total_sells": total_sells,
        "most_traded_ticker": most_traded,
        "bipartisan_count": bipartisan_count,
        "total_activities": len(trades),
    }


def prepare_congress_consensus_moves(
    trades: list[dict], min_members: int = 3, top_n: int = 30
) -> list[dict]:
    """Group trades by ticker to find consensus moves.

    A consensus move is when 3+ Congress members trade the same stock.
    Each cluster shows the ticker, net sentiment, and individual trades.

    Returns:
        List of cluster dicts sorted by total member count DESC.
    """
    from collections import defaultdict

    ticker_groups: dict[str, list[dict]] = defaultdict(list)
    for t in trades:
        ticker = t.get("ticker")
        if ticker:
            ticker_groups[ticker].append(t)

    clusters = []
    for ticker, group in ticker_groups.items():
        # Count unique members
        member_ids = set(t.get("member_id") for t in group)
        if len(member_ids) < min_members:
            continue

        # Aggregate buy/sell values
        buy_value = 0.0
        sell_value = 0.0
        buy_count = 0
        sell_count = 0
        parties = set()

        for t in group:
            mid = _estimate_midpoint(t.get("amount_low"), t.get("amount_high"))
            tt = (t.get("trade_type") or "").lower()
            if tt in ("buy", "purchase"):
                buy_value += mid
                buy_count += 1
            elif tt in ("sell", "sale", "sale (full)", "sale (partial)"):
                sell_value += mid
                sell_count += 1
            party = (t.get("party") or "")[:1]
            if party:
                parties.add(party)

        net_flow = buy_value - sell_value
        if buy_count > sell_count:
            net_sentiment = "BULLISH"
        elif sell_count > buy_count:
            net_sentiment = "BEARISH"
        else:
            net_sentiment = "NEUTRAL"

        # Build summary text
        parts = []
        if buy_count:
            parts.append(f"{buy_count} bought")
        if sell_count:
            parts.append(f"{sell_count} sold")
        action_summary = ", ".join(parts)

        # Bipartisan?
        bipartisan = "D" in parties and "R" in parties

        # Individual trade items (sorted by filing_date DESC)
        items = sorted(
            group, key=lambda t: t.get("filing_date", ""), reverse=True
        )
        enriched_items = []
        for t in items:
            enriched_items.append({
                "member_id": t.get("member_id", ""),
                "politician_name": t.get("politician_name", ""),
                "party": t.get("party", ""),
                "party_short": (t.get("party") or "")[:1],
                "chamber": t.get("chamber", ""),
                "state": t.get("state", ""),
                "trade_type": t.get("trade_type", ""),
                "amount_compact": _format_compact_amount(
                    t.get("amount_low"), t.get("amount_high")
                ),
                "trade_date": t.get("trade_date", ""),
                "filing_date": t.get("filing_date", ""),
            })

        # Issuer name from first trade
        issuer = group[0].get("asset_name", ticker)

        clusters.append({
            "ticker": ticker,
            "issuer_name": issuer,
            "member_count": len(member_ids),
            "net_sentiment": net_sentiment,
            "action_summary": action_summary,
            "buy_value": buy_value,
            "sell_value": sell_value,
            "net_flow": net_flow,
            "bipartisan": bipartisan,
            "trades": enriched_items,
        })

    return heapq.nlargest(top_n, clusters, key=lambda c: c["member_count"])


def prepare_congress_activity(
    trades: list[dict],
    timeframe: str = "ALL",
    chamber: str = "all",
    party: str = "all",
    limit: int = 200,
) -> dict:
    """Full activity dashboard data — stats, consensus, individual trades.

    Args:
        trades: Recent trades (pre-sorted or will be sorted here).
        timeframe: "1W", "1M", "3M", or "ALL".
        chamber: "all", "house", or "senate".
        party: "all", "democrat", or "republican".
        limit: Max individual items to return.

    Returns:
        Dict with keys: stats, clusters, solo_items, total_solo,
        timeframe, chamber, party.
    """
    from datetime import datetime, timezone

    # Apply filters
    filtered = _filter_activity_trades(trades, timeframe, chamber, party)

    # Compute stats on filtered trades
    stats = prepare_congress_activity_stats(filtered)

    # Consensus moves (3+ members on same ticker)
    clusters = prepare_congress_consensus_moves(filtered, min_members=3, top_n=30)

    # Tickers already in clusters
    cluster_tickers = set(c["ticker"] for c in clusters)

    # Individual moves — trades NOT in a consensus cluster
    now = datetime.now(timezone.utc).date()
    sorted_trades = sorted(
        filtered, key=lambda t: t.get("filing_date", ""), reverse=True
    )

    solo_items = []
    for t in sorted_trades:
        ticker = t.get("ticker")
        if ticker in cluster_tickers:
            continue
        if len(solo_items) >= limit:
            break

        filing_date = t.get("filing_date", "")
        days_ago = 0
        if filing_date:
            try:
                fd = datetime.strptime(filing_date, "%Y-%m-%d").date()
                days_ago = (now - fd).days
            except (ValueError, TypeError):
                pass

        solo_items.append({
            "trade_id": t.get("trade_id", ""),
            "member_id": t.get("member_id", ""),
            "politician_name": t.get("politician_name", ""),
            "party": t.get("party", ""),
            "party_short": (t.get("party") or "")[:1],
            "chamber": t.get("chamber", ""),
            "state": t.get("state", ""),
            "ticker": ticker,
            "asset_name": t.get("asset_name", ""),
            "trade_type": t.get("trade_type", ""),
            "trade_date": t.get("trade_date", ""),
            "filing_date": filing_date,
            "amount_display": t.get("amount_display", ""),
            "amount_compact": _format_compact_amount(
                t.get("amount_low"), t.get("amount_high")
            ),
            "owner": t.get("owner", "Self"),
            "days_ago": days_ago,
        })

    return {
        "stats": stats,
        "clusters": clusters,
        "solo_items": solo_items,
        "total_solo": len(solo_items),
        "timeframe": timeframe,
        "chamber": chamber,
        "party": party,
    }


def prepare_trade_frequency(
    members: list[dict],
    recent_trades: list[dict],
    top_n: int = 25,
) -> list[dict]:
    """Top traders by number of trades in the past 12 months.

    Returns a sorted list of politicians with their trade counts,
    party affiliation, state, and recent buy tickers for the tooltip.

    Each entry:
        {"name": "Nancy Pelosi", "state": "CA", "party": "Democrat",
         "party_short": "D", "color": "#2563eb", "trade_count": 46,
         "member_id": "P000197", "chamber": "House",
         "recent_buys": ["NVDA", "AVGO", "VST"],
         "top_holdings": [{"ticker": "NVDA", "name": "NVIDIA Corp"}]}
    """
    from datetime import datetime, timedelta

    _PARTY_COLORS = {
        "Democrat": "#2563eb",
        "Republican": "#dc2626",
        "Independent": "#6b7280",
    }

    # Build member lookup
    member_map: dict[str, dict] = {}
    for m in members:
        mid = m.get("member_id", "")
        if mid:
            member_map[mid] = m

    # Count trades per member in the recent trades window
    cutoff = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
    member_trades: dict[str, int] = {}
    member_recent_buys: dict[str, list[str]] = {}  # member_id -> [ticker, ...]
    member_holdings_agg: dict[str, dict[str, dict]] = {}  # member_id -> ticker -> agg

    for t in recent_trades:
        mid = t.get("member_id", "")
        trade_date = t.get("trade_date", "")
        if not mid or trade_date < cutoff:
            continue

        member_trades[mid] = member_trades.get(mid, 0) + 1

        ticker = t.get("ticker")
        if not ticker:
            continue

        # Track recent buys (newest first)
        if t.get("trade_type") == "buy":
            if mid not in member_recent_buys:
                member_recent_buys[mid] = []
            if ticker not in member_recent_buys[mid]:
                member_recent_buys[mid].append(ticker)

        # Aggregate holdings for top holdings tooltip
        if mid not in member_holdings_agg:
            member_holdings_agg[mid] = {}
        if ticker not in member_holdings_agg[mid]:
            member_holdings_agg[mid][ticker] = {
                "ticker": ticker,
                "name": t.get("asset_name", ticker),
                "net_value": 0,
            }
        low = t.get("amount_low") or 0
        high = t.get("amount_high") or 0
        mid_val = (low + high) // 2 if low and high else 0
        if t.get("trade_type") == "buy":
            member_holdings_agg[mid][ticker]["net_value"] += mid_val
        elif t.get("trade_type") == "sell":
            member_holdings_agg[mid][ticker]["net_value"] -= mid_val

    # Build result list — sort by trade count DESC
    ranked = sorted(member_trades.items(), key=lambda x: -x[1])[:top_n]

    result = []
    for mid, count in ranked:
        m = member_map.get(mid, {})
        party = m.get("party", "Independent")

        # Top 3 holdings by net value
        holdings = member_holdings_agg.get(mid, {})
        top_h = sorted(
            [h for h in holdings.values() if h["net_value"] > 0],
            key=lambda x: -x["net_value"],
        )[:3]

        result.append({
            "name": m.get("full_name", mid),
            "state": m.get("state_abbr", ""),
            "party": party,
            "party_short": party[:1],
            "color": _PARTY_COLORS.get(party, "#6b7280"),
            "trade_count": count,
            "member_id": m.get("member_id", mid),
            "chamber": m.get("chamber", ""),
            "recent_buys": member_recent_buys.get(mid, [])[:3],
            "top_holdings": [
                {"ticker": h["ticker"], "name": h["name"]}
                for h in top_h
            ],
        })

    return result


def prepare_net_worth_leaderboard(members: list[dict], top_n: int = 30) -> list[dict]:
    """Build a net-worth leaderboard from stored estimates.

    Returns a list of dicts sorted by net_worth_estimate DESC,
    ready for the horizontal bar chart on /congress.
    """
    _PARTY_COLORS = {
        "Democrat": "#2563eb",
        "Republican": "#dc2626",
        "Independent": "#6b7280",
    }

    candidates = [
        m for m in (members or [])
        if m.get("net_worth_estimate") and int(m["net_worth_estimate"]) > 0
    ]
    candidates.sort(key=lambda m: int(m["net_worth_estimate"]), reverse=True)

    result: list[dict] = []
    for m in candidates[:top_n]:
        nw = int(m["net_worth_estimate"])
        party = m.get("party", "Independent")
        parts = (m.get("full_name", "") or "").split()
        last_name = parts[-1] if len(parts) > 1 else m.get("full_name", "")

        result.append({
            "name": m.get("full_name", ""),
            "label": f"{last_name} ({m.get('state_abbr', '')})",
            "party": party,
            "chamber": m.get("chamber", ""),
            "color": _PARTY_COLORS.get(party, "#6b7280"),
            "net_worth": nw,
            "net_worth_display": _format_net_worth(nw),
            "member_id": m.get("member_id", ""),
            "state": m.get("state_abbr", ""),
            "source": m.get("net_worth_source", ""),
            "year": m.get("net_worth_year"),
        })

    return result


def prepare_congress_page_data(
    members: list[dict],
    all_trades: list[dict],
    recent_trades: list[dict],
) -> dict:
    """Build all display data for the /congress page.

    Orchestrates all sub-preparation functions and returns a single
    dict that can be unpacked into the template context.

    Args:
        members: All congress_members rows.
        all_trades: All trades with tickers (for consensus calculation).
        recent_trades: Recent trades (last 6 months, for trending/momentum).

    Returns:
        Dict with keys: chamber_viz, trending, consensus, momentum,
        activity, stats.
    """
    chamber_viz = prepare_chamber_visualization(members or [], all_trades or [])

    # Use recent trades for trending and momentum
    trending = prepare_congress_trending(recent_trades or [])
    momentum = prepare_congress_momentum(recent_trades or [])

    # Use all trades for consensus (needs full dataset)
    consensus = prepare_congress_consensus(all_trades or [], members or [])

    # Activity from recent trades (default: all filters, used for initial load)
    activity = prepare_congress_activity(recent_trades or [], limit=200)

    # Trade frequency — most active traders in last 12 months
    trade_frequency = prepare_trade_frequency(members or [], recent_trades or [], top_n=25)

    # Net worth leaderboard
    net_worth_leaderboard = prepare_net_worth_leaderboard(members or [], top_n=30)

    # Summary stats
    all_tickers = set()
    for t in (all_trades or []):
        ticker = t.get("ticker")
        if ticker:
            all_tickers.add(ticker)

    trade_dates = [t.get("trade_date", "") for t in (all_trades or []) if t.get("trade_date")]
    min_date = min(trade_dates) if trade_dates else ""
    max_date = max(trade_dates) if trade_dates else ""

    stats = {
        "total_members": len(members or []),
        "total_trades": len(all_trades or []),
        "unique_tickers": len(all_tickers),
        "date_range_start": min_date,
        "date_range_end": max_date,
        "house_count": sum(1 for m in (members or []) if m.get("chamber") == "House"),
        "senate_count": sum(1 for m in (members or []) if m.get("chamber") == "Senate"),
    }

    return {
        "chamber_viz": chamber_viz,
        "trending": trending,
        "consensus": consensus,
        "momentum": momentum,
        "activity": activity,
        "trade_frequency": trade_frequency,
        "net_worth_leaderboard": net_worth_leaderboard,
        "stats": stats,
    }
