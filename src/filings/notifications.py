"""Notification engine for PaperPanda.

Detects new 13F filings, YouTube events, Reddit velocity spikes, and
notable insider trades — then creates notification rows for the
Supabase ``notifications`` table.

The notifications table is *global* (no per-user rows).  Dismiss state
is tracked client-side via ``localStorage('pp-notifications-last-seen')``.

Notification IDs are deterministic for deduplication:
  - 13F changes:    ``13f-{cik}-{filing_date}-{cusip}``
  - YouTube:         ``yt-{video_id}``
  - Reddit spikes:   ``reddit-{ticker}-{YYYY-MM-DD}``
  - Congress trades: ``congress-{member_id}-{trade_id}``
  - Insider trades:  ``insider-{filing_date}-{ticker}-{hash8}``
"""

from __future__ import annotations

import hashlib
import logging
import re
from datetime import date, datetime, timezone

logger = logging.getLogger(__name__)

_TAG_RE = re.compile(r"<[^>]+>")


def _sanitize(text: str, max_len: int = 300) -> str:
    """Strip HTML tags and clamp length — prevents XSS in toast/templates."""
    if not text:
        return ""
    return _TAG_RE.sub("", str(text))[:max_len]


# Status codes from _compare_two_filings() → user-facing action labels
STATUS_TO_ACTION = {
    "NEW": "NEW BUY",
    "INCREASED": "ADD",
    "DECREASED": "REDUCE",
    "CLOSED": "SOLD",
}

# ── Filing season helpers ────────────────────────────────────────────


def is_filing_season() -> bool:
    """Return True if we're within ±15 days of a 13F filing deadline.

    Filing deadlines (~45 days after quarter-end):
      Q4 (Dec 31) → ~Feb 14
      Q1 (Mar 31) → ~May 15
      Q2 (Jun 30) → ~Aug 14
      Q3 (Sep 30) → ~Nov 14
    """
    today = date.today()
    deadlines = [
        date(today.year, 2, 14),
        date(today.year, 5, 15),
        date(today.year, 8, 14),
        date(today.year, 11, 14),
    ]
    for d in deadlines:
        if abs((today - d).days) <= 15:
            return True
    return False


def get_poll_interval_seconds() -> int:
    """Return polling interval in seconds.

    During filing season: 2 hours (filings appear daily)
    Off-season: 12 hours (minimal activity)
    """
    if is_filing_season():
        return 2 * 3600  # 2 hours
    return 12 * 3600  # 12 hours


# ── 13F Filing Change Detection ─────────────────────────────────────


def detect_13f_changes(
    cik: str,
    fund_name: str,
    new_data: dict,
    old_data: dict | None,
    *,
    max_notifications: int = 5,
) -> list[dict]:
    """Compare new vs old 13F data and return notification rows.

    Detects NEW BUY, ADD, REDUCE, SOLD actions from the ``changes``
    list in the fund data.  Returns at most *max_notifications* rows
    (sorted by portfolio weight change, descending).

    Args:
        cik:       Fund CIK number
        fund_name: Display name (e.g. "Warren Buffett")
        new_data:  Fresh fund_summary dict from SEC EDGAR
        old_data:  Previous fund_summary (from cache) — or None on first sync
        max_notifications: Cap per fund per filing cycle

    Returns:
        List of notification dicts matching the Supabase ``notifications``
        schema (id, type, title, message, icon, toast_type, link, metadata).
    """
    if not new_data:
        return []

    # Compare filing dates to detect a genuinely new filing
    new_filing_date = new_data.get("filing_date", "")
    if not new_filing_date:
        return []

    if old_data:
        old_filing_date = old_data.get("filing_date", "")
        if new_filing_date == old_filing_date:
            return []  # Same filing — no changes

    changes = new_data.get("changes", [])
    if not changes:
        return []

    # Build ticker + pct lookup from current holdings
    ticker_by_cusip: dict[str, str | None] = {}
    pct_by_cusip: dict[str, float] = {}
    for h in new_data.get("all_holdings", []):
        cusip = h.get("cusip", "").upper()
        ticker_by_cusip[cusip] = h.get("ticker")
        pct_by_cusip[cusip] = h.get("pct", 0.0)

    # Build notification rows
    notifs: list[dict] = []
    for change in changes:
        status = change.get("status", "")
        if status not in STATUS_TO_ACTION:
            continue

        cusip = change.get("cusip", "").upper()
        ticker = ticker_by_cusip.get(cusip) or change.get("ticker")
        action = STATUS_TO_ACTION[status]
        issuer = change.get("issuer", "Unknown")
        pct = pct_by_cusip.get(cusip, 0.0)

        is_buy = action in ("NEW BUY", "ADD")
        icon = "📈" if is_buy else "📉"
        toast_type = "bullish" if is_buy else "bearish"
        link = f"/stock/{ticker}" if ticker else ""

        title = f"{_sanitize(fund_name)}: {action}"
        if ticker:
            title += f" — {ticker}"

        pct_str = f" ({pct:.1f}% of portfolio)" if pct > 0 else ""
        message = f"{_sanitize(issuer)} {action.lower()}{pct_str}"

        notifs.append(
            {
                "id": f"13f-{cik}-{new_filing_date}-{cusip}",
                "type": "13f_change",
                "title": title,
                "message": message,
                "icon": icon,
                "toast_type": toast_type,
                "link": link,
                "metadata": {
                    "cik": cik,
                    "fund_name": fund_name,
                    "ticker": ticker,
                    "cusip": cusip,
                    "action": action,
                    "pct_of_portfolio": round(pct, 2),
                    "filing_date": new_filing_date,
                },
                "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            }
        )

    # Sort by portfolio weight (biggest movers first) and cap
    notifs.sort(key=lambda n: n["metadata"].get("pct_of_portfolio", 0), reverse=True)
    return notifs[:max_notifications]


# ── YouTube Notification Creator ─────────────────────────────────────


def create_youtube_notification(event: dict) -> dict | None:
    """Create a notification dict for a high-impact YouTube event.

    Returns ``None`` if the event doesn't qualify (impact_score < 7).

    Args:
        event: A youtube_events row dict (from youtube_sync).

    Returns:
        A notification dict matching the Supabase schema, or None.
    """
    impact = event.get("impact_score", 0)
    if impact < 7:
        return None

    video_id = event.get("video_id", "")
    if not video_id:
        return None

    channel_name = event.get("channel_name", "Unknown Channel")
    title_text = event.get("title", "New Video")
    sentiment = event.get("sentiment", "neutral")
    video_url = event.get("video_url", "")

    # Map sentiment to toast type
    toast_map = {"bullish": "bullish", "bearish": "bearish"}
    toast_type = toast_map.get(sentiment, "alert")

    # Truncate long titles
    display_title = title_text[:100] + ("…" if len(title_text) > 100 else "")

    tickers = event.get("tickers", [])
    ticker_str = ", ".join(f"${t}" for t in tickers[:5]) if tickers else ""

    notif_title = f"{_sanitize(channel_name)}: New Video"
    message = _sanitize(display_title)
    if ticker_str:
        message += f" [{ticker_str}]"

    # Only allow YouTube URLs for the link
    safe_link = ""
    if video_url and video_url.startswith("https://"):
        safe_link = video_url
    elif video_id:
        safe_link = f"https://youtube.com/watch?v={video_id}"

    return {
        "id": f"yt-{video_id}",
        "type": "youtube",
        "title": notif_title,
        "message": message,
        "icon": "🎬",
        "toast_type": toast_type,
        "link": safe_link,
        "metadata": {
            "video_id": video_id,
            "channel_name": channel_name,
            "sentiment": sentiment,
            "impact_score": impact,
            "tickers": tickers[:10] if tickers else [],
        },
        "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }


# ── Reddit Velocity Notification Creator ─────────────────────────────


def create_reddit_notification(
    ticker: str,
    name: str,
    velocity_pct: float,
    mentions: int,
) -> dict | None:
    """Create a notification dict for a Reddit velocity spike.

    Returns ``None`` if velocity_pct < 100 (mentions didn't double).

    Args:
        ticker:       Stock ticker (e.g. "TSLA")
        name:         Company name (e.g. "Tesla Inc")
        velocity_pct: 24h mention velocity percentage
        mentions:     Current mention count

    Returns:
        A notification dict matching the Supabase schema, or None.
    """
    if velocity_pct < 100:
        return None

    today_str = date.today().isoformat()

    sign = "+" if velocity_pct >= 0 else ""
    message = f"{_sanitize(name)} — mentions {sign}{velocity_pct:.0f}% in 24h ({mentions} total)"

    return {
        "id": f"reddit-{ticker}-{today_str}",
        "type": "reddit_velocity",
        "title": f"${ticker} trending on Reddit",
        "message": message,
        "icon": "🔥",
        "toast_type": "alert",
        "link": "/retail?view=leaderboard",
        "metadata": {
            "ticker": ticker,
            "name": name,
            "velocity_pct": round(velocity_pct, 1),
            "mentions": mentions,
        },
        "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }


# ── Congress trade notifications ─────────────────────────────────────


def create_congress_trade_notification(trade: dict) -> dict | None:
    """Create a notification for a new congressional trade filing.

    Deterministic ID: ``congress-{member_id}-{trade_id}``
    Type: ``congress_trade``

    Args:
        trade: Dict with keys from ``congress_trades`` table row.

    Returns:
        A notification dict matching the Supabase schema, or None.
    """
    member_id = trade.get("member_id", "")
    trade_id = trade.get("trade_id", "")
    if not member_id or not trade_id:
        return None

    trade_type = trade.get("trade_type", "")
    action = "bought" if trade_type == "buy" else "sold" if trade_type == "sell" else "traded"
    ticker = trade.get("ticker") or trade.get("asset_name", "assets")

    party = trade.get("party", "")
    party_icon = "🔵" if party == "Democrat" else "🔴" if party == "Republican" else "⚪"

    name = _sanitize(trade.get("politician_name", "Unknown"))
    amount = trade.get("amount_display", "")
    filing_date = trade.get("filing_date", "")

    message = f"{amount}" if amount else ""
    if filing_date:
        message += f" — Filed {filing_date}" if message else f"Filed {filing_date}"

    return {
        "id": f"congress-{member_id}-{trade_id}",
        "type": "congress_trade",
        "title": f"{party_icon} {name} {action} {ticker}",
        "message": _sanitize(message),
        "icon": "🏛️",
        "toast_type": "bullish" if trade_type == "buy" else "bearish",
        "link": f"/politician/{member_id}",
        "metadata": {
            "ticker": trade.get("ticker"),
            "party": party,
            "member_id": member_id,
        },
        "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }


# ── Insider trade notifications ──────────────────────────────────────


def _parse_dollar_value(s: str) -> float:
    """Parse formatted dollar string like ``'$1,234,567'`` → ``1234567.0``."""
    try:
        return float(
            s.replace("$", "").replace(",", "").replace("+", "").strip()
        )
    except (ValueError, AttributeError):
        return 0.0


def create_insider_trade_notification(trade: dict) -> dict | None:
    """Create a notification for a notable insider trade.

    Deterministic ID: ``insider-{filing_date}-{ticker}-{hash8}``
    Type: ``insider_trade``

    Notable trade criteria:
      - Purchases with value >= $100,000
      - Sales with value >= $1,000,000

    Args:
        trade: Dict with keys from ``insider_trades`` table row
               (uses ``value_fmt``, ``trade_type``, ``ticker``, etc.).

    Returns:
        A notification dict matching the Supabase schema, or None.
    """
    ticker = trade.get("ticker", "")
    insider_name = trade.get("insider_name", "")
    trade_type = trade.get("trade_type", "")
    filing_date = trade.get("filing_date", "")
    if not ticker or not insider_name or not filing_date:
        return None

    # Parse formatted value string (e.g. "$1,234,567")
    value_str = trade.get("value_fmt", "") or ""
    value_num = _parse_dollar_value(value_str)

    # Filter: only notable trades
    is_purchase = "Purchase" in trade_type
    is_sale = "Sale" in trade_type
    if is_purchase and value_num < 100_000:
        return None
    if is_sale and value_num < 1_000_000:
        return None
    if not is_purchase and not is_sale:
        return None

    action = "bought" if is_purchase else "sold"
    icon = "📈" if is_purchase else "📉"
    toast = "bullish" if is_purchase else "bearish"

    title_str = trade.get("title", "")
    name = _sanitize(insider_name)

    # Deterministic ID — sec_url + insider_name guarantees uniqueness
    sec_url = trade.get("sec_url", "")
    id_hash = hashlib.md5(f"{sec_url}{insider_name}".encode()).hexdigest()[:8]

    message = f"{value_str}" if value_str else ""
    if title_str:
        message += f" — {title_str}" if message else title_str

    return {
        "id": f"insider-{filing_date}-{ticker}-{id_hash}",
        "type": "insider_trade",
        "title": f"{icon} {name} {action} ${ticker}",
        "message": _sanitize(message),
        "icon": icon,
        "toast_type": toast,
        "link": f"/stock/{ticker}",
        "metadata": {
            "ticker": ticker,
            "insider_name": insider_name,
            "trade_type": trade_type,
        },
        "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
