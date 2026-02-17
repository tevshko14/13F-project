"""Notification engine — detects new 13F filings and matches against watchlist.

Persistence lives at ~/.13f-cache/notifications.json (same directory as cache/watchlist).
Detection works by comparing filing_date in fresh data vs. seen_filing_dates.
"""

import json
from datetime import date, datetime
from pathlib import Path

from filings.cache import CACHE_DIR

NOTIFICATIONS_FILE = CACHE_DIR / "notifications.json"

# Status codes from _compare_two_filings() → user-facing action labels
STATUS_TO_ACTION = {
    "NEW": "NEW BUY",
    "INCREASED": "ADD",
    "DECREASED": "REDUCE",
    "CLOSED": "SOLD",
}

# ── In-memory cache (avoids reading JSON from disk on every request) ──
_state_cache: dict | None = None


# ── Persistence ──────────────────────────────────────────────────────

def load_notification_state() -> dict:
    """Read notification state, using in-memory cache when available."""
    global _state_cache
    if _state_cache is not None:
        return _state_cache
    if not NOTIFICATIONS_FILE.exists():
        return {}
    try:
        _state_cache = json.loads(NOTIFICATIONS_FILE.read_text())
        return _state_cache
    except (json.JSONDecodeError, OSError):
        return {}


def save_notification_state(data: dict) -> None:
    """Atomic write of notification state to disk + update in-memory cache."""
    global _state_cache
    _state_cache = data
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    tmp = NOTIFICATIONS_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2))
    tmp.replace(NOTIFICATIONS_FILE)


def initialize_if_needed(cache_data: dict) -> None:
    """First-run setup: record all current filing_dates as 'already seen'.

    This prevents a flood of notifications when the feature is first activated.
    If notifications.json already exists, this is a no-op.
    """
    state = load_notification_state()
    if state.get("initialized_at"):
        return  # Already initialized

    seen = {}
    for cik, fund_data in cache_data.items():
        filing_date = fund_data.get("filing_date", "")
        if filing_date:
            seen[cik] = filing_date

    state = {
        "initialized_at": datetime.now().isoformat(timespec="seconds"),
        "seen_filing_dates": seen,
        "notifications": [],
    }
    save_notification_state(state)


# ── Detection ────────────────────────────────────────────────────────

def get_seen_filing_dates() -> dict[str, str]:
    """Return {CIK: filing_date} map of already-processed filings."""
    state = load_notification_state()
    return state.get("seen_filing_dates", {})


def is_new_filing(cik: str, new_filing_date: str) -> bool:
    """Check if this filing_date is different from what we've already seen."""
    if not new_filing_date:
        return False
    seen = get_seen_filing_dates()
    old_date = seen.get(cik)
    return new_filing_date != old_date


def mark_filing_seen(cik: str, filing_date: str) -> None:
    """Record that we've processed this filing (prevents re-notification)."""
    state = load_notification_state()
    if "seen_filing_dates" not in state:
        state["seen_filing_dates"] = {}
    state["seen_filing_dates"][cik] = filing_date
    save_notification_state(state)


# ── Watchlist Matching ───────────────────────────────────────────────

def check_watchlist_matches(
    cik: str,
    fund_display_name: str,
    fund_data: dict,
    watchlist_items: list[dict],
) -> list[dict]:
    """Check if any changes in this filing match watchlist tickers.

    Args:
        cik: Fund's CIK number
        fund_display_name: e.g. "Warren Buffett"
        fund_data: Full dict from get_fund_summary()
        watchlist_items: List of watchlist entry dicts

    Returns:
        List of notification dicts ready to persist.
    """
    if not watchlist_items or not fund_data.get("changes"):
        return []

    # Build lookup sets from watchlist
    cusip_set = set()
    ticker_set = set()
    for item in watchlist_items:
        if item.get("cusip"):
            cusip_set.add(item["cusip"].upper())
        if item.get("ticker"):
            ticker_set.add(item["ticker"].upper())

    # Build ticker-by-cusip and pct-by-cusip from fund's current holdings
    ticker_by_cusip: dict[str, str | None] = {}
    pct_by_cusip: dict[str, float] = {}
    for h in fund_data.get("all_holdings", []):
        cusip = h.get("cusip", "").upper()
        ticker_by_cusip[cusip] = h.get("ticker")
        pct_by_cusip[cusip] = h.get("pct", 0.0)

    filing_date = fund_data.get("filing_date", "")
    results = []

    for change in fund_data.get("changes", []):
        status = change.get("status", "")
        if status not in STATUS_TO_ACTION:
            continue

        cusip = change.get("cusip", "").upper()
        ticker = ticker_by_cusip.get(cusip)

        # Match against watchlist by cusip OR ticker
        matched = cusip in cusip_set
        if not matched and ticker and ticker.upper() in ticker_set:
            matched = True

        if not matched:
            continue

        action = STATUS_TO_ACTION[status]
        link = f"/stock/{ticker}" if ticker else f"/stock/cusip/{cusip}"

        notif = {
            "id": f"{cik}-{filing_date}-{cusip}",
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "type": "watchlist_match",
            "fund_cik": cik,
            "fund_name": fund_display_name,
            "ticker": ticker,
            "cusip": cusip,
            "issuer_name": change.get("issuer", "Unknown"),
            "action": action,
            "pct_of_portfolio": pct_by_cusip.get(cusip, 0.0),
            "filing_date": filing_date,
            "read": False,
            "link": link,
        }
        results.append(notif)

    return results


# ── Notification CRUD ────────────────────────────────────────────────

def add_notification(notif: dict) -> None:
    """Append a notification (deduplicates by id). Newest first."""
    state = load_notification_state()
    if "notifications" not in state:
        state["notifications"] = []

    # Deduplicate: skip if id already exists
    existing_ids = {n["id"] for n in state["notifications"]}
    if notif["id"] in existing_ids:
        return

    # Insert at the beginning (newest first)
    state["notifications"].insert(0, notif)

    # Cap at 200 notifications to prevent unbounded growth
    state["notifications"] = state["notifications"][:200]

    save_notification_state(state)


def get_all_notifications(limit: int = 50) -> list[dict]:
    """Return notifications (newest first), capped at limit."""
    state = load_notification_state()
    return state.get("notifications", [])[:limit]


def get_unread_count() -> int:
    """Return count of unread notifications."""
    state = load_notification_state()
    return sum(1 for n in state.get("notifications", []) if not n.get("read"))


def mark_notification_read(notification_id: str) -> None:
    """Mark a single notification as read."""
    state = load_notification_state()
    for n in state.get("notifications", []):
        if n["id"] == notification_id:
            n["read"] = True
            break
    save_notification_state(state)


def mark_all_read() -> None:
    """Mark all notifications as read."""
    state = load_notification_state()
    for n in state.get("notifications", []):
        n["read"] = True
    save_notification_state(state)


# ── Scheduling ───────────────────────────────────────────────────────

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
        return 2 * 3600   # 2 hours
    return 12 * 3600      # 12 hours
