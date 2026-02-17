"""Watchlist persistence – server-side JSON file.

Stores a list of starred/favorited tickers in
~/.13f-cache/watchlist.json (same directory as the fund cache).
"""

import json
from datetime import datetime
from pathlib import Path

from filings.cache import CACHE_DIR

WATCHLIST_FILE = CACHE_DIR / "watchlist.json"

# ── In-memory cache (avoids reading JSON from disk on every request) ──
_watchlist_cache: list[dict] | None = None


def load_watchlist() -> list[dict]:
    """Read watchlist, using in-memory cache when available."""
    global _watchlist_cache
    if _watchlist_cache is not None:
        return _watchlist_cache
    if not WATCHLIST_FILE.exists():
        return []
    try:
        data = json.loads(WATCHLIST_FILE.read_text())
        _watchlist_cache = data.get("tickers", [])
        return _watchlist_cache
    except (json.JSONDecodeError, OSError):
        return []


def save_watchlist(entries: list[dict]) -> None:
    """Atomic write of watchlist to disk + update in-memory cache."""
    global _watchlist_cache
    _watchlist_cache = entries
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    tmp = WATCHLIST_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps({"tickers": entries}, indent=2))
    tmp.replace(WATCHLIST_FILE)


def add_to_watchlist(ticker: str, cusip: str = "", issuer_name: str = "") -> list[dict]:
    """Add a ticker (idempotent). Returns the updated watchlist."""
    entries = load_watchlist()
    # Check if already present
    if any(e["ticker"] == ticker.upper() for e in entries):
        return entries
    entries.append({
        "ticker": ticker.upper(),
        "cusip": cusip,
        "issuer_name": issuer_name,
        "added_at": datetime.now().isoformat(timespec="seconds"),
    })
    save_watchlist(entries)
    return entries


def remove_from_watchlist(ticker: str) -> list[dict]:
    """Remove a ticker. Returns the updated watchlist."""
    entries = load_watchlist()
    entries = [e for e in entries if e["ticker"] != ticker.upper()]
    save_watchlist(entries)
    return entries


def is_in_watchlist(ticker: str) -> bool:
    """Check if a ticker is in the watchlist."""
    if not ticker:
        return False
    return any(e["ticker"] == ticker.upper() for e in load_watchlist())
