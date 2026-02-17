import json
import os
import time
from datetime import datetime, timedelta
from pathlib import Path

CACHE_DIR = Path(os.environ.get("CACHE_DIR", Path.home() / ".13f-cache"))
CACHE_FILE = CACHE_DIR / "fund_data.json"
REFRESH_INTERVAL = timedelta(hours=6)


def load_cache() -> dict:
    """Load all cached fund data from disk. Returns empty dict if no cache."""
    if not CACHE_FILE.exists():
        return {}
    try:
        return json.loads(CACHE_FILE.read_text())
    except (json.JSONDecodeError, OSError):
        return {}


def save_cache(data: dict) -> None:
    """Write all cached fund data to disk (atomic write)."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    tmp_file = CACHE_FILE.with_suffix(".tmp")
    tmp_file.write_text(json.dumps(data, indent=2))
    tmp_file.replace(CACHE_FILE)


def is_cache_stale() -> bool:
    """Check if cache is older than REFRESH_INTERVAL."""
    if not CACHE_FILE.exists():
        return True
    try:
        mtime = datetime.fromtimestamp(CACHE_FILE.stat().st_mtime)
        return datetime.now() - mtime > REFRESH_INTERVAL
    except OSError:
        return True


def get_cache_age_str() -> str:
    """Return human-readable cache age string."""
    if not CACHE_FILE.exists():
        return "No cache"
    try:
        mtime = datetime.fromtimestamp(CACHE_FILE.stat().st_mtime)
        age = datetime.now() - mtime
        if age.total_seconds() < 60:
            return "Just now"
        elif age.total_seconds() < 3600:
            return f"{int(age.total_seconds() / 60)} min ago"
        elif age.total_seconds() < 86400:
            return f"{int(age.total_seconds() / 3600)} hours ago"
        else:
            return f"{int(age.total_seconds() / 86400)} days ago"
    except OSError:
        return "Unknown"


def refresh_single_fund(cik: str) -> dict | None:
    """Fetch fresh data for a single fund from SEC EDGAR. Synchronous.
    Returns cached data dict or None on failure."""
    from filings.client import get_fund_summary
    try:
        return get_fund_summary(cik)
    except Exception:
        return None
