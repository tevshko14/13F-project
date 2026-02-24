"""Fetch official SEC company filings (10-K, 10-Q, 8-K, Form 4, etc.).

Uses the edgartools library (already a project dependency) to query SEC EDGAR.
Results are cached in memory with a per-ticker TTL to avoid repeated API hits.
"""

from __future__ import annotations

import logging
import os
import threading
import time

from edgar import set_identity, Company

# Ensure SEC identity is set (may already be set by client.py at import time)
_identity = os.environ.get("SEC_IDENTITY", "13f-tool-user user@example.com")
set_identity(_identity)

logger = logging.getLogger(__name__)

# ── Thread-safe cache ────────────────────────────────────────────────
_lock = threading.Lock()
_cache: dict[str, tuple[float, list[dict]]] = {}
_TTL = 3_600  # 1 hour – filings don't change often
_MAX_CACHE_ENTRIES = 500

_FORM_TYPES = ["10-K", "10-Q", "8-K", "4"]


def _evict_oldest() -> None:
    """Remove the oldest cache entry when we exceed _MAX_CACHE_ENTRIES."""
    if len(_cache) <= _MAX_CACHE_ENTRIES:
        return
    oldest_key = min(_cache, key=lambda k: _cache[k][0])
    _cache.pop(oldest_key, None)


def get_company_filings(ticker: str, limit: int = 25) -> list[dict]:
    """Return recent SEC filings for *ticker*, with TTL caching.

    Each filing dict contains:
        form, filing_date, description, sec_url, items
    """
    key = ticker.upper()

    with _lock:
        if key in _cache:
            ts, data = _cache[key]
            if time.time() - ts < _TTL:
                return data

    # Fetch fresh data from SEC via edgartools
    try:
        company = Company(key)
        raw = company.get_filings(form=_FORM_TYPES)
        results: list[dict] = []
        for i, f in enumerate(raw):
            if i >= limit:
                break
            results.append(
                {
                    "form": f.form,
                    "filing_date": str(f.filing_date),
                    "description": f.primary_doc_description or f.form,
                    "sec_url": f.homepage_url,
                    "items": f.items or "",
                }
            )
    except Exception:
        logger.exception("Failed to fetch company filings for %s", key)
        results = []

    with _lock:
        _cache[key] = (time.time(), results)
        _evict_oldest()

    return results
