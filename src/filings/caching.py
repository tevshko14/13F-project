"""Shared TTL cache primitive — replaces the duplicated pattern in 8+ modules.

Before this module, sentiment.py / market_data.py / analysts.py / vitals.py /
tiingo.py / tradier.py / google_trends.py / insider_trading.py each
re-implemented:

    _lock = threading.Lock()
    _cache: dict[str, tuple[float, data]] = {}
    _MAX_CACHE_ENTRIES = 500

    def _evict_oldest(cache, max_size):
        if len(cache) <= max_size: return
        sorted_keys = sorted(cache, key=lambda k: cache[k][0])
        for k in sorted_keys[: len(cache) - max_size]:
            del cache[k]

That's ~200 lines of duplicated logic.  This module replaces all of it.

Usage:
    from filings.caching import TTLCache, MISS

    _finnhub_cache = TTLCache(ttl=7200, max_size=500)

    # Normal case: miss = None
    cached = _finnhub_cache.get("AAPL")
    if cached is not None:
        return cached

    # When the cache can legitimately store None (cached negative result):
    cached = _finnhub_cache.get("AAPL", default=MISS)
    if cached is not MISS:
        return cached  # may be None — but we checked and know it

    _finnhub_cache.set("AAPL", {"score": 0.5})
"""

from __future__ import annotations

import threading
import time
from typing import Any

class _Miss:
    """Sentinel type for TTLCache.get() — only one instance exported as ``MISS``.

    Use when the cache stores values that could legitimately be None
    (negative caching, e.g. "we asked the API and it returned nothing").
    """

    __slots__ = ()

    def __repr__(self) -> str:
        return "<TTLCache.MISS>"

    def __bool__(self) -> bool:  # defensive — force identity checks, not truthiness
        return False


MISS: Any = _Miss()


class TTLCache:
    """Thread-safe TTL cache with oldest-first eviction at ``max_size``.

    Semantics match the pattern this module replaces:
      * ``get(key)`` — returns cached value if within TTL, else *default*.
      * ``set(key, value)`` — stores + evicts oldest if over limit.
      * ``get_stale(key)`` — ignores TTL (for fallback / degraded-mode use).

    Eviction is O(n log n) on overflow (sort by insertion time).  Since
    overflow is rare (triggered only when ``len > max_size``), amortized
    cost is O(1) per write.

    Not safe for async contexts that need cooperative yielding during
    eviction, but these caches only hold small in-memory dicts, so the
    lock is held briefly (microseconds).
    """

    __slots__ = ("ttl", "max_size", "_store", "_lock")

    def __init__(self, ttl: int, max_size: int = 500):
        self.ttl = ttl
        self.max_size = max_size
        self._store: dict[str, tuple[float, Any]] = {}
        self._lock = threading.Lock()

    def get(self, key: str, default: Any = None) -> Any:
        """Return cached value if within TTL, else *default*."""
        entry = self._store.get(key)
        if entry is not None and time.time() - entry[0] < self.ttl:
            return entry[1]
        return default

    def get_stale(self, key: str, default: Any = None) -> Any:
        """Return cached value regardless of TTL (fallback scenarios)."""
        entry = self._store.get(key)
        if entry is not None:
            return entry[1]
        return default

    def get_with_age(self, key: str) -> tuple[Any, float] | None:
        """Return (value, age_seconds) for callers that need the raw timestamp,
        or None if the key is absent.  Does not check TTL.
        """
        entry = self._store.get(key)
        if entry is None:
            return None
        return (entry[1], time.time() - entry[0])

    def set(self, key: str, value: Any) -> None:
        """Store *value* with current timestamp; evict oldest on overflow."""
        with self._lock:
            self._store[key] = (time.time(), value)
            if len(self._store) > self.max_size:
                # Evict oldest entries by timestamp
                sorted_keys = sorted(
                    self._store, key=lambda k: self._store[k][0]
                )
                for k in sorted_keys[: len(self._store) - self.max_size]:
                    del self._store[k]

    def seed(self, key: str, value: Any, timestamp: float) -> None:
        """Store *value* with an explicit *timestamp* — for startup hydration
        from disk/Supabase where the original age must be preserved (so TTL
        expiry keeps working).  Skips eviction; call only during startup.
        """
        with self._lock:
            self._store[key] = (timestamp, value)

    def items_with_age(self) -> list[tuple[str, Any, float]]:
        """Return [(key, value, age_seconds), ...] for all stored entries.

        Used by introspection / debug endpoints (e.g. cache-health pages).
        Snapshot semantics — safe against concurrent mutation.
        """
        now = time.time()
        with self._lock:
            return [(k, v, now - ts) for k, (ts, v) in self._store.items()]

    def delete(self, key: str) -> None:
        """Remove a key if present (no-op if absent)."""
        with self._lock:
            self._store.pop(key, None)

    def clear(self) -> None:
        """Remove all entries."""
        with self._lock:
            self._store.clear()

    def __len__(self) -> int:
        return len(self._store)

    def __contains__(self, key: str) -> bool:
        """Return True only if key is present AND fresh (within TTL)."""
        return self.get(key, MISS) is not MISS
