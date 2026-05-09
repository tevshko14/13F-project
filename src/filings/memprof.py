"""Memory profiling helpers — admin endpoint payload + background log line.

Three concerns kept in one module so the wiring in `web.py` stays thin:

1. ``snapshot()`` — JSON-serialisable dict for the admin endpoint
2. ``log_line()`` — one-line string for the background poller (Railway logs)
3. cache + state introspection helpers used by both

No external dependencies beyond ``psutil`` (already in pyproject).  Skip
``tracemalloc`` for now — it's gated behind a separate env var if/when we
need allocation-site granularity.
"""

from __future__ import annotations

import gc
import os
import sys
import threading
import time
from collections import Counter
from typing import Any

import psutil

from filings.caching import TTLCache

# ── Process metrics ──────────────────────────────────────────────────


def process_metrics() -> dict:
    """RSS / VMS / thread count / fd count / GC stats."""
    proc = psutil.Process(os.getpid())
    mem = proc.memory_info()
    try:
        num_fds = proc.num_fds()
    except Exception:
        num_fds = -1  # Windows / sandboxed hosts
    return {
        "pid":        os.getpid(),
        "rss_mb":     round(mem.rss / 1024 / 1024, 1),
        "vms_mb":     round(mem.vms / 1024 / 1024, 1),
        "threads":    proc.num_threads(),
        "active_threading_threads": threading.active_count(),
        "open_fds":   num_fds,
        "gc_counts":  list(gc.get_count()),
        "gc_threshold": list(gc.get_threshold()),
        "uptime_s":   round(time.time() - proc.create_time(), 1),
    }


# ── Cache discovery ──────────────────────────────────────────────────


def _iter_ttl_caches() -> list[tuple[str, TTLCache]]:
    """Find every ``TTLCache`` instance currently held by an imported
    `filings.*` module.  Auto-discovery means new caches show up
    without anyone updating a hard-coded list.
    """
    found: list[tuple[str, TTLCache]] = []
    for mod_name, mod in list(sys.modules.items()):
        if not mod_name.startswith("filings"):
            continue
        if mod is None:
            continue
        for attr in dir(mod):
            if attr.startswith("__"):
                continue
            try:
                val = getattr(mod, attr, None)
            except Exception:
                continue
            if isinstance(val, TTLCache):
                found.append((f"{mod_name}.{attr}", val))
            elif isinstance(val, dict):
                # `cboe_data._CACHES` etc. — dicts of TTLCache.
                for sub_k, sub_v in val.items():
                    if isinstance(sub_v, TTLCache):
                        found.append((f"{mod_name}.{attr}[{sub_k!r}]", sub_v))
    # Stable order for diffing across snapshots.
    found.sort(key=lambda x: x[0])
    return found


def cache_metrics() -> list[dict]:
    """Return a list of dicts describing every TTLCache instance."""
    rows = []
    for name, cache in _iter_ttl_caches():
        try:
            length = len(cache)
        except Exception:
            length = -1
        rows.append({
            "name":     name,
            "len":      length,
            "max_size": cache.max_size,
            "ttl":      cache.ttl,
            "fill_pct": round(length / cache.max_size * 100, 1) if cache.max_size else None,
        })
    return rows


# ── app.state introspection ──────────────────────────────────────────


def _approx_size(obj: Any, max_depth: int = 4, _seen: set | None = None) -> int:
    """Crude recursive ``sys.getsizeof`` walk.  Caps depth so deeply
    nested structures don't blow up the snapshot endpoint.  Identity-
    deduplicates so shared substructures aren't double-counted.
    """
    if _seen is None:
        _seen = set()
    obj_id = id(obj)
    if obj_id in _seen:
        return 0
    _seen.add(obj_id)
    size = sys.getsizeof(obj)
    if max_depth <= 0:
        return size
    if isinstance(obj, dict):
        for k, v in obj.items():
            size += _approx_size(k, max_depth - 1, _seen)
            size += _approx_size(v, max_depth - 1, _seen)
    elif isinstance(obj, (list, tuple, set, frozenset)):
        for v in obj:
            size += _approx_size(v, max_depth - 1, _seen)
    return size


# Keys on ``app.state`` worth measuring; everything else is small.
_STATE_KEYS = (
    "fund_cache",
    "deployment_cache",
    "_ownership_map",
    "_pp_redesign_cusip_ticker",
    "_pp_screener_dataset",
    "market_data_ready",
)


def app_state_metrics(app_state: Any) -> list[dict]:
    """Approximate sizes for the known-large objects on ``app.state``."""
    rows = []
    for key in _STATE_KEYS:
        if not hasattr(app_state, key):
            continue
        val = getattr(app_state, key)
        entry: dict[str, Any] = {"name": f"app.state.{key}"}
        try:
            if isinstance(val, dict):
                entry["dict_len"] = len(val)
            elif isinstance(val, (list, tuple)):
                entry["seq_len"] = len(val)
        except Exception:
            pass
        try:
            entry["approx_kb"] = round(_approx_size(val) / 1024, 1)
        except Exception as exc:
            entry["sizeof_error"] = str(exc)
        rows.append(entry)
    return rows


# ── Object-type histogram ────────────────────────────────────────────


def type_counts(top_n: int = 30) -> list[dict]:
    """Top-N Python types by live instance count.

    Useful for spotting unbounded growth: "dict count went from 50k to
    500k between snapshots" points at a leaking dict factory.
    """
    counts: Counter = Counter()
    for obj in gc.get_objects():
        counts[type(obj).__name__] += 1
    return [{"type": t, "count": n} for t, n in counts.most_common(top_n)]


# ── Composite payloads ───────────────────────────────────────────────


def snapshot(app_state: Any | None = None, *, with_types: bool = True) -> dict:
    """Full memory snapshot for the admin endpoint."""
    payload = {
        "ts":      time.time(),
        "process": process_metrics(),
        "caches":  cache_metrics(),
        "app_state": app_state_metrics(app_state) if app_state is not None else [],
    }
    if with_types:
        payload["type_counts"] = type_counts(30)
    return payload


def log_line(app_state: Any | None = None) -> str:
    """Compact one-line string for the background poller.  Picked up by
    Railway's stdout capture; grep for ``memprof`` to chart over time.
    """
    proc = process_metrics()
    n_caches = sum(c["len"] for c in cache_metrics())
    n_state = sum(s.get("dict_len") or s.get("seq_len") or 0
                  for s in app_state_metrics(app_state)) if app_state else 0
    return (
        f"memprof rss={proc['rss_mb']}MB vms={proc['vms_mb']}MB "
        f"threads={proc['threads']} fds={proc['open_fds']} "
        f"gc={proc['gc_counts']} cache_entries={n_caches} state_entries={n_state}"
    )
