"""Tests for ``cache_l2.l2_cached`` + LKG sidecar / strict-mode behavior.

The cache is structured around five operating modes (Phase 1):
  * Fresh L2 hit                              — return cached, no compute, no refresh
  * Stale L2 hit, within stale_budget         — return cached, kick bg refresh
  * Stale L2 hit, over stale_budget           — treat as miss
  * Cold miss + block_on_miss=True            — compute synchronously, write back
  * Cold miss + block_on_miss=False           — return LKG / None, kick bg compute

Plus the LKG sidecar: every successful writeback also writes a parallel
``lkg:{key}`` row with no TTL, so degraded-mode renders can fall back
to a real (slightly stale) snapshot instead of design-time mocks.

These tests monkeypatch ``supabase_cache`` so they run without a live
Supabase connection.  The fixture stores rows in a per-test dict and
returns them through the same shape ``get_cached_full_row_async`` does
in production.
"""

from __future__ import annotations

import asyncio
import pytest

from filings import cache_l2, supabase_cache


# ── Fixture: in-memory Supabase substitute ────────────────────────────


class FakeSupabase:
    """In-memory stand-in for the supabase_cache module surface that
    ``cache_l2`` depends on.  Exposes the same async functions and
    records every write so tests can assert LKG sidecar behavior."""

    def __init__(self):
        self.rows: dict[str, dict] = {}
        self.writes: list[tuple[str, str, object, int | None]] = []

    async def get_cached_full_row_async(self, key):
        row = self.rows.get(key)
        if row is None:
            return None
        # The row dict is exactly what cache_l2 expects.
        return dict(row)

    async def set_cached_async(self, key, category, data, ttl_seconds):
        self.writes.append((key, category, data, ttl_seconds))
        # Persist into the in-memory store with computed freshness
        # metadata so subsequent reads return what we wrote.
        self.rows[key] = {
            "data":         data,
            "is_fresh":     True,
            "expires_at":   None,
            "ttl_seconds":  ttl_seconds,
            "as_of_ts":     "2026-05-11T18:00:00+00:00",
            "age_seconds":  0,
        }
        return True


@pytest.fixture
def fake_supabase(monkeypatch):
    fake = FakeSupabase()
    monkeypatch.setattr(supabase_cache, "get_cached_full_row_async",
                        fake.get_cached_full_row_async)
    monkeypatch.setattr(supabase_cache, "set_cached_async",
                        fake.set_cached_async)
    # gate_supabase_async is a passthrough wrapper — call awaitable directly
    # so we don't need to mock the concurrency module.
    async def _passthrough(awaitable, *, allow_drop=False):
        return await awaitable
    monkeypatch.setattr(cache_l2, "gate_supabase_async", _passthrough)
    return fake


# ── Helpers ──────────────────────────────────────────────────────────


def _row(data, *, is_fresh=True, age_seconds=0, as_of_ts="2026-05-11T18:00:00+00:00"):
    """Build a row dict in the shape `_read_l2_row` returns."""
    return {
        "data":         data,
        "is_fresh":     is_fresh,
        "expires_at":   None,
        "ttl_seconds":  60,
        "as_of_ts":     as_of_ts,
        "age_seconds":  age_seconds,
    }


# ── Tests: fresh / stale / miss matrix ───────────────────────────────


@pytest.mark.asyncio
async def test_fresh_hit_returns_cached_no_compute(fake_supabase):
    """Fresh L2 hit: return cached, never call compute, no bg refresh."""
    fake_supabase.rows["k1"] = _row({"v": 1}, is_fresh=True)
    compute_called = 0
    def compute():
        nonlocal compute_called
        compute_called += 1
        return {"v": 99}

    data = await cache_l2.l2_cached("k1", 60, compute, category="test")
    await asyncio.sleep(0)  # drain any pending tasks

    assert data == {"v": 1}
    assert compute_called == 0


@pytest.mark.asyncio
async def test_stale_within_budget_returns_cached_kicks_refresh(fake_supabase):
    """Stale within budget: return cached immediately, refresh in background."""
    fake_supabase.rows["k2"] = _row({"v": 1}, is_fresh=False, age_seconds=120)
    compute_called = 0
    def compute():
        nonlocal compute_called
        compute_called += 1
        return {"v": 2}

    data = await cache_l2.l2_cached(
        "k2", 60, compute, category="test", stale_budget_seconds=600,
    )
    # Let the background refresh task run.
    for _ in range(5):
        await asyncio.sleep(0)

    assert data == {"v": 1}      # stale value returned to caller
    assert compute_called == 1   # bg refresh fired


@pytest.mark.asyncio
async def test_stale_over_budget_treated_as_miss(fake_supabase):
    """Stale beyond budget: treat as cold miss, compute synchronously."""
    fake_supabase.rows["k3"] = _row({"v": "old"}, is_fresh=False, age_seconds=10_000)
    compute_called = 0
    def compute():
        nonlocal compute_called
        compute_called += 1
        return {"v": "fresh"}

    data = await cache_l2.l2_cached(
        "k3", 60, compute, category="test", stale_budget_seconds=600,
    )
    assert data == {"v": "fresh"}
    assert compute_called == 1


@pytest.mark.asyncio
async def test_cold_miss_block_on_miss_true_runs_compute(fake_supabase):
    """No L2 row, block_on_miss=True: compute runs synchronously on caller."""
    compute_called = 0
    def compute():
        nonlocal compute_called
        compute_called += 1
        return {"v": "computed"}

    data = await cache_l2.l2_cached("k4", 60, compute, category="test")
    assert data == {"v": "computed"}
    assert compute_called == 1


@pytest.mark.asyncio
async def test_cold_miss_block_on_miss_false_returns_none(fake_supabase):
    """No L2 row, block_on_miss=False, lkg_fallback=False: returns None
    immediately; compute kicks in the background."""
    compute_called = 0
    def compute():
        nonlocal compute_called
        compute_called += 1
        return {"v": "later"}

    data = await cache_l2.l2_cached(
        "k5", 60, compute, category="test", block_on_miss=False,
    )
    assert data is None
    # Drain background refresh task.
    for _ in range(5):
        await asyncio.sleep(0)
    assert compute_called == 1  # bg compute fired


# ── Tests: LKG sidecar ───────────────────────────────────────────────


@pytest.mark.asyncio
async def test_writeback_persists_lkg_sidecar(fake_supabase):
    """Every successful writeback writes both the primary row AND lkg:key."""
    def compute():
        return {"v": 42}

    await cache_l2.l2_cached("k6", 60, compute, category="test")
    # Drain async writebacks.
    for _ in range(5):
        await asyncio.sleep(0)

    primary_writes = [w for w in fake_supabase.writes if w[0] == "k6"]
    lkg_writes     = [w for w in fake_supabase.writes if w[0] == "lkg:k6"]
    assert len(primary_writes) == 1
    assert len(lkg_writes) == 1
    # LKG row has no TTL (lives indefinitely).
    assert lkg_writes[0][3] is None
    # LKG row carries the lkg_<category> namespace so admin queries
    # can target sidecar rows distinctly.
    assert lkg_writes[0][1] == "lkg_test"


@pytest.mark.asyncio
async def test_lkg_fallback_serves_snapshot_when_primary_missing(fake_supabase):
    """block_on_miss=False + lkg_fallback=True: L2 miss reads the
    parallel lkg:{key} row instead of returning None."""
    # Seed an LKG row but NO primary row.
    fake_supabase.rows["lkg:k7"] = _row({"v": "last_good"}, is_fresh=True)
    compute_called = 0
    def compute():
        nonlocal compute_called
        compute_called += 1
        return {"v": "refreshed"}

    data = await cache_l2.l2_cached(
        "k7", 60, compute, category="test",
        block_on_miss=False, lkg_fallback=True,
    )
    assert data == {"v": "last_good"}
    # Background compute still kicks so the next caller gets fresh data.
    for _ in range(5):
        await asyncio.sleep(0)
    assert compute_called == 1


@pytest.mark.asyncio
async def test_lkg_fallback_returns_none_when_no_lkg_row(fake_supabase):
    """LKG fallback with no LKG row → None (degraded mode handled by caller)."""
    data = await cache_l2.l2_cached(
        "k8", 60, lambda: {"v": "x"}, category="test",
        block_on_miss=False, lkg_fallback=True,
    )
    assert data is None


# ── Tests: meta surface ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_meta_source_fresh(fake_supabase):
    fake_supabase.rows["m1"] = _row({"v": 1}, is_fresh=True, age_seconds=5)
    _, meta = await cache_l2.l2_cached_with_meta(
        "m1", 60, lambda: None, category="test",
    )
    assert meta["source"] == "l2_fresh"
    assert meta["age_seconds"] == 5


@pytest.mark.asyncio
async def test_meta_source_stale(fake_supabase):
    fake_supabase.rows["m2"] = _row({"v": 1}, is_fresh=False, age_seconds=200)
    _, meta = await cache_l2.l2_cached_with_meta(
        "m2", 60, lambda: {"v": 2}, category="test",
        stale_budget_seconds=600,
    )
    assert meta["source"] == "l2_stale"


@pytest.mark.asyncio
async def test_meta_source_lkg(fake_supabase):
    fake_supabase.rows["lkg:m3"] = _row({"v": "old"}, is_fresh=True, age_seconds=3600)
    _, meta = await cache_l2.l2_cached_with_meta(
        "m3", 60, lambda: None, category="test",
        block_on_miss=False, lkg_fallback=True,
    )
    assert meta["source"] == "lkg"
    assert meta["age_seconds"] == 3600


@pytest.mark.asyncio
async def test_meta_source_compute(fake_supabase):
    _, meta = await cache_l2.l2_cached_with_meta(
        "m4", 60, lambda: {"v": "computed"}, category="test",
    )
    assert meta["source"] == "compute"


@pytest.mark.asyncio
async def test_meta_source_miss_when_no_lkg_and_no_block(fake_supabase):
    _, meta = await cache_l2.l2_cached_with_meta(
        "m5", 60, lambda: {"v": "x"}, category="test",
        block_on_miss=False, lkg_fallback=False,
    )
    assert meta["source"] == "miss"


# ── Async compute support ────────────────────────────────────────────


@pytest.mark.asyncio
async def test_async_compute_runs_without_thread_pool(fake_supabase):
    """Async coroutine fns are awaited directly — no `to_heavy` slot held."""
    async def compute_async():
        await asyncio.sleep(0)
        return {"v": "async"}

    data = await cache_l2.l2_cached("a1", 60, compute_async, category="test")
    assert data == {"v": "async"}
