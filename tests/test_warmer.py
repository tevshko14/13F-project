"""Tests for the tiered L2 cache warmer (filings.warmer).

The warmer is the source-of-truth registry of every L2 key the request
path can read after Phase 3 flips callers to ``block_on_miss=False``.
These tests verify:

  * the registry contract — every entry has the required fields,
    valid tier, valid category, sane TTL given its tier interval
  * tier filtering returns the right subset
  * ``warm_tier`` calls ``l2_cached`` once per matched target with the
    correct args
  * upstream failures don't tank the whole cycle
"""

from __future__ import annotations

import asyncio
import pytest

from filings import warmer


# ── Registry invariants ──────────────────────────────────────────────


def test_every_target_has_required_fields():
    """Every WarmerTarget must carry key/ttl/compute/tier/category."""
    for t in warmer.WARMER_TARGETS:
        assert t.key,                 f"missing key on {t}"
        assert t.ttl_seconds > 0,     f"non-positive ttl on {t.key}"
        assert callable(t.compute),   f"non-callable compute on {t.key}"
        assert t.tier in ("hot", "warm", "cold"), f"bad tier on {t.key}"
        assert t.category,            f"missing category on {t.key}"


def test_no_duplicate_keys():
    """Every L2 key in the registry is unique."""
    keys = [t.key for t in warmer.WARMER_TARGETS]
    assert len(keys) == len(set(keys)), f"duplicate keys: {keys}"


def test_ttl_at_least_2_5x_tier_interval():
    """Rule: TTL ≥ tier interval × 2.5 so SWR refresh always lands inside
    the TTL window even if one warmer cycle is missed (e.g., worker restart).
    Prevents the 'TTL < warmer interval' race that caused stale-then-refresh
    cascades during yfinance degradation in May 2026."""
    interval = {
        "hot":  warmer.HOT_INTERVAL_SECONDS,
        "warm": warmer.WARM_INTERVAL_SECONDS,
        "cold": warmer.COLD_INTERVAL_SECONDS,
    }
    for t in warmer.WARMER_TARGETS:
        min_ttl = interval[t.tier] * 2.5
        assert t.ttl_seconds >= min_ttl, (
            f"{t.key}: TTL {t.ttl_seconds}s < {min_ttl:.0f}s "
            f"({t.tier} interval × 2.5)"
        )


# ── Tier filtering ───────────────────────────────────────────────────


def test_targets_by_tier_filters_correctly():
    """targets_by_tier returns exactly the entries with matching tier."""
    tiers: tuple[warmer.Tier, ...] = ("hot", "warm", "cold")
    for tier in tiers:
        subset = warmer.targets_by_tier(tier)
        assert all(t.tier == tier for t in subset)
        assert len(subset) == sum(1 for t in warmer.WARMER_TARGETS if t.tier == tier)


def test_hot_tier_contains_yfinance_critical_keys():
    """The two highest-fanout yfinance entry points MUST be hot-tier —
    they're what every homepage hit needs immediately and were the
    smoking gun in the 36101b2 post-deploy investigation."""
    hot_keys = {t.key for t in warmer.targets_by_tier("hot")}
    assert "redesign:home:sp500_1d"     in hot_keys
    assert "redesign:home:index_market" in hot_keys


def test_cold_tier_contains_constituents():
    """Constituent lists rebalance quarterly — they belong in cold tier."""
    cold_keys = {t.key for t in warmer.targets_by_tier("cold")}
    assert "redesign:home:sp500_constituents"     in cold_keys
    assert "redesign:home:nasdaq100_constituents" in cold_keys


# ── warm_tier execution ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_warm_tier_calls_l2_cached_for_each_target(monkeypatch):
    """warm_tier dispatches one l2_cached call per matched target,
    using the versioned key shape."""
    calls: list[tuple[str, int, str]] = []

    async def fake_l2_cached(key, ttl_seconds, compute, *, category, **kwargs):
        calls.append((key, ttl_seconds, category))
        return {"ok": True}

    monkeypatch.setattr(warmer, "l2_cached", fake_l2_cached)

    result = await warmer.warm_tier("hot")

    expected_keys = {warmer.versioned_key(t.key)
                     for t in warmer.targets_by_tier("hot")}
    called_keys = {c[0] for c in calls}
    assert called_keys == expected_keys
    assert result["warmed"] == len(expected_keys)
    assert result["failed"] == []
    assert result["total"]  == len(expected_keys)


def test_versioned_key_at_v1_returns_bare_key():
    """At the inaugural CACHE_SCHEMA_VERSION (1), versioned_key returns
    the bare key so existing L2 rows continue to be read.  Bumping to 2+
    adds the ``:vN`` suffix and invalidates."""
    assert warmer.CACHE_SCHEMA_VERSION == 1
    assert warmer.versioned_key("foo:bar") == "foo:bar"


def test_versioned_key_above_v1_appends_suffix(monkeypatch):
    """When CACHE_SCHEMA_VERSION > 1 the suffix is appended, abandoning
    prior rows (they expire on their own TTL).  Bumping the constant is
    the clean schema-rotation primitive."""
    monkeypatch.setattr(warmer, "CACHE_SCHEMA_VERSION", 2)
    assert warmer.versioned_key("foo:bar") == "foo:bar:v2"


@pytest.mark.asyncio
async def test_warm_tier_swallows_upstream_failures(monkeypatch):
    """One failing compute shouldn't tank the whole cycle — failed keys
    are tracked, succeeded ones still warmed."""
    async def fake_l2_cached(key, ttl_seconds, compute, *, category, **kwargs):
        if "sp500_1d" in key:
            raise RuntimeError("yfinance returned 504")
        return {"ok": True}

    monkeypatch.setattr(warmer, "l2_cached", fake_l2_cached)

    result = await warmer.warm_tier("hot")

    assert "redesign:home:sp500_1d" in result["failed"]
    assert result["warmed"] == len([t for t in warmer.targets_by_tier("hot")
                                    if "sp500_1d" not in t.key])


@pytest.mark.asyncio
async def test_warm_tier_none_result_counted_as_failure(monkeypatch):
    """A compute that returns None (upstream came back empty) counts
    as a failure for warmer accounting — we want to know the row didn't
    actually refresh, even if no exception fired."""
    async def fake_l2_cached(key, ttl_seconds, compute, *, category, **kwargs):
        return None if "cnn_fg" in key else {"ok": True}

    monkeypatch.setattr(warmer, "l2_cached", fake_l2_cached)

    result = await warmer.warm_tier("warm")

    assert "redesign:home:cnn_fg" in result["failed"]


@pytest.mark.asyncio
async def test_warm_all_runs_every_tier(monkeypatch):
    """warm_all returns a dict keyed by tier with per-tier status."""
    async def fake_l2_cached(key, ttl_seconds, compute, *, category, **kwargs):
        return {"ok": True}

    monkeypatch.setattr(warmer, "l2_cached", fake_l2_cached)

    result = await warmer.warm_all()

    assert set(result.keys()) == {"hot", "warm", "cold"}
    for tier_key, status in result.items():
        tier: warmer.Tier = tier_key  # type: ignore[assignment]
        assert status["tier"]   == tier
        assert status["failed"] == []
        assert status["total"]  == len(warmer.targets_by_tier(tier))


# ── Batched L2 read + bounded concurrency (Supabase pressure relief) ─


@pytest.mark.asyncio
async def test_warm_tier_issues_one_batch_read(monkeypatch):
    """A single ``get_cached_full_rows_async`` call must cover every
    target in the tier — collapses N per-key reads into one round-trip,
    the dominant fix for Supabase Micro saturation under warmer pressure."""
    batch_calls: list[list[str]] = []

    async def fake_batch_read(keys):
        batch_calls.append(list(keys))
        return {k: None for k in keys}

    async def fake_l2_cached(key, ttl_seconds, compute, *, category, **kwargs):
        return {"ok": True}

    # Passthrough the supabase backpressure gate.
    async def passthrough(awaitable, *, allow_drop=False):
        return await awaitable

    monkeypatch.setattr(warmer.supabase_cache, "get_cached_full_rows_async", fake_batch_read)
    monkeypatch.setattr(warmer, "gate_supabase_async", passthrough)
    monkeypatch.setattr(warmer, "l2_cached", fake_l2_cached)

    await warmer.warm_tier("warm")

    assert len(batch_calls) == 1, "expected exactly one batch read per tier"
    warm_keys = {warmer.versioned_key(t.key)
                 for t in warmer.targets_by_tier("warm")}
    assert set(batch_calls[0]) == warm_keys


@pytest.mark.asyncio
async def test_warm_tier_passes_prefetched_rows_to_l2_cached(monkeypatch):
    """Each per-target l2_cached call receives the row from the batch
    read in its ``prefetched_row`` kwarg — proves the per-key inline
    Supabase read is bypassed."""
    seen: dict[str, object] = {}

    async def fake_batch_read(keys):
        # Pretend half the rows came back fresh from L2.
        return {k: ({"hit": True} if i % 2 == 0 else None)
                for i, k in enumerate(keys)}

    async def fake_l2_cached(key, ttl_seconds, compute, *, category, **kwargs):
        seen[key] = kwargs.get("prefetched_row")
        return {"ok": True}

    async def passthrough(awaitable, *, allow_drop=False):
        return await awaitable

    monkeypatch.setattr(warmer.supabase_cache, "get_cached_full_rows_async", fake_batch_read)
    monkeypatch.setattr(warmer, "gate_supabase_async", passthrough)
    monkeypatch.setattr(warmer, "l2_cached", fake_l2_cached)

    await warmer.warm_tier("warm")

    warm_keys = [warmer.versioned_key(t.key)
                 for t in warmer.targets_by_tier("warm")]
    # Every l2_cached call received SOMETHING for prefetched_row — even
    # the misses are explicit None (telling l2_cached "skip inline read,
    # treat as miss").  Without prefetched_row, l2_cached would do its
    # own per-key Supabase read, defeating the batch.
    for key in warm_keys:
        assert key in seen, f"l2_cached not called for {key}"
    # Alternation matches what the fake batch returned.
    for i, key in enumerate(warm_keys):
        expected = {"hit": True} if i % 2 == 0 else None
        assert seen[key] == expected


@pytest.mark.asyncio
async def test_warm_tier_bounds_compute_concurrency(monkeypatch):
    """Concurrent compute+writeback chains never exceed WARMER_CONCURRENCY
    — protects PostgREST→Postgres pool on Supabase Micro."""
    inflight = 0
    peak = 0

    async def fake_batch_read(keys):
        return {k: None for k in keys}

    async def fake_l2_cached(key, ttl_seconds, compute, *, category, **kwargs):
        nonlocal inflight, peak
        inflight += 1
        peak = max(peak, inflight)
        # Yield so other tasks can interleave; without an await the
        # semaphore would never see contention.
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        inflight -= 1
        return {"ok": True}

    async def passthrough(awaitable, *, allow_drop=False):
        return await awaitable

    monkeypatch.setattr(warmer.supabase_cache, "get_cached_full_rows_async", fake_batch_read)
    monkeypatch.setattr(warmer, "gate_supabase_async", passthrough)
    monkeypatch.setattr(warmer, "l2_cached", fake_l2_cached)
    monkeypatch.setattr(warmer, "WARMER_CONCURRENCY", 2)

    await warmer.warm_tier("warm")

    assert peak <= 2, f"observed peak={peak} concurrent computes (cap=2)"


@pytest.mark.asyncio
async def test_warm_tier_falls_back_when_batch_read_fails(monkeypatch):
    """If the batch read itself raises (Supabase down), fall back to
    treating every target as a cache miss — the tier still warms via
    compute, just without the L2-fresh shortcut."""
    async def failing_batch_read(keys):
        raise RuntimeError("Cloudflare 522: Supabase origin unreachable")

    seen_prefetched: list[object] = []

    async def fake_l2_cached(key, ttl_seconds, compute, *, category, **kwargs):
        seen_prefetched.append(kwargs.get("prefetched_row"))
        return {"ok": True}

    async def passthrough(awaitable, *, allow_drop=False):
        return await awaitable

    monkeypatch.setattr(warmer.supabase_cache, "get_cached_full_rows_async", failing_batch_read)
    monkeypatch.setattr(warmer, "gate_supabase_async", passthrough)
    monkeypatch.setattr(warmer, "l2_cached", fake_l2_cached)

    result = await warmer.warm_tier("warm")

    # Every target was still attempted, with prefetched_row=None (cache miss).
    assert result["total"]  == len(warmer.targets_by_tier("warm"))
    assert result["warmed"] == len(warmer.targets_by_tier("warm"))
    assert all(p is None for p in seen_prefetched)
