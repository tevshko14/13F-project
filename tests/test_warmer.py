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
    for tier in ("hot", "warm", "cold"):
        subset = warmer.targets_by_tier(tier)  # type: ignore[arg-type]
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


def test_versioned_key_appends_schema_version():
    """versioned_key always adds the current CACHE_SCHEMA_VERSION suffix.

    Bumping CACHE_SCHEMA_VERSION abandons all prior rows (they live out
    their TTL untouched) and the warmer refills the new versioned keys
    on its next cycle — clean schema rotation without manual purge.
    """
    assert warmer.versioned_key("foo:bar") == f"foo:bar:v{warmer.CACHE_SCHEMA_VERSION}"


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
    for tier, status in result.items():
        assert status["tier"]   == tier
        assert status["failed"] == []
        assert status["total"]  == len(warmer.targets_by_tier(tier))  # type: ignore[arg-type]
