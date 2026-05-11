"""Tiered L2 cache warmer registry.

Owns every L2 cache key the redesign request path can read.  Three
tiers run at independent intervals:

  * **hot**  — every 90 s.  Tick-by-tick data (sp500 1D quote map,
               index quotes, hero intraday).
  * **warm** — every 4 min.  Slow-moving (news, sentiment, sector
               ETFs, F&G, ApeWisdom, FRED, earnings).
  * **cold** — every 6 h.   Near-immutable (S&P / NASDAQ constituents).

TTL discipline: ``ttl_seconds ≥ tier_interval × 2.5`` so SWR refresh
always lands inside the TTL window even if one warmer cycle is missed.
Enforced by ``test_ttl_at_least_2_5x_tier_interval``.

Compute functions lazy-import their upstream modules so this file has
zero load-time dependencies — the registry can be imported and walked
from anywhere (lifespan, admin endpoints, tests) without triggering
market_data / fred_indicators / earnings_calendar loads.

This file IS the source of truth for what data the request path
needs pre-populated.  Every key read via ``read_via_l2`` must be
registered here, otherwise the bg refresh path can't refill it.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Literal

from filings.cache_l2 import l2_cached

logger = logging.getLogger(__name__)


Tier = Literal["hot", "warm", "cold"]


# Per-tier scheduling.  These are read by web.py lifespan to register
# the three periodic tasks.  Adjust together: the rule is TTL ≥ interval × 2.5.
HOT_INTERVAL_SECONDS  = 90
WARM_INTERVAL_SECONDS = 4 * 60
COLD_INTERVAL_SECONDS = 6 * 60 * 60


# Bump when a registered payload's SHAPE changes incompatibly.
# See ``versioned_key`` — v1 keeps bare keys, v2+ adds ``:vN`` suffix
# so old rows are abandoned and the warmer refills the new keys on
# its next cycle.  Schema-rotation primitive: no manual purge.
CACHE_SCHEMA_VERSION = 1


@dataclass(frozen=True)
class WarmerTarget:
    """One row in the warmer registry.

    ``compute`` is a zero-arg callable (sync or async coroutine fn).
    The L2 wrapper handles dispatch.  ``category`` matches the
    Supabase row's ``category`` column for ops queries.
    """
    key:          str
    ttl_seconds:  int
    compute:      Callable[[], Any] | Callable[[], Awaitable[Any]]
    tier:         Tier
    category:     str


# ── Compute functions (lazy-import upstream modules) ─────────────────


def _compute_sp500_1d() -> dict:
    """S&P 500 quote map for 1D percentage change.  Powers home-page
    top-movers, ticker tape, heatmap, retail leaderboard."""
    from filings import market_data
    return market_data.get_sp500_market_data("1D") or {}


def _compute_index_market() -> dict:
    """Index quotes (^GSPC / ^IXIC / ^DJI / ^VIX / ^TNX).  Powers
    home KPI strip, hero chart fallback, congress perf chart."""
    from filings import market_data
    return market_data.get_index_market_data() or {}


def _compute_sp500_constituents() -> list[dict]:
    """S&P 500 ticker list.  Quarterly rebalance — cold tier is fine."""
    from filings import market_data
    return market_data.get_sp500_constituents() or []


def _compute_nasdaq100_constituents() -> list[dict]:
    """NASDAQ 100 ticker list.  Quarterly rebalance."""
    from filings import market_data
    return market_data.get_nasdaq100_constituents() or []


def _compute_news_general() -> list[dict]:
    """Top 14 general-market news articles for the home news rail."""
    from filings import market_data
    result = market_data.get_market_news("general", 14)
    return result if result else []


def _compute_sector_etfs() -> dict[str, float]:
    """11 sector ETF % changes for the heatmap legend / market mood strip."""
    # Lazy import — _fetch_sector_etfs_sync lives in home.py and
    # depends on _redesign helpers, so registering it here would create
    # a circular import at module load.
    from filings.routers._redesign import home
    return home._fetch_sector_etfs_sync() or {}


async def _compute_cnn_fg() -> dict | None:
    """CNN Fear & Greed gauge — async-native httpx, no thread held."""
    from filings.routers._redesign import home
    return await home._fetch_cnn_fg_async()


async def _compute_apewisdom() -> list[dict]:
    """ApeWisdom retail-sentiment leaderboard — async-native."""
    from filings.routers._redesign import home
    return await home._fetch_apewisdom_async()


async def _compute_hero_chart() -> Any:
    """Hero S&P/NASDAQ/DOW intraday + 5Y combined payload."""
    from filings.routers._redesign import home
    return await home._hero_chart_compute()


def _compute_earnings_4w() -> dict:
    """Next 4 weeks of earnings releases for the calendar pane.

    Returns the full calendar payload ({entries, by_date, ...}); see
    ``earnings_calendar.get_earnings_calendar`` for the shape.
    """
    from filings import earnings_calendar
    return earnings_calendar.get_earnings_calendar(None, None, 4) or {}


def _compute_fred_indicators() -> dict:
    """FRED macro indicators (CPI / PCE / Unrate / DFF / DGS10 / curve)."""
    from filings import fred_indicators
    return fred_indicators.fetch_indicators() or {}


# ── Registry ─────────────────────────────────────────────────────────
# Ordered by tier → key (alphabetical within tier) for readability.
# TTLs follow the rule: ttl ≥ tier_interval × 2.5.


WARMER_TARGETS: list[WarmerTarget] = [
    # ── HOT (90 s interval, TTL 240+ s) ──────────────────────────────
    WarmerTarget("redesign:home:hero_chart",      240, _compute_hero_chart,
                 "hot",  "redesign_home"),
    WarmerTarget("redesign:home:index_market",    240, _compute_index_market,
                 "hot",  "redesign_home"),
    WarmerTarget("redesign:home:sp500_1d",        240, _compute_sp500_1d,
                 "hot",  "redesign_home"),

    # ── WARM (4 min interval, TTL 600+ s) ────────────────────────────
    WarmerTarget("redesign:home:apewisdom",       1800, _compute_apewisdom,
                 "warm", "redesign_home"),
    WarmerTarget("redesign:home:cnn_fg",          1800, _compute_cnn_fg,
                 "warm", "redesign_home"),
    WarmerTarget("redesign:home:earnings_4w",     3600, _compute_earnings_4w,
                 "warm", "redesign_home"),
    WarmerTarget("redesign:home:fred_indicators", 1800, _compute_fred_indicators,
                 "warm", "redesign_home"),
    WarmerTarget("redesign:home:news_general",    1800, _compute_news_general,
                 "warm", "redesign_home"),
    WarmerTarget("redesign:home:sector_etfs",      600, _compute_sector_etfs,
                 "warm", "redesign_home"),

    # ── COLD (6 h interval, TTL 24 h+) ───────────────────────────────
    WarmerTarget("redesign:home:nasdaq100_constituents", 86400,
                 _compute_nasdaq100_constituents, "cold", "redesign_home"),
    WarmerTarget("redesign:home:sp500_constituents",     86400,
                 _compute_sp500_constituents,     "cold", "redesign_home"),
]


def versioned_key(base_key: str) -> str:
    """Append the current CACHE_SCHEMA_VERSION to a registry key.

    Always use this when computing the actual L2 key from a registry
    entry — guarantees the warmer's writes and the request path's
    reads target the same row.  Old rows from prior versions live out
    their TTL untouched and disappear without explicit cleanup.

    Version 1 is the inaugural state — we return the bare key (no
    suffix) so existing L2 rows continue to be read.  Versions >= 2
    add ``:vN`` to invalidate.  Bumping CACHE_SCHEMA_VERSION above 1
    is the schema-rotation primitive.
    """
    if CACHE_SCHEMA_VERSION <= 1:
        return base_key
    return f"{base_key}:v{CACHE_SCHEMA_VERSION}"


def targets_by_tier(tier: Tier) -> list[WarmerTarget]:
    """All registered targets matching the given tier."""
    return [t for t in WARMER_TARGETS if t.tier == tier]


def get_target(key: str) -> WarmerTarget | None:
    """Look up the registered target for an L2 key.  Returns None if
    the key isn't in the registry — callers should treat that as a
    programming error (the request path should only read keys that
    the warmer can refresh)."""
    for t in WARMER_TARGETS:
        if t.key == key:
            return t
    return None


# ── Request-path readers (strict L2) ─────────────────────────────────


async def read_via_l2_with_meta(
    key: str,
    *,
    block_on_miss: bool = False,
    lkg_fallback: bool = True,
    stale_budget_seconds: int | None = None,
):
    """Strict L2 read for the request path.  Returns ``(data, CacheMeta)``.

    Looks up the warmer target for ``key`` and calls ``l2_cached_with_meta``
    with that target's TTL / compute / category.  Defaults to strict mode
    (``block_on_miss=False, lkg_fallback=True``) so the request path
    never blocks on upstream — fresh L2 hit → stale-in-budget → LKG
    snapshot → None.

    Using this instead of a raw ``l2_cached`` call guarantees the
    request-path compute fn matches what the warmer registered, so the
    bg refresh (when L2 is stale) writes data shaped identically to
    what the warmer would have written.

    The meta surfaces source provenance (l2_fresh / l2_stale / lkg /
    miss) + as_of_ts — use for "Cached · 2m ago" badges or
    degraded-mode telemetry.  The thin ``read_via_l2`` wrapper drops
    it for callers that don't need it.

    Raises ``KeyError`` if the key isn't registered — surfacing that as
    a programming error rather than a silent None.
    """
    from filings.cache_l2 import l2_cached_with_meta
    target = get_target(key)
    if target is None:
        raise KeyError(
            f"warmer.read_via_l2: '{key}' is not in WARMER_TARGETS. "
            f"Register it in filings/warmer.py before reading from the "
            f"request path; otherwise the bg refresh path can't refill it."
        )
    return await l2_cached_with_meta(
        versioned_key(target.key), ttl_seconds=target.ttl_seconds,
        compute=target.compute, category=target.category,
        block_on_miss=block_on_miss,
        lkg_fallback=lkg_fallback,
        stale_budget_seconds=stale_budget_seconds,
    )


async def read_via_l2(
    key: str,
    *,
    block_on_miss: bool = False,
    lkg_fallback: bool = True,
    stale_budget_seconds: int | None = None,
) -> Any:
    """Data-only wrapper around ``read_via_l2_with_meta`` — same lookup,
    drops the freshness metadata for callers that don't need it."""
    data, _meta = await read_via_l2_with_meta(
        key,
        block_on_miss=block_on_miss,
        lkg_fallback=lkg_fallback,
        stale_budget_seconds=stale_budget_seconds,
    )
    return data


# ── Tier runners ─────────────────────────────────────────────────────


async def warm_tier(tier: Tier) -> dict:
    """Refresh every L2 entry for *tier* in parallel.

    Calls ``l2_cached(block_on_miss=True, ...)`` for each target — that
    populates the primary row AND the LKG sidecar (via writeback) so
    the next request-path read finds both fresh.

    Returns a status dict ``{tier, warmed, failed, total}`` for logging.
    Failures are swallowed (logged at debug) — individual upstream
    flakiness shouldn't tank the whole warmer cycle.
    """
    targets = targets_by_tier(tier)
    if not targets:
        return {"tier": tier, "warmed": 0, "failed": [], "total": 0}

    results = await asyncio.gather(
        *(
            l2_cached(
                versioned_key(t.key), ttl_seconds=t.ttl_seconds,
                compute=t.compute, category=t.category,
            )
            for t in targets
        ),
        return_exceptions=True,
    )

    succeeded = 0
    failed: list[str] = []
    for target, result in zip(targets, results):
        if isinstance(result, Exception):
            logger.debug("warmer[%s]: %s raised: %s", tier, target.key, result)
            failed.append(target.key)
        elif result is None:
            failed.append(target.key)
        else:
            succeeded += 1
    return {"tier": tier, "warmed": succeeded, "failed": failed, "total": len(targets)}


async def warm_all() -> dict[str, dict]:
    """Run every tier in parallel.  Tiers share no compute, so parallel
    fanout gives boot wall-time = max(tier) instead of sum(tier).  Each
    tier's internal gather is rate-limited by the heavy semaphore +
    ``gate_supabase_async`` so this stays within the same backpressure
    budget as the existing per-tier periodic tasks."""
    hot_r, warm_r, cold_r = await asyncio.gather(
        warm_tier("hot"), warm_tier("warm"), warm_tier("cold"),
    )
    return {"hot": hot_r, "warm": warm_r, "cold": cold_r}
