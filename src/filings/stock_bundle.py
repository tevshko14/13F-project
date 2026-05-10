"""Cache layer for the pre-aggregated stock-page bundle.

Background warmers call ``redesign_preview.build_stock_data_bundle``
and store results here via :func:`set_bundle`.  The page handler
reads via :func:`get_bundle` first and only falls back to the live
fanout when the cache is missing or stale-past-tier-TTL.

All Supabase calls are SYNC (postgrest sync client); call from async
contexts via ``asyncio.to_thread`` or :func:`filings.concurrency.to_light`.
"""

from __future__ import annotations

import collections as _collections
import dataclasses as _dc
import json as _json
import logging
import re as _re
import statistics as _stats
from datetime import datetime, timezone
from decimal import Decimal

from filings.supabase_cache import _get_client

logger = logging.getLogger(__name__)


# Hot TTL deliberately tighter than the warmer's 5-min cycle interval
# so users see at-most-5-min-old data on cache hits.  Combined with
# the warmer's "always refresh oldest N" strategy below, hot tickers
# spend most of their lifetime fresh, with a brief gap window each
# cycle while the warmer is in-flight.
BUNDLE_TTL_HOT_S  = 280
BUNDLE_TTL_WARM_S = 30 * 60
BUNDLE_TTL_COLD_S = 60 * 60

_TTL_BY_TIER = {
    "hot":  BUNDLE_TTL_HOT_S,
    "warm": BUNDLE_TTL_WARM_S,
    "cold": BUNDLE_TTL_COLD_S,
}

_VALID_TIERS = frozenset(_TTL_BY_TIER)

# Tier names exposed for callers so the strings aren't sprinkled around.
HOT_TIER:  str = "hot"
WARM_TIER: str = "warm"
COLD_TIER: str = "cold"

# Warmer-attempt status values written to ``last_attempt_status``.
# Exposed so callers (web.py warmer, admin endpoints) reference the
# constants instead of repeating the raw strings.
STATUS_SUCCESS: str = "success"
STATUS_PARTIAL: str = "partial"
STATUS_FAILED:  str = "failed"

# Tiers the periodic warmer refreshes proactively.  ``cold`` is
# materialised on-demand from request misses, never warmed in batch.
WARMABLE_TIERS: tuple[str, ...] = (HOT_TIER, WARM_TIER)

# How long to back off after a failed warmer attempt before re-trying
# the same ticker.  Without this, a permanently-broken ticker (delisted,
# foreign with no yfinance coverage, etc.) is re-picked every cycle and
# burns ~10 upstream calls per failure forever.
FAILURE_BACKOFF_S = 5 * 60


# ── Request-path counters ───────────────────────────────────────────
# Monotonic counters bumped from ``preview_stock``'s cache check.
# Reset on worker recycle (acceptable for v1; if we add cross-worker
# rollups later, push to a Supabase row instead).  Read via
# :func:`get_request_metrics` for /admin/stock-cache-status.

_request_metrics: dict[str, int] = {
    "hits":         0,  # cached row found + within tier TTL
    "miss_stale":   0,  # cached row found but past tier TTL
    "miss_cold":    0,  # no cached row at all (first-time materialisation)
}


def record_hit() -> None:
    _request_metrics["hits"] += 1


def record_miss(*, had_cached: bool) -> None:
    if had_cached:
        _request_metrics["miss_stale"] += 1
    else:
        _request_metrics["miss_cold"] += 1


def get_request_metrics() -> dict:
    """Snapshot of request-path counters since worker startup."""
    snap = dict(_request_metrics)
    total = snap["hits"] + snap["miss_stale"] + snap["miss_cold"]
    snap["total"] = total
    snap["hit_rate_pct"] = round(snap["hits"] * 100 / total, 1) if total else 0.0
    return snap


def derive_attempt_status(source_status: dict | None) -> str:
    """Reduce a per-source status map to a single warmer-attempt status.

    ``source_status`` is the dict ``build_stock_data_bundle`` returns
    alongside the bundle (``{"overview": "ok", "financials": "timeout",
    ...}``).  Returns :data:`STATUS_SUCCESS` only if every recorded
    source is ``"ok"``; otherwise :data:`STATUS_PARTIAL`.  Empty /
    missing dict treats as partial so we never claim a fully-successful
    refresh on no evidence.
    """
    if not source_status:
        return STATUS_PARTIAL
    return STATUS_SUCCESS if all(s == "ok" for s in source_status.values()) else STATUS_PARTIAL


def is_fresh(refreshed_at: datetime | None, tier: str) -> bool:
    """True if the bundle's age is within its tier's TTL."""
    if not refreshed_at:
        return False
    ttl = _TTL_BY_TIER.get(tier, BUNDLE_TTL_COLD_S)
    age = (datetime.now(timezone.utc) - refreshed_at).total_seconds()
    return age < ttl


def _to_json_safe(obj):
    """Convert non-JSON types (datetime/Decimal/dataclasses) in place.

    Single-pass walker; mutates dicts and lists rather than rebuilding
    via ``json.dumps(default=str)`` round-trip (supabase-py serialises
    again before POST, so the round-trip would be the second of three
    full passes).  Caller should pass a bundle that's about to be
    written and discarded.

    Handles:
      * ``dict`` / ``list``: recursed in place
      * ``tuple``: converted to ``list`` (JSONB has no tuple type)
      * ``datetime``: ISO 8601 string
      * ``Decimal``: numeric string
      * ``@dataclass`` (e.g. ``InsiderTrade``): converted via
        ``dataclasses.asdict`` then recursed
    """
    if isinstance(obj, dict):
        for k, v in obj.items():
            obj[k] = _to_json_safe(v)
        return obj
    if isinstance(obj, list):
        for i, v in enumerate(obj):
            obj[i] = _to_json_safe(v)
        return obj
    if isinstance(obj, tuple):
        return [_to_json_safe(v) for v in obj]
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return str(obj)
    if _dc.is_dataclass(obj) and not isinstance(obj, type):
        return _to_json_safe(_dc.asdict(obj))
    return obj


def _parse_ts(value) -> datetime | None:
    """Parse a Supabase timestamptz column value into ``datetime``."""
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def get_bundle(ticker: str) -> dict | None:
    """Read the cached bundle row for *ticker*.

    Returns a dict with keys ``bundle`` / ``refreshed_at`` / ``tier``
    / ``source_status``, or ``None`` on miss or Supabase unavailable.
    Caller decides freshness via :func:`is_fresh`.
    """
    client = _get_client()
    if client is None:
        return None
    try:
        resp = (
            client.table("stock_overview_cache")
            .select("ticker, bundle, refreshed_at, tier, source_status")
            .eq("ticker", ticker.upper())
            .maybe_single()
            .execute()
        )
        row = resp.data
    except Exception as exc:
        logger.debug("stock bundle read failed for %s: %s", ticker, exc)
        return None
    if not row:
        return None
    return {
        "bundle":        row.get("bundle") or {},
        "refreshed_at":  _parse_ts(row.get("refreshed_at")),
        "tier":          row.get("tier") or "cold",
        "source_status": row.get("source_status") or {},
    }


def get_stale_tickers(tier: str, limit: int = 50) -> list[str]:
    """The next *limit* tickers in *tier* the warmer should refresh.

    Returns the OLDEST tickers in the tier ordered ``refreshed_at ASC``
    -- not just those past TTL.  Originally we filtered to "past TTL"
    only, but that meant a cycle could skip the entire hot tier when
    every hot ticker was just-under-TTL at scan time; users hitting
    in the next 30-60s fell to the cold path until the next cycle
    expired them.  Always picking the oldest keeps the tier in
    perpetual refresh, which is what we actually want for hot/warm.

    Excludes tickers that just FAILED a warmer attempt within the
    last :data:`FAILURE_BACKOFF_S` so a permanently-broken upstream
    can't drive infinite retry loops.  Failed tickers re-enter the
    pool once the backoff window passes.
    """
    if tier not in _VALID_TIERS:
        return []
    client = _get_client()
    if client is None:
        return []

    backoff_cutoff_iso = datetime.fromtimestamp(
        datetime.now(timezone.utc).timestamp() - FAILURE_BACKOFF_S,
        tz=timezone.utc,
    ).isoformat()

    # Over-fetch so the post-filter on recent failures still yields
    # ``limit`` eligible rows in the common case.
    try:
        resp = (
            client.table("stock_overview_cache")
            .select("ticker, last_attempt_status, last_attempt_at")
            .eq("tier", tier)
            .order("refreshed_at", desc=False)
            .limit(limit * 2)
            .execute()
        )
        rows = resp.data or []
    except Exception as exc:
        logger.warning("stock bundle stale lookup failed (tier=%s): %s",
                       tier, exc)
        return []

    # Postgrest's NOT-of-AND syntax is awkward; filter in Python.
    # ISO timestamps are lexicographically sortable in this format.
    eligible: list[str] = []
    for r in rows:
        if r.get("last_attempt_status") == STATUS_FAILED:
            la = r.get("last_attempt_at") or ""
            if la > backoff_cutoff_iso:
                continue  # within backoff window, skip
        ticker = r.get("ticker")
        if ticker:
            eligible.append(ticker)
            if len(eligible) >= limit:
                break
    return eligible


def get_all_tickers_in_tier(tier: str) -> list[str]:
    """Every ticker currently classified in *tier*."""
    if tier not in _VALID_TIERS:
        return []
    client = _get_client()
    if client is None:
        return []
    try:
        resp = (
            client.table("stock_overview_cache")
            .select("ticker")
            .eq("tier", tier)
            .execute()
        )
        rows = resp.data or []
    except Exception as exc:
        logger.warning("stock bundle tier lookup failed (tier=%s): %s",
                       tier, exc)
        return []
    return [r["ticker"] for r in rows if r.get("ticker")]


def set_bundle(
    ticker: str,
    bundle: dict,
    *,
    tier: str = "warm",
    source_status: dict | None = None,
    last_attempt_status: str | None = None,
) -> bool:
    """Upsert the bundle for *ticker*; True on success.

    When ``last_attempt_status`` is ``None``, it's derived from
    ``source_status`` via :func:`derive_attempt_status`
    (:data:`STATUS_SUCCESS` iff every source is ``"ok"``, else
    :data:`STATUS_PARTIAL`).  Pass an explicit value only if the
    caller knows something the source-status map doesn't (e.g.
    force-marking a write as :data:`STATUS_FAILED`).
    """
    client = _get_client()
    if client is None:
        return False
    if tier not in _VALID_TIERS:
        tier = "cold"

    now_iso = datetime.now(timezone.utc).isoformat()
    if last_attempt_status is None:
        last_attempt_status = derive_attempt_status(source_status)

    payload = {
        "ticker":              ticker.upper(),
        "bundle":              _to_json_safe(bundle) if bundle is not None else {},
        "refreshed_at":        now_iso,
        "tier":                tier,
        "source_status":       _to_json_safe(source_status) if source_status else {},
        "last_attempt_at":     now_iso,
        "last_attempt_status": last_attempt_status,
    }
    try:
        client.table("stock_overview_cache").upsert(payload).execute()
        return True
    except Exception as exc:
        logger.warning("stock bundle write failed for %s: %s", ticker, exc)
        return False


def set_tier(ticker: str, tier: str) -> bool:
    """Update only the tier classification.

    Used by traffic-driven re-ranking (promote a frequently-hit
    ticker from warm to hot etc.) without re-fetching the bundle.
    """
    if tier not in _VALID_TIERS:
        return False
    client = _get_client()
    if client is None:
        return False
    try:
        (
            client.table("stock_overview_cache")
            .update({"tier": tier})
            .eq("ticker", ticker.upper())
            .execute()
        )
        return True
    except Exception as exc:
        logger.warning("stock bundle tier update failed for %s: %s", ticker, exc)
        return False


def record_attempt(
    ticker: str,
    *,
    status: str,
    source_status: dict | None = None,
) -> bool:
    """Record a warmer attempt without touching the bundle.

    Used when a refresh failed entirely -- still want to mark the
    attempt so the warmer's oldest-first scheduler doesn't immediately
    re-pick the same ticker.
    """
    client = _get_client()
    if client is None:
        return False
    update = {
        "last_attempt_at":     datetime.now(timezone.utc).isoformat(),
        "last_attempt_status": status,
    }
    if source_status is not None:
        update["source_status"] = _to_json_safe(source_status)
    try:
        (
            client.table("stock_overview_cache")
            .update(update)
            .eq("ticker", ticker.upper())
            .execute()
        )
        return True
    except Exception as exc:
        logger.debug("stock bundle attempt-record failed for %s: %s", ticker, exc)
        return False


# ── Initial tier population ─────────────────────────────────────────

# "Always-hot" core: high-traffic names that should be in the hot tier
# even if they aren't massively over-represented in 13F holdings.  The
# 13F-derived count below picks up most popular names organically; this
# list is the safety net for retail-facing tickers that may be under-held
# by the superinvestor cohort but still see lots of pageviews.
#
# Single-source-of-truth for Berkshire is "BRK.B" -- ``BRK-B`` would
# fail ``app_state.valid_ticker`` since that regex doesn't allow ``-``.
DEFAULT_HOT_TICKERS: tuple[str, ...] = (
    "AAPL", "MSFT", "GOOG", "GOOGL", "META", "AMZN", "NVDA",
    "TSLA", "BRK.B",
    "JPM", "V", "MA", "BAC",
    "WMT", "COST", "HD", "MCD", "NKE", "DIS",
    "JNJ", "UNH", "LLY",
    "XOM", "CVX",
    "AMD", "AVGO", "ORCL", "ADBE", "CRM", "INTC", "NFLX",
    "SOFI",
)

# Ticker validation: alphanumeric + dot/dash, 1-6 chars.  Filters out
# mutual-fund / option-chain detritus that occasionally appears in 13F
# holdings (e.g. "VWLUX", "CASH", multi-character class shares).
_TICKER_OK_RE = _re.compile(r"^[A-Z][A-Z0-9.\-]{0,5}$")

# Epoch zero used as ``refreshed_at`` for newly-seeded rows so the
# warmer's "oldest-refresh-first" filter picks them up immediately on
# the next cycle.
_EPOCH_ZERO_ISO = "1970-01-01T00:00:00+00:00"

# Age threshold (seconds) above which a row is treated as "unrefreshed"
# rather than included in age percentiles.  Pairs with the epoch-zero
# seed timestamp -- those rows have ages of ~56 years which would
# skew p50/mean wildly if mixed with real ages.  Anything older than
# 1 year wasn't refreshed by the warmer in any reasonable past, so
# safely treated as seed-only.
_UNREFRESHED_AGE_THRESHOLD_S = 365 * 24 * 3600


def populate_initial_tiers(
    fund_cache: dict,
    *,
    hot_count: int = 50,
    warm_count: int = 200,
) -> dict:
    """Seed ``stock_overview_cache`` with hot/warm tier classifications.

    Counts how many 13F filers hold each ticker across the in-memory
    fund cache; assigns top ``hot_count`` to the hot tier and the next
    ``warm_count`` to warm.  :data:`DEFAULT_HOT_TICKERS` is unioned
    into the hot set so retail-popular names (e.g. SOFI, NFLX) make
    the cut even when the 13F overlap is thin.

    Idempotent: existing rows are NOT overwritten -- the seeder only
    creates rows for tickers not already in the table.  This means
    re-running won't downgrade a ticker the warmer has since promoted
    or demoted.

    Returns ``{"hot_seeded": [...], "warm_seeded": [...], "skipped": int}``.
    """
    if not fund_cache:
        logger.info("seed: empty fund_cache, skipping")
        return {"hot_seeded": [], "warm_seeded": [], "skipped": 0}

    counts: _collections.Counter[str] = _collections.Counter()
    for fund_data in fund_cache.values():
        if not isinstance(fund_data, dict):
            continue
        for h in (fund_data.get("all_holdings") or []):
            if not isinstance(h, dict):
                continue
            t = (h.get("ticker") or "").upper().strip()
            if t and _TICKER_OK_RE.match(t):
                counts[t] += 1

    most_held = [t for t, _ in counts.most_common(hot_count + warm_count)]

    always_hot = {t for t in DEFAULT_HOT_TICKERS if _TICKER_OK_RE.match(t)}
    hot_set:  set[str] = always_hot | set(most_held[:hot_count])
    warm_set: set[str] = set(most_held[hot_count : hot_count + warm_count]) - hot_set

    client = _get_client()
    if client is None:
        return {"hot_seeded": [], "warm_seeded": [], "skipped": 0}

    # Build candidate rows.  Tier promotions/demotions are preserved
    # by the upsert below (``ignore_duplicates=True`` -> ON CONFLICT
    # DO NOTHING), so we don't need to read existing tickers first --
    # which also avoids a full-table scan and the read-then-insert
    # race window that occurs when multiple workers seed concurrently.
    new_rows: list[dict] = []
    hot_list  = sorted(hot_set)
    warm_list = sorted(warm_set)
    for t in hot_list:
        new_rows.append({
            "ticker":        t,
            "bundle":        {},
            "refreshed_at":  _EPOCH_ZERO_ISO,
            "tier":          HOT_TIER,
            "source_status": {},
        })
    for t in warm_list:
        new_rows.append({
            "ticker":        t,
            "bundle":        {},
            "refreshed_at":  _EPOCH_ZERO_ISO,
            "tier":          WARM_TIER,
            "source_status": {},
        })

    if not new_rows:
        return {"hot_seeded": [], "warm_seeded": [], "skipped": 0}

    try:
        # ON CONFLICT DO NOTHING -- existing rows aren't touched, so
        # any tier the warmer/admin promoted is preserved.  Returns
        # only the rows actually inserted, which the caller can use to
        # report the seed delta.
        resp = (
            client.table("stock_overview_cache")
            .upsert(new_rows, on_conflict="ticker", ignore_duplicates=True)
            .execute()
        )
        inserted = {r["ticker"] for r in (resp.data or []) if r.get("ticker")}
    except Exception as exc:
        logger.warning("seed: upsert failed: %s", exc)
        return {"hot_seeded": [], "warm_seeded": [], "skipped": 0}

    hot_seeded  = [t for t in hot_list  if t in inserted]
    warm_seeded = [t for t in warm_list if t in inserted]
    skipped     = len(new_rows) - len(inserted)

    logger.info(
        "seed: inserted hot=%d warm=%d (skipped %d existing); warmer will fill bundles on next cycle",
        len(hot_seeded), len(warm_seeded), skipped,
    )
    return {
        "hot_seeded":  hot_seeded,
        "warm_seeded": warm_seeded,
        "skipped":     skipped,
    }


# ── Telemetry helpers (read-only Supabase queries for /admin) ───────


def get_tier_distribution() -> dict:
    """Row count per tier; aggregated for /admin/stock-cache-status.

    Returns ``{"hot": N, "warm": M, "cold": K, "total": N+M+K}``.
    Values are ``-1`` on Supabase error so callers can distinguish
    "Supabase unreachable" from "tier genuinely empty".
    """
    client = _get_client()
    out: dict = {}
    if client is None:
        return {t: -1 for t in (HOT_TIER, WARM_TIER, COLD_TIER)} | {"total": -1}

    total = 0
    for tier in (HOT_TIER, WARM_TIER, COLD_TIER):
        try:
            resp = (
                client.table("stock_overview_cache")
                .select("ticker", count="exact", head=True)
                .eq("tier", tier)
                .execute()
            )
            n = resp.count or 0
            out[tier] = n
            total += n
        except Exception as exc:
            logger.debug("tier count failed for %s: %s", tier, exc)
            out[tier] = -1
    out["total"] = total
    return out


def get_tier_age_stats(tier: str) -> dict:
    """Mean/max/min bundle age (seconds) for *tier*.

    Spots warmer-falling-behind: if a hot ticker's mean age is well
    over its TTL (10 min for hot), the warmer isn't keeping up with
    the tier's churn.

    Seeder rows that haven't been warmed yet have ``refreshed_at``
    set to epoch zero (1970-01-01); those are counted separately as
    ``unrefreshed`` rather than skewing the percentiles toward 56
    years.
    """
    if tier not in _VALID_TIERS:
        return {"count": 0, "unrefreshed": 0}
    client = _get_client()
    if client is None:
        return {"count": 0, "unrefreshed": 0}

    try:
        resp = (
            client.table("stock_overview_cache")
            .select("refreshed_at")
            .eq("tier", tier)
            .execute()
        )
        rows = resp.data or []
    except Exception as exc:
        logger.debug("tier age stats failed for %s: %s", tier, exc)
        return {"count": 0, "unrefreshed": 0}

    now = datetime.now(timezone.utc)
    ages: list[float] = []
    unrefreshed = 0
    for r in rows:
        ts = _parse_ts(r.get("refreshed_at"))
        if not ts:
            unrefreshed += 1
            continue
        age = (now - ts).total_seconds()
        if age > _UNREFRESHED_AGE_THRESHOLD_S:
            unrefreshed += 1
        else:
            ages.append(age)

    return {
        "count":       len(ages),
        "unrefreshed": unrefreshed,
        "ttl_s":       _TTL_BY_TIER[tier],
        "min_s":       int(min(ages))         if ages else None,
        "p50_s":       int(_stats.median(ages)) if ages else None,
        "max_s":       int(max(ages))         if ages else None,
        "mean_s":      int(_stats.mean(ages))   if ages else None,
    }


def get_recently_failed(limit: int = 20) -> list[dict]:
    """Most-recent tickers whose last warmer attempt failed.

    Useful for diagnosing broken upstreams: if the same ticker keeps
    showing up here, the issue is upstream-specific (yfinance
    rate-limit, delisted security, etc.) -- not a cache bug.
    """
    client = _get_client()
    if client is None:
        return []
    try:
        resp = (
            client.table("stock_overview_cache")
            .select("ticker, tier, last_attempt_at, last_attempt_status, source_status")
            .eq("last_attempt_status", STATUS_FAILED)
            .order("last_attempt_at", desc=True)
            .limit(limit)
            .execute()
        )
        return resp.data or []
    except Exception as exc:
        logger.debug("recently failed lookup failed: %s", exc)
        return []
