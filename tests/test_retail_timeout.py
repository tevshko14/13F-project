"""Tests for /retail timeout protection and graceful degradation.

Covers:
  - _safe_fetch: timeout enforcement, exception swallowing, success passthrough
  - _fetch_retail_data: graceful fallback when upstream fails
  - _fetch_apewisdom_pages: partial results on timeout, future cancellation, ordering

The _safe_fetch / _fetch_retail_data tests replicate the exact function
logic to avoid importing web.py's full dependency chain (edgar, yfinance,
supabase, etc.).  The apewisdom tests import sentiment.py directly.
"""

from __future__ import annotations

import asyncio
import logging
import sys
import time
import types
from unittest.mock import patch

import pytest

# ── Stub heavy deps so filings.sentiment can be imported ──────────
for _mod in ("edgar", "yfinance", "supabase", "postgrest", "gotrue",
             "storage3", "realtime", "supafunc"):
    if _mod not in sys.modules:
        sys.modules[_mod] = types.ModuleType(_mod)


logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════
# Replicas of web.py helpers (same logic, tested in isolation)
# ═══════════════════════════════════════════════════════════════════════

async def _safe_fetch(coro, label: str, timeout: int = 10):
    """Exact replica of filings.web._safe_fetch for isolated testing."""
    try:
        return await asyncio.wait_for(coro, timeout=timeout)
    except asyncio.TimeoutError:
        logger.warning("/retail: %s timed out (%ds)", label, timeout)
        return None
    except Exception:
        logger.warning("/retail: %s failed", label, exc_info=True)
        return None


async def _fetch_retail_data(
    _to_heavy, apewisdom_fn, fear_greed_fn
) -> tuple[list[dict], dict | None]:
    """Exact replica of filings.web._fetch_retail_data logic."""
    try:
        all_data, fear_greed = await asyncio.wait_for(
            asyncio.gather(
                _to_heavy(apewisdom_fn),
                _to_heavy(fear_greed_fn),
            ),
            timeout=10,
        )
    except Exception:
        logger.warning("_fetch_retail_data: data fetch failed", exc_info=True)
        all_data, fear_greed = [], None
    return all_data or [], fear_greed


# ═══════════════════════════════════════════════════════════════════════
# _safe_fetch unit tests
# ═══════════════════════════════════════════════════════════════════════


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.anyio
async def test_safe_fetch_returns_value_on_success():
    """_safe_fetch passes through the coroutine's return value."""
    async def _ok():
        return {"data": 42}

    result = await _safe_fetch(_ok(), "test_ok")
    assert result == {"data": 42}


@pytest.mark.anyio
async def test_safe_fetch_returns_none_on_timeout():
    """_safe_fetch returns None when coroutine exceeds timeout."""
    async def _slow():
        await asyncio.sleep(5)
        return "should not reach"

    result = await _safe_fetch(_slow(), "test_slow", timeout=1)
    assert result is None


@pytest.mark.anyio
async def test_safe_fetch_returns_none_on_exception():
    """_safe_fetch returns None when coroutine raises."""
    async def _boom():
        raise ConnectionError("upstream down")

    result = await _safe_fetch(_boom(), "test_boom")
    assert result is None


@pytest.mark.anyio
async def test_safe_fetch_timeout_completes_quickly():
    """_safe_fetch should return within ~timeout seconds, not hang."""
    async def _hang():
        await asyncio.sleep(60)

    start = time.monotonic()
    result = await _safe_fetch(_hang(), "test_hang", timeout=1)
    elapsed = time.monotonic() - start

    assert result is None
    assert elapsed < 3, f"_safe_fetch took {elapsed:.1f}s — should be ~1s"


@pytest.mark.anyio
async def test_safe_fetch_preserves_none_return():
    """_safe_fetch returns None if the coro itself returns None (not an error)."""
    async def _none():
        return None

    result = await _safe_fetch(_none(), "test_none")
    assert result is None


# ═══════════════════════════════════════════════════════════════════════
# _fetch_retail_data unit tests
# ═══════════════════════════════════════════════════════════════════════


@pytest.mark.anyio
async def test_fetch_retail_data_returns_defaults_on_exception():
    """_fetch_retail_data returns ([], None) when _to_heavy raises."""
    async def _broken_heavy(fn):
        raise Exception("pool down")

    all_data, fear_greed = await _fetch_retail_data(
        _broken_heavy, lambda: [], lambda: None
    )
    assert all_data == []
    assert fear_greed is None


@pytest.mark.anyio
async def test_fetch_retail_data_returns_defaults_on_timeout():
    """_fetch_retail_data returns ([], None) when sources time out."""
    async def _slow_heavy(fn):
        await asyncio.sleep(60)

    start = time.monotonic()
    all_data, fear_greed = await _fetch_retail_data(
        _slow_heavy, lambda: [], lambda: None
    )
    elapsed = time.monotonic() - start

    assert all_data == []
    assert fear_greed is None
    assert elapsed < 15, f"_fetch_retail_data took {elapsed:.1f}s — 10s timeout expected"


@pytest.mark.anyio
async def test_fetch_retail_data_normalizes_none_to_empty_list():
    """_fetch_retail_data converts None all_data to []."""
    async def _mock_heavy(fn):
        return fn()

    all_data, fear_greed = await _fetch_retail_data(
        _mock_heavy, lambda: None, lambda: {"score": 50}
    )
    assert all_data == []  # None normalized to []
    assert fear_greed == {"score": 50}


@pytest.mark.anyio
async def test_fetch_retail_data_passes_through_valid_data():
    """_fetch_retail_data returns real data when sources succeed."""
    mock_stocks = [{"ticker": "AAPL", "mentions": 100}]
    mock_fg = {"score": 72, "label": "Greed"}

    async def _mock_heavy(fn):
        return fn()

    all_data, fear_greed = await _fetch_retail_data(
        _mock_heavy, lambda: mock_stocks, lambda: mock_fg
    )
    assert all_data == mock_stocks
    assert fear_greed == mock_fg


# ═══════════════════════════════════════════════════════════════════════
# _fetch_apewisdom_pages tests (imports sentiment.py directly)
# ═══════════════════════════════════════════════════════════════════════


def _run_apewisdom_test(mock_fn):
    """Helper: run _fetch_apewisdom_pages with mocked HTTP, restoring cache."""
    from filings import sentiment

    with patch.object(sentiment, "_http_get_json", side_effect=mock_fn):
        old_cache = sentiment._apewisdom_cache
        old_index = sentiment._apewisdom_index
        sentiment._apewisdom_cache = None
        try:
            return sentiment._fetch_apewisdom_pages()
        finally:
            sentiment._apewisdom_cache = old_cache
            sentiment._apewisdom_index = old_index


def test_apewisdom_pages_all_succeed():
    """_fetch_apewisdom_pages collects all 5 pages in order when all succeed."""
    def _mock(url, timeout=6):
        page = int(url.rstrip("/").split("/")[-1])
        return {"results": [{"ticker": f"PAGE{page}", "rank": page}]}

    results = _run_apewisdom_test(_mock)

    assert len(results) == 5
    tickers = [r["ticker"] for r in results]
    assert tickers == ["PAGE1", "PAGE2", "PAGE3", "PAGE4", "PAGE5"]


def test_apewisdom_pages_returns_empty_on_total_failure():
    """_fetch_apewisdom_pages returns [] when all pages fail."""
    def _mock(url, timeout=6):
        raise ConnectionError("ApeWisdom is down")

    results = _run_apewisdom_test(_mock)
    assert results == []


def test_apewisdom_pages_updates_cache():
    """_fetch_apewisdom_pages updates the global cache after fetch."""
    from filings import sentiment

    def _mock(url, timeout=6):
        return {"results": [{"ticker": "AAPL", "rank": 1}]}

    with patch.object(sentiment, "_http_get_json", side_effect=_mock):
        old_cache = sentiment._apewisdom_cache
        old_index = sentiment._apewisdom_index
        try:
            sentiment._apewisdom_cache = None
            sentiment._apewisdom_index = None
            results = sentiment._fetch_apewisdom_pages()

            assert sentiment._apewisdom_cache is not None
            ts, cached_data = sentiment._apewisdom_cache
            assert cached_data == results
            assert time.time() - ts < 2  # freshly cached

            assert sentiment._apewisdom_index is not None
            assert "AAPL" in sentiment._apewisdom_index
        finally:
            sentiment._apewisdom_cache = old_cache
            sentiment._apewisdom_index = old_index


def test_apewisdom_pages_handles_malformed_response():
    """_fetch_apewisdom_pages handles non-dict / missing 'results' gracefully."""
    responses = iter([
        "not a dict",       # page 1: string
        None,               # page 2: None
        {"no_results": []}, # page 3: missing key
        {"results": None},  # page 4: None results
        {"results": [{"ticker": "OK", "rank": 1}]},  # page 5: valid
    ])

    def _mock(url, timeout=6):
        return next(responses)

    results = _run_apewisdom_test(_mock)

    # Only page 5 should produce a result
    assert len(results) == 1
    assert results[0]["ticker"] == "OK"


def test_apewisdom_pages_completes_within_timeout():
    """_fetch_apewisdom_pages must complete in <12s even when all pages hang."""
    def _mock(url, timeout=6):
        time.sleep(15)  # simulate all pages hanging
        return {"results": []}

    start = time.monotonic()
    results = _run_apewisdom_test(_mock)
    elapsed = time.monotonic() - start

    assert elapsed < 12, f"_fetch_apewisdom_pages took {elapsed:.1f}s — 8s timeout expected"
    assert results == []


def test_apewisdom_pages_partial_results_on_timeout():
    """_fetch_apewisdom_pages returns fast pages when slow pages time out."""
    def _mock(url, timeout=6):
        page = int(url.rstrip("/").split("/")[-1])
        if page >= 4:
            time.sleep(15)  # pages 4-5 hang
        return {"results": [{"ticker": f"T{page}", "rank": page}]}

    start = time.monotonic()
    results = _run_apewisdom_test(_mock)
    elapsed = time.monotonic() - start

    # Should have pages 1-3 (the fast ones), possibly not 4-5
    assert len(results) >= 3, f"Expected at least 3 results, got {len(results)}"
    assert elapsed < 12, f"Took {elapsed:.1f}s — should be bounded by 8s timeout"
    # Results should be in page order
    tickers = [r["ticker"] for r in results]
    assert tickers == sorted(tickers, key=lambda t: int(t[1:]))
