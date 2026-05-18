"""Tests for ``stock_bundle._merge_preserving_lkg`` — the v2 backstop
that preserves prior bundle values when sub-source fetches fail.

The v2 redesign replaced v1's per-tab HTML cache (``_with_l2_fallback``)
with a single per-ticker JSON bundle.  Without this merge step, a single
failed sub-fetch (yfinance rate-limited, OpenInsider scrape returns 0,
sync supabase poisoned) would overwrite the cached non-empty value with
the bounded-fallback empty shape — and that empty shape would then serve
on the stock page for up to 30 days (``MAX_STALE_AGE_S``).  These tests
pin the merge semantics so future refactors don't regress.
"""

from __future__ import annotations

from filings.stock_bundle import _merge_preserving_lkg


def test_no_source_status_is_passthrough():
    """When source_status is None/empty, return new_bundle unchanged."""
    new = {"insiders": [], "financials": {"revenue": 100}}
    existing = {"insiders": [{"name": "old"}], "financials": {}}
    assert _merge_preserving_lkg(new, existing, None) == new
    assert _merge_preserving_lkg(new, existing, {}) == new


def test_no_existing_bundle_is_passthrough():
    """First write for a ticker — no LKG yet, return new_bundle unchanged."""
    new = {"insiders": [], "financials": {"revenue": 100}}
    src = {"insiders": "timeout"}
    assert _merge_preserving_lkg(new, {}, src) == new
    assert _merge_preserving_lkg(new, None, src) == new  # type: ignore[arg-type]


def test_ok_status_trusts_new_value_even_if_empty():
    """A successful fetch returning [] is real data (e.g. small-cap with
    no insider trades).  Don't preserve a stale prior value over it."""
    new = {"insiders": []}
    existing = {"insiders": [{"name": "old_trade"}]}
    src = {"insiders": "ok"}
    out = _merge_preserving_lkg(new, existing, src)
    assert out["insiders"] == []


def test_failed_status_with_empty_new_preserves_existing():
    """The core fix: when a sub-fetch failed AND new value is empty,
    keep the existing non-empty value instead of overwriting."""
    new = {"insiders": [], "financials": {"revenue": 100}}
    existing = {"insiders": [{"name": "old_trade"}], "financials": {"revenue": 95}}
    # insiders fetch failed → preserve.  financials fetch OK → trust new.
    src = {"insiders": "timeout", "financials": "ok"}
    out = _merge_preserving_lkg(new, existing, src)
    assert out["insiders"] == [{"name": "old_trade"}]
    assert out["financials"] == {"revenue": 100}  # new value wins


def test_failed_status_with_nonempty_new_trusts_new():
    """A failed source that nonetheless returned non-empty data
    (partial result, half-cached etc.) is still preferred over the
    stale existing value — we never reach for the prior value when
    the caller has anything to write."""
    new = {"insiders": [{"name": "partial_new"}]}
    existing = {"insiders": [{"name": "old_trade"}]}
    src = {"insiders": "error:RuntimeError"}
    out = _merge_preserving_lkg(new, existing, src)
    assert out["insiders"] == [{"name": "partial_new"}]


def test_failed_status_with_existing_empty_keeps_new_empty():
    """Both old and new are empty — the source is genuinely empty
    (e.g. brand-new IPO with no fundamentals yet).  No preservation."""
    new = {"insiders": []}
    existing = {"insiders": []}
    src = {"insiders": "circuit_open"}
    out = _merge_preserving_lkg(new, existing, src)
    assert out["insiders"] == []


def test_multiple_failed_sources_all_preserved():
    """All sub-fetches that match the preservation rule are merged
    in a single pass — no per-source serial work needed."""
    new = {
        "insiders":  [],
        "financials": {},
        "forecasts": [],
        "overview":  {"price": 101.0},
    }
    existing = {
        "insiders":  [{"name": "A"}],
        "financials": {"revenue": 1000},
        "forecasts": [{"analyst": "B"}],
        "overview":  {"price": 99.0},
    }
    src = {
        "insiders":   "timeout",
        "financials": "error:HTTPError",
        "forecasts":  "circuit_open",
        "overview":   "ok",
    }
    out = _merge_preserving_lkg(new, existing, src)
    assert out["insiders"] == [{"name": "A"}]
    assert out["financials"] == {"revenue": 1000}
    assert out["forecasts"] == [{"analyst": "B"}]
    assert out["overview"] == {"price": 101.0}  # ok → new wins


def test_returns_new_dict_not_mutation():
    """The function should not mutate its inputs — callers may still
    be holding references to the original new/existing bundles."""
    new = {"insiders": []}
    existing = {"insiders": [{"x": 1}]}
    src = {"insiders": "timeout"}
    out = _merge_preserving_lkg(new, existing, src)
    assert out is not new
    assert new["insiders"] == []  # unchanged
