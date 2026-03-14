"""Tests for ticker correction, validation, and display fallback logic.

Covers:
- _TICKER_CORRECTIONS: known ticker renames (FB→META, TWTR→X)
- _CUSIP_OVERRIDES: CUSIPs missing from edgartools ct.pq mapping
- _is_valid_ticker(): validation rules (length, characters, format)
- _safe_ticker(): end-to-end resolution with CUSIP overrides + validation
- _validate_tickers(): post-ingestion logging for missing tickers
- Display fallback: top_tickers list excludes None (no truncated issuer names)
"""

import logging
import sys
import types
from unittest.mock import patch

import pytest

# ── Minimal stubs for heavy deps (edgar, yfinance, supabase, etc.) ──
# Only install stubs for modules not already present.
_STUBS = ["yfinance", "supabase", "postgrest"]
for mod_name in _STUBS:
    if mod_name not in sys.modules:
        sys.modules[mod_name] = types.ModuleType(mod_name)

if "edgar" not in sys.modules:
    # Provide just enough for client.py to import
    edgar = types.ModuleType("edgar")
    edgar.set_identity = lambda *a, **kw: None
    edgar.find = lambda *a, **kw: None
    edgar.Company = type("Company", (), {})
    edgar.ThirteenF = type("ThirteenF", (), {})
    sys.modules["edgar"] = edgar

    edgar_entity = types.ModuleType("edgar.entity")
    sys.modules["edgar.entity"] = edgar_entity

    edgar_search = types.ModuleType("edgar.entity.search")
    edgar_search.CompanySearchResults = type("CompanySearchResults", (), {})
    sys.modules["edgar.entity.search"] = edgar_search

sys.path.insert(0, "src")

from filings.client import (
    _CUSIP_OVERRIDES,
    _TICKER_CORRECTIONS,
    _is_valid_ticker,
    _safe_ticker,
    _validate_tickers,
)


# ── Helper: mock DataFrame row ──────────────────────────────────────

class MockRow:
    """Simulate a pandas DataFrame itertuples() row."""

    def __init__(self, ticker=None, issuer="Test Corp", cusip="000000000"):
        if ticker is not None:
            self.Ticker = ticker
        self.Issuer = issuer
        self.Cusip = cusip


# ══════════════════════════════════════════════════════════════════════
# _TICKER_CORRECTIONS table
# ══════════════════════════════════════════════════════════════════════

class TestTickerCorrections:
    def test_fb_corrected_to_meta(self):
        assert _TICKER_CORRECTIONS["FB"] == "META"

    def test_twtr_corrected_to_x(self):
        assert _TICKER_CORRECTIONS["TWTR"] == "X"

    def test_bmnrd_corrected_to_bmnr(self):
        assert _TICKER_CORRECTIONS["BMNRD"] == "BMNR"


# ══════════════════════════════════════════════════════════════════════
# _CUSIP_OVERRIDES table
# ══════════════════════════════════════════════════════════════════════

class TestCUSIPOverrides:
    def test_hilton_grand_vacations(self):
        assert _CUSIP_OVERRIDES["46321A104"] == "HGV"

    def test_hilton_worldwide(self):
        assert _CUSIP_OVERRIDES["432848101"] == "HLT"

    def test_richemont(self):
        assert _CUSIP_OVERRIDES["H25662105"] == "CFRUY"


# ══════════════════════════════════════════════════════════════════════
# _is_valid_ticker
# ══════════════════════════════════════════════════════════════════════

class TestIsValidTicker:
    def test_standard_tickers(self):
        for t in ["AAPL", "A", "KKR", "GOOGL", "X", "META"]:
            assert _is_valid_ticker(t), f"{t} should be valid"

    def test_tickers_with_dots(self):
        """BRK.A, BRK.B etc. are valid."""
        assert _is_valid_ticker("BRK.A")
        assert _is_valid_ticker("BRK.B")

    def test_adr_tickers_6_chars(self):
        """Some ADRs have 5-6 char tickers."""
        assert _is_valid_ticker("CFRUY")

    def test_rejects_spaces(self):
        assert not _is_valid_ticker("KKR & CO")
        assert not _is_valid_ticker("HILTON G")

    def test_rejects_too_long(self):
        assert not _is_valid_ticker("CARDLYTI")
        assert not _is_valid_ticker("Compagni")

    def test_rejects_special_chars(self):
        assert not _is_valid_ticker("KKR&CO")
        assert not _is_valid_ticker("A/B")

    def test_rejects_empty(self):
        assert not _is_valid_ticker("")
        assert not _is_valid_ticker(None)

    def test_rejects_lowercase_company_name_fragments(self):
        """Truncated company names like 'General' should fail."""
        assert not _is_valid_ticker("General")
        assert not _is_valid_ticker("Compagni")


# ══════════════════════════════════════════════════════════════════════
# _safe_ticker
# ══════════════════════════════════════════════════════════════════════

class TestSafeTicker:
    def test_normal_ticker(self):
        row = MockRow(ticker="AAPL")
        assert _safe_ticker(row) == "AAPL"

    def test_fb_corrected_to_meta(self):
        row = MockRow(ticker="FB", cusip="30303M102")
        assert _safe_ticker(row) == "META"

    def test_cusip_override_takes_priority(self):
        """CUSIP override wins over whatever edgartools returns."""
        row = MockRow(ticker="WRONG", cusip="46321A104")
        assert _safe_ticker(row) == "HGV"

    def test_cusip_override_with_no_ticker_column(self):
        """Even without a Ticker column, CUSIP override works."""
        row = MockRow(cusip="46321A104")
        assert _safe_ticker(row) == "HGV"

    def test_nan_returns_none(self):
        row = MockRow(ticker="nan")
        assert _safe_ticker(row) is None

    def test_none_ticker_returns_none(self):
        row = MockRow(ticker=None)
        assert _safe_ticker(row) is None

    def test_no_ticker_attr_returns_none(self):
        row = MockRow()  # No Ticker attribute set
        assert _safe_ticker(row) is None

    def test_rejects_malformed_ticker(self):
        """Tickers with spaces or >6 chars are rejected."""
        row = MockRow(ticker="HILTON G")
        assert _safe_ticker(row) is None

    def test_rejects_truncated_issuer(self):
        """8-char company name fragments should be rejected."""
        row = MockRow(ticker="CARDLYTI")
        assert _safe_ticker(row) is None

    def test_strips_whitespace(self):
        row = MockRow(ticker="AAPL ")
        assert _safe_ticker(row) == "AAPL"

    def test_valid_dot_ticker(self):
        row = MockRow(ticker="BRK.A")
        assert _safe_ticker(row) == "BRK.A"


# ══════════════════════════════════════════════════════════════════════
# _validate_tickers
# ══════════════════════════════════════════════════════════════════════

class TestValidateTickers:
    def test_logs_missing_tickers(self, caplog):
        holdings = [
            {"issuer": "Apple Inc", "ticker": "AAPL"},
            {"issuer": "Hilton Grand Vacations", "ticker": None},
            {"issuer": "Mystery Corp", "ticker": None},
        ]
        with caplog.at_level(logging.INFO):
            _validate_tickers("123", "Test Fund", holdings)
        assert "2/3 holdings without a valid ticker" in caplog.text
        assert "Hilton Grand Vacations" in caplog.text

    def test_no_log_when_all_valid(self, caplog):
        holdings = [
            {"issuer": "Apple Inc", "ticker": "AAPL"},
            {"issuer": "Meta", "ticker": "META"},
        ]
        with caplog.at_level(logging.INFO):
            _validate_tickers("123", "Test Fund", holdings)
        assert "without a valid ticker" not in caplog.text

    def test_truncates_long_lists(self, caplog):
        """Only first 10 issuer names are logged."""
        holdings = [{"issuer": f"Corp {i}", "ticker": None} for i in range(15)]
        with caplog.at_level(logging.INFO):
            _validate_tickers("123", "Test Fund", holdings)
        assert "15/15 holdings without a valid ticker" in caplog.text
        assert "..." in caplog.text


# ══════════════════════════════════════════════════════════════════════
# Display fallback (top_tickers filtering)
# ══════════════════════════════════════════════════════════════════════

class TestTopTickersFiltering:
    """Test that the top_tickers list-comprehension pattern used in web.py
    correctly skips None tickers instead of falling back to truncated names."""

    def test_filters_out_none_tickers(self):
        """Simulate the pattern from web.py."""
        cached_top_holdings = [
            {"ticker": "AAPL", "issuer": "Apple Inc"},
            {"ticker": None, "issuer": "Hilton Grand Vacations"},
            {"ticker": "META", "issuer": "Meta Platforms"},
            {"ticker": None, "issuer": "Compagnie Financiere Richemont"},
        ]
        # New pattern (no truncated issuer fallback)
        top_tickers = [
            h.get("ticker")
            for h in cached_top_holdings[:5]
            if h.get("ticker")
        ]
        assert top_tickers == ["AAPL", "META"]

    def test_old_pattern_produced_garbage(self):
        """Show that the old pattern would have produced bad tickers."""
        cached_top_holdings = [
            {"ticker": None, "issuer": "Hilton Grand Vacations"},
            {"ticker": None, "issuer": "Compagnie Financiere Richemont"},
        ]
        # Old pattern (truncated issuer fallback)
        old_top_tickers = [
            h.get("ticker") or h.get("issuer", "?")[:8]
            for h in cached_top_holdings[:5]
        ]
        # These are the garbage values we fixed
        assert old_top_tickers == ["Hilton G", "Compagni"]

    def test_all_valid_tickers_pass_through(self):
        cached_top_holdings = [
            {"ticker": "AAPL"},
            {"ticker": "META"},
            {"ticker": "GOOGL"},
        ]
        top_tickers = [
            h.get("ticker")
            for h in cached_top_holdings[:5]
            if h.get("ticker")
        ]
        assert top_tickers == ["AAPL", "META", "GOOGL"]
