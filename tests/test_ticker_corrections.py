"""Tests for ticker correction, validation, and display fallback logic.

Covers:
- _TICKER_CORRECTIONS: known ticker renames (FB→META, TWTR→X)
- _CUSIP_OVERRIDES: CUSIPs missing from edgartools ct.pq mapping
- _VALID_TICKER_RE: pre-compiled regex for ticker validation
- _is_valid_ticker(): validation rules (length, characters, format)
- _safe_ticker(): end-to-end resolution with CUSIP overrides + validation
- _validate_tickers(): post-ingestion logging for missing tickers
- _top_tickers(): web.py helper for extracting valid tickers from cache
- Display fallback: top_tickers list excludes None (no truncated issuer names)
- End-to-end: specific bug-report tickers resolved correctly
"""

import logging
import re
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
    _VALID_TICKER_RE,
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


# ══════════════════════════════════════════════════════════════════════
# _VALID_TICKER_RE (pre-compiled regex)
# ══════════════════════════════════════════════════════════════════════

class TestValidTickerRegex:
    def test_is_precompiled(self):
        """Regex should be compiled at module level, not re-compiled per call."""
        assert isinstance(_VALID_TICKER_RE, re.Pattern)

    def test_matches_standard_tickers(self):
        for t in ["AAPL", "A", "X", "META", "GOOGL", "BRK.A"]:
            assert _VALID_TICKER_RE.match(t), f"{t} should match"

    def test_rejects_invalid(self):
        for t in ["HILTON G", "CARDLYTI", "KKR&CO", ""]:
            assert not _VALID_TICKER_RE.match(t), f"{t!r} should not match"

    def test_case_insensitive(self):
        """Regex has re.IGNORECASE — lowercase tickers should match."""
        assert _VALID_TICKER_RE.match("aapl")
        assert _VALID_TICKER_RE.match("Brk.a")


# ══════════════════════════════════════════════════════════════════════
# _is_valid_ticker — boundary cases
# ══════════════════════════════════════════════════════════════════════

class TestIsValidTickerBoundary:
    def test_exactly_6_chars_valid(self):
        assert _is_valid_ticker("ABCDEF")

    def test_exactly_7_chars_invalid(self):
        assert not _is_valid_ticker("ABCDEFG")

    def test_single_char_valid(self):
        assert _is_valid_ticker("A")
        assert _is_valid_ticker("X")

    def test_digits_only_valid(self):
        """Some tickers are numeric (rare but valid format)."""
        assert _is_valid_ticker("1234")

    def test_mixed_case_valid(self):
        """Case-insensitive: 'Aapl' passes validation."""
        assert _is_valid_ticker("Aapl")
        assert _is_valid_ticker("meta")

    def test_dot_at_boundaries(self):
        """Dot at start or end is valid format (regex allows it)."""
        assert _is_valid_ticker(".A")
        assert _is_valid_ticker("A.")

    def test_multiple_dots_valid(self):
        """Technically valid format (e.g. BF.B)."""
        assert _is_valid_ticker("BF.B")

    def test_only_whitespace_invalid(self):
        assert not _is_valid_ticker("   ")

    def test_newline_stripped_by_safe_ticker(self):
        """Newlines are stripped by _safe_ticker before validation."""
        row = MockRow(ticker="AA\n")
        # _safe_ticker strips whitespace, so "AA\n" becomes "AA" (valid)
        assert _safe_ticker(row) == "AA"

    def test_tab_invalid(self):
        assert not _is_valid_ticker("AA\t")


# ══════════════════════════════════════════════════════════════════════
# _safe_ticker — additional edge cases
# ══════════════════════════════════════════════════════════════════════

class TestSafeTickerEdgeCases:
    def test_no_cusip_attr_falls_through_to_ticker(self):
        """Row without Cusip attribute — skips CUSIP override, uses Ticker."""
        row = type("Row", (), {"Ticker": "AAPL", "Issuer": "Apple"})()
        # No Cusip attribute at all
        assert not hasattr(row, "Cusip")
        assert _safe_ticker(row) == "AAPL"

    def test_no_cusip_attr_no_ticker_returns_none(self):
        """Row without Cusip or Ticker — returns None."""
        row = type("Row", (), {"Issuer": "Mystery Corp"})()
        assert _safe_ticker(row) is None

    def test_cusip_not_in_overrides_uses_ticker(self):
        """CUSIP present but not in override table — falls through to Ticker."""
        row = MockRow(ticker="AAPL", cusip="037833100")
        assert _safe_ticker(row) == "AAPL"

    def test_correction_applied_after_strip(self):
        """Whitespace stripped before correction lookup: 'FB ' → 'FB' → 'META'."""
        row = MockRow(ticker="FB ", cusip="30303M102")
        assert _safe_ticker(row) == "META"

    def test_bmnrd_correction_passes_validation(self):
        """BMNRD (5 chars, valid format) corrected to BMNR before validation."""
        row = MockRow(ticker="BMNRD")
        assert _safe_ticker(row) == "BMNR"

    def test_nan_variants(self):
        """Various NaN-like strings all return None."""
        for nan_str in ["nan", "NaN", "None", ""]:
            row = MockRow(ticker=nan_str)
            assert _safe_ticker(row) is None, f"'{nan_str}' should return None"

    def test_cusip_override_skips_validation(self):
        """CUSIP overrides are trusted — not validated against regex.
        This matters for CFRUY (5 chars, valid) but also for hypothetical
        future overrides that might be unusual formats."""
        row = MockRow(cusip="H25662105")
        result = _safe_ticker(row)
        assert result == "CFRUY"

    def test_all_three_cusip_overrides_resolve(self):
        """Every entry in _CUSIP_OVERRIDES actually works end-to-end."""
        for cusip, expected_ticker in _CUSIP_OVERRIDES.items():
            row = MockRow(cusip=cusip)
            assert _safe_ticker(row) == expected_ticker, (
                f"CUSIP {cusip} should resolve to {expected_ticker}"
            )

    def test_all_ticker_corrections_resolve(self):
        """Every entry in _TICKER_CORRECTIONS works end-to-end."""
        for old_ticker, expected in _TICKER_CORRECTIONS.items():
            row = MockRow(ticker=old_ticker)
            assert _safe_ticker(row) == expected, (
                f"Ticker {old_ticker} should correct to {expected}"
            )


# ══════════════════════════════════════════════════════════════════════
# _validate_tickers — additional edge cases
# ══════════════════════════════════════════════════════════════════════

class TestValidateTickersEdgeCases:
    def test_empty_holdings_no_log(self, caplog):
        """No holdings → no log message."""
        with caplog.at_level(logging.INFO):
            _validate_tickers("123", "Empty Fund", [])
        assert "without a valid ticker" not in caplog.text

    def test_empty_string_ticker_counted_as_missing(self, caplog):
        """Empty string is falsy — should be counted as missing."""
        holdings = [{"issuer": "Ghost Corp", "ticker": ""}]
        with caplog.at_level(logging.INFO):
            _validate_tickers("123", "Test Fund", holdings)
        assert "1/1 holdings without a valid ticker" in caplog.text

    def test_log_includes_fund_name_and_cik(self, caplog):
        holdings = [{"issuer": "Missing Inc", "ticker": None}]
        with caplog.at_level(logging.INFO):
            _validate_tickers("9876543", "Acme Capital", holdings)
        assert "Acme Capital" in caplog.text
        assert "9876543" in caplog.text


# ══════════════════════════════════════════════════════════════════════
# _top_tickers helper (web.py)
# ══════════════════════════════════════════════════════════════════════

class TestTopTickersHelper:
    """Tests for the _top_tickers() helper extracted during simplify."""

    @pytest.fixture(autouse=True)
    def _import_helper(self):
        """Import _top_tickers from web.py (may need stubs)."""
        # web.py has heavy imports; import the function directly
        from filings.web import _top_tickers
        self._top_tickers = _top_tickers

    # _top_tickers used to read from ``top_holdings``; the prefix was
    # dropped (it duplicated ``all_holdings[:N]``) and the helper now
    # reads from ``all_holdings`` directly.  These tests carry the new
    # contract.

    def test_extracts_valid_tickers(self):
        cached = {"all_holdings": [
            {"ticker": "AAPL", "issuer": "Apple"},
            {"ticker": "META", "issuer": "Meta"},
        ]}
        assert self._top_tickers(cached) == ["AAPL", "META"]

    def test_skips_none_tickers(self):
        cached = {"all_holdings": [
            {"ticker": "AAPL", "issuer": "Apple"},
            {"ticker": None, "issuer": "Hilton Grand Vacations"},
            {"ticker": "GOOGL", "issuer": "Alphabet"},
        ]}
        assert self._top_tickers(cached) == ["AAPL", "GOOGL"]

    def test_respects_n_parameter(self):
        cached = {"all_holdings": [
            {"ticker": "AAPL"}, {"ticker": "META"}, {"ticker": "GOOGL"},
            {"ticker": "AMZN"}, {"ticker": "MSFT"},
        ]}
        assert self._top_tickers(cached, n=3) == ["AAPL", "META", "GOOGL"]

    def test_default_n_is_5(self):
        cached = {"all_holdings": [
            {"ticker": f"T{i}"} for i in range(10)
        ]}
        assert len(self._top_tickers(cached)) == 5

    def test_empty_holdings(self):
        assert self._top_tickers({"all_holdings": []}) == []

    def test_missing_all_holdings_key(self):
        assert self._top_tickers({}) == []

    def test_all_none_tickers(self):
        cached = {"all_holdings": [
            {"ticker": None}, {"ticker": None}, {"ticker": None},
        ]}
        assert self._top_tickers(cached) == []

    def test_missing_ticker_key_in_holding(self):
        """Holdings without a 'ticker' key at all should be skipped."""
        cached = {"all_holdings": [
            {"issuer": "Apple"},  # no ticker key
            {"ticker": "META"},
        ]}
        assert self._top_tickers(cached) == ["META"]


# ══════════════════════════════════════════════════════════════════════
# End-to-end: specific bug-report tickers
# ══════════════════════════════════════════════════════════════════════

class TestBugReportTickers:
    """Verify each specific ticker from the bug report is handled correctly."""

    def test_hilton_g_rejected(self):
        """'HILTON G' was showing for Clifford Sosin — should be rejected."""
        row = MockRow(ticker="HILTON G", issuer="Hilton Grand Vacations Inc")
        assert _safe_ticker(row) is None

    def test_hilton_grand_vacations_cusip_override(self):
        """CUSIP 46321A104 resolves to HGV via override."""
        row = MockRow(issuer="Hilton Grand Vacations Inc", cusip="46321A104")
        assert _safe_ticker(row) == "HGV"

    def test_cardlyti_rejected(self):
        """'CARDLYTI' was showing for Clifford Sosin — should be rejected."""
        row = MockRow(ticker="CARDLYTI", issuer="Cardlytics Inc")
        assert _safe_ticker(row) is None

    def test_compagni_rejected(self):
        """'Compagni' was showing for Thomas Russo — should be rejected."""
        row = MockRow(ticker="Compagni", issuer="Compagnie Financiere Richemont")
        assert _safe_ticker(row) is None

    def test_richemont_cusip_override(self):
        """Swiss CUSIP H25662105 resolves to CFRUY via override."""
        row = MockRow(issuer="Compagnie Financiere Richemont", cusip="H25662105")
        assert _safe_ticker(row) == "CFRUY"

    def test_general_rejected(self):
        """'General' was showing for Greenhaven — 7 chars, rejected."""
        row = MockRow(ticker="General", issuer="General Electric Co")
        assert _safe_ticker(row) is None

    def test_kkr_and_co_rejected(self):
        """'KKR & CO' was showing for Chuck Akre — spaces + &, rejected."""
        row = MockRow(ticker="KKR & CO", issuer="KKR & Co Inc")
        assert _safe_ticker(row) is None

    def test_fb_to_meta(self):
        """'FB' was showing for multiple funds — corrected to META."""
        row = MockRow(ticker="FB", cusip="30303M102", issuer="Meta Platforms Inc")
        assert _safe_ticker(row) == "META"

    def test_twtr_to_x(self):
        """Twitter ticker correction."""
        row = MockRow(ticker="TWTR", issuer="X Corp")
        assert _safe_ticker(row) == "X"
