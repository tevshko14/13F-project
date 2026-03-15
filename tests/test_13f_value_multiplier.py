"""Tests for SEC 13F value handling.

edgartools converts SEC 13F values from thousands to actual dollars
internally.  Our code must NOT apply any additional multiplier — it
should pass through the edgartools values as-is.

These tests verify:
  - _filing_total_value() and _row_value() return raw values (no multiplier)
  - get_fund_summary() preserves edgartools values unchanged
  - get_holdings() preserves edgartools values unchanged
  - _compare_two_filings() preserves edgartools values unchanged
  - _validate_fund_values() flags anomalously low portfolio values
"""

from __future__ import annotations

import logging
import sys
import types
from unittest.mock import MagicMock, patch
from dataclasses import dataclass

import pytest

# ── Ensure heavy deps are importable ─────────────────────────────────
for _mod in ("yfinance", "supabase", "postgrest", "gotrue",
             "storage3", "realtime", "supafunc"):
    if _mod not in sys.modules:
        sys.modules[_mod] = types.ModuleType(_mod)


# ═══════════════════════════════════════════════════════════════════════
# Value pass-through tests (no multiplier)
# ═══════════════════════════════════════════════════════════════════════


def test_filing_total_value_no_multiplier():
    """_filing_total_value returns the raw edgartools value."""
    from filings.client import _filing_total_value

    mock_tf = MagicMock()
    mock_tf.total_value = 6_568_000_000  # edgartools already in actual dollars
    assert _filing_total_value(mock_tf) == 6_568_000_000


def test_filing_total_value_none():
    """_filing_total_value returns 0 for None."""
    from filings.client import _filing_total_value

    mock_tf = MagicMock()
    mock_tf.total_value = None
    assert _filing_total_value(mock_tf) == 0


def test_row_value_no_multiplier():
    """_row_value returns the raw edgartools value."""
    from filings.client import _row_value

    mock_row = MagicMock()
    mock_row.Value = 3_000_000_000  # $3B, already in actual dollars
    assert _row_value(mock_row) == 3_000_000_000


def test_values_in_get_fund_summary():
    """get_fund_summary must pass through edgartools values without multiplying."""
    from filings import client
    import pandas as pd
    from decimal import Decimal

    mock_tf = MagicMock()
    mock_tf.total_value = Decimal("6568000000")  # $6.568B in actual dollars
    mock_tf.management_company_name = "AKO Capital LLP"
    mock_tf.report_period = "2025-03-31"
    mock_tf.filing_date = "2025-05-15"

    holdings_df = pd.DataFrame({
        "Issuer": ["APPLE INC", "MICROSOFT CORP"],
        "Class": ["COM", "COM"],
        "Cusip": ["037833100", "594918104"],
        "Value": [3_000_000_000, 3_568_000_000],  # actual dollars from edgartools
        "SharesPrnAmount": [100, 200],
        "Type": ["SH", "SH"],
        "Ticker": ["AAPL", "MSFT"],
    })
    mock_tf.holdings = holdings_df

    mock_company = MagicMock()
    mock_filings = MagicMock()
    mock_filings.__len__ = lambda s: 1
    mock_filings.__getitem__ = lambda s, i: MagicMock()
    mock_company.get_filings.return_value = mock_filings

    with patch.object(client, "Company", return_value=mock_company), \
         patch.object(client, "ThirteenF", return_value=mock_tf):
        result = client.get_fund_summary("12345")

    # Values should be passed through unchanged
    assert result["total_value"] == 6_568_000_000

    for h in result["all_holdings"]:
        assert h["value"] >= 3_000_000_000  # actual dollars, not thousands


def test_values_in_get_holdings():
    """get_holdings must pass through edgartools values without multiplying."""
    from filings import client
    import pandas as pd
    from decimal import Decimal

    mock_tf = MagicMock()
    mock_tf.total_value = Decimal("5278000000")  # $5.278B actual
    mock_tf.management_company_name = "Baupost Group"
    mock_tf.report_period = "2025-03-31"
    mock_tf.filing_date = "2025-05-15"

    holdings_df = pd.DataFrame({
        "Issuer": ["LIBERTY BROADBAND"],
        "Class": ["COM"],
        "Cusip": ["530307305"],
        "Value": [2_000_000_000],  # $2B actual
        "SharesPrnAmount": [500],
        "Type": ["SH"],
        "Ticker": ["LBRDA"],
    })
    mock_tf.holdings = holdings_df

    mock_company = MagicMock()
    mock_filings = MagicMock()
    mock_filings.__len__ = lambda s: 1
    mock_filings.__getitem__ = lambda s, i: MagicMock()
    mock_company.get_filings.return_value = mock_filings
    mock_company.name = "Baupost Group"

    with patch.object(client, "Company", return_value=mock_company), \
         patch.object(client, "ThirteenF", return_value=mock_tf):
        fund_info, holdings = client.get_holdings("67890")

    assert fund_info.total_value == 5_278_000_000
    assert holdings[0].value == 2_000_000_000


def test_compare_two_filings_no_multiplier():
    """_compare_two_filings must pass through values without multiplying."""
    from filings import client
    import pandas as pd

    current_df = pd.DataFrame({
        "Issuer": ["APPLE INC"],
        "Cusip": ["037833100"],
        "Value": [5_000_000_000],  # $5B actual
        "SharesPrnAmount": [100],
    })
    previous_df = pd.DataFrame({
        "Issuer": ["APPLE INC"],
        "Cusip": ["037833100"],
        "Value": [3_000_000_000],  # $3B actual
        "SharesPrnAmount": [80],
    })

    changes = client._compare_two_filings(current_df, previous_df)
    assert len(changes) == 1
    assert changes[0].current_value == 5_000_000_000
    assert changes[0].previous_value == 3_000_000_000


# ═══════════════════════════════════════════════════════════════════════
# Validation function tests
# ═══════════════════════════════════════════════════════════════════════


def test_validate_flags_low_value_many_holdings(caplog):
    """_validate_fund_values warns when value is suspiciously low."""
    from filings.client import _validate_fund_values

    with caplog.at_level(logging.WARNING):
        _validate_fund_values("12345", "Suspicious Fund", 5_000_000, 25)

    assert "VALIDATION" in caplog.text


def test_validate_flags_low_avg_per_holding(caplog):
    """_validate_fund_values warns when avg value per holding is too low."""
    from filings.client import _validate_fund_values

    with caplog.at_level(logging.WARNING):
        # 10 holdings with $1M total = $100K avg — below $500K threshold
        _validate_fund_values("12345", "Low Avg Fund", 1_000_000, 10)

    assert "VALIDATION" in caplog.text
    assert "unusually low" in caplog.text


def test_validate_passes_normal_fund(caplog):
    """_validate_fund_values does NOT warn for normal values."""
    from filings.client import _validate_fund_values

    with caplog.at_level(logging.WARNING):
        # $6.5B with 40 holdings = $162M avg — totally normal
        _validate_fund_values("12345", "Normal Fund", 6_500_000_000, 40)

    assert "VALIDATION" not in caplog.text


def test_validate_handles_zero_holdings(caplog):
    """_validate_fund_values does not crash on zero holdings."""
    from filings.client import _validate_fund_values

    with caplog.at_level(logging.WARNING):
        _validate_fund_values("12345", "Empty Fund", 0, 0)

    assert "VALIDATION" not in caplog.text


# ═══════════════════════════════════════════════════════════════════════
# Percentage calculation tests
# ═══════════════════════════════════════════════════════════════════════


def test_pct_of_portfolio_correct():
    """Portfolio percentage calculation should be correct with raw values."""
    from filings import client
    import pandas as pd
    from decimal import Decimal

    mock_tf = MagicMock()
    mock_tf.total_value = Decimal("10000000000")  # $10B actual
    mock_tf.management_company_name = "Test Fund"
    mock_tf.report_period = "2025-03-31"
    mock_tf.filing_date = "2025-05-15"

    holdings_df = pd.DataFrame({
        "Issuer": ["AAPL", "MSFT"],
        "Class": ["COM", "COM"],
        "Cusip": ["037833100", "594918104"],
        "Value": [7_000_000_000, 3_000_000_000],  # 70% and 30%
        "SharesPrnAmount": [100, 50],
        "Type": ["SH", "SH"],
        "Ticker": ["AAPL", "MSFT"],
    })
    mock_tf.holdings = holdings_df

    mock_company = MagicMock()
    mock_filings = MagicMock()
    mock_filings.__len__ = lambda s: 1
    mock_filings.__getitem__ = lambda s, i: MagicMock()
    mock_company.get_filings.return_value = mock_filings

    with patch.object(client, "Company", return_value=mock_company), \
         patch.object(client, "ThirteenF", return_value=mock_tf):
        result = client.get_fund_summary("12345")

    pcts = [h["pct"] for h in result["all_holdings"]]
    assert pcts == [70.0, 30.0]
