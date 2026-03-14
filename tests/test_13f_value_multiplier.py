"""Tests for the SEC 13F x1000 value multiplier fix.

SEC 13F filings report all dollar values in *thousands*. The edgartools
library returns these raw values without conversion. Our code must apply
a x1000 multiplier at every ingestion point.

These tests verify:
  - The multiplier constant exists and equals 1000
  - get_fund_summary() applies the multiplier to total_value and all holdings
  - get_holdings() applies the multiplier to FundInfo and Holding objects
  - _compare_two_filings() applies the multiplier to current/previous values
  - _validate_fund_values() flags anomalously low portfolio values
"""

from __future__ import annotations

import logging
import sys
import types
from unittest.mock import MagicMock, patch, PropertyMock
from dataclasses import dataclass

import pytest

# ── Ensure heavy deps are importable ─────────────────────────────────
# edgartools is installed; stub only the deps that aren't available
for _mod in ("yfinance", "supabase", "postgrest", "gotrue",
             "storage3", "realtime", "supafunc"):
    if _mod not in sys.modules:
        sys.modules[_mod] = types.ModuleType(_mod)


# ═══════════════════════════════════════════════════════════════════════
# Multiplier constant tests
# ═══════════════════════════════════════════════════════════════════════


def test_multiplier_constant_exists():
    """The _SEC_13F_VALUE_MULTIPLIER constant must be 1000."""
    from filings.client import _SEC_13F_VALUE_MULTIPLIER
    assert _SEC_13F_VALUE_MULTIPLIER == 1000


def test_multiplier_is_used_in_get_fund_summary():
    """get_fund_summary must multiply total_value by 1000."""
    from filings import client
    import pandas as pd
    from decimal import Decimal

    # Mock a ThirteenF object
    mock_tf = MagicMock()
    mock_tf.total_value = Decimal("6568")  # AKO Capital example: 6568 thousands
    mock_tf.management_company_name = "AKO Capital LLP"
    mock_tf.report_period = "2025-03-31"
    mock_tf.filing_date = "2025-05-15"

    # Mock holdings DataFrame
    holdings_df = pd.DataFrame({
        "Issuer": ["APPLE INC", "MICROSOFT CORP"],
        "Class": ["COM", "COM"],
        "Cusip": ["037833100", "594918104"],
        "Value": [3000, 3568],  # in thousands
        "SharesPrnAmount": [100, 200],
        "Type": ["SH", "SH"],
        "Ticker": ["AAPL", "MSFT"],
    })
    mock_tf.holdings = holdings_df

    # Mock the filings list
    mock_company = MagicMock()
    mock_filings = MagicMock()
    mock_filings.__len__ = lambda s: 1
    mock_filings.__getitem__ = lambda s, i: MagicMock()
    mock_company.get_filings.return_value = mock_filings

    with patch.object(client, "Company", return_value=mock_company), \
         patch.object(client, "ThirteenF", return_value=mock_tf):
        result = client.get_fund_summary("12345")

    # total_value should be 6568 * 1000 = 6,568,000
    assert result["total_value"] == 6_568_000

    # Individual holding values should also be multiplied
    for h in result["all_holdings"]:
        assert h["value"] >= 3_000_000  # 3000 * 1000

    for h in result["top_holdings"]:
        assert h["value"] >= 3_000_000


def test_multiplier_is_used_in_get_holdings():
    """get_holdings must multiply FundInfo.total_value and Holding.value."""
    from filings import client
    import pandas as pd
    from decimal import Decimal

    mock_tf = MagicMock()
    mock_tf.total_value = Decimal("5278")  # Baupost example
    mock_tf.management_company_name = "Baupost Group"
    mock_tf.report_period = "2025-03-31"
    mock_tf.filing_date = "2025-05-15"

    holdings_df = pd.DataFrame({
        "Issuer": ["LIBERTY BROADBAND"],
        "Class": ["COM"],
        "Cusip": ["530307305"],
        "Value": [2000],  # in thousands
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

    assert fund_info.total_value == 5_278_000  # 5278 * 1000
    assert holdings[0].value == 2_000_000  # 2000 * 1000


def test_compare_two_filings_applies_multiplier():
    """_compare_two_filings must multiply current/previous values."""
    from filings import client
    import pandas as pd

    current_df = pd.DataFrame({
        "Issuer": ["APPLE INC"],
        "Cusip": ["037833100"],
        "Value": [5000],  # 5000 thousands = $5M
        "SharesPrnAmount": [100],
    })
    previous_df = pd.DataFrame({
        "Issuer": ["APPLE INC"],
        "Cusip": ["037833100"],
        "Value": [3000],  # 3000 thousands = $3M
        "SharesPrnAmount": [80],
    })

    changes = client._compare_two_filings(current_df, previous_df)
    assert len(changes) == 1
    assert changes[0].current_value == 5_000_000  # 5000 * 1000
    assert changes[0].previous_value == 3_000_000  # 3000 * 1000


# ═══════════════════════════════════════════════════════════════════════
# Validation function tests
# ═══════════════════════════════════════════════════════════════════════


def test_validate_flags_low_value_many_holdings(caplog):
    """_validate_fund_values warns when value is suspiciously low."""
    from filings.client import _validate_fund_values

    with caplog.at_level(logging.WARNING):
        _validate_fund_values("12345", "Suspicious Fund", 5_000_000, 25)

    assert "VALIDATION" in caplog.text
    assert "likely missing x1000" in caplog.text


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

    # Should not crash, should not warn
    assert "VALIDATION" not in caplog.text


# ═══════════════════════════════════════════════════════════════════════
# Percentage calculation tests (ensure pct is still correct after x1000)
# ═══════════════════════════════════════════════════════════════════════


def test_pct_of_portfolio_still_correct():
    """Portfolio percentage calculation should be unaffected by the x1000
    multiplier since both numerator and denominator are multiplied."""
    from filings import client
    import pandas as pd
    from decimal import Decimal

    mock_tf = MagicMock()
    mock_tf.total_value = Decimal("10000")  # 10000 thousands = $10M
    mock_tf.management_company_name = "Test Fund"
    mock_tf.report_period = "2025-03-31"
    mock_tf.filing_date = "2025-05-15"

    holdings_df = pd.DataFrame({
        "Issuer": ["AAPL", "MSFT"],
        "Class": ["COM", "COM"],
        "Cusip": ["037833100", "594918104"],
        "Value": [7000, 3000],  # 70% and 30%
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

    # Percentages should still be 70% and 30%
    pcts = [h["pct"] for h in result["all_holdings"]]
    assert pcts == [70.0, 30.0]
