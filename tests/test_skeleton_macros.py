"""Tests for the reusable skeleton loading-placeholder Jinja2 macros.

Each macro in ``templates/macros/skeleton.html`` is rendered in isolation using
a minimal Jinja2 environment so the tests stay fast and dependency-free.
"""

from pathlib import Path

import pytest
from jinja2 import Environment, FileSystemLoader

TEMPLATES_DIR = Path(__file__).resolve().parent.parent / "src" / "filings" / "templates"


@pytest.fixture
def env():
    """Jinja2 environment pointing at the project templates directory."""
    return Environment(loader=FileSystemLoader(str(TEMPLATES_DIR)))


@pytest.fixture
def macros(env):
    """Import the skeleton macros module so tests can call individual macros."""
    tpl = env.from_string(
        '{% from "macros/skeleton.html" import '
        "skeleton_rows, skeleton_chart, skeleton_cards, skeleton_fund_row %}"
        "{{ caller_placeholder }}"
    )
    # Return a helper that renders a macro call expression.
    def _render(expression: str) -> str:
        t = env.from_string(
            '{% from "macros/skeleton.html" import '
            "skeleton_rows, skeleton_chart, skeleton_cards, skeleton_fund_row %}"
            + expression
        )
        return t.render()

    return _render


# ── skeleton_rows ────────────────────────────────────────────────────


class TestSkeletonRows:
    def test_default_produces_six_rows(self, macros):
        html = macros("{{ skeleton_rows() }}")
        assert html.count("pp-skeleton-row") == 6

    def test_custom_count(self, macros):
        html = macros("{{ skeleton_rows(3) }}")
        assert html.count("pp-skeleton-row") == 3

    def test_one_row(self, macros):
        html = macros("{{ skeleton_rows(1) }}")
        assert html.count("pp-skeleton-row") == 1

    def test_zero_rows(self, macros):
        html = macros("{{ skeleton_rows(0) }}")
        assert "pp-skeleton-row" not in html
        # Container div should still be present
        assert "pp-skeleton-table" in html

    def test_wrapping_div_present(self, macros):
        html = macros("{{ skeleton_rows(2) }}")
        assert html.startswith("<div")
        assert "pp-skeleton-table" in html

    def test_each_row_has_skeleton_class(self, macros):
        html = macros("{{ skeleton_rows(4) }}")
        # Every row div should have both pp-skeleton AND pp-skeleton-row
        assert html.count("pp-skeleton pp-skeleton-row") == 4

    def test_large_count(self, macros):
        html = macros("{{ skeleton_rows(50) }}")
        assert html.count("pp-skeleton-row") == 50


# ── skeleton_chart ───────────────────────────────────────────────────


class TestSkeletonChart:
    def test_default_height(self, macros):
        html = macros("{{ skeleton_chart() }}")
        assert "height:200px;" in html

    def test_custom_height(self, macros):
        html = macros("{{ skeleton_chart('350px') }}")
        assert "height:350px;" in html

    def test_css_classes(self, macros):
        html = macros("{{ skeleton_chart() }}")
        assert "pp-skeleton" in html
        assert "pp-skeleton-chart" in html

    def test_is_single_div(self, macros):
        html = macros("{{ skeleton_chart() }}")
        assert html.count("<div") == 1
        assert html.count("</div>") == 1


# ── skeleton_cards ───────────────────────────────────────────────────


class TestSkeletonCards:
    def test_default_produces_four_cards(self, macros):
        html = macros("{{ skeleton_cards() }}")
        assert html.count("pp-skeleton-card") == 4

    def test_custom_count(self, macros):
        html = macros("{{ skeleton_cards(2) }}")
        assert html.count("pp-skeleton-card") == 2

    def test_grid_layout(self, macros):
        html = macros("{{ skeleton_cards(4) }}")
        assert "display:grid" in html
        assert "grid-template-columns:1fr 1fr" in html

    def test_zero_cards(self, macros):
        html = macros("{{ skeleton_cards(0) }}")
        assert "pp-skeleton-card" not in html

    def test_each_card_has_skeleton_class(self, macros):
        html = macros("{{ skeleton_cards(3) }}")
        assert html.count("pp-skeleton pp-skeleton-card") == 3


# ── skeleton_fund_row ────────────────────────────────────────────────


class TestSkeletonFundRow:
    def test_has_flex_layout(self, macros):
        html = macros("{{ skeleton_fund_row() }}")
        assert "display:flex" in html

    def test_four_placeholder_columns(self, macros):
        html = macros("{{ skeleton_fund_row() }}")
        # The fund row has 4 inner skeleton divs (60px, 40px, 80px, 100px)
        # Outer div + 4 inner divs = 5 total
        inner_divs = html.count("pp-skeleton")
        assert inner_divs == 4

    def test_column_widths(self, macros):
        html = macros("{{ skeleton_fund_row() }}")
        assert "width:60px" in html
        assert "width:40px" in html
        assert "width:80px" in html
        assert "width:100px" in html

    def test_consistent_height(self, macros):
        html = macros("{{ skeleton_fund_row() }}")
        assert html.count("height:14px") == 4


# ── Cross-cutting concerns ──────────────────────────────────────────


class TestCrossCutting:
    def test_no_whitespace_leak_rows(self, macros):
        """Macros use -%} and {%- to strip leading/trailing whitespace."""
        html = macros("{{ skeleton_rows(2) }}")
        assert not html.startswith("\n")
        assert not html.endswith("\n")

    def test_no_whitespace_leak_chart(self, macros):
        html = macros("{{ skeleton_chart() }}")
        assert not html.startswith("\n")
        assert not html.endswith("\n")

    def test_no_whitespace_leak_cards(self, macros):
        html = macros("{{ skeleton_cards(2) }}")
        assert not html.startswith("\n")
        assert not html.endswith("\n")

    def test_no_whitespace_leak_fund_row(self, macros):
        html = macros("{{ skeleton_fund_row() }}")
        assert not html.startswith("\n")
        assert not html.endswith("\n")

    def test_macros_importable(self, env):
        """The import statement itself should not raise."""
        tpl = env.from_string(
            '{% from "macros/skeleton.html" import skeleton_rows %}'
        )
        tpl.render()

    def test_all_macros_importable_together(self, env):
        """All four macros can be imported in a single statement."""
        tpl = env.from_string(
            '{% from "macros/skeleton.html" import '
            "skeleton_rows, skeleton_chart, skeleton_cards, skeleton_fund_row %}"
        )
        tpl.render()
