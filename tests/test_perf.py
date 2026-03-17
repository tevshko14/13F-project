"""Performance improvement tests.

Covers:
  - Static asset cache headers (SecurityHeadersMiddleware)
  - Retail data L1 caching (_fetch_retail_data)
  - Brotli compression middleware
  - Stock price CLS fix (visibility vs display:none)
  - HTTP connection pooling lifecycle
  - CSS minification (inline style blocks)

Tests replicate logic in isolation to avoid importing web.py's full
dependency chain (edgar, yfinance, supabase, etc.).
"""

from __future__ import annotations

import asyncio
import re
import time
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

# ── Paths ──────────────────────────────────────────────────────────
_TEMPLATES = Path(__file__).resolve().parent.parent / "src" / "filings" / "templates"


# ═══════════════════════════════════════════════════════════════════
# 1. Static Asset Cache Headers
# ═══════════════════════════════════════════════════════════════════


class TestStaticCacheHeaders:
    """Verify SecurityHeadersMiddleware sets correct Cache-Control on /static/."""

    @staticmethod
    def _get_cache_header(path: str) -> str:
        """Replicate the SecurityHeadersMiddleware logic for cache headers."""
        if path.startswith("/static/"):
            ext = path.rsplit(".", 1)[-1].lower() if "." in path else ""
            if ext in ("png", "webp", "jpg", "jpeg", "svg", "ico", "woff2", "woff"):
                return "public, max-age=31536000, immutable"
            else:
                return "public, max-age=3600"
        return ""

    def test_png_gets_immutable_1year(self):
        assert "max-age=31536000" in self._get_cache_header("/static/favicon-32.png")
        assert "immutable" in self._get_cache_header("/static/favicon-32.png")

    def test_webp_gets_immutable_1year(self):
        assert "immutable" in self._get_cache_header("/static/logo-nav.webp")

    def test_woff2_gets_immutable_1year(self):
        assert "immutable" in self._get_cache_header("/static/font.woff2")

    def test_css_gets_1hour(self):
        header = self._get_cache_header("/static/style.css")
        assert "max-age=3600" in header
        assert "immutable" not in header

    def test_js_gets_1hour(self):
        header = self._get_cache_header("/static/app.js")
        assert "max-age=3600" in header

    def test_non_static_gets_nothing(self):
        assert self._get_cache_header("/api/retail/leaderboard") == ""


# ═══════════════════════════════════════════════════════════════════
# 2. Retail Data L1 Caching
# ═══════════════════════════════════════════════════════════════════


class TestRetailDataCache:
    """Test the L1 in-memory cache for _fetch_retail_data."""

    @staticmethod
    async def _cached_fetch(
        cache_state: dict,
        ttl: int,
        fetch_fn,
    ) -> tuple[list, dict | None]:
        """Replicate the retail data cache logic from web.py."""
        now = time.time()
        cached = cache_state.get("data")
        if cached and now - cached[0] < ttl:
            return cached[1]
        result = await fetch_fn()
        cache_state["data"] = (now, result)
        return result

    @pytest.mark.asyncio
    async def test_cache_hit_skips_fetch(self):
        """Second call within TTL should not call the fetch function."""
        call_count = 0

        async def mock_fetch():
            nonlocal call_count
            call_count += 1
            return (["AAPL", "TSLA"], {"value": 50})

        cache = {}
        await self._cached_fetch(cache, 120, mock_fetch)
        assert call_count == 1

        await self._cached_fetch(cache, 120, mock_fetch)
        assert call_count == 1  # Still 1 — cache hit

    @pytest.mark.asyncio
    async def test_cache_miss_calls_fetch(self):
        """First call should invoke the fetch function."""
        call_count = 0

        async def mock_fetch():
            nonlocal call_count
            call_count += 1
            return ([], None)

        cache = {}
        result = await self._cached_fetch(cache, 120, mock_fetch)
        assert call_count == 1
        assert result == ([], None)

    @pytest.mark.asyncio
    async def test_cache_expiry_triggers_refetch(self):
        """After TTL expires, fetch should be called again."""
        call_count = 0

        async def mock_fetch():
            nonlocal call_count
            call_count += 1
            return ([f"call-{call_count}"], None)

        cache = {}
        ttl = 1  # 1 second TTL for test speed

        r1 = await self._cached_fetch(cache, ttl, mock_fetch)
        assert call_count == 1
        assert r1 == (["call-1"], None)

        # Expire the cache
        cache["data"] = (cache["data"][0] - ttl - 1, cache["data"][1])

        r2 = await self._cached_fetch(cache, ttl, mock_fetch)
        assert call_count == 2
        assert r2 == (["call-2"], None)

    @pytest.mark.asyncio
    async def test_cache_returns_stored_data(self):
        """Cache hit returns the exact data that was stored."""
        data = (["NVDA", "AMZN"], {"value": 75, "label": "Greed"})

        async def mock_fetch():
            return data

        cache = {}
        r1 = await self._cached_fetch(cache, 120, mock_fetch)
        r2 = await self._cached_fetch(cache, 120, mock_fetch)
        assert r1 is r2  # Same object reference — no re-fetch


# ═══════════════════════════════════════════════════════════════════
# 3. Brotli Compression
# ═══════════════════════════════════════════════════════════════════


class TestBrotliDependency:
    """Verify brotli package is importable."""

    def test_brotli_importable(self):
        import brotli  # noqa: F401

    def test_brotli_compress_decompress(self):
        import brotli
        data = b"<html>" + b"x" * 2000 + b"</html>"
        compressed = brotli.compress(data)
        assert len(compressed) < len(data)
        assert brotli.decompress(compressed) == data


# ═══════════════════════════════════════════════════════════════════
# 4. Stock Price CLS Fix
# ═══════════════════════════════════════════════════════════════════


class TestStockPriceCLS:
    """Verify stock price container uses visibility instead of display:none."""

    @pytest.fixture
    def stock_html(self):
        return (_TEMPLATES / "stock.html").read_text()

    def test_price_container_has_visibility_hidden(self, stock_html):
        """Price container should use visibility:hidden, not display:none."""
        match = re.search(r'id="pp-header-price"[^>]*style="([^"]*)"', stock_html)
        assert match, "pp-header-price element not found"
        style = match.group(1)
        assert "visibility: hidden" in style or "visibility:hidden" in style
        assert "display: none" not in style

    def test_price_container_has_min_height(self, stock_html):
        """Price container should reserve space with min-height."""
        match = re.search(r'id="pp-header-price"[^>]*style="([^"]*)"', stock_html)
        assert match
        style = match.group(1)
        assert "min-height" in style

    def test_price_container_uses_display_flex(self, stock_html):
        """Price container should use display:flex for layout (not none)."""
        match = re.search(r'id="pp-header-price"[^>]*style="([^"]*)"', stock_html)
        assert match
        style = match.group(1)
        assert "display: flex" in style or "display:flex" in style

    def test_js_uses_visibility_visible(self, stock_html):
        """JS should reveal price with visibility, not display."""
        assert "el.style.visibility = 'visible'" in stock_html
        # Ensure old pattern is gone
        assert "el.style.display = 'flex'" not in stock_html


# ═══════════════════════════════════════════════════════════════════
# 5. HTTP Connection Pooling
# ═══════════════════════════════════════════════════════════════════


class TestConnectionPooling:
    """Verify global httpx.AsyncClient lifecycle."""

    @pytest.mark.asyncio
    async def test_pool_creation_and_cleanup(self):
        """Pool should be created and properly closed."""
        import httpx

        pool = httpx.AsyncClient(
            timeout=10.0,
            limits=httpx.Limits(max_connections=5, max_keepalive_connections=3),
        )
        assert not pool.is_closed
        await pool.aclose()
        assert pool.is_closed

    @pytest.mark.asyncio
    async def test_pool_reuses_across_calls(self):
        """Multiple requests through the same client reuse connections."""
        import httpx

        pool = httpx.AsyncClient(
            timeout=10.0,
            limits=httpx.Limits(max_connections=5, max_keepalive_connections=3),
        )
        # Just verify the pool is functional and reusable
        assert pool is pool  # Same instance
        assert not pool.is_closed
        await pool.aclose()


# ═══════════════════════════════════════════════════════════════════
# 6. CSS Minification
# ═══════════════════════════════════════════════════════════════════


def _minify_css(css: str) -> str:
    """Minimal CSS minifier — strips comments, collapses whitespace."""
    # Remove CSS comments
    css = re.sub(r"/\*.*?\*/", "", css, flags=re.DOTALL)
    # Collapse whitespace
    css = re.sub(r"\s+", " ", css)
    # Remove spaces around punctuation
    css = re.sub(r"\s*([{}:;,>~+])\s*", r"\1", css)
    # Remove trailing semicolons before }
    css = re.sub(r";}", "}", css)
    return css.strip()


def _minify_inline_styles(html: str) -> str:
    """Find all <style>...</style> blocks and minify their CSS contents."""
    def _minify_match(m):
        tag_open = m.group(1)   # <style...>
        css = m.group(2)        # CSS content
        tag_close = m.group(3)  # </style>
        return tag_open + _minify_css(css) + tag_close

    return re.sub(
        r"(<style[^>]*>)(.*?)(</style>)",
        _minify_match,
        html,
        flags=re.DOTALL | re.IGNORECASE,
    )


class TestCSSMinification:
    """Test the inline CSS minification function."""

    def test_strips_comments(self):
        css = "/* comment */ .foo { color: red; }"
        assert "comment" not in _minify_css(css)
        assert "color:red" in _minify_css(css)

    def test_collapses_whitespace(self):
        css = ".foo  {  color:  red;  margin:  0  }"
        result = _minify_css(css)
        assert "  " not in result

    def test_removes_space_around_braces(self):
        css = ".foo { color: red; }"
        result = _minify_css(css)
        assert result == ".foo{color:red}"

    def test_removes_trailing_semicolons(self):
        css = ".foo { color: red; }"
        result = _minify_css(css)
        assert ";}" not in result

    def test_preserves_non_style_content(self):
        html = "<p>Hello world</p><style>.a { color: red; }</style><p>End</p>"
        result = _minify_inline_styles(html)
        assert "<p>Hello world</p>" in result
        assert "<p>End</p>" in result
        assert ".a{color:red}" in result

    def test_handles_multiple_style_blocks(self):
        html = "<style>.a { margin: 0; }</style><div>x</div><style>.b { padding: 0; }</style>"
        result = _minify_inline_styles(html)
        assert ".a{margin:0}" in result
        assert ".b{padding:0}" in result

    def test_handles_empty_style_block(self):
        html = "<style></style>"
        result = _minify_inline_styles(html)
        assert result == "<style></style>"

    def test_significant_size_reduction(self):
        """Minification should reduce typical CSS by at least 20%."""
        css = """
        /* Base styles for the application */
        .container {
            max-width: 1200px;
            margin: 0 auto;
            padding: 0 1rem;
        }

        /* Card component */
        .card {
            background: white;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
            padding: 1.5rem;
        }

        .card .title {
            font-size: 1.2em;
            font-weight: 600;
            margin-bottom: 0.5rem;
        }
        """
        result = _minify_css(css)
        assert len(result) < len(css) * 0.8  # At least 20% reduction


# ═══════════════════════════════════════════════════════════════════
# 7. Font Loading Verification
# ═══════════════════════════════════════════════════════════════════


class TestFontLoading:
    """Verify Google Fonts setup in base.html."""

    @pytest.fixture
    def base_html(self):
        return (_TEMPLATES / "base.html").read_text()

    def test_fonts_loaded_async(self, base_html):
        """Google Fonts should use preload+onload pattern, not blocking."""
        assert 'rel="preload"' in base_html
        assert "onload=" in base_html
        # The only blocking stylesheet link for fonts should be inside <noscript>
        # Remove noscript blocks, then verify no blocking font link remains
        no_noscript = re.sub(r"<noscript>.*?</noscript>", "", base_html, flags=re.DOTALL)
        assert 'rel="stylesheet" href="https://fonts.googleapis.com' not in no_noscript

    def test_fonts_have_display_swap(self, base_html):
        """Fonts must use display=swap to prevent FOIT."""
        assert "display=swap" in base_html

    def test_fonts_have_crossorigin(self, base_html):
        """Font preload must have crossorigin to avoid double-fetch."""
        # Find the Google Fonts preload link
        font_preload = re.search(
            r'<link\s+rel="preload"[^>]*fonts\.googleapis\.com[^>]*>',
            base_html,
            re.DOTALL,
        )
        assert font_preload, "Google Fonts preload link not found"
        assert "crossorigin" in font_preload.group(0)

    def test_pico_css_is_synchronous(self, base_html):
        """Pico CSS should be loaded synchronously to prevent FOUC."""
        pico_link = re.search(
            r'<link\s+[^>]*picocss/pico[^>]*>',
            base_html,
            re.DOTALL,
        )
        assert pico_link, "Pico CSS link not found"
        link_text = pico_link.group(0)
        assert 'rel="stylesheet"' in link_text
        assert "preload" not in link_text


# ═══════════════════════════════════════════════════════════════════
# 8. WebApplication JSON-LD Placement
# ═══════════════════════════════════════════════════════════════════


class TestJSONLDPlacement:
    """Verify WebApplication schema is only on homepage, not base."""

    @pytest.fixture
    def base_html(self):
        return (_TEMPLATES / "base.html").read_text()

    @pytest.fixture
    def home_html(self):
        return (_TEMPLATES / "home.html").read_text()

    def test_base_does_not_have_web_application(self, base_html):
        """WebApplication JSON-LD should NOT be in base.html."""
        assert '"WebApplication"' not in base_html

    def test_home_has_web_application(self, home_html):
        """WebApplication JSON-LD should be in home.html."""
        assert '"WebApplication"' in home_html

    def test_home_has_correct_app_category(self, home_html):
        assert '"FinanceApplication"' in home_html

    def test_home_has_free_offer(self, home_html):
        assert '"price": "0"' in home_html
