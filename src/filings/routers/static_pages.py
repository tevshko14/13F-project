"""Static pages router — zero-logic routes that just render a template
or return a fixed string.  Moved out of web.py during audit-sprint-4.

Routes:
  GET /privacy    — privacy policy (template)
  GET /faq        — frequently-asked questions (template)
  GET /robots.txt — crawler directives
  GET /llms.txt   — AI-readable site summary

Note: ``/sitemap.xml`` is NOT here — it depends on SUPERINVESTORS,
``_fund_cache()``, and congress member lookups, so it's logically part
of the main application state and stays in web.py for now.
``/support`` and ``/support/thank-you`` also stay in web.py because
they need Stripe config and the shared ``_support_page_context`` helper.
"""

from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, PlainTextResponse

from filings.app_state import templates

router = APIRouter()


@router.get("/privacy", response_class=HTMLResponse)
async def privacy_page(request: Request):
    return templates.TemplateResponse("privacy.html", {"request": request})


@router.get("/faq", response_class=HTMLResponse)
async def faq_page(request: Request):
    return templates.TemplateResponse("faq.html", {"request": request})


_ROBOTS_TXT = (
    "# ── Standard crawlers ─────────────────────────────────\n"
    "User-agent: *\n"
    "Allow: /\n"
    "Disallow: /api/\n"
    "\n"
    "# ── AI crawlers — explicitly welcomed ────────────────\n"
    "# Tier 1: Primary AI search & assistant crawlers\n"
    "User-agent: GPTBot\n"
    "Allow: /\n"
    "Disallow: /api/\n"
    "\n"
    "User-agent: OAI-SearchBot\n"
    "Allow: /\n"
    "Disallow: /api/\n"
    "\n"
    "User-agent: ChatGPT-User\n"
    "Allow: /\n"
    "Disallow: /api/\n"
    "\n"
    "User-agent: ClaudeBot\n"
    "Allow: /\n"
    "Disallow: /api/\n"
    "\n"
    "User-agent: PerplexityBot\n"
    "Allow: /\n"
    "Disallow: /api/\n"
    "\n"
    "User-agent: anthropic-ai\n"
    "Allow: /\n"
    "Disallow: /api/\n"
    "\n"
    "# Tier 2: Platform AI training & indexing crawlers\n"
    "User-agent: Google-Extended\n"
    "Allow: /\n"
    "Disallow: /api/\n"
    "\n"
    "User-agent: GoogleOther\n"
    "Allow: /\n"
    "Disallow: /api/\n"
    "\n"
    "User-agent: Applebot-Extended\n"
    "Allow: /\n"
    "Disallow: /api/\n"
    "\n"
    "User-agent: Amazonbot\n"
    "Allow: /\n"
    "Disallow: /api/\n"
    "\n"
    "User-agent: cohere-ai\n"
    "Allow: /\n"
    "Disallow: /api/\n"
    "\n"
    "# Tier 3: Social & discovery\n"
    "User-agent: FacebookBot\n"
    "Allow: /\n"
    "Disallow: /api/\n"
    "\n"
    "User-agent: Bytespider\n"
    "Allow: /\n"
    "Disallow: /api/\n"
    "\n"
    "Sitemap: https://paperpanda.io/sitemap.xml\n"
    "\n"
    "# ── AI-readable site summary ─────────────────────────\n"
    "# See https://paperpanda.io/llms.txt\n"
)


@router.get("/robots.txt")
async def robots_txt():
    return PlainTextResponse(_ROBOTS_TXT, media_type="text/plain")


_LLMS_TXT = (
    "# PaperPanda\n"
    "\n"
    "> Free, open-source investment research dashboard tracking superinvestor 13F filings, insider trades, congressional stock activity, and unusual options flow.\n"
    "\n"
    "## Main Pages\n"
    "- [Home](https://paperpanda.io/): Market dashboard with S&P 500 heatmap, market news, and retail sentiment overview\n"
    "- [Funds](https://paperpanda.io/funds): 13F portfolio intelligence across 85 tracked superinvestors with consensus and momentum charts\n"
    "- [Insider Trading](https://paperpanda.io/insiders): Real-time SEC Form 4 filings showing insider purchases and sales across public companies\n"
    "- [Congress Trading](https://paperpanda.io/congress): STOCK Act disclosures tracking what 201 House and Senate members are buying and selling\n"
    "- [Retail Sentiment](https://paperpanda.io/retail): Reddit sentiment, trending tickers, market fear and greed index, and finance YouTuber schedules\n"
    "- [Options Screener](https://paperpanda.io/options): Advanced unusual options scanner with premium filtering, OI delta tracking, moneyness scoring, urgency weighting, cluster detection, and convergence engine\n"
    "- [Alternative Signals](https://paperpanda.io/alternative-signals): Short interest, analyst ratings, earnings calendar, and economic events from FRED\n"
    "- [Macro Dashboard](https://paperpanda.io/macro): Federal Reserve economic indicators, GDP, CPI, unemployment, and interest rates from FRED\n"
    "- [FAQ](https://paperpanda.io/faq): Frequently asked questions about PaperPanda, 13F filings, insider trading, congressional trading, and more\n"
    "\n"
    "## Data & Features\n"
    "- [Stock Lookup](https://paperpanda.io/stock/AAPL): Per-ticker pages with superinvestor ownership, congressional trades, analyst forecasts, and sentiment\n"
    "- [Grand Portfolio](https://paperpanda.io/funds): Aggregated superinvestor consensus — most-held and most-added stocks across all tracked funds\n"
    "- [Options Clusters](https://paperpanda.io/api/options/clusters): Grouped unusual activity showing tickers with multiple flagged contracts, direction, and strength\n"
    "\n"
    "## Options Scanner Features\n"
    "- Premium floor filter: only surfaces contracts with $100K+ estimated premium to eliminate noise\n"
    "- OI delta tracking: compares today's open interest to previous day, flags new positioning (50%+ OI growth)\n"
    "- Near-expiry urgency: weights 0-DTE and weekly contracts higher (up to 2x boost)\n"
    "- Moneyness scoring: labels contracts as Deep ITM, ITM, ATM, OTM, or Deep OTM with conviction multipliers\n"
    "- Cluster detection: groups 2+ unusual contracts on the same ticker, labels strong clusters (3+ contracts)\n"
    "- Greeks: delta, gamma, theta, vega displayed when available from Tradier options data\n"
    "- Convergence engine: cross-references options with insider buys, congress trades, short interest, and 13F adds\n"
    "\n"
    "## Key Facts\n"
    "- Tracks 85 superinvestor funds via SEC EDGAR 13F filings, updated quarterly\n"
    "- Covers 201 politicians (41 senators, 160 representatives) from STOCK Act disclosures\n"
    "- Monitors over 1,000 stocks with real-time insider trading from SEC Form 4\n"
    "- Unusual options scanner covers S&P 500 plus top superinvestor holdings\n"
    "- Convergence engine cross-references 5 signal types: options, insider buys, congress trades, short interest, and 13F adds\n"
    "- Data sourced from SEC EDGAR (sec.gov), Tiingo, Tradier, FRED (fred.stlouisfed.org), and Reddit\n"
    "- Free and open-source project\n"
    "\n"
    "## Macro Dashboard\n"
    "- Earnings scorecard: EPS and revenue beat rates for S&P 500 and NASDAQ 100, with stock price reactions\n"
    "- Market performance: advance/decline ratios, new highs vs. lows, 50-day MA participation\n"
    "- Economic indicators from FRED: GDP, CPI, unemployment, consumer sentiment, industrial production, retail sales\n"
    "- Treasury yield curves: 2s10s spread tracking, historical curve comparison\n"
    "- CBOE volatility: VIX term structure, SKEW index, put/call ratios\n"
    "- FX rates: EUR, GBP, JPY, CNY with 30-day sparkline charts (ECB reference rates via Frankfurter)\n"
    "\n"
    "## Stock Pages (per-ticker)\n"
    "- 8 tabs: Overview, Financials, Holdings, Insider Trades, Congressional Trades, Analyst Ratings, Sentiment, Options\n"
    "- SEC XBRL financial statements (income, balance sheet, cash flow) with insight charts\n"
    "- Superinvestor ownership table with quarter-over-quarter changes\n"
    "- Interactive candlestick chart with 1M-5Y timeframes\n"
    "- Short interest tracking: % of float shorted, days to cover, trend\n"
    "- Google Trends search interest with 12-month sparklines\n"
    "\n"
    "## Data Sources\n"
    "- SEC EDGAR (sec.gov): 13F filings, Form 4 insider trades, XBRL financial statements\n"
    "- Capitol Trades: Congressional STOCK Act disclosures\n"
    "- Tiingo (tiingo.com): Real-time IEX stock quotes, EOD history\n"
    "- Tradier (tradier.com): Options chains with ORATS greeks\n"
    "- FRED (fred.stlouisfed.org): Federal Reserve macroeconomic data\n"
    "- Frankfurter (frankfurter.app): ECB foreign exchange reference rates\n"
    "- ApeWisdom: Reddit mention aggregation (r/wallstreetbets, r/stocks, r/investing)\n"
    "- FINRA: Short interest data\n"
    "\n"
    "## Contact\n"
    "- Website: https://paperpanda.io\n"
    "- GitHub: https://github.com/tevshko14/13F-project\n"
)


@router.get("/llms.txt")
async def llms_txt():
    """Machine-readable site overview for AI assistants and LLMs."""
    return PlainTextResponse(_LLMS_TXT, media_type="text/plain")
