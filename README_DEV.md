# 13F Filing Viewer - Developer Reference

> **This file is the source of truth for this project.**
> If context is ever drifting, re-read this file first before making changes.
> Last updated: 2026-03-20 (L2 stale-fallback caching on 22 endpoints, CSS minifier calc() fix)

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Architecture Map](#2-architecture-map)
3. [File Inventory](#3-file-inventory)
4. [Data Schema](#4-data-schema)
5. [Core Logic Definitions](#5-core-logic-definitions)
6. [Web Routes](#6-web-routes)
7. [Templates](#7-templates)
8. [CLI Interface](#8-cli-interface)
9. [Known Bugs & Gotchas](#9-known-bugs--gotchas)
10. [Pending Tasks & Future Work](#10-pending-tasks--future-work)
11. [Historical Bug Fixes](#11-historical-bug-fixes)

---

## 1. Project Overview

A tool for tracking SEC 13F institutional holdings filings from 85 hardcoded
"superinvestors" (Buffett, Ackman, Burry, Einhorn, etc.). Provides both a CLI and a
web dashboard. All data comes from SEC EDGAR (public, free, no API key needed).

### Tech Stack

| Layer         | Technology                          |
|---------------|-------------------------------------|
| Language      | Python 3.12+                        |
| Package mgr   | `uv` (entry points via pyproject.toml) |
| Web framework | FastAPI + Jinja2 + HTMX             |
| CSS           | Pico CSS v2 (classless, from CDN)   |
| CLI output    | Rich (tables, panels, colors)       |
| Charts        | Chart.js v4 (bar charts) + ECharts v5 (heatmap treemap) |
| Search        | Fuse.js v7 (client-side fuzzy search, CDN) |
| SEC data      | `edgartools` library (wraps EDGAR API) |
| Market data   | Tiingo IEX (real-time, primary) + `yfinance` (fallback) + NASDAQ Trader (~8K listings) + Wikipedia (sectors) |
| Options data  | Tradier (ORATS greeks, primary) + `yfinance` (fallback) |
| Analyst data  | `yfinance` (free) + `finnhub-python` (free tier, optional key) |
| Sentiment     | CNN Fear & Greed, Finnhub, ApeWisdom, Alpha Vantage |
| Vitals        | People Data Labs, Glassdoor (RapidAPI), Apple iTunes Search |
| Caching       | 3-tier: in-memory (L1) → Supabase Postgres (L2) → disk JSON (L3) |
| Hosting       | Railway (auto-deploy from main) at [paperpanda.io](https://paperpanda.io) |
| Email         | Resend (transactional email for watchlist digests) |
| Entry points  | `filings` (CLI), `filings-web` (web, port 8000), `filings-digest` (digest cron) |

### How to Run

```bash
# CLI
uv run filings search "Berkshire"
uv run filings holdings 1067983
uv run filings compare 1067983

# Web
uv run filings-web          # starts at http://localhost:8000
```

---

## 2. Architecture Map

```
                  ┌─────────────────────────────────┐
                  │        SEC EDGAR API             │
                  │  (edgartools: Company, ThirteenF)│
                  │  (EFTS full-text search fallback)│
                  └──────────────┬──────────────────┘
                                 │
                   Fetches 13F-HR filings (non-amendments)
                   Up to 9 filings per fund (8 quarter pairs)
                   Rate limited: 1 sec between funds
                                 │
                                 ▼
┌────────────────────────────────────────────────────────────┐
│                      client.py                             │
│  SEC Data Access Layer — the brain of the application      │
│                                                            │
│  LIVE API calls (only used on cache miss):                  │
│  • search_managers()       — find fund managers by name    │
│  • get_holdings()          — single fund's current 13F     │
│  • get_enriched_holdings() — holdings + activity badges    │
│  • compare_quarters()      — diff last 2 quarters         │
│  • get_fund_summary()      — full fund data for caching   │
│                                                            │
│  CACHE-FIRST helpers (zero API calls, built from cache):   │
│  • get_enriched_holdings_from_cache() — from cached dict   │
│  • get_compare_from_cache()  — quarter diff from cache     │
│                                                            │
│  CACHE-ONLY functions (zero API calls, read from cache):   │
│  • build_activity_feed()   — recent changes, all funds     │
│  • build_grand_portfolio() — aggregated cross-fund view    │
│  • build_stock_detail()    — who holds a specific stock    │
│  • build_stock_history()   — multi-quarter stock activity  │
│                                                            │
│  INTERNAL helpers:                                         │
│  • _compare_two_filings()  — CUSIP-based diff algorithm    │
│  • _report_period_to_quarter_label() — date→"Q3 2025"     │
│  • _safe_ticker()          — NaN-safe ticker extraction    │
│  • _search_edgar_efts()    — EFTS fallback search          │
└────────────────────┬───────────────────────────────────────┘
                     │
                     ▼
┌────────────────────────────────────────────────────────────┐
│                      cache.py                              │
│  3-Tier Cache: L1 in-memory → L2 Supabase → L3 disk       │
│  Stale-While-Revalidate pattern                            │
│                                                            │
│  • load_cache_from_supabase()  (L2: Supabase hydration)    │
│  • load_cache() → dict         (L3: read from disk)        │
│  • save_cache(data)            (L3: atomic write)          │
│  • is_cache_stale() → bool     (overall file staleness)    │
│  • is_fund_stale(fund_data)    (per-fund _last_refreshed)  │
│  • get_stale_ciks(cache, ciks) (selective refresh list)    │
│  • stamp_fund_data(data)       (add _last_refreshed ts)    │
│  • refresh_single_fund(cik)    (fetch + write-through L2)  │
│  • _get_effective_ttl_seconds()(TTL in seconds for L2)     │
│  • get_cache_age_str() → str   ("5 min ago")               │
│                                                            │
│  TTL: 7 days (off-season) / 12 hours (filing season)       │
│  Cache keys = CIK without leading zeros ("1067983")        │
│  Supabase keys = "13f:{CIK}" with category "13f"           │
└────────────────────┬───────────────────────────────────────┘
                     │
          ┌──────────┴──────────┐
          ▼                     ▼
┌──────────────────┐  ┌─────────────────────────────────────┐
│     cli.py       │  │              web.py                  │
│  Rich terminal   │  │  FastAPI + Jinja2 + HTMX             │
│                  │  │                                       │
│  3 commands:     │  │  Lifespan: loads cache on startup,    │
│  • search        │  │  triggers background refresh if stale │
│  • holdings      │  │                                       │
│  • compare       │  │  HTMX lazy-loads fund rows on homepage│
│                  │  │  Background refresh: sequential,      │
│  Uses display.py │  │  1-sec rate limit between funds       │
│  for formatting  │  │  Watchlist: server-side JSON + HTMX   │
│                  │  │  Notifications: SSE + polling + JSON  │
└──────────────────┘  │  30+ routes (see Section 6)            │
                      └──────────┬──────────────────────────┘
                                 │
                                 ▼
                      ┌─────────────────────────┐
                      │  templates/ (Jinja2)     │
                      │  10 pages + 12 partials  │
                      │  (see Section 7)         │
                      └─────────────────────────┘
```

### Data Flow: Homepage Load

```
1. User visits http://localhost:8000/
2. web.py lifespan (startup):
   a. load_cache_from_supabase() → ~100 funds from Supabase (single query)
   b. If Supabase empty/down → load_cache() from disk (fallback)
   c. Starts _prefetch_market_data(app) background task (S&P 500 data, ~30-60s)
   d. If any funds stale → triggers _background_refresh() (per-fund TTL)
3. index() renders:
   a. Heatmap section: HTMX fires GET /api/heatmap → "loading" stub with
      auto-retry (hx-trigger="load delay:5s") until market data is ready
   b. Most-added section: HTMX fires GET /api/most-added → renders table from cache
   c. 85 superinvestor rows, each marked for lazy-load
4. Browser HTMX fires GET /api/fund-row/{cik} for each row
5. fund_row() checks cache FIRST:
   a. Cache hit → returns immediately (zero SEC calls)
   b. Cache miss → calls get_fund_summary(cik) in a thread, writes through
      to L1 (in-memory) + L2 (Supabase) + L3 (disk)
6. HTMX replaces the loading row with rendered fund_row.html partial
7. Once market data prefetch completes, heatmap auto-retry succeeds →
   full ECharts treemap rendered with sector groups, colors, gold borders

After first deploy with Supabase: all ~100 funds populate in Supabase.
Subsequent deploys: instant startup from Supabase, zero SEC API calls needed.
```

### Data Flow: Stock Detail Page

```
1. User clicks ticker link (e.g., /stock/AAPL)
2. stock_detail() loads app.state.fund_cache (preloaded dict)
3. build_stock_detail("AAPL", cache, superinvestors):
   a. Iterates all 85 cached funds
   b. For each fund, scans all_holdings for ticker match
   c. Builds StockHolder with value, shares, activity from flat changes
   d. Returns StockDetail (or None → 404)
4. build_stock_history("AAPL", cache, superinvestors):
   a. Phase 1: Build matching_cusips set from all_holdings ticker match
   b. Phase 2: Also scan quarterly_changes for matching CUSIPs (sold stocks)
   c. Phase 3: Walk each fund's quarterly_changes, match CUSIPs, group by quarter
   d. Compute pct_change = share_change / previous_shares * 100
   e. Sort: within quarter (buy→add→reduce→sell), across quarters (newest first)
5. Render stock.html with both sections
```

---

## 3. File Inventory

```
13F-project/
├── scripts/
│   └── backfill_fundamentals.py      # CLI: bulk SEC XBRL backfill (--tier sp500/nasdaq/all, --force, --dry-run, --report)
├── pyproject.toml                    # deps, entry points, build config
├── README.md                         # Project overview
├── README_DEV.md                     # THIS FILE — source of truth
├── .python-version                   # 3.12
├── .gitignore
├── uv.lock
└── src/filings/
    ├── __init__.py                   # version = "0.1.0"
    ├── models.py                     # 13 dataclasses (data contracts)
    ├── superinvestors.py             # 85 hardcoded funds + CIK lookup dict
    ├── cache.py                      # 3-tier cache: L1 in-memory → L2 Supabase → L3 disk
    ├── supabase_cache.py             # Supabase L2 persistent cache (api_cache, insider_trades, notifications, user_watchlist, user_notification_preferences, watchlist_digest_log, admin_users tables)
    ├── watchlist.py                  # Watchlist persistence (JSON, ~/.13f-cache/watchlist.json)
    ├── notifications.py              # Notification creators (13F, YouTube, Reddit, Congress trades) + filing season detection
    ├── analysts.py                   # Analyst ratings (Finnhub + yfinance, 5-min TTL cache)
    ├── sentiment.py                  # Market sentiment (CNN, Finnhub, ApeWisdom, Alpha Vantage)
    ├── vitals.py                     # Alternative data (Glassdoor, People Data Labs, App Store)
    ├── market_data.py                # S&P 500 heatmap, most-added, ticker search (~8K NYSE/NASDAQ listings), NASDAQ 100 constituents (Wikipedia)
    ├── company_filings.py            # SEC filing links for stock pages
    ├── aum_data.py                   # Capital Deployed: AUM (Form ADV), XBRL cash, deployment ratios, leaderboard builder
    ├── unusual_options.py             # Unusual options detection: UnusualOption dataclass, detect_unusual(), premium floor ($100K), OI delta tracking, urgency weighting, moneyness scoring, cluster detection, greek extraction
    ├── options_sync.py               # Cron worker: scan S&P 500 + superinvestor holdings for unusual options activity (every 30 min, market hours)
    ├── convergence.py                # Convergence Engine: cross-signal analysis (options + insider + congress + short interest + 13F) with urgency/OTM/cluster boosts
    ├── tiingo.py                     # Tiingo REST client: real-time IEX quotes, batch quotes (100/call), EOD history, S&P 500 close matrix; 5-min/1-hr caches
    ├── tradier.py                    # Tradier REST client: options chains with ORATS greeks (delta, gamma, theta, vega), expiration dates, stock quotes; DataFrame adapter for detect_unusual()
    ├── insider_trading.py            # Form 4 insider transaction data (4-tier: L1→Supabase→scrape→stale) + display helpers (quarterly groups, insider cards, chart data, title resolution via SEC XML)
    ├── insider_sync.py               # Cron worker: scrape OpenInsider → upsert to Supabase (every 30 min)
    ├── congress_trading.py           # STOCK Act: Capitol Trades scraper (with date cutoff) + 6 display prep functions (chamber viz, trending, consensus, momentum, activity)
    ├── fundamentals.py               # SEC XBRL CompanyFacts: income/balance/cashflow/ratios with 52 GAAP concepts, 2-tier cache (L1+L2), full history backfill support
    ├── cold_storage.py               # Supabase Storage (S3-compatible) cold archive: upload/download JSON blobs, delete protection for fundamentals/
    ├── screener.py                   # Stock Valuation Screener: DCF model, Monte Carlo simulation, peer suggestions/valuation, financial data aggregation (3-tier price fallback)
    ├── cboe_data.py                  # CBOE volatility data: Put/Call ratios (CBOE CSV→yfinance fallback), VIX term structure, SKEW index, IV Rank batch computation
    ├── fred_data.py                  # FRED (Federal Reserve) economic data: GDP, CPI, unemployment, fed funds rate, 10Y yield, yield spread (requires FRED_API_KEY)
    ├── fred_calendar.py              # Economic events calendar: FRED release dates + Finnhub merged, 3-tier cache (L1 mem → L2 economic_events table → L3 API). FMP economic-calendar disabled (402)
    ├── fred_indicators.py            # FRED macro indicator cards: sparkline data for rates, inflation, employment, consumer, credit categories
    ├── treasury_data.py              # US Treasury data: daily yield curve (treasury.gov CSV), national debt (Fiscal Data API), free/no key
    ├── wsb_sentiment.py              # Reddit/WSB sentiment: top mentioned tickers via ApeWisdom API, per-ticker sentiment lookup, free/no key
    ├── openfigi.py                   # OpenFIGI CUSIP→ticker resolution: batch mapping (100/request), 7-day cache, free tier (no key for basic)
    ├── frankfurter.py                # FX rates: 12 major currencies vs USD, 30-day sparklines, Frankfurter API (ECB rates), free/no key/no rate limits
    ├── fmp_cache.py                  # Shared FMP earnings-calendar cache: single bulk fetch every 6h, L1 mem (6h) → L2 api_cache (24h) → API; serves all FMP consumers
    ├── earnings.py                   # Per-ticker earnings history (yfinance + Finnhub + FMP via fmp_cache, 3-tier cache) + shared fetch_finnhub_calendar_raw()
    ├── earnings_scorecard.py         # Macro earnings season metrics (Supabase 5-tier L1→L5 cache) + build_company_lookup()
    ├── earnings_calendar.py          # Earnings calendar page (Finnhub + FMP via fmp_cache, week/month views, 1h in-memory cache)
    ├── digest_worker.py              # Cron worker: daily watchlist digest emails via Resend (hourly check, per-user timezone)
    ├── auth.py                       # Authentication (sign-in, sessions)
    ├── client.py                     # SEC EDGAR client (13 functions)
    ├── display.py                    # CLI Rich formatters (3 functions)
    ├── cli.py                        # CLI entry point (search/holdings/compare)
    ├── web.py                        # FastAPI app (40+ routes + background refresh + Stripe/support + SSE + polling)
    ├── static/
    │   ├── logo-nav.png              # Light-mode navbar logo
    │   └── logo-nav-dark.png         # Dark-mode navbar logo (transparent bg; switched via CSS [data-theme="dark"])
    └── templates/
        ├── base.html                 # Master layout: nav (Home|Retail|Funds|Insiders|Support), PicoCSS, HTMX, ECharts, Fuse.js, sidebar, sortable tables, dark mode toggle + CSS custom properties
        ├── home.html                 # Homepage: heatmap + most-added + cards + Panda Fund support widget (Stripe Pricing Table); TradingView widget rebuilt on theme change
        ├── retail.html               # Retail page: Sentiment, Leaderboard, Calendar sub-tabs
        ├── grand_portfolio.html      # Top Funds page: Funds, Holdings, Activity sub-tabs (URL: /funds)
        ├── insider_trading.html      # Insider trading screener: global buys/sells with chart
        ├── congress.html             # Congress Trading page: 3-tab (Congress dots, Holdings charts, Activity feed)
        ├── politician.html           # Politician profile: stats, donut chart, portfolio table, trade history
        ├── search.html               # Fund manager search
        ├── investor.html             # Individual fund page (tabbed: Holdings + Compare Quarters)
        ├── activity.html             # Cross-fund activity feed (top 100)
        ├── stock.html                # Stock detail (8 tabs: Overview, Ownership, Analysts, Signals, Vitals, Filings, Insider, Congress)
        ├── screener.html              # Stock Valuation Screener: DCF, Monte Carlo, Relative Value tabs + assumptions sidebar with user-controlled peer selector (Fuse.js search, chip tags). Clerk auth gate (blur overlay + sign-in card, same pattern as options page)
        ├── screener_gate.html        # Password gate (used by /macro and /options)
        ├── support.html              # Panda Fund: progress bar, Stripe Buy Button + Pricing Table, cost breakdown, funding history chart
        ├── earnings_calendar.html    # Earnings calendar page (weekly/monthly toggle, HTMX-driven grid + day detail)
        ├── deployment.html           # Capital Deployed standalone page (/deployment)
        ├── notifications.html        # Notification history page
        ├── error.html                # Error page
        └── partials/
            ├── fund_row.html           # HTMX partial: loaded fund row
            ├── fund_row_error.html     # HTMX partial: error fund row
            ├── ticker_link.html        # Jinja2 macro: clickable ticker/CUSIP
            ├── watchlist_sidebar.html   # Sidebar content: ticker list + remove buttons
            ├── watchlist_star.html      # Star button (filled/outline) for stock pages
            ├── watchlist_response.html  # OOB response: star + sidebar update
            ├── notification_bell.html   # Navbar bell icon with red dot indicator (HTMX-polled every 120s)
            ├── notification_dropdown.html # Notification dropdown (latest 8, "View all" footer)
            ├── live_activity.html      # Live activity feed for homepage bento card (notifications stream)
            ├── heatmap.html            # S&P 500 ECharts treemap with Sectors/Companies toggle (lazy-loaded via HTMX)
            ├── most_added.html         # Most-added-by-superinvestors table (lazy-loaded)
            ├── ticker_search.html      # Nav autocomplete search input (Fuse.js fuzzy search)
            ├── analyst_ratings.html    # Analyst consensus + ratings table (lazy-loaded)
            ├── sentiment.html          # Market/news sentiment cards (CNN, Finnhub, Reddit, Alpha Vantage) — standalone, used by /retail
            ├── signals.html            # Unified Signals tab: sentiment + search interest (Google Trends) + web traffic (Cloudflare, Tranco, Wikipedia)
            ├── vitals.html             # Employee pulse, culture, product sentiment (3-card grid)
            ├── company_filings.html    # SEC filing links (lazy-loaded)
            ├── insider_trades.html     # Insider trading table — global screener (lazy-loaded)
            ├── stock_insider_trades.html # Insider trading table — per-ticker (lazy-loaded)
            ├── retail_leaderboard.html  # ApeWisdom Reddit leaderboard (lazy-loaded into retail page)
            ├── compare_content.html    # Compare quarters partial (lazy-loaded into investor page)
            ├── deployment_leaderboard.html # Capital Deployed leaderboard (HTMX partial for /funds Deployment tab)
            ├── deployment_card.html    # Capital Deployed stat card (HTMX partial for investor page)
            ├── congress_activity.html  # Congress activity feed (HTMX lazy-loaded, filter buttons)
            ├── congress_trending.html  # Homepage "Trending with Congress" bar chart (HTMX lazy-loaded)
            ├── stock_congress.html     # Per-ticker Congress trading subtab (HTMX lazy-loaded)
            ├── options_feed.html       # Unusual options activity table: OI Δ (green/red), Moneyness badges (5 color variants), Delta, urgency
            ├── options_clusters.html   # Clustered unusual options cards: ticker, direction, contract count, premium, vol/OI, urgency
            ├── financials.html        # SEC financial statements: 4 sub-tabs (Income, Balance, Cash Flow, Ratios) with ECharts insight charts + annual/quarterly toggle
            ├── earnings.html             # Per-ticker earnings history tab (lazy-loaded)
            ├── earnings_scorecard.html   # Macro earnings scorecard partial (lazy-loaded)
            ├── earnings_calendar_grid.html # Earnings calendar grid/heatmap (HTMX partial: weekly 5-day cards + monthly color-coded heatmap)
            ├── earnings_calendar_day.html # Earnings calendar day detail table (HTMX partial: company/timing/estimates/actuals)
            └── data_error.html         # Reusable error partial (rate limit CTA, generic fallback, HTMX-aware)
```

---

## 4. Data Schema

### 4.1 Superinvestor Registry (`superinvestors.py`)

The system tracks exactly **85 hardcoded superinvestors**. This is the source
list — there is no database of investors. The full list matches
[Dataroma's manager list](https://www.dataroma.com/m/managers.php) plus a few
additional notable investors.

```python
SuperinvestorInfo:
    cik: str            # "1067983" (no leading zeros)
    display_name: str   # "Warren Buffett"
    fund_name: str      # "Berkshire Hathaway"
    crd_number: str     # SEC CRD number for Form ADV lookup (optional, "" if none)
    is_public_company: bool  # True for Berkshire, Markel, Fairfax (XBRL 10-K/10-Q filers)
```

**Lookup dict:** `SUPERINVESTORS_BY_CIK` maps CIK → SuperinvestorInfo.
This is used everywhere to filter cache data to only known superinvestors.

| # | Display Name | Fund | CIK |
|---|---|---|---|
| 1 | AKO Capital | AKO Capital | 1376879 |
| 2 | Chuck Akre | Akre Capital Management | 1112520 |
| 3 | Alex Roepers | Atlantic Investment Mgmt | 1063296 |
| 4 | AltaRock Partners | AltaRock Partners | 1631014 |
| 5 | John Rogers | Ariel Investments | 936753 |
| 6 | Seth Klarman | Baupost Group | 1061768 |
| 7 | Warren Buffett | Berkshire Hathaway | 1067983 |
| 8 | Bill & Melinda Gates Foundation | Gates Foundation Trust | 1166559 |
| 9 | Bill Ackman | Pershing Square | 1336528 |
| 10 | Bill Miller | Miller Value Partners | 1135778 |
| 11 | Bill Nygren | Harris Associates | 813917 |
| 12 | Glenn Greenberg | Brave Warrior Advisors | 1553733 |
| 13 | Ray Dalio | Bridgewater Associates | 1350694 |
| 14 | Bruce Berkowitz | Fairholme Capital | 1056831 |
| 15 | Bryan Lawrence | Oakcliff Capital | 1657335 |
| 16 | William Von Mueffling | Cantillon Capital Mgmt | 1279936 |
| 17 | Clifford Sosin | CAS Investment Partners | 1697591 |
| 18 | Sarah Ketterer | Causeway Capital Management | 1165797 |
| 19 | Francis Chou | Chou Associates | 1389403 |
| 20 | Ken Griffin | Citadel Advisors | 1423053 |
| 21 | Philippe Laffont | Coatue Management | 1135730 |
| 22 | Greg Alexander | Conifer Management | 1773994 |
| 23 | Mohnish Pabrai | Dalal Street LLC | 1549575 |
| 24 | Christopher Davis | Davis Selected Advisers | 1036325 |
| 25 | Dodge & Cox | Dodge & Cox | 200217 |
| 26 | Pat Dorsey | Dorsey Asset Management | 1671657 |
| 27 | Stanley Druckenmiller | Duquesne Family Office | 1536411 |
| 28 | Henry Ellenbogen | Durable Capital Partners | 1798849 |
| 29 | John Armitage | Egerton Capital | 1581811 |
| 30 | Glenn Welling | Engaged Capital | 1559771 |
| 31 | Prem Watsa | Fairfax Financial Holdings | 915191 |
| 32 | First Eagle Investment Mgmt | First Eagle Investment Mgmt | 1325447 |
| 33 | Steven Romick | First Pacific Advisors | 1377581 |
| 34 | FPA Queens Road | Bragg Financial Advisors | 1327055 |
| 35 | Terry Smith | Fundsmith | 1569205 |
| 36 | Thomas Russo | Gardner Russo & Quinn | 860643 |
| 37 | Francois Rochon | Giverny Capital | 1641864 |
| 38 | David Einhorn | Greenlight Capital | 1079114 |
| 39 | Greenhaven Associates | Greenhaven Associates | 846222 |
| 40 | Josh Tarasoff | Greenlea Lane Capital | 1766504 |
| 41 | Guy Spier | Aquamarine Capital | 1404599 |
| 42 | Duan Yongping | H&H International Investment | 1759760 |
| 43 | Hillman Capital | Hillman Capital Management | 1314620 |
| 44 | Li Lu | Himalaya Capital | 1709323 |
| 45 | Carl Icahn | Icahn Capital | 921669 |
| 46 | Jensen Investment Mgmt | Jensen Investment Management | 1106129 |
| 47 | Kahn Brothers | Kahn Brothers Group | 1039565 |
| 48 | Lindsell Train | Lindsell Train | 1484150 |
| 49 | Steve Mandel | Lone Pine Capital | 1061165 |
| 50 | Mairs & Power | Mairs & Power | 1070134 |
| 51 | Tom Bancroft | Makaira Partners | 1540866 |
| 52 | Tom Gayner | Markel Group | 1096343 |
| 53 | David Katz | Matrix Asset Advisors | 1016287 |
| 54 | Lee Ainslie | Maverick Capital | 934639 |
| 55 | Howard Marks | Oaktree Capital | 949509 |
| 56 | Robert Olstein | Olstein Capital Management | 947996 |
| 57 | Leon Cooperman | Omega Advisors | 898202 |
| 58 | Samantha McLemore | Patient Capital Management | 1854794 |
| 59 | Polen Capital | Polen Capital Management | 1034524 |
| 60 | Norbert Lou | Punch Card Management | 1631664 |
| 61 | Richard Pzena | Pzena Investment Management | 1027796 |
| 62 | Jim Simons | Renaissance Technologies | 1037389 |
| 63 | Robert Vinall | RV Capital | 1766596 |
| 64 | Michael Burry | Scion Asset Mgmt | 1649339 |
| 65 | Christopher Bloomstran | Semper Augustus | 1115373 |
| 66 | Dennis Hong | ShawSpring Partners | 1766908 |
| 67 | Harry Burn | Sound Shore Management | 820124 |
| 68 | Mason Hawkins | Southeastern Asset Mgmt | 807985 |
| 69 | Chris Hohn | TCI Fund Management | 1647251 |
| 70 | Third Avenue Management | Third Avenue Management | 1099281 |
| 71 | Dan Loeb | Third Point | 1040273 |
| 72 | Chase Coleman | Tiger Global | 1167483 |
| 73 | Torray LLC | Torray Investment Partners | 98758 |
| 74 | Nelson Peltz | Trian Fund Management | 1345471 |
| 75 | Triple Frond Partners | Triple Frond Partners | 1454502 |
| 76 | Tweedy Browne | Tweedy Browne Co. | 732905 |
| 77 | Valley Forge Capital | Valley Forge Capital Mgmt | 1697868 |
| 78 | ValueAct Capital | ValueAct Holdings | 1418814 |
| 79 | Andreas Halvorsen | Viking Global | 1103804 |
| 80 | David Rolfe | Wedgewood Partners | 859804 |
| 81 | Wallace Weitz | Weitz Investment Management | 883965 |
| 82 | Yacktman Asset Mgmt | Yacktman Asset Management | 905567 |
| 83 | David Tepper | Appaloosa LP | 1656456 |
| 85 | David Abrams | Abrams Capital Management | 1358706 |

### 4.2 Cache File Schema (`~/.13f-cache/fund_data.json`)

Top-level: `{ "CIK": { fund_data }, ... }`
Keys are CIK strings **without** leading zeros (e.g., `"1067983"`).

```
{
  "<CIK>": {
    "name": str,                    # "Berkshire Hathaway Inc"
    "cik": str,                     # "1067983"
    "report_period": str,           # "09-30-2025"
    "filing_date": str,             # "2025-11-15"
    "total_value": int,             # Portfolio value in dollars
    "total_holdings": int,          # Number of holdings

    "top_holdings": [               # Top 10 by value
      {
        "issuer": str,              # "APPLE INC"
        "ticker": str | null,       # "AAPL"
        "cusip": str,               # "037833100"
        "value": int,               # Dollars
        "shares": int
      }, ...
    ],

    "all_holdings": [               # All holdings, sorted by value desc
      {
        "issuer": str,
        "ticker": str | null,
        "cusip": str,
        "value": int,
        "shares": int,
        "pct": float                # % of portfolio (e.g., 10.84)
      }, ...
    ],

    "changes": [                    # Flat changes (MOST RECENT quarter only)
      {                             # Used by: activity feed, stock detail holders
        "issuer": str,
        "cusip": str,
        "status": str,              # "NEW"|"CLOSED"|"INCREASED"|"DECREASED"
        "share_change": int,
        "current_value": int
      }, ...
    ],

    "quarterly_changes": [          # Multi-quarter history (up to 8 quarters)
      {                             # Used by: stock history page
        "period": str,              # "Q3 2025"
        "report_period": str,       # "09-30-2025"
        "filing_date": str,
        "changes": [
          {
            "issuer": str,
            "cusip": str,
            "status": str,          # "NEW"|"CLOSED"|"INCREASED"|"DECREASED"
            "share_change": int,
            "current_value": int,
            "current_shares": int,  # (only in quarterly_changes, not flat)
            "previous_shares": int  # (only in quarterly_changes, not flat)
          }, ...
        ]
      }, ...
    ]
  }, ...
}
```

**Important distinctions:**
- `changes` (flat) = most recent quarter only. Missing `current_shares`/`previous_shares`.
- `quarterly_changes` = up to 8 quarters. Has `current_shares`/`previous_shares`.
- Old cache entries (pre-multi-quarter) may lack `quarterly_changes` entirely.
  Code uses `.get("quarterly_changes", [])` for graceful degradation.

### 4.3 Watchlist File Schema (`~/.13f-cache/watchlist.json`)

Separate from the fund cache. Stores the user's starred tickers.

```json
{
  "tickers": [
    {
      "ticker": "AAPL",
      "cusip": "037833100",
      "issuer_name": "APPLE INC",
      "added_at": "2026-02-16T10:30:00"
    }
  ]
}
```

Functions in `watchlist.py`:
- `load_watchlist() -> list[dict]` — read from disk, `[]` if missing
- `save_watchlist(entries)` — atomic write via tmp swap
- `add_to_watchlist(ticker, cusip, issuer_name) -> list[dict]` — idempotent
- `remove_from_watchlist(ticker) -> list[dict]`
- `is_in_watchlist(ticker) -> bool`

### 4.4 Notifications (Supabase `notifications` table)

Global notifications visible to all visitors. No per-user storage — dismiss state
is tracked client-side via `localStorage('pp-notifications-last-seen')`.

**Schema:**

```sql
CREATE TABLE notifications (
    id          TEXT PRIMARY KEY,      -- deterministic: "{source}-{unique_key}"
    type        TEXT NOT NULL,         -- "13f_change" | "youtube" | "reddit_velocity"
    title       TEXT NOT NULL,         -- e.g. "Buffett: NEW BUY — AAPL"
    message     TEXT NOT NULL,         -- e.g. "Apple Inc added to portfolio (5.5% weight)"
    icon        TEXT DEFAULT '🔔',
    toast_type  TEXT DEFAULT 'alert',  -- "bullish" | "bearish" | "alert"
    link        TEXT,                  -- URL to navigate on click
    metadata    JSONB DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

**Deterministic IDs (deduplication via PRIMARY KEY):**
- 13F: `13f-{cik}-{filing_date}-{cusip}`
- YouTube: `yt-{video_id}`
- Reddit: `reddit-{ticker}-{date}`

**Retention:** 48 hours. `cleanup_old_notifications(days=2)` runs at start of each sync cycle.

**Functions in `notifications.py`:**
- `detect_13f_changes(cik, fund_name, current, previous)` — compare holdings, return notification dicts
- `create_youtube_notification(event)` — notification dict for high-impact video
- `create_reddit_notification(ticker, velocity_pct, mentions, name)` — notification dict for velocity spike
- `is_filing_season() -> bool` — True within ±15 days of filing deadlines
- `get_poll_interval_seconds() -> int` — 2h during season, 12h outside

**Functions in `supabase_cache.py`:**
- `upsert_notifications(rows)` — batch upsert with `ignore_duplicates=True`, chunked at 50
- `get_recent_notifications(limit, types, offset)` — paginated fetch, optional type filter
- `get_bell_state(since_iso) -> (count, latest)` — single query for bell poll (count + newest row)
- `cleanup_old_notifications(days=2) -> int` — delete old rows

### 4.5 Dataclasses (models.py)

All models are `@dataclass`. No ORM. No database.

| Dataclass | Purpose | Used By |
|---|---|---|
| `SearchResult` | Fund manager search result | CLI search, web search |
| `Holding` | Single position in a 13F filing | CLI holdings |
| `FundInfo` | Metadata about a fund's filing | CLI, web holdings/compare |
| `HoldingChange` | Diff between two quarters for one position | compare_quarters, _compare_two_filings |
| `EnrichedHolding` | Holding + ticker + activity + pct | Web holdings page |
| `SuperinvestorSummary` | Homepage card data | Web index |
| `ActivityItem` | One entry in the activity feed | Web activity feed |
| `GrandPortfolioEntry` | Aggregated stock across all funds | Web grand portfolio |
| `StockHolder` | One fund's position in a stock | Web stock detail |
| `StockDetail` | All holders of a specific stock | Web stock detail |
| `StockQuarterEntry` | One fund's activity on a stock in one quarter | Web stock history |
| `StockQuarter` | All activity on a stock in one quarter | Web stock history |
| `AnalystRating` | A single firm-level analyst rating | Web analyst tab |
| `Notification` | A notification dict (13F/YouTube/Reddit) — stored in Supabase | Notification system |

### 4.6 How a Superinvestor Links to a CIK

```
superinvestors.py          cache.py / fund_data.json          SEC EDGAR
─────────────────          ──────────────────────────          ─────────
SuperinvestorInfo  ──CIK──▶  Cache key "1067983"  ◀──fetched──  Company(1067983)
  .cik = "1067983"            contains all data                  .get_filings()
  .display_name              for this fund                       ThirteenF()
  .fund_name

SUPERINVESTORS_BY_CIK["1067983"] → SuperinvestorInfo("Warren Buffett", ...)
```

The CIK is the universal join key across every layer:
- SEC EDGAR uses it to identify filing entities
- Our cache uses it as the top-level key (without leading zeros)
- The superinvestor registry uses it to map CIK → display name
- Web routes accept it as a URL parameter for fund pages

---

## 5. Core Logic Definitions

### 5.1 CUSIP-Based Filing Comparison (`_compare_two_filings`)

**This is the most critical algorithm in the system.** It powers quarter-over-quarter
diffs, activity feeds, and multi-quarter stock history.

```
Input:  Two pandas DataFrames (current quarter, previous quarter)
Output: List of HoldingChange objects

Algorithm:
1. Index both DataFrames by CUSIP (unique security identifier)
2. Compute the union of all CUSIPs across both quarters
3. For each CUSIP:
   - In current only → status = "NEW"
   - In previous only → status = "CLOSED"
   - In both, shares increased → status = "INCREASED"
   - In both, shares decreased → status = "DECREASED"
   - In both, shares unchanged → status = "UNCHANGED"
4. Record: issuer name, share counts (both quarters), value (both quarters)
```

**Status → User-Facing Label Mapping** (used throughout):

| Internal Status | Display Label | Badge Color |
|---|---|---|
| `NEW` | `NEW BUY` | Green (`badge-new`) |
| `INCREASED` | `ADD` | Green (`badge-add`) |
| `DECREASED` | `REDUCE` | Red (`badge-reduce`) |
| `CLOSED` | `SOLD` | Red (`badge-sold`) |
| `UNCHANGED` | (hidden/filtered) | — |

### 5.2 Multi-Quarter History Builder (`get_fund_summary`)

**Second most complex logic.** Builds 8 quarters of comparison data per fund.

```
Input:  CIK, history_quarters=8
Output: Dict with fund info + up to 8 quarter-pair diffs

Algorithm:
1. Fetch all 13F-HR filings (non-amendments) for the fund
2. Take filings[0] as the latest quarter
3. For i in range(min(8, len(filings)-1)):
   a. tf_newer = filings[i] (reuse filings[0] ThirteenF object when i=0)
   b. tf_older = filings[i+1]
   c. Run _compare_two_filings(newer.holdings, older.holdings)
   d. Filter out UNCHANGED entries
   e. Store as quarterly_changes[i] with period label ("Q3 2025")
   f. When i=0, also populate flat "changes" for backwards compatibility
4. Wrap exception per pair (try/except continue) so one bad filing
   doesn't abort the entire history
```

**Key design decision:** The flat `changes` field duplicates pair-0 data from
`quarterly_changes` but WITHOUT `current_shares`/`previous_shares`. This exists
for backwards compatibility with `build_activity_feed()` and `build_stock_detail()`,
which predate the multi-quarter feature.

### 5.3 Stock History Aggregation (`build_stock_history`)

**Third most complex.** Cross-fund, cross-quarter aggregation. Zero API calls.

```
Input:  Ticker (or CUSIP), full cache dict, superinvestor registry
Output: List of StockQuarter objects (most recent first)

Algorithm:
Phase 1 — Resolve matching CUSIPs:
  a. Scan all_holdings across all funds for ticker match → collect CUSIPs
  b. Also scan quarterly_changes for CUSIP match (catches stocks that
     were completely sold and no longer appear in all_holdings)

Phase 2 — Collect entries:
  For each fund × each quarter × each change:
    If change.cusip in matching_cusips:
      Create StockQuarterEntry with:
        - fund display name and CIK
        - activity label (mapped from status)
        - share_change
        - pct_change = (share_change / previous_shares) * 100
          Special cases: NEW BUY → 100%, SOLD with 0 previous → 0%

Phase 3 — Sort and return:
  Within each quarter: NEW BUY → ADD → REDUCE → SOLD (then alphabetical)
  Across quarters: most recent first (by year desc, quarter num desc)
```

### 5.4 13F Filing Lag (Important Context)

SEC 13F filings have a **mandatory 45-day filing deadline** after each quarter-end:
- Q1 (Jan-Mar) filings due by May 15
- Q2 (Apr-Jun) filings due by August 14
- Q3 (Jul-Sep) filings due by November 14
- Q4 (Oct-Dec) filings due by February 14

**This means the data is always at least 45 days old.** When you see "Q3 2025"
data, it reflects holdings as of September 30, 2025, but may not have been
filed until mid-November 2025. The system does not attempt to calculate or
display this lag explicitly, but it's important context for understanding
data freshness.

### 5.5 CIK Normalization

CIKs come in two formats depending on the source:
- SEC EDGAR URLs/APIs use 10-digit zero-padded: `0001067983`
- Our internal system strips leading zeros: `1067983`

**Normalization rule:** `cik.lstrip("0") or cik`

This is applied in the `fund_row` endpoint (web.py line 87) when storing cache
data, ensuring all cache keys match the format in `SUPERINVESTORS_BY_CIK`.

### 5.6 Ticker Resolution

Not all 13F holdings have tickers (they're not part of the official filing).
The `edgartools` library enriches some rows with tickers, but many are `NaN`.

**Resolution chain:**
1. If the holding has a ticker → use it, link to `/stock/{ticker}`
2. If no ticker but has CUSIP → link to `/stock/cusip/{cusip}` (shows first 6 chars)
3. If neither → show plain text issuer name

This logic lives in the `ticker_link.html` Jinja2 macro.

### 5.7 Cache Refresh Strategy

#### 13F Fund Data (3-tier stale-while-revalidate)

```
Cache Tiers:
  L1: app.state.fund_cache (in-memory dict, process lifetime)
  L2: Supabase api_cache table, category="13f" (survives deploys)
  L3: Disk JSON at ~/.13f-cache/fund_data.json (local fallback)

Key behavior:
  - load_cache_from_supabase() uses get_cached_with_stale() so expired
    data is returned as stale fallback — NEVER dropped on TTL expiry
  - Fund endpoints (fund-row, holdings, compare, portfolio-chart) have
    L2 Supabase fallback: on L1 miss, query Supabase directly, then
    promote result to L1 for fast subsequent access

TTL Configuration:
  Off-season: 7 days (13F data only changes quarterly)
  Filing season (±15 days of deadline): 12 hours
  Filing deadlines: Feb 14, May 15, Aug 14, Nov 14

Startup (hydration priority):
  1. load_cache_from_supabase() → all ~100 funds from Supabase (single query)
     wrapped in asyncio.wait_for(..., timeout=30) — if Supabase is slow or
     unavailable, the timeout fires and the app falls back to disk rather than
     hanging until Railway's gunicorn worker kills it.
  2. If Supabase empty/down/timeout → load_cache() from disk (fallback)
  3. Check per-fund staleness → trigger background refresh for stale funds

Background refresh (selective, write-through):
  1. get_stale_ciks(cache, all_ciks) → only CIKs whose _last_refreshed is expired
  2. For each stale CIK (sequential):
     a. Call get_fund_summary(cik) in a background thread
     b. stamp_fund_data(data) → adds _last_refreshed ISO timestamp
     c. Write to L1: app.state.fund_cache[cik] (in-memory, instant)
     d. Write to L2: supabase_cache.set_cached() (Supabase, non-fatal)
     e. Write to L3: save_cache() in batches every 10 funds (non-blocking)
     f. Sleep 1 second between funds (SEC rate limiting)
  3. Fresh funds are SKIPPED (not re-fetched)
  4. On API failure, old data is preserved (stale-while-revalidate)

Request-triggered refresh (self-healing):
  When fund_row() or holdings() detects stale data:
    1. Check _ENABLE_BACKGROUND_REFRESH flag (env: ENABLE_BACKGROUND_REFRESH)
    2. Check cache.is_fund_stale(cached) for per-fund staleness
    3. Check cik not already in _refresh_in_progress set
    4. If all pass → asyncio.create_task(_trigger_single_refresh(app, cik))
  Uses asyncio.Lock + asyncio.timeout(300) for concurrency control
  _refresh_in_progress set prevents duplicate refreshes for same CIK

Manual refresh:
  POST /refresh → creates same background task
  Prevented from running concurrently via app.state.refreshing flag

HTMX lazy-load:
  GET /api/fund-row/{cik} checks L1 cache first:
    Cache hit → returns immediately (zero SEC calls)
    L1 miss → try L2 Supabase (stale OK) → promote to L1
    Both miss → fetches from SEC, writes through to L1 + L2 + L3
```

#### Insider Trades (hot/cold with 6-tier stale-while-revalidate)

```
Cache Tiers (per-ticker):
  L1:   in-memory dict with 10 min TTL (sub-ms)
  L2:   Supabase insider_trades table (hot, 30-day rolling window)
  L2.5: OpenInsider screener scrape (full history: fd=0, td=0, paginated up to 300 trades)
  L2.6: Supabase insider_purchases_history table (cold, permanent purchases archive)
  L3:   OpenInsider scrape + backfill to Supabase (when L2 is empty)
  L4:   Stale L1 data (last resort — never show empty/error to users)

Data flow (insider_trading.py → get_ticker_insider_trades):
  _get_cached_with_stale(key, ttl) → returns (data, is_fresh) tuple
  - Fresh L1 hit → return immediately
  - L1 expired → L2 hot table query (by ticker)
  - Merge L2.5 OpenInsider screener (all-time history, dedup by sec_url)
  - Merge L2.6 cold table purchases (fill gaps OpenInsider might miss)
  - All fail → L3 scrape + backfill → L4 stale L1

Display pipeline (prepare_ticker_display):
  1. Resolve "See Remarks" titles via SEC Form 4 XML (cached in memory)
  2. Group by insider → cards with quarterly breakdown tooltips
  3. Group by quarter → collapsible <details> sections
  4. Build Chart.js data → stacked bar chart (buy/sell per quarter)
  5. Pre-compute per-insider chart data → instant dropdown filter switching

Sync worker (insider_sync.py, Railway cron every 30 min):
  1. Scrape 3 OpenInsider pages (all, purchases, sales)
  2. Deduplicate by sec_url
  3. Upsert to Supabase (ON CONFLICT sec_url DO UPDATE)
  4. Never deletes old data — only adds/updates
```

#### Automatic Retention Cleanup (`supabase_cache.run_retention_cleanup`)

Runs as a fire-and-forget background task on every app startup (`asyncio.to_thread`),
keeping the Supabase free-tier row limit from being exceeded.

```
Table                       Retention    Cutoff column
insider_trades              6 months     filing_date
youtube_events              30 days      updated_at
sync_logs                   30 days      started_at
unusual_options_activity    7 days       fetched_at
options_oi_snapshots        7 days       scan_date
api_cache                   expired      expires_at  (only rows where expires_at < now())
```

Returns a `{table: rows_deleted}` dict logged at INFO level. Safe to call when
Supabase is unavailable — each table's delete is independently try/except'd.

### 5.8 Market Data Module (`market_data.py`)

Central module for all homepage market data features. Follows the same TTL
cache pattern as `analysts.py`.

| Function | Data Source | TTL | Purpose |
|---|---|---|---|
| `get_sp500_constituents()` | Wikipedia (pd.read_html) | 24h | Ticker + sector list (~500 items) |
| `get_sp500_market_data()` | Tiingo EOD (primary) + yfinance (fallback) | 30min | Daily % change for all S&P 500 tickers |
| `build_heatmap_data()` | Pure computation | — | ECharts treemap format with market-cap weighting + colors |
| `build_most_added_table()` | Cache (fund_data changes) | 30min | Top 25 stocks by superinvestor add count |
| `get_52_week_range_bulk()` | yfinance bulk download (1y) | 30min | 52-week high/low/current for enrichment |
| `get_all_listed_tickers()` | NASDAQ Trader (nasdaqtraded.txt) | 24h | All ~8K NYSE/NASDAQ/AMEX listings |
| `get_ticker_search_list()` | Listings + S&P 500 + cache + investors | — | Unified search index (~8K items, Fuse.js) |
| `_pct_to_color()` | Pure computation | — | Map [-5%, +5%] to red→gray→green hex color |

**Cold start behavior:**
- `_prefetch_market_data(app)` runs in lifespan background task
- Takes ~30-60s for first yfinance bulk download
- Heatmap HTMX auto-retries every 5s until `app.state.market_data_ready = True`
- After first load, 30-min cache makes all subsequent requests instant

**Fallback:** If Wikipedia is unreachable, falls back to hardcoded top ~50 S&P 500
tickers with sectors. The heatmap will be smaller but functional.

**Ticker search:** `get_all_listed_tickers()` fetches the NASDAQ Trader public directory
(`nasdaqtraded.txt`, pipe-delimited, updated daily) containing ~8K NYSE/NASDAQ/AMEX
securities. `get_ticker_search_list()` merges 4 sources: all listings, S&P 500 constituents,
superinvestor holdings, and investor profiles. The frontend uses Fuse.js v7 weighted fuzzy
search (ticker weight 1.0, name weight 0.5) with 150ms debounce.

### 5.9 Sentiment Module (`sentiment.py`)

Fetches market/news sentiment from 4 free sources. Each source is fetched independently;
failures in one do not affect the others. All results are cached in memory with per-source TTLs.

| Provider | Function | TTL | API Key | Returns |
|---|---|---|---|---|
| CNN Fear & Greed | `_get_cnn_fear_greed()` | 1h | None | score (0-100), rating, historical comparisons |
| Finnhub | `_get_finnhub_sentiment()` | 2h | `FINNHUB_API_KEY` | bullish/bearish %, buzz metrics, sector comparison |
| ApeWisdom | `_get_apewisdom_for_ticker()` | 1h | None | Reddit mention rank, count, upvotes, 24h delta |
| Alpha Vantage | `_get_alphavantage_sentiment()` | 12h | `ALPHAVANTAGE_API_KEY` | NLP-scored news articles, avg sentiment label |

Alpha Vantage has a daily budget tracker (`_AV_DAILY_MAX = 20`) to stay within the
25/day free tier limit.

### 5.10 Vitals Module (`vitals.py`)

Alternative data signals for the Vitals tab. Each source is fetched independently
with aggressive caching (7-30 day TTLs). All three providers now persist to Supabase
(L2 cache) so data survives deploys.

| Provider | Function | TTL | API Key | Supabase Category | Returns |
|---|---|---|---|---|---|
| People Data Labs | `_fetch_pdl_from_api()` | 7d | `PDL_API_KEY` | `pdl` | employee_count, size, industry, founded, location |
| Glassdoor (RapidAPI) | `_get_glassdoor_data()` | 30d | `GLASSDOOR_RAPIDAPI_KEY` | `glassdoor` | overall_rating, CEO approval, recommend %, outlook |
| Apple iTunes Search | `_fetch_appstore_from_api()` | 7d | None (free) | `appstore` | app_name, rating, rating_count, version trend |

**Supabase persistence (PDL + App Store):**
- Lazy hydration: `_hydrate_pdl_cache()` and `_hydrate_appstore_cache()` load all rows for a category on first request
- Every API fetch persists to Supabase via `_persist_pdl_entry()` / `_persist_appstore_entry()`
- Stale-while-revalidate: PDL returns stale data and conserves API quota rather than re-fetching
- Quota tracking: PDL has `MAX_MONTHLY_PDL_QUOTA = 100`, tracked via `supabase_cache.increment_quota("pdl", month)`
- Diagnostic helper: `get_vitals_cache_info()` returns cache status for /health endpoint

**Company name resolution:** Both Glassdoor and App Store use `_resolve_company_name(ticker)`
which calls `yfinance.Ticker(ticker).info["longName"]` to map ticker to company name.

**App Store matching:** Uses `_TICKER_APP_OVERRIDES` dict for 20+ major companies where
the ticker doesn't map to the main consumer app (e.g., GOOG → "Google", META → "Instagram").
Falls back to yfinance company name search. Picks the result with the most ratings for
override tickers.

### 5.11 Capital Deployed Module (`aum_data.py`)

Tracks how much of each superinvestor's total AUM is deployed in public equities.
Combines three SEC data sources: Form ADV (regulatory AUM), XBRL 10-K/10-Q (exact
cash balances), and 13F filings (equity holdings).

**Data Sources:**

| Source | Data | Frequency | Access Method |
|---|---|---|---|
| SEC Form ADV | Regulatory AUM (RAUM) | Annual | Bulk XML from SEC IAPD |
| SEC XBRL | Cash & cash equivalents | Quarterly (10-K/10-Q) | SEC Company Facts API |
| SEC 13F-HR | Equity holdings value | Quarterly | Already cached in fund_data |

**Key functions:**

| Function | Description |
|---|---|
| `fetch_adv_bulk_data()` | Downloads SEC Form ADV bulk XML, extracts RAUM for funds with CRD numbers |
| `fetch_xbrl_cash(cik)` | Queries SEC XBRL Company Facts API for cash & equivalents from 10-K/10-Q |
| `compute_deployment_metrics(cik, ...)` | Combines ADV + XBRL + 13F into a single metrics dict |
| `sync_all_deployment_data()` | Syncs all 85 funds, writes results to Supabase `api_cache` (category: `deployment`) |
| `load_all_deployment_data()` | Loads all deployment entries from Supabase cache |
| `build_deployment_leaderboard(data)` | Builds sorted list for table display, sorted by cash value descending |

**Deployment metrics dict:**

```python
{
    "cik": str,
    "display_name": str,
    "fund_name": str,
    "crd_number": str,
    "raum": float | None,           # Form ADV regulatory AUM (dollars)
    "thirteenf_value": float,       # Sum of 13F equity holdings (dollars)
    "deployment_ratio": float | None,  # 13F / RAUM (0.0–1.0+, None if no RAUM)
    "non_equity_pct": float | None,    # 1 - deployment_ratio (percentage)
    "estimated_non_equity": float | None,  # RAUM - 13F value (dollars, can be negative)
    "exact_cash": float | None,     # XBRL cash & equivalents (dollars)
    "exact_cash_period": str | None,  # XBRL reporting period (e.g., "2025-09-30")
    "data_source": str,             # "adv+xbrl" | "adv" | "xbrl" | "13f_only"
}
```

**Leaderboard sort:** Sorted by best available cash value descending (highest cash first).
Uses `exact_cash` (XBRL) if available, else `estimated_non_equity` (AUM gap), else 0.

**CRD numbers:** Each superinvestor can optionally have a `crd_number` in `superinvestors.py`.
This is the SEC's Central Registration Depository number for investment advisers.
Funds without a CRD (public companies like Berkshire, or terminated registrations) cannot
get Form ADV data and fall back to 13F-only metrics.

**XBRL cash limitation:** Standard GAAP concepts (`CashAndCashEquivalentsAtCarryingValue`,
`CashCashEquivalentsAndShortTermInvestments`) are queried. Company-specific XBRL tags
(e.g., Berkshire's U.S. Treasury Bill holdings) are not captured. XBRL cash should be
treated as a lower bound on total liquidity.

**Caching:** Results stored in Supabase `api_cache` with `category="deployment"` and
cache keys like `deployment:{cik}`. TTL: 30 days. Data is loaded into
`app.state.deployment_cache` on startup.

**Templates:**

| Template | Route | Description |
|---|---|---|
| `deployment.html` | `/deployment` | Standalone Capital Deployed page with sortable table |
| `partials/deployment_leaderboard.html` | `/api/deployment-leaderboard` | HTMX partial for /funds Deployment tab |
| `partials/deployment_card.html` | `/api/deployment/{cik}` | Stat card for individual investor pages |

**Tooltip system (`.pp-tooltip`):** All three templates use a unified CSS-only tooltip
system. Tooltips appear on hover for column headers (with ⓘ info icons) and data cells
(with dotted underlines). Tooltip variants: `.pp-tip-left` (left-anchored), `.pp-tip-center`
(center-anchored), `.pp-tooltip-data` (dotted underline on data values). Header tooltips
drop below via `thead .pp-tooltip .pp-tip-box { bottom: auto; top: calc(...) }`.

**Number formatting tiers:** All monetary values use consistent formatting:
`$X.XB` (billions) → `$XM` (millions) → `$XK` (thousands) → `<$1K`.
Negative values (leverage/stale ADV) shown in blue with same B/M tiers.

**13F fallback for AUM:** When a fund has no Form ADV data, the Total AUM column shows
the 13F equity value in muted text with a `13F` superscript badge. Deployment ratio
cannot be calculated without ADV data.

### 5.12 Insider Trading Module (`insider_trading.py` + `insider_sync.py`)
Dedicated insider trading system with its own Supabase tables, sync worker, and rich per-ticker display pipeline.

**Architecture:**
- **Hot table** (`insider_trades`): 30-day rolling window, synced every 30 min from OpenInsider
- **Cold table** (`insider_purchases_history`): permanent record of all historical purchases, never deleted
- `insider_sync.py` cron worker scrapes OpenInsider every 30 min, upserts via `ON CONFLICT (sec_url) DO UPDATE`
- `insider_trading.py` serves data with 4-tier stale-while-revalidate fallback
- Per-ticker data merges: hot table → OpenInsider screener (full history) → cold table, deduplicated by `sec_url`

**Data model (`InsiderTrade` dataclass):**
- `filing_date`, `trade_date`, `ticker`, `company_name`, `insider_name`, `title`
- `trade_type` (Purchase, Sale, Sale+OE), `price`, `qty`, `owned`, `delta_own`, `value`
- `sec_url` (unique key, link to SEC Form 4 filing)
- `to_db_row()` for hot table, `to_history_row()` for cold table, `from_db_row()` / `from_history_row()` class methods

**Key functions:**
| Function | Description |
|---|---|
| `get_latest_insider_trades(trade_type, count)` | Global screener: L1→L2→L3→L4 stale fallback |
| `get_ticker_insider_trades(ticker)` | Per-ticker: L1→L2 hot→L2.5 OI screener→L2.6 cold→L3→L4 stale |
| `aggregate_top_tickers(trades, limit, mixed)` | Aggregate by ticker for chart (net flow, insider details) |
| `prepare_ticker_display(trades)` | Builds structured display data: insiders, quarters, chart, per_insider_chart |
| `_resolve_see_remarks_titles(trades)` | Fetches raw SEC Form 4 XML, extracts real titles from `<remarks>`, abbreviates (CEO, CFO, etc.), caches in memory |
| `_scrape_openinsider_ticker(ticker, max_pages)` | Screener endpoint with `fd=0`/`td=0` for full history, paginated (up to 300 trades) |
| `_scrape_openinsider_global(trade_type, count)` | L3 fallback: direct scrape of OpenInsider |
| `_scrape_and_backfill_ticker(ticker)` | L3 fallback: scrape + upsert back to Supabase |
| `_shorten_title(title)` | Abbreviates verbose officer titles: "Chief Executive Officer" → "CEO" |

**Per-ticker display pipeline (`prepare_ticker_display`):**
1. **Title resolution**: fetches SEC Form 4 XML for "See Remarks" insiders, extracts real title from `<remarks>`, falls back to `<isDirector>`/`<isOfficer>` flags
2. **Insider grouping**: aggregates trades by insider name with buy/sell counts, total values, and per-quarter breakdown (for hover tooltip)
3. **Quarterly grouping**: groups trades by quarter (newest-first) with buy/sell counts and total value
4. **Chart data**: builds Chart.js stacked bar data (chronological) with buy/sell values per quarter
5. **Per-insider chart**: pre-computes per-insider-per-quarter values for the dropdown filter to swap chart datasets instantly

**Title resolution (`_resolve_see_remarks_titles`):**
- Strips `/xslF345X03/` XSLT prefix from sec_url to get raw XML
- Primary: extracts title from `<remarks>` via regex `r"Officer title:\s*(.+?)\."`
- Fallback: checks `<officerTitle>` (if not "See Remarks"), then `<isDirector>` → "Dir", `<isTenPercentOwner>` → "10% Owner"
- Abbreviates via `_TITLE_ABBREVS` mapping (20+ C-suite abbreviations)
- Cached in `_title_cache` dict (keyed by insider name + ticker) — zero SEC requests on repeat views
- Rate-limited to 0.15s between SEC requests, capped at 10 insiders per call

**Sync worker (`insider_sync.py`):**
- Entry point: `uv run filings-insider-sync`
- Scrapes 3 OpenInsider pages (all, purchases, sales) with 3-second delays
- Deduplicates by `sec_url`, upserts to Supabase in chunks of 50
- Logs to `sync_logs` table for observability

### 5.13 Panda Fund & Stripe Integration (`web.py` + `support.html`)

The Panda Fund is the project's donation/support system, displayed on both the
`/support` page and the homepage widget.

**Architecture (zero backend Stripe SDK):**
- Uses Stripe's OOTB web components -- purely frontend, configured via Stripe Dashboard
- `<stripe-buy-button>` for one-time donations (backed by Payment Link with "Customer chooses what to pay")
- `<stripe-pricing-table>` for recurring monthly subscriptions (Bamboo $5, Panda $15, Giant Panda $30)
- Scripts loaded async: `https://js.stripe.com/v3/buy-button.js` and `https://js.stripe.com/v3/pricing-table.js`

**Env vars:**
| Variable | Purpose |
|---|---|
| `STRIPE_PUBLISHABLE_KEY` | Stripe publishable key (pk_live_...) |
| `STRIPE_BUY_BUTTON_ID` | Buy Button ID (buy_btn_...) for one-time tab |
| `STRIPE_PRICING_TABLE_ID` | Pricing Table ID (prctbl_...) for monthly tab + homepage widget |
| `PANDA_FUND_RAISED` | Current month's total raised in dollars (manual or webhook-updated) |
| `FEEDBACK_LINK` | URL for feedback form CTA |

**Progress bar logic:**
- `_PANDA_FUND_MONTHLY_GOAL = 400` (capped on frontend, even if more is collected)
- `raised_this_month = min(raw_raised, monthly_goal)` -- never shows more than goal
- `progress_pct = min(100, round(...))` -- capped at 100%
- Goal-reached badge shown when `raw_raised >= monthly_goal`

**Support page (`/support`) sections:**
1. Hero + progress bar with goal-reached badge
2. Donate widget: tab toggle (One-time / Monthly) with Stripe Buy Button + Pricing Table
3. "Where the money goes" -- line items (no dollar amounts): Data APIs, Cloud hosting, Database, Domain, AI coding assistants
4. Funding history -- ECharts bar chart (Y-axis capped at goal), green = funded, gray = subsidized
5. "Another way to support" -- YouTube @funofinvesting link
6. "Help shape PaperPanda" -- feedback CTA
7. "Built with bamboo and late nights by Tevis" footer

**Homepage widget (`home.html`):**
- Compact card with panda emoji header, copy text, thin green progress bar (6px, pulse animation)
- Embeds the same `<stripe-pricing-table>` component from /support
- Falls back to "View Support Tiers" button when Stripe env vars not set
- "Learn more about the Panda Fund" link to /support

**HTMX-aware error handling:**
- Exception handlers detect HTMX requests (`HX-Request` header) or API paths
- 429 errors return `partials/data_error.html` inline instead of full error page
- Error partial shows "Token-Limit Reached" with link to /support (Panda Fund CTA)

### 5.14 Retail Page (`retail.html`)

The `/retail` page aggregates retail trader sentiment data with 3 sub-tabs.

**Sub-tab pattern:** Uses `.rt-subtab` buttons + `.rt-panel` divs + `switchRetailView()`
JS function (same pattern as grand_portfolio's `.gp-subtab`). URL synced via `history.replaceState`.

| Tab | Data Source | Rendering |
|---|---|---|
| Sentiment | CNN Fear & Greed + ApeWisdom top stocks | Server-rendered: gauge + summary cards |
| Leaderboard | ApeWisdom all-stocks (pages 1-5) | Lazy-loaded via `fetch('/api/retail/leaderboard')` |
| Calendar | `_FINANCE_YOUTUBERS` static list in `web.py` | Server-rendered: YouTuber schedule table |

**Summary cards (Sentiment tab):**
- Most Mentioned: ticker with highest mention count
- Biggest Rank Mover: ticker with largest positive rank change (24h)
- Top 5 Trending: links to top 5 tickers by mention count

**Leaderboard features:**
- Sortable columns (Rank, Ticker, Name, Mentions, ΔMentions, Upvotes, Rank Change)
- Accordion: top 25 shown by default, expandable to all ~250 tickers
- Green/red badges for rank changes and mention deltas
- Fear & Greed badge at the top with color-coded mood

### 5.15 Congress Trading Module (`congress_trading.py` + `supabase_cache.py`)

Congressional stock trading data from STOCK Act disclosures, scraped from Capitol Trades.

**Architecture:**
- `congress_trading.py` — Scraper (Capitol Trades) + display preparation functions
- `supabase_cache.py` — Cold archive storage (two tables: `congress_members`, `congress_trades`)
- `congress_trades_prices` — Separate join table for price enrichment (write-once main table has UPDATE trigger protection)

**Data flow:**
```
Capitol Trades → scraper → congress_members + congress_trades (Supabase cold archive)
                                                    ↓
yfinance backfill → congress_trades_prices (forward returns at +30d/90d/180d/365d)

Daily sync (Railway Cron, 24h):
  sync_congress_trades.py → incremental scrape (newest-first, stop on known)
                          → upsert new members + trades
                          → log to congress_sync_log

Notifications (on congress page cache refresh, every 15 min):
  _emit_congress_notifications() → filing-date watermark → create_congress_trade_notification()
                                 → upsert to notifications table
```

**Cold archive protection** (3 levels):
1. Application: `INSERT ... ON CONFLICT DO NOTHING` (no overwrites)
2. Database: `BEFORE UPDATE` trigger on `congress_trades` raises exception
3. Script: `load_congress_history.py` has `--dry-run` and confirmation prompts

**Key files:**
| File | Purpose |
|---|---|
| `congress_trading.py` | Scraper (with `min_trade_date` cutoff) + 6 display prep functions (chamber viz, trending, consensus, momentum, activity, page orchestrator) |
| `supabase_cache.py` (congress section) | 9 query/upsert functions for members, trades, prices, sync log |
| `scripts/sync_congress_trades.py` | Daily incremental sync cron job (Railway Cron Service, 24h cadence) |
| `scripts/backfill_congress_prices.py` | yfinance batch price backfill (50 tickers/batch, 2s sleep) |
| `scripts/load_congress_history.py` | Historical data load from Capitol Trades (default cutoff: 2019-01-01) |
| `sql/002_congress_cold_table_protection.sql` | Write-once trigger protection |
| `sql/003_congress_trades_prices.sql` | Price enrichment join table |
| `sql/004_congress_trades_indexes.sql` | Performance indexes on congress_trades |
| `templates/congress.html` | Main Congress page (3 tabs: Congress, Holdings, Activity) |
| `templates/politician.html` | Politician profile page (stats, portfolio donut chart, trade history) |
| `templates/partials/congress_activity.html` | Activity feed partial (HTMX lazy-loaded) |
| `templates/partials/congress_trending.html` | Homepage trending chart partial (HTMX lazy-loaded) |

**Health monitoring:**
- `/health` — fast probe (no DB calls), returns `"status": "ok"` for UptimeRobot
- `/health/detail` — includes `congress_sync` summary with recent run history, staleness detection (>48h = stale, 3 consecutive errors = failing)
- `congress_sync_log` table tracks every cron run: status, pages scraped, new trades, duration, errors

**Congress page `/congress` — 3-tab structure:**
1. **Congress tab** — Chamber dot visualizations (ECharts scatter). Each dot = one politician, colored by party (blue/red/gray), labeled with 2-letter state code. Hover shows name, party, state, trades, top holdings. Click → politician profile.
2. **Holdings tab** — Trending bar chart (top 15 bought stocks), Consensus Leaders (horizontal bar, top 10 by holders), Recent Momentum (stacked buys/sells), All Holdings sortable table.
3. **Activity tab** — Recent trade filings feed with filter buttons (All/Buys/Sells/House/Senate). HTMX lazy-loaded.

**Politician profile `/politician/{member_id}`:**
- Stats grid (trades, buys, sells, est. net worth, last trade)
- Portfolio donut chart (ECharts, top 10 holdings by estimated value, matching investor.html pattern)
- Estimated Portfolio sortable table
- Full Trade History table

**Caching:**
- `/congress` page: 15-min in-memory TTL cache with asyncio.Lock (thundering-herd protection)
- `/politician/{id}`: 15-min per-politician LRU cache (max 100 entries)
- DB fetches use `asyncio.gather` for concurrent Supabase calls

**Security:**
- All ECharts tooltip HTML uses `window.escHtml()` to prevent stored XSS from database strings
- `member_id` route parameter validated against `^[A-Za-z0-9_-]{1,40}$` regex
- Jinja2 autoescaping active on all templates (no `| safe` usage)

### 5.16 Unusual Options Activity (`unusual_options.py` + `options_sync.py` + `convergence.py`)

Advanced options screener scanning S&P 500 + top superinvestor holdings during market hours.

**Architecture:**
- `unusual_options.py` — Detection engine: `UnusualOption` dataclass, `detect_unusual()`, cluster detection, feed enrichment
- `options_sync.py` — Cron worker: build ticker watchlist → batch fetch chains → detect → upsert to Supabase
- `convergence.py` — Cross-signal analysis incorporating urgency, OTM bias, and cluster boosts
- `tiingo.py` — Primary price source for underlying stock prices (IEX real-time)
- `tradier.py` — Primary options chain source with ORATS greeks; DataFrame adapter for seamless `detect_unusual()` integration

**Data source cascade:**
```
Underlying prices: Tiingo IEX → Tradier quote → yfinance (fallback)
Options chains:    Tradier (greeks) → yfinance (no greeks, fallback)
```

**Detection criteria (all must pass):**
1. Volume ≥ 5× open interest
2. Estimated premium ≥ $100K (`OPTIONS_MIN_PREMIUM` env var, default 100000)
3. Valid contract data (non-zero volume and OI)

**Scoring dimensions:**
| Dimension | Computation | Range |
|---|---|---|
| Urgency | 2.0× for 0-DTE, sliding 1.0–2.0× for ≤7 DTE, 1.0× otherwise | 1.0–2.0 |
| Moneyness | strike/underlying for calls, underlying/strike for puts | 0.8×–1.5× |
| Cluster boost | 1.15× for 2 contracts, 1.30× for 3+ contracts on same ticker | 1.0–1.3 |

**OI delta tracking:**
- `options_oi_snapshots` table stores daily OI per contract
- On each scan, previous day's OI is looked up and delta computed
- New positioning flagged when OI grows ≥50% with volume ≥500

**Env vars:**
| Variable | Default | Description |
|---|---|---|
| `TIINGO_API_KEY` | — | Tiingo API key for real-time IEX prices |
| `TRADIER_API_KEY` | — | Tradier API key for options chains with greeks |
| `TRADIER_SANDBOX` | `true` | `true` for sandbox.tradier.com, `false` for production |
| `OPTIONS_MIN_PREMIUM` | `100000` | Minimum estimated premium filter ($) |

**Cron worker (`options_sync.py`, Railway cron every 30 min):**
1. Check market hours (9:30 AM – 4:30 PM ET, weekdays only)
2. Build ticker watchlist: S&P 500 constituents + top 50 superinvestor holdings
3. Batch fetch chains (5 concurrent, 2s between batches for rate limiting)
4. Detect unusual contracts per ticker
5. Compute OI deltas from previous snapshots
6. Upsert to `unusual_options_activity` table
7. Run cluster detection and cache results
8. Emit notifications for notable trades (≥$500K premium or ≥20× vol/OI)

### 5.17 Tiingo Module (`tiingo.py`)

Real-time IEX stock prices and EOD historical data via Tiingo API ($10/mo).

| Function | TTL | Description |
|---|---|---|
| `get_quote(ticker)` | 5 min | Single IEX real-time quote |
| `get_quotes_batch(tickers)` | 5 min | Batch IEX quotes (up to 100 per call) |
| `get_price(ticker)` | 5 min | Convenience wrapper returning `last` price |
| `get_eod_history(ticker, start, end)` | 1 hr | EOD OHLCV data |
| `get_close_df_for_sp500(tickers)` | 1 hr | Batch historical closes for heatmap (chunks of 50) |

**Integration points:**
- `market_data._ensure_close_df()` — Tiingo-first for S&P 500 close matrix
- `client.get_yfinance_info()` — Tiingo overlay for real-time price fields
- `options_sync._get_underlying_price()` — Tiingo-first for underlying prices
- `web.lifespan()` — Tiingo warm-check on startup

### 5.18 Tradier Module (`tradier.py`)

Options chains with ORATS greeks via Tradier API (free sandbox, production with brokerage).

| Function | TTL | Description |
|---|---|---|
| `get_expirations(ticker)` | 1 hr | Available option expiration dates |
| `get_option_chain(ticker, exp, greeks)` | 5 min | Full chain with ORATS greeks |
| `get_quote(ticker)` | 5 min | Stock quote (fallback for underlying) |
| `chain_to_dataframes(chain)` | — | **Critical adapter**: converts Tradier response to yfinance DataFrame column names |

**DataFrame adapter (`chain_to_dataframes`):**
Maps Tradier field names to yfinance equivalents so `detect_unusual()` works unchanged:
- `symbol` → `contractSymbol`
- `open_interest` → `openInterest`
- `last` → `lastPrice`
- `greeks.mid_iv` → `impliedVolatility`
- `greeks.delta/gamma/theta/vega` → `delta/gamma/theta/vega`

**Sandbox/production toggle:** Zero code changes needed — just swap env vars:
- `TRADIER_API_KEY` → production token
- `TRADIER_SANDBOX` → `false`

### 5.19 Performance Optimizations

**Problem:** With 85 superinvestors, synchronous file I/O was blocking the async event loop.
Per-fund TTL now skips fresh funds during background refresh, reducing API calls significantly.

| Optimization | Before | After |
|---|---|---|
| Cache saves during refresh | Save entire JSON to disk after EVERY fund (84×) | Batch: save every 10 funds, via `asyncio.to_thread` |
| Notification bell polls | Direct DB query on every poll (every tab, every user) | 15-second in-memory `_bell_cache` collapses identical polls; single DB query returns count + latest |
| Watchlist reads | Read `watchlist.json` from disk on every request | In-memory cache (`_watchlist_cache`), disk reads only on cold start |
| HTMX lazy-load (cold start) | All 85 rows fire simultaneously on page load | Staggered: 3 rows per second (`delay:{{ loop.index0 // 3 }}s`) |
| Fund row disk save | Synchronous `save_cache()` blocking response | `asyncio.create_task(asyncio.to_thread(...))` — fire-and-forget |

**In-memory caching pattern** (used by `watchlist.py` and notification bell):
- Watchlist: module-level `_watchlist_cache`, reads disk on cold start only
- Bell: `_bell_cache` dict keyed by `since` timestamp, 15-second TTL, auto-evicts stale keys

### 5.17 Earnings Calendar Module (`earnings_calendar.py`)

Provides a week-by-week or month-level view of upcoming/recent quarterly earnings reports with BMO/AMC timing.

**Feature flag:** `EARNINGS_CALENDAR_ENABLED` env var (default `"1"`). When `"0"`, the main page returns `under_construction.html`.

**Architecture:**

```
User visits /earnings-calendar
         │
         ▼
earnings_calendar.html (main page — JS + HTMX controls)
         │ hx-get on load
         ▼
/api/earnings-calendar/grid?view=weekly&offset=0
         │
         ▼
earnings_calendar.get_earnings_calendar(start, end)
  ├── L1: in-memory _cal_cache (1h TTL, keyed by "start:end", max 50)
  ├── L2: Finnhub /calendar/earnings
  │        └── via earnings.fetch_finnhub_calendar_raw() (shared 1h cache)
  ├── L3: FMP /stable/earnings-calendar (fallback)
  │        └── via fmp_cache.get_earnings_in_range() (shared bulk cache, 6h TTL)
  └── L4: deterministic mock data (dev-only, no API keys)
         │
         ▼
_enrich_entries()
  └── _get_company_lookup()
       └── earnings_scorecard._build_company_lookup("sp500")
            └── market_data.get_sp500_constituents() (24h cache)
         │
         ▼
_build_response() → {entries, by_date, weeks, stats, range, source}
         │
         ▼
Render partials/earnings_calendar_grid.html
```

**Module dependency graph:**

```
earnings_calendar.py
  ├── imports from earnings.py:
  │   ├── fetch_finnhub_calendar_raw()  — shared Finnhub raw fetch (1h cached)
  │   └── _fmt_revenue()               — revenue formatting ($94.2B, $12.3M, etc.)
  ├── imports from fmp_cache.py:
  │   ├── get_earnings_in_range()     — shared FMP bulk cache (6h TTL)
  │   ├── actual_eps() / actual_rev() — field-name resolution helpers
  └── imports from earnings_scorecard.py:
      └── build_company_lookup()      — S&P 500 {ticker: {name, sector}} dict
```

**No circular imports.** All cross-module imports are lazy (inside function bodies), so modules load independently.

**Key functions:**

| Function | Description |
|---|---|
| `get_earnings_calendar(start, end, weeks)` | Main entry point — returns structured calendar dict |
| `get_week_view(target_date)` | Convenience: single Mon–Fri week centered on a date |
| `get_month_view(year, month)` | Convenience: full calendar month for heatmap view |
| `_fetch_finnhub_calendar(start, end)` | Parse raw Finnhub JSON into normalized entries |
| `_fetch_fmp_calendar(start, end)` | Parse raw FMP JSON into normalized entries |
| `_enrich_entries(entries)` | Add company name, sector, formatted estimates, beat/miss |
| `_build_response(entries, start, end, source)` | Build weeks structure, stats, sort by date/timing |
| `_build_mock_entries(start, end)` | Deterministic mock data for dev mode (30 companies, hash-based) |

**Normalized entry schema (internal):**

```python
{
    "ticker": str,           # "AAPL"
    "date": str,             # "2026-03-10"
    "timing": str,           # "bmo" | "amc" | "unknown"
    "eps_estimate": float,   # 1.58
    "eps_actual": float,     # None until reported
    "revenue_estimate": float,  # 98_300_000_000
    "revenue_actual": float,
    "year": int,
    "quarter": int,
    "confirmed": bool,       # True if Finnhub has timing data
    # Added by _enrich_entries():
    "name": str,             # "Apple Inc."
    "sector": str,           # "Technology"
    "eps_estimate_fmt": str,  # "$1.58"
    "revenue_estimate_fmt": str,  # "$98.3B"
    "eps_actual_fmt": str,
    "revenue_actual_fmt": str,
    "beat_eps": bool | None,
    "beat_revenue": bool | None,
}
```

**Caching strategy:**

| Cache | Location | TTL | Key | Max Size |
|---|---|---|---|---|
| `_cal_cache` (enriched entries) | `earnings_calendar.py` | 1h | `"start:end"` | 50 |
| `_finnhub_raw_cache` (shared raw API) | `earnings.py` | 1h | `"start:end"` | 50 |
| `_finnhub_cal_cache` (parsed revenue) | `earnings.py` | 6h | global (single) | 1 |
| S&P 500 constituents | `market_data.py` | 24h | global (single) | 1 |

The two Finnhub caches (`_finnhub_raw_cache` at 1h and `_finnhub_cal_cache` at 6h) intentionally have different TTLs. The raw cache is shared between the calendar page and revenue enrichment. The parsed revenue cache lives longer because revenue data rarely changes intra-day. When the 6h cache expires, it re-fetches through the raw layer (which may serve from its own 1h cache).

**Shared functions (reuse points):**

These functions are shared across earnings modules:

| Function | Module | Used By |
|---|---|---|
| `get_bulk_earnings()` | `fmp_cache.py` | `earnings.py`, `earnings_scorecard.py`, `earnings_calendar.py` (all FMP data) |
| `get_earnings_in_range(start, end)` | `fmp_cache.py` | `earnings_scorecard.py`, `earnings_calendar.py` (date-filtered FMP data) |
| `get_revenue_for_ticker(ticker)` | `fmp_cache.py` | `earnings.py` (per-ticker revenue enrichment) |
| `actual_eps(item)` / `actual_rev(item)` | `fmp_cache.py` | `earnings_scorecard.py`, `earnings_calendar.py` (field-name resolution) |
| `fetch_finnhub_calendar_raw(start, end)` | `earnings.py` | `earnings_calendar.py`, `earnings.py` (revenue enrichment) |
| `_fmt_revenue(val)` | `earnings.py` | `earnings_calendar.py`, `earnings.py` (display formatting) |
| `_build_company_lookup(index)` | `earnings_scorecard.py` | `earnings_calendar.py`, `earnings_scorecard.py` (enrichment) |

**Web routes (web.py, lines 3694–3859):**

| Route | Query Params | Handler Logic |
|---|---|---|
| `GET /earnings-calendar` | — | Feature flag check → render full page or under_construction |
| `GET /api/earnings-calendar/grid` | `view` (weekly\|monthly), `offset` (int, default 0) | Compute date range from offset → `get_earnings_calendar()` or `get_month_view()` → render grid partial |
| `GET /api/earnings-calendar/day` | `date` (YYYY-MM-DD, defaults to today) | `get_week_view(date)` → filter entries for target date → render day partial |

**HTMX interaction flow:**

```
1. Page load → earnings_calendar.html renders controls + empty #ec-content div
2. hx-trigger="load" → HTMX fires GET /api/earnings-calendar/grid?view=weekly
   → grid partial replaces #ec-content innerHTML
3. View toggle (Weekly/Monthly buttons) → JS ecSwitchView()
   → resets offset to 0, re-fires HTMX via ecLoadGrid()
4. Navigation arrows (← →) → JS ecNavigate(±1)
   → increments/decrements offset, re-fires HTMX
5. "Today" button → JS ecGoToday()
   → resets offset to 0, re-fires HTMX
6. Click day card (weekly) or heatmap cell (monthly) → JS ecShowDayDetail(date)
   → HTMX GET /api/earnings-calendar/day?date=...
   → day detail table slides in below grid in #ec-day-detail
7. Hover company logo → JS ecShowHoverCard(event, el)
   → reads data-* attributes → positions 280px tooltip card
   → shows company name, sector, EPS/revenue estimates, actuals, beat/miss badges
8. Click company logo → navigates to /stock/{ticker}
```

**Template structure:**

| Template | Content |
|---|---|
| `earnings_calendar.html` | Full page: controls bar (view toggle, nav arrows, "Today" button), `#ec-content` HTMX container, `#ec-day-detail` panel, JS functions |
| `earnings_calendar_grid.html` | **Weekly**: stats bar + 5-day cards (BMO/AMC/TBD logo sections, overflow counter, hover cards). **Monthly**: 7-column heatmap grid (5 color intensities by report count, mini logos, clickable, weekends faded) |
| `earnings_calendar_day.html` | Scrollable table: Company (logo + name), Ticker (linked to /stock/), Timing badge (BMO green / AMC blue / TBD gray), Est. Revenue, Est. EPS, Actual EPS (conditional column), Beat/Miss badge (green/red) |

**Logo rendering:**
- Company logos lazy-loaded from `/api/logo/{ticker}.png` (existing logo proxy)
- Grid view: 24×24px logos, hover card: 32×32px
- Fallback: 2-letter ticker abbreviation in colored circle
- `logo_tickers` set passed from route handler to template for conditional rendering

**Mock data (dev mode):**

When both `FINNHUB_API_KEY` and `FMP_API_KEY` are unset, the calendar generates deterministic mock entries using `_MOCK_EARNINGS` (30 well-known S&P 500 companies). The MD5 hash of each date determines how many companies report (2–6 per weekday), ensuring consistent output across page reloads. Mock entries have estimates but no actuals (all beat/miss fields are None).

---

## 6. Web Routes

| Method | Path | Handler | Data Source | Template |
|---|---|---|---|---|
| GET | `/` | `homepage` | Cache + Panda Fund env vars + Stripe IDs | `home.html` |
| GET | `/retail` | `retail_page` | CNN, ApeWisdom, YouTubers (static) | `retail.html` |
| GET | `/funds` | `funds_page` | Cache only | `grand_portfolio.html` |
| GET | `/insider-trading` | `insider_trading_page` | — (JS lazy-loads) | `insider_trading.html` |
| GET | `/grand-portfolio` | `grand_portfolio_redirect` | 301 redirect → `/funds` | — |
| GET | `/superinvestors` | `superinvestors_page` | 301 redirect → `/funds?view=funds` | — |
| GET | `/api/fund-row/{cik}` | `fund_row` | Cache first → SEC on miss (L2 Supabase fallback) | `partials/fund_row.html` |
| GET | `/search` | `search_page` | SEC API (live) | `search.html` |
| GET | `/holdings/{cik}` | `holdings` | Cache first → SEC on miss | `investor.html` |
| GET | `/compare/{cik}` | `compare` | Redirect | → `/holdings/{cik}` (302) |
| GET | `/api/compare/{cik}` | `compare_api` | Cache first → SEC on miss | `partials/compare_content.html` |
| GET | `/activity` | `activity_feed` | Cache only | `activity.html` |
| GET | `/stock/{ticker}` | `stock_detail` | Cache only | `stock.html` |
| GET | `/stock/cusip/{cusip}` | `stock_detail_by_cusip` | Cache only | `stock.html` |
| GET | `/api/analysts/{ticker}` | `analyst_ratings` | yfinance + Finnhub (live, 5-min cache) | `partials/analyst_ratings.html` |
| GET | `/api/sentiment/{ticker}` | `sentiment_data` | CNN, Finnhub, ApeWisdom, Alpha Vantage | `partials/sentiment.html` |
| GET | `/api/signals/{ticker}` | `signals_data` | Sentiment + Google Trends + Web Traffic (parallel) | `partials/signals.html` |
| GET | `/api/vitals/{ticker}` | `vitals_data` | Glassdoor, PDL, Apple iTunes | `partials/vitals.html` |
| GET | `/api/company-filings/{ticker}` | `company_filings_tab` | SEC EDGAR | `partials/company_filings.html` |
| GET | `/api/insider-trades` | `insider_trades_api` | Supabase → OpenInsider → stale L1 | `partials/insider_trades.html` |
| GET | `/api/insider-trades/{ticker}` | `stock_insider_trades_api` | Hot→OI screener→Cold→stale L1 + title resolution | `partials/stock_insider_trades.html` |
| GET | `/api/retail/leaderboard` | `retail_leaderboard_api` | ApeWisdom + CNN Fear & Greed | `partials/retail_leaderboard.html` |
| GET | `/api/ticker-search-index` | `ticker_search_index` | NASDAQ Trader + S&P 500 + cache | JSON response |
| GET | `/api/live-activity` | `live_activity_api` | Supabase `notifications` (5-min HTML cache) | `partials/live_activity.html` |
| GET | `/api/heatmap` | `heatmap` | yfinance (30-min cache) + Wikipedia | `partials/heatmap.html` |
| GET | `/api/most-added` | `most_added` | Cache + analysts + yfinance | `partials/most_added.html` |
| POST | `/api/watchlist/{ticker}` | `watchlist_add` | Watchlist JSON | `partials/watchlist_response.html` |
| DELETE | `/api/watchlist/{ticker}` | `watchlist_remove` | Watchlist JSON + Cache | `partials/watchlist_response.html` or `partials/watchlist_sidebar.html` |
| GET | `/api/watchlist-sidebar` | `watchlist_sidebar_refresh` | Watchlist JSON | `partials/watchlist_sidebar.html` |
| GET | `/api/notifications/bell` | `notification_bell` | Supabase `notifications` | `partials/notification_bell.html` |
| GET | `/api/notifications/recent` | `notification_recent` | Supabase `notifications` | `partials/notification_dropdown.html` |
| GET | `/api/notifications/count` | `notification_count` | Supabase `notifications` | JSON `{"count": N}` |
| GET | `/notifications` | `notifications_page` | Supabase `notifications` | `notifications.html` |
| GET | `/support` | `support_page` | Env vars (PANDA_FUND_RAISED, Stripe IDs) | `support.html` |
| GET | `/deployment` | `deployment_page` | Deployment cache | `deployment.html` |
| GET | `/api/deployment-leaderboard` | `deployment_leaderboard_partial` | Deployment cache | `partials/deployment_leaderboard.html` |
| GET | `/api/deployment/{cik}` | `deployment_card_partial` | Deployment cache | `partials/deployment_card.html` |
| POST | `/api/deployment/sync` | `trigger_deployment_sync` | ADV + XBRL + 13F (background) | JSON response |
| GET | `/congress` | `congress_page` | Supabase (15-min cache) | `congress.html` |
| GET | `/politician/{member_id}` | `politician_page` | Supabase (15-min LRU cache) | `politician.html` |
| GET | `/api/stock/{ticker}/congress` | `stock_congress_api` | Supabase | `partials/stock_congress.html` |
| GET | `/api/congress-activity` | `congress_activity_api` | Congress page cache | `partials/congress_activity.html` |
| GET | `/api/congress-trending` | `congress_trending_api` | Congress page cache | `partials/congress_trending.html` |
| GET | `/api/financials/{ticker}` | `api_financials` | SEC XBRL (L1+L2 cache) | `partials/financials.html` |
| GET | `/api/financials/{ticker}/history` | `api_financials_history` | Cold storage (full history) | `partials/financials.html` |
| GET | `/api/options/clusters` | `options_clusters_api` | Unusual options cache | `partials/options_clusters.html` |
| GET | `/screener` | `screener_page` | Clerk auth gate (client-side blur overlay) | `screener.html` |
| GET | `/api/screener/{ticker}` | `api_screener` | yfinance + SEC XBRL + Tiingo (parallel) | JSON response |
| GET | `/api/screener/peers` | `api_screener_peers` | yfinance + Tiingo + market_data (parallel batch) | JSON response |
| GET | `/macro` | `macro_page` | Screener auth gate → macro dashboard | `screener_gate.html` or `macro.html` |
| POST | `/macro/auth` | `macro_auth` | Password check → set `scr_auth` cookie (30d) | Redirect → `/macro` or `screener_gate.html` |
| GET | `/api/macro/scorecard` | `macro_scorecard_api` | earnings_scorecard (L1+L2 cache) | `partials/earnings_scorecard.html` |
| GET | `/api/macro/breadth` | `macro_breadth_api` | market_breadth (yfinance) | `partials/market_breadth.html` |
| GET | `/api/macro/calendar` | `macro_calendar_api` | earnings_scorecard calendar | `partials/earnings_calendar_macro.html` |
| GET | `/api/macro/economic` | `macro_economic_api` | fred_calendar (FRED+Finnhub+FMP merged) | `partials/economic_dashboard.html` |
| GET | `/api/macro/indicators` | `macro_indicators_api` | fred_indicators + fred_data (sparklines + charts) | `partials/macro_indicators.html` |
| GET | `/api/macro/volatility` | `macro_volatility_api` | cboe_data (P/C, VIX, SKEW) | `partials/macro_volatility.html` |
| GET | `/api/macro/fred` | `macro_fred_api` | fred_data (GDP, CPI, rates) | `partials/macro_economic.html` |
| GET | `/api/macro/treasury` | `macro_treasury_api` | treasury_data (yield curve, debt) | `partials/macro_treasury.html` |
| GET | `/api/macro/fx` | `macro_fx_api` | frankfurter (FX rates) | `partials/macro_fx.html` |
| GET | `/api/options/ivrank` | `options_ivrank_api` | cboe_data (IV rank batch) | `partials/options_ivrank.html` |
| GET | `/api/stock/{ticker}/wsb` | `stock_wsb_api` | wsb_sentiment (ApeWisdom) | `partials/stock_wsb_sentiment.html` |
| GET | `/earnings-calendar` | `earnings_calendar_page` | Feature flag check | `earnings_calendar.html` (or `under_construction.html` if disabled) |
| GET | `/api/earnings-calendar/grid` | `earnings_calendar_grid_api` | Finnhub → FMP → mock (1h cache) | `partials/earnings_calendar_grid.html` |
| GET | `/api/earnings-calendar/day` | `earnings_calendar_day_api` | Same as grid (from cached calendar data) | `partials/earnings_calendar_day.html` |
| GET | `/api/logo/{ticker}.png` | `logo_proxy` | Logo cache / external fetch | PNG image |
| POST | `/refresh` | `trigger_refresh` | SEC API (background) | Raw HTML response |
| GET | `/watchlist` | `watchlist_page` | — (JS lazy-loads) | `watchlist.html` |
| POST | `/api/watchlist` | `api_watchlist_add` | Supabase `user_watchlist` | JSON response |
| DELETE | `/api/watchlist/{ticker}` | `api_watchlist_remove` | Supabase `user_watchlist` | JSON response |
| GET | `/api/watchlist` | `api_watchlist_list` | Supabase + notifications enrichment | JSON response |
| GET | `/api/watchlist/check/{ticker}` | `api_watchlist_check` | Supabase `user_watchlist` (point query) | JSON response |
| GET | `/api/watchlist/preferences` | `api_watchlist_prefs_get` | Supabase `user_notification_preferences` | JSON response |
| PUT | `/api/watchlist/preferences` | `api_watchlist_prefs_update` | Supabase `user_notification_preferences` | JSON response |
| GET | `/admin` | `admin_dashboard` | Supabase (6 parallel queries, admin-only, 404 for non-admin) | `admin.html` |
| GET | `/admin/user/{user_id}` | `admin_user_detail_page` | Supabase (admin-only, 404 for non-admin) | `admin_user.html` |

**Key patterns:**
- All endpoints are cache-first. SEC EDGAR is only called on cache miss or during background refresh.
- Fund data endpoints (`/api/fund-row`, `/api/holdings`, `/api/compare`) have L2 Supabase fallback with L1 promotion when L1 misses.
- Fund data endpoints trigger self-healing background refresh when stale data is detected (request-triggered via `_trigger_single_refresh`).
- Insider trade endpoints use 4-tier fallback: L1 fresh → L2 Supabase → L3 scrape → L4 stale L1 (never empty).
- Backward-compat redirects: `/grand-portfolio` → `/funds` (301), `/superinvestors` → `/funds?view=funds` (301).
- Old CLI watchlist routes read/write to `~/.13f-cache/watchlist.json` (legacy, separate from fund cache).
- New watchlist API routes use Supabase `user_watchlist` table (Clerk auth required, user_id from JWT `sub` claim).
- Admin routes return 404 for non-admin users (checked via `admin_users` table, 5-min in-memory TTL cache).
- Exception handlers detect HTMX requests (`HX-Request` header) and API paths to return inline `data_error.html` partial instead of full error pages.
- `/support` and homepage widget use Stripe OOTB web components (Buy Button + Pricing Table) -- zero backend Stripe SDK needed.
- `/health` endpoint is a fast probe (no DB calls) returning `"status": "ok"` for UptimeRobot. `/health/detail` includes `stale_funds`, `refresh_status`, `refresh_progress`, `vitals_cache`, and `congress_sync` diagnostics.

---

## 7. Templates

### Styling System (in base.html `<style>`)

#### Dark Mode — CSS Custom Properties

The entire color system is defined as CSS custom properties (design tokens) on `:root`,
with separate values for light and dark themes. Components reference tokens via `var(--pp-*)`;
never use raw hex values.

| Token | Light | Dark | Purpose |
|---|---|---|---|
| `--pp-surface` | `#fff` | `#1a1a1e` | Card / page background |
| `--pp-surface-alt` | `#fafafa` | `#222226` | Table header, alternating rows |
| `--pp-surface-hover` | `#f4f4f5` | `#2a2a2e` | Row hover |
| `--pp-border` | `#e4e4e7` | `#333338` | Primary borders |
| `--pp-border-light` | `#f4f4f5` | `#2a2a2e` | Subtle borders / dividers |
| `--pp-text` | `#18181b` | `#e4e4e7` | Primary text |
| `--pp-text-secondary` | `#27272a` | `#d4d4d8` | Body / table cell text |
| `--pp-text-muted` | `#71717a` | `#a1a1aa` | Secondary / placeholder text |
| `--pp-text-faint` | `#a1a1aa` | `#71717a` | Tertiary / disabled text |
| `--pp-nav-bg` | `rgba(255,255,255,0.85)` | `rgba(24,24,27,0.85)` | Sticky nav backdrop |
| `--pp-support-btn-bg` | `#18181b` | `#1976d2` | "Support the Panda" button |
| `--pp-tag-bg/text/hover` | zinc-100 tones | zinc-800 tones | Tag pills |
| `--pp-toast-bg/text` | dark | gray | Notification toast cards |
| `--pp-card-shadow` | light box-shadow | heavier box-shadow | Card depth |
| `--pp-overlay-{5,8,10,35,40,50,65}` | `rgba(0,0,0,N)` | `rgba(255,255,255,N)` | Semi-transparent overlays |

**Theme toggle (`.theme-toggle`):**
- A 52px wide pill button in the navbar with a sliding `.theme-toggle-thumb` (sun/moon icon)
- Clicking it flips `data-theme` on `<html>` and persists the choice to `localStorage('pp-theme')`
- On first visit, defaults to the OS `prefers-color-scheme` setting
- Anti-FOUC: an inline `<script>` at the top of `<head>` reads localStorage/media-query and sets `data-theme` before any CSS renders

**Logo swap:** Two `<img>` tags are rendered side-by-side in the nav:
- `.logo-light` (`logo-nav.png`) — visible in light mode via `display:none` on `[data-theme="dark"]`
- `.logo-dark` (`logo-nav-dark.png`) — visible in dark mode

**TradingView widgets:** When the theme changes, all TradingView `<iframe>` embed containers
are cleared and the widget script is re-injected with the new `theme` param, so charts
always match the active theme.

**Stripe Pricing Table:** Stripe's web component doesn't natively support a `theme` prop,
so in dark mode a CSS `filter: invert(0.85) hue-rotate(180deg)` is applied to the
`<stripe-pricing-table>` element (toggled via a `dark-stripe` class on the container).

| CSS Class / Selector | Purpose |
|---|---|
| `.theme-toggle` | 52px pill button for light/dark switch |
| `.theme-toggle-thumb` | Sliding circle with icon; `transform: translateX(24px)` in dark |
| `.logo-light` / `.logo-dark` | Show/hide logos based on `[data-theme]` |
| `.dark-stripe` on Stripe container | Applies CSS filter inversion to Stripe embeds in dark mode |
| `.badge` | Inline-block pill label for activity |
| `.badge-new` | Green background: NEW BUY |
| `.badge-add` | Green background: ADD |
| `.badge-reduce` | Red background: REDUCE |
| `.badge-sold` | Red background: SOLD |
| `.activity-new-buy` / `.activity-add` | Green text for positive changes |
| `.activity-reduce` / `.activity-sold` | Red text for negative changes |
| `.ticker` | Monospace bold for ticker symbols |
| `.tag` / `.tag-list` | Small blue pills for top holdings lists |
| `.text-right` | Right-aligned table cells |
| `.text-muted` | Dimmed secondary text |
| `.spinner` | CSS-only loading animation |
| `.app-layout` | Flexbox container: main + watchlist sidebar |
| `#watchlist-sidebar` | 200px sticky sidebar, border-left, collapses on mobile |
| `.watchlist-list` | Unstyled `<ul>` for ticker items |
| `.watchlist-item` | Flex row: ticker link + × remove button |
| `.watchlist-ticker` | Bold 0.9em ticker in sidebar |
| `.watchlist-remove` | Red × button, 50% opacity → 100% on hover |
| `.star-btn` | Star toggle button (border, gray) |
| `.star-btn.starred` | Filled gold star (★) |
| `.pp-bell-btn` | Notification bell button in navbar (relative positioned) |
| `.pp-bell-dot` | 8px red circle indicator on bell (absolute positioned) |
| `.pp-notif-dropdown` | Dropdown container (absolute, 340px wide, max-height 480px) |
| `.pp-notif-item` | Clickable notification entry in dropdown |
| `.pp-notif-new` | Left border highlight on new (unseen) notification items |
| `.pp-notif-footer` | "View all notifications →" link at bottom of dropdown |
| `.pp-toast` | Slide-in toast card (fixed bottom-right, auto-dismiss 8s, max 2 visible) |
| `.pp-toast-icon` / `.pp-toast-title` / `.pp-toast-msg` | Toast content elements (textContent, XSS-safe) |
| `@keyframes ppToastIn` / `ppToastOut` | Toast entrance/exit animations |
| `.notif-chip` | Filter pill on /notifications page (All, 13F, YouTube, Reddit) |
| `.notif-chip--active` | Active filter chip (colored background) |
| `.notif-page-item` | Clickable notification entry on full history page |
| `th[data-sort]` | Sortable column header (cursor:pointer, hover highlight) |
| `.sort-indicator` | Sort direction arrow (▲/▼/▴) appended to sortable headers |

### Template Hierarchy

```
base.html (nav: Home|Retail|Funds|Insiders|Support the Panda + 🔔 bell + styles + HTMX + Chart.js + ECharts + Fuse.js CDN + sidebar + sortable tables + toast system)
  ├── includes partials/watchlist_sidebar.html (in <aside> via hx-preserve)
  ├── includes partials/notification_bell.html (in <nav>, HTMX-polls every 120s)
  ├── includes partials/notification_dropdown.html (loaded on bell click via HTMX)
  ├── includes partials/ticker_search.html (in <nav>, Fuse.js fuzzy autocomplete)
  ├── home.html (homepage)
  │     ├── Live Activity / Market News feed toggle (lazy-loads partials/live_activity.html via /api/live-activity)
  │     ├── lazy-loads partials/heatmap.html via HTMX (/api/heatmap) — Sectors/Companies + 1D/1W/1M toggles
  │     ├── lazy-loads partials/most_added.html via HTMX (/api/most-added)
  │     ├── 4 quick-access cards: Retail, Funds, Insiders, Congress
  │     └── Panda Fund support widget: progress bar + Stripe Pricing Table embed
  ├── retail.html (sub-tabs: Sentiment | Leaderboard | Calendar)
  │     ├── Sentiment tab: CNN Fear & Greed gauge + summary cards (server-rendered)
  │     ├── Leaderboard tab: lazy-loads partials/retail_leaderboard.html via fetch(/api/retail/leaderboard)
  │     └── Calendar tab: static YouTuber table (server-rendered)
  ├── grand_portfolio.html (URL: /funds, sub-tabs: Funds | Holdings | Activity | Deployment)
  │     ├── Funds tab: HTMX lazy-loads partials/fund_row.html for each of 84 investors
  │     ├── Holdings tab: aggregated cross-fund portfolio
  │     ├── Activity tab: recent buys/sells across all funds
  │     └── Deployment tab: lazy-loads partials/deployment_leaderboard.html via HTMX
  ├── deployment.html (Capital Deployed standalone page)
  │     └── Sortable table: AUM, 13F equity, deployment bars, cash estimates, sources
  ├── insider_trading.html (global insider trades screener)
  │     └── lazy-loads partials/insider_trades.html via fetch(/api/insider-trades)
  ├── options page (/options): unusual options activity screener
  │     ├── partials/options_feed.html — main table with OI Δ, Moneyness, Delta columns
  │     └── partials/options_clusters.html — clustered unusual options cards (via /api/options/clusters)
  ├── congress.html (URL: /congress, sub-tabs: Congress | Holdings | Activity)
  │     ├── Congress tab: ECharts scatter dot viz for Senate + House (state labels, party colors)
  │     ├── Holdings tab: Trending bar chart, Consensus Leaders, Momentum, All Holdings table
  │     └── Activity tab: lazy-loads partials/congress_activity.html via HTMX
  ├── politician.html (URL: /politician/{member_id})
  │     ├── Stats grid + ECharts donut portfolio chart (top 10 holdings)
  │     ├── Estimated Portfolio sortable table
  │     └── Trade History sortable table
  ├── search.html
  ├── investor.html (Tabbed: Holdings + Compare Quarters, lazy-loads compare)
  │     ├── imports partials/ticker_link.html (macro)
  │     ├── lazy-loads partials/compare_content.html via fetch(/api/compare/{cik})
  │     └── lazy-loads partials/deployment_card.html via HTMX (/api/deployment/{cik})
  ├── activity.html
  │     └── imports partials/ticker_link.html (macro)
  ├── stock.html (8 Tabs: Overview, Ownership, Analysts, Signals, Vitals, Filings, Insider, Congress)
  │     ├── Heart button (.pp-watch-heart): optimistic toggle, Clerk auth check, POST/DELETE /api/watchlist
  │     ├── includes partials/watchlist_star.html
  │     ├── lazy-loads partials/analyst_ratings.html via fetch(/api/analysts/{ticker})
  │     ├── lazy-loads partials/signals.html via fetch(/api/signals/{ticker}) — parallel fetch of sentiment + Google Trends + web traffic
  │     ├── lazy-loads partials/vitals.html via fetch(/api/vitals/{ticker})
  │     ├── lazy-loads partials/company_filings.html via fetch(/api/company-filings/{ticker})
  │     └── lazy-loads partials/stock_insider_trades.html via fetch(/api/insider-trades/{ticker})
  │           ├── Insider summary cards row (scrollable, with hover tooltip showing quarterly breakdown)
  │           ├── Chart.js stacked bar chart (buy/sell values per quarter, chronological)
  │           ├── Filter bar: All|Buys|Sells buttons + per-insider dropdown select
  │           └── Quarterly <details> sections (collapsible, newest-first, trade tables with data-insider-name/type attrs)
  ├── support.html (Panda Fund transparency dashboard)
  │     ├── Progress bar with goal-reached badge ($400 monthly goal, capped on frontend)
  │     ├── Stripe Buy Button (one-time) + Pricing Table (recurring) with tab toggle
  │     ├── Cost breakdown (line items, no dollar amounts), funding history ECharts bar chart
  │     ├── YouTube @funofinvesting section + feedback CTA
  │     └── Auto-switches to monthly tab when linked from homepage (#monthly hash)
  ├── notifications.html (notification history, ♥ My Watchlist filter tab, heart badges on watched tickers)
  ├── watchlist.html (URL: /watchlist, auth-gated dashboard with stock cards, signal enrichment, settings panel)
  │     └── includes partials/watchlist_preferences.html (notification preferences form: signal toggles, insider filters, digest settings)
  ├── admin.html (URL: /admin, admin-only dashboard: watchlist overview, notification prefs stats, user list, digest monitor)
  ├── admin_user.html (URL: /admin/user/{id}, admin-only user detail: watchlist, prefs, digest history)
  └── error.html
```

### Ticker Link Macro (`partials/ticker_link.html`)

```jinja2
{% macro ticker_link(ticker, cusip=None) %}
  {% if ticker %}
    <a href="/stock/{{ ticker }}" class="ticker">{{ ticker }}</a>
  {% elif cusip %}
    <a href="/stock/cusip/{{ cusip }}" class="ticker">{{ cusip[:6] }}</a>
  {% else %}
    <span class="text-muted">-</span>
  {% endif %}
{% endmacro %}
```

Used in: `investor.html`, `activity.html`, `grand_portfolio.html`.

### Interactive Chart (`stock.html` inline script)

**Library:** Chart.js v4 loaded via CDN in `base.html` `<head>`
**CDN:** `https://cdn.jsdelivr.net/npm/chart.js@4/dist/chart.umd.min.js` (~60KB gzipped)

The stock detail page includes an interactive stacked bar chart showing quarterly
buy/sell activity. The chart sits between the navigation buttons and the holders table.

**Two view modes (toggled via button):**

| View | Datasets | Description |
|---|---|---|
| Simple (default) | 2 datasets | Green bar = total adds, red bar = total reduces per quarter |
| Detailed | N datasets | Each investor is a separate stacked segment, green/red shaded |

**Key design decisions:**
- Diverging bars: adds stack upward (positive Y), reduces stack downward (negative Y)
- `share_change` is already signed from the backend (negative for sells/reduces)
- Two separate Chart.js `stack` groups (`"adds"` / `"reduces"`) in detailed view
- Color palette interpolates dark→light green/red for per-investor segments
- Data serialized from Jinja2 via `history | reverse` (chronological, oldest-left)
- Script is inline in `{% block content %}` so HTMX re-executes it on boosted nav
- Zero-line is emphasized with thicker/darker grid line
- Y-axis abbreviated ("1.2M", "450K"); tooltips show full numbers with commas

**Tooltips:**
- Simple: "Total Adds: +1,234,567 shares (3 investors)" + net change line
- Detailed: "Warren Buffett: +1,234,567 shares" per segment
- Zero values hidden from tooltip

**Chart hidden** when `history` is empty (wrapped in `{% if history %}`).

### Watchlist Sidebar

**Persistence:** `~/.13f-cache/watchlist.json` (same directory as fund cache)

The watchlist is a right-side sidebar that appears on every page, showing starred tickers.
Users add tickers from stock detail pages (star ☆/★ button) and remove them from the
sidebar (× button) or by un-starring. Clicking a ticker navigates to its stock page.

**Architecture:**

| Component | Implementation |
|---|---|
| Storage | `watchlist.py` — JSON file, atomic writes, same pattern as `cache.py` |
| Template globals | `get_watchlist()` and `is_in_watchlist()` registered as Jinja2 globals |
| Sidebar | `base.html` includes `watchlist_sidebar.html` in `<aside hx-preserve>` |
| Star button | `watchlist_star.html` included in `stock.html` `.actions` div |
| HTMX updates | OOB swaps via `watchlist_response.html` (star + sidebar in one response) |
| Layout | `.app-layout` flexbox: `<main>` (flex:1) + `<aside>` (200px sticky) |
| Mobile | `@media (max-width: 768px)` — sidebar collapses below content |

**Watchlist JSON schema** (`~/.13f-cache/watchlist.json`):
```json
{
  "tickers": [
    {
      "ticker": "AAPL",
      "cusip": "037833100",
      "issuer_name": "APPLE INC",
      "added_at": "2026-02-16T10:30:00"
    }
  ]
}
```

**HTMX interaction flow:**

```
Star button click (Add):
  POST /api/watchlist/AAPL  →  watchlist_response.html
    ├── Primary swap: filled star ★ replaces #watchlist-star innerHTML
    └── OOB swap: updated sidebar replaces #watchlist-sidebar-content innerHTML

Star button click (Remove):
  DELETE /api/watchlist/AAPL (HX-Target: watchlist-star)
    ├── Primary swap: outline star ☆ replaces #watchlist-star innerHTML
    └── OOB swap: updated sidebar replaces #watchlist-sidebar-content innerHTML

Sidebar × button click:
  DELETE /api/watchlist/AAPL (HX-Target: watchlist-sidebar-content)
    └── Returns sidebar HTML only (no star button update)
```

**Key design decisions:**
- `hx-preserve` on `<aside>` keeps sidebar DOM across HTMX boosted navigation
- Jinja2 globals avoid modifying all 10+ route handlers to pass watchlist data
- Reads from disk on each request (file is tiny, <1KB) — no in-memory state needed
- CUSIP-only stocks (no ticker) do not show the star button
- `add_to_watchlist()` is idempotent — adding the same ticker twice is a no-op
- DELETE route checks `HX-Target` header to determine response format

### Notification System

**Persistence:** Supabase `notifications` table (global, no per-user rows).
Dismiss state tracked client-side via `localStorage('pp-notifications-last-seen')`.

The notification system creates global notifications from three sources (13F filing
changes, YouTube uploads, Reddit velocity spikes) and delivers them via a bell icon
in the navbar with a dropdown preview and toast popups. No login required, no SSE,
no browser push notifications.

**Architecture:**

| Component | Implementation |
|---|---|
| 13F Detection | `sync_worker.py` — after each fund refresh, compare old vs new holdings → `detect_13f_changes()` |
| YouTube Detection | `youtube_sync.py` — after upserting events, create notifications for impact_score ≥ 7 |
| Reddit Detection | `web.py` background task — every 30 min, scan `reddit_sentiment` for velocity > 100% |
| Persistence | Supabase `notifications` table, `upsert` with `ignore_duplicates=True` |
| Deduplication | Deterministic IDs via PRIMARY KEY: `13f-{cik}-{date}-{cusip}`, `yt-{video_id}`, `reddit-{ticker}-{date}` |
| Bell indicator | `notification_bell.html` — HTMX polls every 120s, red dot (on/off) |
| Dropdown | `notification_dropdown.html` — latest 8 notifications, loaded on bell click via HTMX |
| Toast popups | `ppToast()` in `base.html` — triggered by `HX-Trigger` header, max 2 visible, auto-dismiss 8s |
| History page | `notifications.html` — paginated (30/page), type filter chips (All/13F/YouTube/Reddit) |
| Retention | 48 hours — `cleanup_old_notifications(days=2)` runs at start of each sync cycle |
| First-visit | `first_visit=1` query param → always show red dot so new users discover the bell |
| Server-side cache | `_bell_cache` — 15-second TTL in-memory cache for bell state, collapses identical polls |
| Input sanitization | `_sanitize()` strips HTML tags from all external text; link URLs restricted to relative paths or `https://` |
| Rate limiting | Bell: 120/min, Dropdown: 60/min, Count: 120/min, History: 30/min |

**Notification sources:**

| Source | Hook Location | Trigger | Max per Cycle | Link |
|---|---|---|---|---|
| 13F changes | `sync_worker.py` after fund refresh | New/changed filing detected | 5 per fund (top by weight) | `/stock/{ticker}` |
| YouTube | `youtube_sync.py` after event upsert | `impact_score >= 7` | 1 per video | YouTube video URL |
| Reddit | `web.py` background task (every 30 min) | `velocity_pct > 100` | 1 per ticker per day | `/retail?view=leaderboard` |

**Client-side flow:**

```
On page load:
  1. Read lastSeen from localStorage (or '2000-01-01' if first visit)
  2. HTMX polls /api/notifications/bell?since={lastSeen} every 120s
  3. Server returns bell icon + red dot (if count > 0)
  4. If new notifications, server sends HX-Trigger header → ppToast() fires

On bell click:
  1. HTMX loads /api/notifications/recent?since={lastSeen} into dropdown
  2. localStorage updated to now() → red dot clears on next poll
  3. Dropdown shows latest 8 + "View all notifications →" footer if more exist
```

**Key design decisions:**
- HTMX polling (120s) over SSE — simpler, no WebSocket infrastructure, sufficient for data that updates every 30 min+
- Global notifications (no per-user rows) — all visitors see the same notifications
- Client-side dismiss via `localStorage` — no auth needed, no server-side read tracking
- 15-second in-memory cache on bell state — collapses hundreds of identical polls into one DB query
- Single DB query for bell (`get_bell_state`) — returns count + latest in one call
- Deterministic IDs + `ignore_duplicates=True` — no pre-fetch dedup needed, DB handles via PRIMARY KEY
- SQL-level pagination — only the current page is fetched from DB (not all prior pages)
- Input sanitization on all notification text — HTML tags stripped, links validated
- Toast cap of 2 prevents notification overload

---

## 8. CLI Interface

Entry point: `cli.py:main()`

| Command | Function Called | Display Function |
|---|---|---|
| `filings search "<query>"` | `client.search_managers(query)` | `display.display_search_results()` |
| `filings holdings <CIK>` | `client.get_holdings(cik)` | `display.display_holdings()` |
| `filings compare <CIK>` | `client.compare_quarters(cik)` | `display.display_comparison()` |

The CLI uses basic `sys.argv` parsing (no argparse/click). It does NOT use
the cache — every CLI command makes live SEC API calls.

---

## 9. Known Bugs & Gotchas

### Active Issues

1. **CLI doesn't use cache.** Every `filings holdings` or `filings compare`
   command hits the SEC API fresh. This is slow (~5-10 seconds) and doesn't
   benefit from the web dashboard's cached data.

2. **No 45-day lag calculation displayed.** The system doesn't show users how
   old the data actually is relative to real-time. A "data as of" label on
   each page would help.

3. ~~**get_enriched_holdings() calls compare_quarters() with top_n=9999.**~~
   **FIXED:** The web endpoint now uses `get_enriched_holdings_from_cache()`
   which reads activity data from the cached `changes` dict — zero SEC calls.
   The original function is only used on cache miss (rare).

4. **HTMX lazy-load has no retry on failure.** If a fund fails to load
   (SEC rate limit, network error), the row shows an error with no way to
   retry without a full page refresh.

5. **No pagination.** Activity feed, grand portfolio, and stock history
   are capped at 100/100/unlimited items respectively but have no
   pagination controls.

### Gotchas for Future Development

- **CIK format mismatch:** Always normalize CIKs by stripping leading zeros.
  The SEC uses 10-digit zero-padded CIKs; our system uses stripped versions.
  A single inconsistency breaks lookups silently (returns empty data, not errors).

- **Cache backwards compatibility:** Old cache entries lack `quarterly_changes`.
  Always use `.get("quarterly_changes", [])`. Deleting the cache file and
  refreshing fixes any schema mismatch.

- **Value units:** The `edgartools` library returns values in dollars (already
  multiplied by 1,000 from the SEC's raw format). Do NOT multiply by 1,000
  again. This was a bug that was fixed in an earlier session.

- **Amendment filtering:** Always pass `amendments=False` to `get_filings()`.
  Amendments (13F-HR/A) can duplicate or overwrite data. We only want originals.

- **SEC rate limiting:** EDGAR has a soft rate limit of ~10 requests/second.
  The background refresh uses a 1-second delay between funds. Aggressive
  concurrent fetching will get your IP temporarily blocked.

---

## 10. Pending Tasks & Future Work

### Planned Features (from user conversations)

- [x] Interactive chart on stock pages (Chart.js stacked bar, simple/detailed toggle)
- [x] Watchlist sidebar (star tickers, persistent JSON, HTMX OOB swaps)
- [x] In-app notification system (Supabase-backed, 3 sources: 13F/YouTube/Reddit, HTMX bell + dropdown + toast + history page)
- [x] Clickable investor names in stock history (links to `/holdings/{cik}`)
- [x] Analyst ratings tab on stock pages (Finnhub + yfinance, firm-level, lazy-loaded)
- [x] Sortable tables across all pages (vanilla JS, `data-sort` attributes)
- [x] Consolidated superinvestor page (investor.html with Holdings + Compare Quarters tabs)
- [x] Homepage: S&P 500 heatmap (ECharts), most-added table, ticker search autocomplete
- [x] Insider trading tab on stock pages (SEC Form 4 data)
- [x] SEC Filings tab on stock pages (direct links to EDGAR)
- [x] Sentiment tab (CNN Fear & Greed, Finnhub news, Reddit buzz, Alpha Vantage NLP)
- [x] Expanded ticker search: ~8K NYSE/NASDAQ listings via NASDAQ Trader + Fuse.js fuzzy search
- [x] Persistent caching: per-fund TTL (7d/12h adaptive), stale-while-revalidate, selective refresh
- [x] Supabase L2 persistent cache: 13F funds, Glassdoor, insider trades survive deploys
- [x] Cache-first endpoints: all 13F pages serve from cache, SEC only on miss
- [x] Vitals tab: Glassdoor ratings, People Data Labs employee data, Apple App Store ratings
- [x] Insider trading global screener page (dedicated `/insider-trading` route)
- [x] Insider sync cron worker: scrape OpenInsider → upsert to Supabase every 30 min
- [x] Retail page (`/retail`): Sentiment, Leaderboard (ApeWisdom), Calendar (YouTuber schedules)
- [x] Nav restructure: Home | Retail | Funds | Insiders (renamed `/grand-portfolio` → `/funds`)
- [x] Stale-while-revalidate for 13F fund data (never drop data on TTL expiry)
- [x] Stale-while-revalidate for insider trades (4-tier fallback, never show errors)
- [x] Self-healing background refresh: request-triggered per-fund refresh with asyncio.Lock concurrency control
- [x] Supabase persistence for PDL + App Store vitals data (lazy hydration, stale-while-revalidate, quota tracking)
- [x] Panda Fund support page (`/support`): progress bar, cost breakdown, funding history chart, Stripe donations
- [x] Stripe OOTB embed: Buy Button (one-time) + Pricing Table (recurring) on /support page
- [x] Homepage support widget: Panda Fund progress bar + Stripe Pricing Table embed
- [x] HTMX-aware error handling: inline data_error.html partial for 429/rate-limit errors with Panda Fund CTA
- [x] Nav update: replaced auth buttons with "Support the Panda" CTA
- [x] Dark mode toggle (sun/moon pill in navbar, CSS custom properties refactor, `localStorage` persistence, system-preference default, anti-FOUC inline script)
- [x] Dark mode logo: `logo-nav-dark.png` (transparent bg) swapped in via CSS `[data-theme="dark"]`
- [x] TradingView widgets dynamically rebuilt when theme changes (re-inject script with new `theme` param)
- [x] Stripe Pricing Table dark mode via CSS `filter: invert` on container
- [x] Supabase startup cache load wrapped in 30-second timeout (falls back to disk on timeout)
- [x] Automatic retention cleanup on startup: insider_trades (6 mo), youtube_events (30 d), sync_logs (30 d), api_cache expired entries
- [x] Capital Deployed feature: AUM from Form ADV, XBRL cash from 10-K/10-Q, deployment ratio leaderboard, cash estimation, comprehensive tooltips
- [x] Capital Deployed: CRD number cleanup (Appaloosa fix, terminated CRDs cleared for Greenlight/Scion/Omega/Aquamarine)
- [x] Capital Deployed: 13F equity fallback for AUM column when no Form ADV data exists
- [x] Capital Deployed: Unified `.pp-tooltip` CSS system with info icons, data cell tooltips, contextual explanations
- [x] Insider trading UX overhaul: quarterly groups, insider summary cards, stacked bar timeline chart, buy/sell/all filters, collapsible `<details>` sections
- [x] Insider card hover tooltips: quarterly breakdown with buys, sells, dollar values, and % of position sold
- [x] Per-insider dropdown filter: filters chart AND quarterly accordions by individual insider (dual-filter AND logic with type filter)
- [x] Full trade history: switched from per-ticker URL (~40 trades) to screener endpoint with `fd=0`/`td=0` for all-time history (paginated, up to 300 trades)
- [x] Hot/cold table architecture: `insider_trades` (30-day rolling) + `insider_purchases_history` (permanent), merged with OpenInsider scrape
- [x] SEC Form 4 XML title resolution: "See Remarks" → real officer titles (CEO, CFO, CTO, etc.) via `<remarks>` parsing + `<isDirector>`/`<isOfficer>` fallback
- [x] Congress Trading feature: STOCK Act trade tracker with Capitol Trades scraper, chamber visualization, trending/consensus/momentum charts, activity feed, politician profiles
- [x] Congress notifications: filing-date watermark trigger, 🏛️ Congress chip in notification filters
- [x] Congress daily sync cron job: incremental scrape every 24h via Railway Cron Service, `congress_sync_log` monitoring table
- [x] Congress historical backfill: full Capitol Trades dataset (~35K trades, 200+ politicians, 2023–2026)
- [x] Health endpoint staleness detection: `/health/detail` includes congress sync status, consecutive error tracking
- [x] Pie chart UX: centered layout with side legend, 480x480 canvas, transparent borders, outside-slice labels (politician + fund charts)
- [x] Momentum chart dark mode: improved label contrast, Unicode strikethrough on legend toggle
- [x] Unusual Options Activity screener: premium floor ($100K), OI delta tracking, near-expiry urgency, moneyness scoring, cluster detection, greek display
- [x] Tiingo integration: real-time IEX quotes, batch quotes, EOD history, S&P 500 close matrix (primary price source, yfinance fallback)
- [x] Tradier integration: options chains with ORATS greeks (delta, gamma, theta, vega), sandbox/production toggle, DataFrame adapter for detect_unusual()
- [x] Convergence Engine: urgency × OTM × cluster boosts in signal strength scoring
- [x] Options sync cron worker: scans S&P 500 + superinvestor holdings every 30 min during market hours
- [x] Financials tab: SEC XBRL CompanyFacts with 4 sub-tabs (Income, Balance, Cash Flow, Ratios), ECharts insight charts, annual/quarterly toggle
- [x] Fundamentals historical backfill: S&P 500 + NASDAQ cold storage archive (back to 2007), merge-only writes, 3-layer delete protection
- [x] Politician profiles: age display from congress-legislators GitHub dataset, cleaner bio section with subtle party badge
- [x] Supabase client race condition fix: `_initialised` flag moved after `_client` assignment (headshots + logos now load on startup)
- [x] Stock Valuation Screener: DCF model (10 sliders + growth fade), Monte Carlo simulation (10K iterations, ECharts histogram), Relative Value (user-controlled peer selector with S&P 500 suggestions, Fuse.js search, chip tags, parallel batch fetch)
- [x] Screener password gate: cookie-based auth (`scr_auth`, SHA-256, 30-day, `SCREENER_PASSWORD` env var), beta feature badge
- [x] Macro Dashboard: glassmorphism hero + glass nav shell, teal pill toggles, Fraunces serif fonts, aurora blobs, dark mode support
- [x] Macro password gate: reuses screener's `scr_auth` cookie, parameterized `screener_gate.html` template
- [x] NASDAQ 100 toggle fix: `build_company_lookup()` was calling `get_sp500_constituents()` for both indices; added `get_nasdaq100_constituents()` (Wikipedia scrape, 24h cache)
- [x] Revenue estimate backfill: scraped historical revenue estimates for Q1-Q4 2025 (~5500 rows), coverage 0% → 92-95%
- [x] Finnhub bulk calendar: week-by-week fetching to avoid 1500-result API limit, expanded from 7 → 10 weeks
- [x] Sidebar nav: added Macro link under Tools group (pie-chart icon)
- [x] Cold-start optimization: L2 Supabase caching for breadth, 52w range, heatmap data; screener parallelization (3-way asyncio.gather); market overview HTML cache (5-min TTL); activity feed TTL extended to 1h
- [x] Homepage SEO: enriched meta tags, JSON-LD WebPage schema with `about[]` + `mentions[]`, expanded Organization `knowsAbout` (28 entities)
- [x] Earnings scorecard: stock reaction enrichment via Tiingo EOD (close-to-close % change around report date, 8-worker parallel fetch, cached in scorecard_cache)
- [x] Sidebar nav: moved Options from Signals → Tools group, removed PRO badge
- [x] Earnings scorecard: price_change column persisted to DB via yfinance backfill (`scripts/backfill_price_change.py`), 1422/1422 Q1 2026 rows populated
- [x] Earnings scorecard: "All Stocks" index shows all 1400+ tickers (was incorrectly filtering to S&P 500 only)
- [x] Earnings scorecard: removed Guide column (never populated), added pie chart % labels + EPS/Revenue titles
- [x] Supabase query pagination: `query_earnings_history()` paginates past 1000-row client cap
- [x] Upsert protection: `upsert_earnings_history()` strips None keys to prevent clobbering backfilled data (price_change)
- [x] S&P 500 heatmap: market-cap weighted treemap (top 50 stocks sized by weight, sectors sorted by total weight)
- [x] Retail leaderboard: visual heat meter (5-bar scale Cold→Viral), centered table, rank column centered, removed gold guru border, rank change moved beside velocity
- [x] Macro subtabs reordered: Economic (default) → Treasury → FX Rates → Sentiment (was Sentiment first)
- [x] FX Rates: sparkline charts moved above exchange rates table
- [x] Economic indicators: responsive chart grid (1/2/3 columns based on chart count), chart height 140→160px
- [x] Macro page: removed BETA badge from nav, removed WIP disclaimer from earnings scorecard
- [x] Earnings scorecard: reaction % y-axis clamped to ±3% for readability
- [x] Performance: Brotli compression middleware (~15-20% better than GZip), inline CSS minification (~34% reduction), HTTP connection pooling (global httpx.AsyncClient), retail data L1 cache (120s TTL), stock price CLS fix (visibility:hidden)
- [x] Font loading: centralized Google Fonts to base.html (async preload+onload pattern, removed from ~15 child templates), crossorigin attribute to prevent double-fetch
- [x] SEO: WebApplication JSON-LD moved from base.html to home.html, 5 new FAQ entries with structured data, data source citations on congress/insider/funds pages, expanded llms.txt with macro/stock/data-source sections
- [x] Fix: investor.html duplicate DOM IDs (portfolio-donut, holdings-table-wrap) causing portfolio concentration chart to not render in tab
- [x] Static asset cache headers: 1yr immutable for images/fonts, 1hr for CSS/JS (SecurityHeadersMiddleware)
- [x] Homepage L2 stale-fallback caching: market overview (1h), market news (2h), heatmap (1h per period), retail sentiment (24h) — serves last-known-good HTML from Supabase when external APIs fail
- [x] Retail sentiment "Data as of" timestamp disclaimer, pretty-formatted (e.g. "Mar 20, 2026 3:22 PM UTC")
- [x] Feed toggle fix: `activateScripts()` called on cached HTML restore (market news scripts now execute on tab switch-back)
- [ ] Congress price backfill: run `scripts/backfill_congress_prices.py` to populate forward returns
- [ ] Custom donor fields: name + opt-in to feature on support page (Phase 2, requires FastAPI endpoint + Stripe Checkout Sessions)
- [ ] User-configurable superinvestor list (currently hardcoded in superinvestors.py)
- [ ] Export to CSV / PDF
- [ ] Comparison across multiple funds on the same page
- [x] Watchlist system: heart button on stock pages, `/watchlist` dashboard, notification preferences UI, daily digest email worker (Resend), admin panel (`/admin`) with user monitoring
- [x] Email notification automation: `filings-digest` cron worker sends personalized daily digest emails via Resend, grouped by ticker, timezone-aware scheduling
- [ ] Activate daily digest emails: set up `filings-digest` cron on Railway, add `RESEND_API_KEY` env var
- [x] L2 stale-fallback caching: `_with_l2_fallback` decorator applied to 22 HTML endpoints (11 stock tabs, 9 macro tabs, 1 options, 1 filings). On API timeout/failure, serves last-known-good HTML from Supabase instead of error. L1 5–10min, L2 1–4h TTLs.
- [x] CSS minifier bug fix: removed `+` from `_CSS_PUNCT_RE` regex — was breaking `calc()` expressions by collapsing required spaces (e.g. `calc(100% + 10px)` → `calc(100%+10px)`, invalid CSS). Fixed homepage search dropdown appearing above input.

### Shelved: Crypto Whale Tracker (branch: `feature/crypto-whale-tracker`)

Full MVP built and shelved — **not merged to main**. All code lives on the
`feature/crypto-whale-tracker` branch. See GitHub issue for full pickup context.

**What it does:** Tracks on-chain crypto whale wallets (Vitalik, EF, Galaxy
Digital, Justin Sun, etc.) via Etherscan V2 + Blockchain.com APIs. Shows
holdings and transfers on `/crypto` and `/crypto/{slug}` pages. Background
sync loop refreshes every 6 hours.

**What works:** Live ETH/ERC-20 balance fetching, multi-wallet aggregation
per entity, spam token filtering, rate limiting with retry, 4 Supabase tables,
seed script with verified wallet addresses from Etherscan labels + Arkham.

**What's missing for production:**
- USD price enrichment (all usd_value = 0, needs CoinGecko or similar)
- Token discovery limited to 100 most recent `tokentx` — misses older staking
  positions (e.g. Justin Sun's 156K stETH). Needs portfolio API (Moralis/Alchemy)
- Solana support (Helius API stubbed but not wired)
- Arkham Intelligence API (on waitlist)
- Bitmine Immersion wallet coverage (only 1 of many wallets publicly known)
- MicroStrategy BTC is fully custodial — not trackable on-chain

**Files on the branch (not on main):** `crypto.py`, `crypto.html`,
`crypto_entity.html`, `seed_crypto.py`, plus modifications to
`supabase_cache.py`, `web.py`, `base.html`, `.env.example`, `README_DEV.md`.

**Env vars needed:** `ETHERSCAN_API_KEY`, plus existing `SUPABASE_URL` /
`SUPABASE_SERVICE_KEY`. Supabase tables must be created manually (see
`_SCHEMA_SQL` in `supabase_cache.py` on the branch).

### Technical Debt

- [ ] CLI should optionally read from cache instead of always hitting SEC API
- [x] `get_enriched_holdings()` bypassed with `get_enriched_holdings_from_cache()` —
      now reads from cached data instead of calling SEC API + compare_quarters()
- [ ] Add proper logging (currently all errors are silently caught with `pass`)
- [ ] Add error handling for malformed SEC data (corrupt DataFrames, missing columns)
- [x] Unit tests: `test_web.py` (12 route/integration tests), `test_perf.py` (34 performance tests covering cache, compression, CLS, fonts, CSS minification, JSON-LD)
- [ ] Type checking with mypy (type hints are used but not enforced)

---

## 11. Historical Bug Fixes

These bugs were identified and fixed during development. Documented here so
they don't get re-introduced.

| Bug | Root Cause | Fix |
|---|---|---|
| Values displayed 1000x too large | `display.py` multiplied `value * 1000` but edgartools already returns dollars | Removed the `*1000` multiplication |
| Amendments duplicating data | `get_filings()` returned both original and amendment filings | Added `amendments=False` parameter |
| Search missing hedge fund results | `edgartools.find()` doesn't index all 13F filers | Added EFTS full-text search fallback (`_search_edgar_efts`) |
| Bridgewater returning wrong entity | Multiple CIK matches for "Bridgewater" | CIK verification + hardcoded superinvestor list |
| CIK cache key mismatch | `fund_row` stored cache with zero-padded CIK ("0001067983") but lookups used stripped CIK ("1067983") | Added `cik_normalized = cik.lstrip("0") or cik` in fund_row endpoint |
| Stale cache after multi-quarter update | Old cache lacked `quarterly_changes` field | Graceful degradation with `.get("quarterly_changes", [])` + delete cache to regenerate |
| Server unresponsive after 85 superinvestor expansion | `save_cache()` called synchronously after every fund during background refresh (84× disk writes blocking event loop); `notifications.json` and `watchlist.json` read from disk on every HTTP request; 84 HTMX lazy-loads fired simultaneously | Batched cache writes (every 10 funds, via `asyncio.to_thread`); in-memory caching for notification/watchlist state; staggered HTMX lazy-loads (3 per second); fire-and-forget disk saves in fund_row endpoint |
| 13F fund data disappearing after TTL expiry | `load_cache_from_supabase()` called `get_cached()` which returns `None` when TTL expires, causing all fund data to vanish until the sync worker refreshes | Switched to `get_cached_with_stale()` which returns expired data as stale fallback; added L2 Supabase fallback to all fund web endpoints with L1 promotion |
| Insider trades "failed to load" on deploy | When L1 TTL expires and Supabase query fails (transient), `get_latest_insider_trades()` fell through to OpenInsider scrape which also failed, returning empty list | Added `_get_cached_with_stale()` to insider_trading.py; both global and per-ticker functions now use 4-tier fallback (L1 fresh → L2 Supabase → L3 scrape → L4 stale L1), never returning empty results if data was previously loaded |

---

## Reference Rule

> **When context drifts, re-read this file.**
>
> This file documents the system as of 2026-03-09 (Options screener upgrade:
> premium floor, OI delta tracking, urgency weighting, moneyness scoring, cluster
> detection, greek display; Tiingo integration for real-time IEX prices; Tradier
> integration for options chains with ORATS greeks; Convergence Engine signal
> boosts; plus all prior features: notification bell, Capital Deployed, dark mode,
> Panda Fund, Congress trading, insider trading, Supabase caching).
> If told "Context is drifting," the first action should be to re-read
> `/Users/Tevis_1/13F-project/README_DEV.md` and reconcile any discrepancies
> with the actual code.
