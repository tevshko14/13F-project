# 13F Filing Viewer - Developer Reference

> **This file is the source of truth for this project.**
> If context is ever drifting, re-read this file first before making changes.
> Last updated: 2026-02-23 (YouTube calendar sync, cold storage, PostHog analytics, egress optimization, circuit-breaker patterns)

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

A tool for tracking SEC 13F institutional holdings filings from 84 hardcoded
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
| Market data   | `yfinance` + NASDAQ Trader (~8K listings) + Wikipedia (sectors) |
| Analyst data  | `yfinance` (free) + `finnhub-python` (free tier, optional key) |
| Sentiment     | CNN Fear & Greed, Finnhub, ApeWisdom, Alpha Vantage |
| Vitals        | People Data Labs, Glassdoor (RapidAPI), Apple iTunes Search |
| Caching       | 3-tier: in-memory (L1) → Supabase Postgres (L2) → disk JSON (L3) |
| Hosting       | Railway (auto-deploy from main) at [paperpanda.io](https://paperpanda.io) |
| Entry points  | `filings` (CLI), `filings-web` (web, port 8000) |

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
│  Stale-While-Revalidate + Delta Detection                  │
│                                                            │
│  • load_cache_from_supabase()  (L2: delta-aware hydration) │
│  • _load_cache_from_supabase_full() (full fallback load)   │
│  • load_cache() → dict         (L3: read from disk)        │
│  • save_cache(data)            (L3: atomic write)          │
│  • is_cache_stale() → bool     (overall file staleness)    │
│  • is_fund_stale(fund_data)    (per-fund _last_refreshed)  │
│  • get_stale_ciks(cache, ciks) (selective refresh list)    │
│  • stamp_fund_data(data)       (add _last_refreshed ts)    │
│  • refresh_single_fund(cik)    (fetch + archive + trim)    │
│  • _archive_old_quarters()     (cold storage archival)     │
│  • load_historical_quarters()  (cold storage retrieval)    │
│  • _get_effective_ttl_seconds()(TTL in seconds for L2)     │
│  • get_cache_age_str() → str   ("5 min ago")               │
│                                                            │
│  TTL: 7 days (off-season) / 12 hours (filing season)       │
│  Cache keys = CIK without leading zeros ("1067983")        │
│  Supabase keys = "13f:{CIK}" with category "13f"           │
│  Content-hash: SHA-256 change detection (skip unchanged)   │
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
   a. load_cache_from_supabase() → delta detection via content hashes (~2 KB),
      only fetches funds that changed since last deploy (zero egress if unchanged)
   b. If Supabase empty/down → load_cache() from disk (fallback)
   c. Starts _prefetch_market_data(app) background task (S&P 500 data, ~30-60s)
   d. If any funds stale → triggers _background_refresh() (per-fund TTL)
3. index() renders:
   a. Heatmap section: HTMX fires GET /api/heatmap → "loading" stub with
      auto-retry (hx-trigger="load delay:5s") until market data is ready
   b. Most-added section: HTMX fires GET /api/most-added → renders table from cache
   c. 84 superinvestor rows, each marked for lazy-load
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
   a. Iterates all 84 cached funds
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
├── pyproject.toml                    # deps, entry points, build config
├── README.md                         # Project overview
├── README_DEV.md                     # THIS FILE — source of truth
├── .python-version                   # 3.12
├── .gitignore
├── uv.lock
└── src/filings/
    ├── __init__.py                   # version = "0.1.0"
    ├── models.py                     # 13 dataclasses (data contracts)
    ├── superinvestors.py             # 84 hardcoded funds + CIK lookup dict
    ├── cache.py                      # 3-tier cache: L1 in-memory → L2 Supabase → L3 disk
    ├── supabase_cache.py             # Supabase L2 persistent cache (api_cache + insider_trades + youtube_events/channels + sync_logs)
    ├── watchlist.py                  # Watchlist persistence (JSON, ~/.13f-cache/watchlist.json)
    ├── notifications.py              # Notification engine: detection, matching, persistence, filing season
    ├── analysts.py                   # Analyst ratings (Finnhub + yfinance, 5-min TTL cache)
    ├── sentiment.py                  # Market sentiment (CNN, Finnhub, ApeWisdom, Alpha Vantage)
    ├── vitals.py                     # Alternative data (Glassdoor, People Data Labs, App Store)
    ├── market_data.py                # S&P 500 heatmap, most-added, ticker search (~8K NYSE/NASDAQ listings)
    ├── company_filings.py            # SEC filing links for stock pages
    ├── insider_trading.py            # Form 4 insider transaction data (4-tier: L1→Supabase→scrape→stale)
    ├── insider_sync.py               # Cron worker: scrape OpenInsider → upsert to Supabase (every 30 min)
    ├── youtube.py                    # YouTube calendar data layer (L1→L2→L3 tiered cache, channel fallbacks)
    ├── youtube_sync.py               # Cron worker: YouTube Data API v3 → upsert to Supabase (every 6h)
    ├── cold_storage.py               # Cold storage: Supabase Storage bucket for archived 13F quarters (circuit-breaker)
    ├── sync_worker.py                # Cron worker: SEC EDGAR 13F sync (every 12h, hot/cold archival, OOM-safe)
    ├── auth.py                       # Authentication (sign-in, sessions)
    ├── client.py                     # SEC EDGAR client (13 functions)
    ├── display.py                    # CLI Rich formatters (3 functions)
    ├── cli.py                        # CLI entry point (search/holdings/compare)
    ├── web.py                        # FastAPI app (40+ routes + background refresh + Stripe/support + SSE + polling)
    └── templates/
        ├── base.html                 # Master layout: nav (Home|Retail|Funds|Insiders|Support), PicoCSS, HTMX, ECharts, Fuse.js, sidebar, sortable tables
        ├── home.html                 # Homepage: heatmap + most-added + cards + Panda Fund support widget (Stripe Pricing Table)
        ├── retail.html               # Retail page: Sentiment, Leaderboard, Calendar sub-tabs
        ├── grand_portfolio.html      # Top Funds page: Funds, Holdings, Activity sub-tabs (URL: /funds)
        ├── insider_trading.html      # Insider trading screener: global buys/sells with chart
        ├── search.html               # Fund manager search
        ├── investor.html             # Individual fund page (tabbed: Holdings + Compare Quarters)
        ├── activity.html             # Cross-fund activity feed (top 100)
        ├── stock.html                # Stock detail (7 tabs: Overview, Ownership, Analysts, Sentiment, Vitals, Filings, Insider)
        ├── support.html              # Panda Fund: progress bar, Stripe Buy Button + Pricing Table, cost breakdown, funding history chart
        ├── notifications.html        # Notification history page
        ├── error.html                # Error page
        └── partials/
            ├── fund_row.html           # HTMX partial: loaded fund row
            ├── fund_row_error.html     # HTMX partial: error fund row
            ├── ticker_link.html        # Jinja2 macro: clickable ticker/CUSIP
            ├── watchlist_sidebar.html   # Sidebar content: ticker list + remove buttons
            ├── watchlist_star.html      # Star button (filled/outline) for stock pages
            ├── watchlist_response.html  # OOB response: star + sidebar update
            ├── notification_bell.html   # Navbar bell icon with unread badge
            ├── heatmap.html            # S&P 500 ECharts treemap (lazy-loaded via HTMX)
            ├── most_added.html         # Most-added-by-superinvestors table (lazy-loaded)
            ├── ticker_search.html      # Nav autocomplete search input (Fuse.js fuzzy search)
            ├── analyst_ratings.html    # Analyst consensus + ratings table (lazy-loaded)
            ├── sentiment.html          # Market/news sentiment cards (CNN, Finnhub, Reddit, Alpha Vantage)
            ├── vitals.html             # Employee pulse, culture, product sentiment (3-card grid)
            ├── company_filings.html    # SEC filing links (lazy-loaded)
            ├── insider_trades.html     # Insider trading table — global screener (lazy-loaded)
            ├── stock_insider_trades.html # Insider trading table — per-ticker (lazy-loaded)
            ├── retail_leaderboard.html  # ApeWisdom Reddit leaderboard (lazy-loaded into retail page)
            ├── retail_calendar.html    # YouTube calendar: upcoming streams + recent uploads with channel filters
            ├── compare_content.html    # Compare quarters partial (lazy-loaded into investor page)
            └── data_error.html         # Reusable error partial (rate limit CTA, generic fallback, HTMX-aware)
```

---

## 4. Data Schema

### 4.1 Superinvestor Registry (`superinvestors.py`)

The system tracks exactly **84 hardcoded superinvestors**. This is the source
list — there is no database of investors. The full list matches
[Dataroma's manager list](https://www.dataroma.com/m/managers.php) plus a few
additional notable investors.

```python
SuperinvestorInfo:
    cik: str            # "1067983" (no leading zeros)
    display_name: str   # "Warren Buffett"
    fund_name: str      # "Berkshire Hathaway"
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
| 84 | David Abrams | Abrams Capital Management | 1358706 |

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

### 4.4 Notifications State (`~/.13f-cache/notifications.json`)

Stores seen filing dates (for detection) and notification history.

```json
{
  "initialized_at": "2026-02-16T10:00:00",
  "seen_filing_dates": { "1067983": "2025-11-14", "1336528": "2025-11-12" },
  "notifications": [
    {
      "id": "1067983-2025-11-14-037833100",
      "timestamp": "2026-02-16T10:30:00",
      "type": "watchlist_match",
      "fund_cik": "1067983",
      "fund_name": "Warren Buffett",
      "ticker": "AAPL",
      "cusip": "037833100",
      "issuer_name": "APPLE INC",
      "action": "ADD",
      "pct_of_portfolio": 10.5,
      "filing_date": "2025-11-14",
      "read": false,
      "link": "/stock/AAPL"
    }
  ]
}
```

Functions in `notifications.py`:
- `initialize_if_needed(cache_data)` — first-run: marks all current filings as "seen"
- `is_new_filing(cik, new_filing_date) -> bool` — compare against seen dates
- `check_watchlist_matches(cik, name, fund_data, watchlist) -> list[dict]` — match changes vs tickers
- `add_notification(notif)` — persist (deduplicates by id, caps at 200)
- `get_unread_count() -> int`, `mark_all_read()`, `mark_notification_read(id)`
- `is_filing_season() -> bool` — True within ±15 days of filing deadlines
- `get_poll_interval_seconds() -> int` — 2h during season, 12h outside

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
| `Notification` | A notification about a filing/watchlist match | Notification system |

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

Startup (hydration priority — delta-aware):
  1. load_cache_from_supabase() with content-hash change detection:
     a. Fetch all {cache_key, content_hash} pairs (~2 KB for 84 funds)
     b. Compare against local fund_hashes.json from last startup
     c. Only download funds whose hash changed (typically 0 outside filing season)
     d. Unchanged funds loaded from local disk cache (zero Supabase egress)
  2. If no hashes exist (first deploy) → _load_cache_from_supabase_full()
  3. If Supabase empty/down → load_cache() from disk (fallback)
  4. Check per-fund staleness → trigger background refresh for stale funds

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

#### Insider Trades (4-tier stale-while-revalidate)

```
Cache Tiers:
  L1: in-memory dict with 5-10 min TTL (sub-ms)
  L2: Supabase insider_trades dedicated table (no TTL, typed columns)
  L3: OpenInsider scrape (fallback when DB empty/unavailable)
  L4: Stale L1 data (last resort — never show empty/error to users)

Data flow (insider_trading.py):
  _get_cached_with_stale(key, ttl) → returns (data, is_fresh) tuple
  - Fresh L1 hit → return immediately
  - L1 expired → try L2 Supabase query
  - L2 fails → try L3 OpenInsider scrape
  - All fail → return stale L1 data (L4) instead of empty list

Sync worker (insider_sync.py, Railway cron every 30 min):
  1. Scrape 3 OpenInsider pages (all, purchases, sales)
  2. Deduplicate by sec_url
  3. Upsert to Supabase (ON CONFLICT sec_url DO UPDATE)
  4. Never deletes old data — only adds/updates
```

### 5.8 Market Data Module (`market_data.py`)

Central module for all homepage market data features. Follows the same TTL
cache pattern as `analysts.py`.

| Function | Data Source | TTL | Purpose |
|---|---|---|---|
| `get_sp500_constituents()` | Wikipedia (pd.read_html) | 24h | Ticker + sector list (~500 items) |
| `get_sp500_market_data()` | yfinance bulk download (5d) | 30min | Daily % change for all S&P 500 tickers |
| `build_heatmap_data()` | Pure computation | — | ECharts treemap format with colors + gold borders |
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

### 5.11 Insider Trading Module (`insider_trading.py` + `insider_sync.py`)

Dedicated insider trading system with its own Supabase table and sync worker.

**Architecture:**
- `insider_trades` table in Supabase (dedicated, typed columns — NOT the `api_cache` JSONB table)
- `insider_sync.py` cron worker scrapes OpenInsider every 30 min, upserts via `ON CONFLICT (sec_url) DO UPDATE`
- `insider_trading.py` serves data with 4-tier stale-while-revalidate fallback

**Data model (`InsiderTrade` dataclass):**
- `filing_date`, `trade_date`, `ticker`, `company_name`, `insider_name`, `title`
- `trade_type` (Purchase, Sale, Sale+OE), `price`, `qty`, `owned`, `delta_own`, `value`
- `sec_url` (unique key, link to SEC Form 4 filing)

**Key functions:**
| Function | Description |
|---|---|
| `get_latest_insider_trades(trade_type, count)` | Global screener: L1→L2→L3→L4 stale fallback |
| `get_ticker_insider_trades(ticker)` | Per-ticker: L1→L2→L3 scrape+backfill→L4 stale |
| `aggregate_top_tickers(trades, limit)` | Aggregate by ticker for chart (net flow, insider details) |
| `_scrape_openinsider_global(trade_type, count)` | L3 fallback: direct scrape of OpenInsider |
| `_scrape_and_backfill_ticker(ticker)` | L3 fallback: scrape + upsert back to Supabase |

**Sync worker (`insider_sync.py`):**
- Entry point: `uv run filings-insider-sync`
- Scrapes 3 OpenInsider pages (all, purchases, sales) with 3-second delays
- Deduplicates by `sec_url`, skips existing rows (fetches `sec_url` keys first), upserts only new trades
- Logs to `sync_logs` table for observability

### 5.15 YouTube Sync Worker (`youtube_sync.py`)

Polls 11 tracked finance YouTube channels every 6 hours for upcoming livestreams and recent uploads.

**Entry point:** `uv run filings-youtube-sync`

**YouTube Data API v3 usage:**
- `activities.list` (1 unit/request) — detect recent uploads
- `search.list` (100 units/request) — find upcoming livestreams
- `videos.list` with `contentDetails,liveStreamingDetails,statistics,snippet` — fetch duration, view count, live status

**Quota:** ~4,488 units/day across 4 runs (~45% of 10,000 daily limit)

**Features:**
- ISO 8601 duration parsing (`PT1H2M30S` → `1:02:30`)
- Content type detection (video/live/upcoming/was_live)
- Skip-existing optimization: fetches existing `video_id` keys before upserting
- Still re-upserts `upcoming` events since their status may change

**Tracked channels:** 11 finance YouTubers including Meet Kevin, Graham Stephan, Steven Fiorillo, Couch Investor, etc.

### 5.16 13F Sync Worker (`sync_worker.py`)

Refreshes all 84 superinvestor fund data from SEC EDGAR.

**Entry point:** `uv run filings-sync`

**Hot/cold archival:**
- Fetches fresh data from SEC EDGAR for each stale fund
- Archives quarters 3+ to cold storage (`paperpanda-archive` Supabase Storage bucket)
- Trims to 2 quarters in hot Postgres cache (prevents OOM)
- If cold storage unavailable, still trims (full history available from SEC EDGAR)

**Content-hash change detection:**
- Computes SHA-256 hash of fund data before writing
- If hash matches stored `content_hash`, skips full JSONB upsert (only bumps TTL)
- ~95% of syncs are no-ops outside filing season

**OOM prevention:**
- Lightweight CIK lookup (`get_cache_keys_by_category` fetches only keys, not 30MB of JSONB)
- `gc.collect()` after each fund
- Memory logging via `resource.getrusage` every 10 funds

### 5.17 Cold Storage (`cold_storage.py`)

Archives older 13F quarterly data to Supabase Storage (S3-compatible).

**Bucket:** `paperpanda-archive` (private, created via `ensure_bucket()`)

**Circuit-breaker pattern:**
- `_bucket_status` module-level flag tracks bucket availability
- `is_available()` performs one-time lazy check on first call
- If bucket missing and creation fails, all subsequent calls short-circuit instantly
- Prevents 84× failing HTTP calls during sync when bucket is unavailable

### 5.18 Egress Optimization

Supabase free tier has 5 GB/month egress. Optimizations across all layers:

| Layer | Before | After |
|---|---|---|
| **Startup hydration** | Pull all 84 fund blobs (~30-40 MB) | Delta: compare content hashes (~2 KB), only fetch changed funds |
| **13F sync writes** | Upsert all 84 funds every run | Content-hash: skip unchanged funds, TTL-only bump |
| **Insider sync** | Upsert ~300 trades every 30 min | Skip-existing: fetch `sec_url` keys first, only send new trades |
| **YouTube sync** | Upsert all events every 6h | Skip-existing: fetch `video_id` keys first, only send new events |

### 5.12 Panda Fund & Stripe Integration (`web.py` + `support.html`)

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

### 5.13 Retail Page (`retail.html`)

The `/retail` page aggregates retail trader sentiment data with 3 sub-tabs.

**Sub-tab pattern:** Uses `.rt-subtab` buttons + `.rt-panel` divs + `switchRetailView()`
JS function (same pattern as grand_portfolio's `.gp-subtab`). URL synced via `history.replaceState`.

| Tab | Data Source | Rendering |
|---|---|---|
| Sentiment | CNN Fear & Greed + ApeWisdom top stocks | Server-rendered: gauge + summary cards |
| Leaderboard | ApeWisdom all-stocks (pages 1-5) | Lazy-loaded via `fetch('/api/retail/leaderboard')` |
| Calendar | YouTube Data API v3 → Supabase `youtube_events` + `youtube_channels` | Lazy-loaded via `fetch('/api/retail/calendar')` |

**Calendar tab details:**
- 11 tracked finance YouTube channels (synced every 6h by `youtube_sync.py`)
- Upcoming livestreams section + recent uploads grid
- Channel avatar filter strip (click to filter by channel)
- LIVE/VIDEO type tags on video cards with duration badges
- Powered by `youtube.py` data layer with L1 memory → L2 Supabase → L3 static fallback

**Summary cards (Sentiment tab):**
- Most Mentioned: ticker with highest mention count
- Biggest Rank Mover: ticker with largest positive rank change (24h)
- Top 5 Trending: links to top 5 tickers by mention count

**Leaderboard features:**
- Sortable columns (Rank, Ticker, Name, Mentions, ΔMentions, Upvotes, Rank Change)
- Accordion: top 25 shown by default, expandable to all ~250 tickers
- Green/red badges for rank changes and mention deltas
- Fear & Greed badge at the top with color-coded mood

### 5.14 Performance Optimizations

**Problem:** With 84 superinvestors, synchronous file I/O was blocking the async event loop.
Per-fund TTL now skips fresh funds during background refresh, reducing API calls significantly.

| Optimization | Before | After |
|---|---|---|
| Cache saves during refresh | Save entire JSON to disk after EVERY fund (84×) | Batch: save every 10 funds, via `asyncio.to_thread` |
| Notification state reads | Read `notifications.json` from disk on every request | In-memory cache (`_state_cache`), disk reads only on cold start |
| Watchlist reads | Read `watchlist.json` from disk on every request | In-memory cache (`_watchlist_cache`), disk reads only on cold start |
| HTMX lazy-load (cold start) | All 84 rows fire simultaneously on page load | Staggered: 3 rows per second (`delay:{{ loop.index0 // 3 }}s`) |
| Fund row disk save | Synchronous `save_cache()` blocking response | `asyncio.create_task(asyncio.to_thread(...))` — fire-and-forget |

**In-memory caching pattern** (used by `notifications.py` and `watchlist.py`):
- Module-level `_state_cache` / `_watchlist_cache` variable
- `load_*()` returns cache on hit, reads disk on miss (cold start only)
- `save_*()` updates in-memory cache first, then writes to disk
- All subsequent reads are instant (0.1ms for 1000 calls)

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
| GET | `/api/vitals/{ticker}` | `vitals_data` | Glassdoor, PDL, Apple iTunes | `partials/vitals.html` |
| GET | `/api/company-filings/{ticker}` | `company_filings_tab` | SEC EDGAR | `partials/company_filings.html` |
| GET | `/api/insider-trades` | `insider_trades_api` | Supabase → OpenInsider → stale L1 | `partials/insider_trades.html` |
| GET | `/api/insider-trades/{ticker}` | `stock_insider_trades_api` | Supabase → OpenInsider → stale L1 | `partials/stock_insider_trades.html` |
| GET | `/api/retail/leaderboard` | `retail_leaderboard_api` | ApeWisdom + CNN Fear & Greed | `partials/retail_leaderboard.html` |
| GET | `/api/ticker-search-index` | `ticker_search_index` | NASDAQ Trader + S&P 500 + cache | JSON response |
| GET | `/api/heatmap` | `heatmap` | yfinance (30-min cache) + Wikipedia | `partials/heatmap.html` |
| GET | `/api/most-added` | `most_added` | Cache + analysts + yfinance | `partials/most_added.html` |
| POST | `/api/watchlist/{ticker}` | `watchlist_add` | Watchlist JSON | `partials/watchlist_response.html` |
| DELETE | `/api/watchlist/{ticker}` | `watchlist_remove` | Watchlist JSON + Cache | `partials/watchlist_response.html` or `partials/watchlist_sidebar.html` |
| GET | `/api/watchlist-sidebar` | `watchlist_sidebar_refresh` | Watchlist JSON | `partials/watchlist_sidebar.html` |
| GET | `/api/notifications/stream` | `notification_stream` | SSE (real-time) | StreamingResponse (text/event-stream) |
| GET | `/api/notifications/bell` | `notification_bell` | Notifications JSON | `partials/notification_bell.html` |
| GET | `/notifications` | `notifications_page` | Notifications JSON | `notifications.html` |
| POST | `/api/notifications/read/{id}` | `mark_read` | Notifications JSON | Empty HTML |
| POST | `/api/notifications/read-all` | `mark_all_read` | Notifications JSON | `partials/notification_bell.html` |
| GET | `/support` | `support_page` | Env vars (PANDA_FUND_RAISED, Stripe IDs) | `support.html` |
| POST | `/refresh` | `trigger_refresh` | SEC API (background) | Raw HTML response |

**Key patterns:**
- All endpoints are cache-first. SEC EDGAR is only called on cache miss or during background refresh.
- Fund data endpoints (`/api/fund-row`, `/api/holdings`, `/api/compare`) have L2 Supabase fallback with L1 promotion when L1 misses.
- Fund data endpoints trigger self-healing background refresh when stale data is detected (request-triggered via `_trigger_single_refresh`).
- Insider trade endpoints use 4-tier fallback: L1 fresh → L2 Supabase → L3 scrape → L4 stale L1 (never empty).
- Backward-compat redirects: `/grand-portfolio` → `/funds` (301), `/superinvestors` → `/funds?view=funds` (301).
- Watchlist routes read/write to `~/.13f-cache/watchlist.json` (separate from fund cache).
- Exception handlers detect HTMX requests (`HX-Request` header) and API paths to return inline `data_error.html` partial instead of full error pages.
- `/support` and homepage widget use Stripe OOTB web components (Buy Button + Pricing Table) -- zero backend Stripe SDK needed.
- `/health` endpoint includes `stale_funds`, `refresh_status`, `refresh_progress`, and `vitals_cache` diagnostics.

---

## 7. Templates

### Styling System (in base.html `<style>`)

| CSS Class | Purpose |
|---|---|
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
| `.notif-badge` | Red circle badge on notification bell (absolute positioned) |
| `.notification-toast` | Slide-in toast card (fixed bottom-right, auto-dismiss 10s) |
| `.notification-unread` | Left border highlight on unread notification cards |
| `.notification-card` | Clickable notification entry in history page |
| `@keyframes slideIn` | Toast entrance animation (translateX 100% → 0) |
| `th[data-sort]` | Sortable column header (cursor:pointer, hover highlight) |
| `.sort-indicator` | Sort direction arrow (▲/▼/▴) appended to sortable headers |

### Template Hierarchy

```
base.html (nav: Home|Retail|Funds|Insiders|Support the Panda + styles + HTMX + Chart.js + ECharts + Fuse.js CDN + sidebar + SSE + sortable tables)
  ├── includes partials/watchlist_sidebar.html (in <aside> via hx-preserve)
  ├── includes partials/notification_bell.html (in <nav>, HTMX-polls every 60s)
  ├── includes partials/ticker_search.html (in <nav>, Fuse.js fuzzy autocomplete)
  ├── home.html (homepage)
  │     ├── lazy-loads partials/heatmap.html via HTMX (/api/heatmap)
  │     ├── lazy-loads partials/most_added.html via HTMX (/api/most-added)
  │     ├── 3 quick-access cards: Retail, Funds, Insiders
  │     └── Panda Fund support widget: progress bar + Stripe Pricing Table embed
  ├── retail.html (sub-tabs: Sentiment | Leaderboard | Calendar)
  │     ├── Sentiment tab: CNN Fear & Greed gauge + summary cards (server-rendered)
  │     ├── Leaderboard tab: lazy-loads partials/retail_leaderboard.html via fetch(/api/retail/leaderboard)
  │     └── Calendar tab: static YouTuber table (server-rendered)
  ├── grand_portfolio.html (URL: /funds, sub-tabs: Funds | Holdings | Activity)
  │     ├── Funds tab: HTMX lazy-loads partials/fund_row.html for each of 84 investors
  │     ├── Holdings tab: aggregated cross-fund portfolio
  │     └── Activity tab: recent buys/sells across all funds
  ├── insider_trading.html (global insider trades screener)
  │     └── lazy-loads partials/insider_trades.html via fetch(/api/insider-trades)
  ├── search.html
  ├── investor.html (Tabbed: Holdings + Compare Quarters, lazy-loads compare)
  │     ├── imports partials/ticker_link.html (macro)
  │     └── lazy-loads partials/compare_content.html via fetch(/api/compare/{cik})
  ├── activity.html
  │     └── imports partials/ticker_link.html (macro)
  ├── stock.html (7 Tabs: Overview, Ownership, Analysts, Sentiment, Vitals, Filings, Insider)
  │     ├── includes partials/watchlist_star.html
  │     ├── lazy-loads partials/analyst_ratings.html via fetch(/api/analysts/{ticker})
  │     ├── lazy-loads partials/sentiment.html via fetch(/api/sentiment/{ticker})
  │     ├── lazy-loads partials/vitals.html via fetch(/api/vitals/{ticker})
  │     ├── lazy-loads partials/company_filings.html via fetch(/api/company-filings/{ticker})
  │     └── lazy-loads partials/stock_insider_trades.html via fetch(/api/insider-trades/{ticker})
  ├── support.html (Panda Fund transparency dashboard)
  │     ├── Progress bar with goal-reached badge ($400 monthly goal, capped on frontend)
  │     ├── Stripe Buy Button (one-time) + Pricing Table (recurring) with tab toggle
  │     ├── Cost breakdown (line items, no dollar amounts), funding history ECharts bar chart
  │     ├── YouTube @funofinvesting section + feedback CTA
  │     └── Auto-switches to monthly tab when linked from homepage (#monthly hash)
  ├── notifications.html (notification history)
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

**Persistence:** `~/.13f-cache/notifications.json` (seen filing dates + notification history)

The notification system detects new 13F filings via polling, matches changes against
the user's watchlist, and delivers in-app notifications (toasts + bell badge + history page).
No browser push notifications — everything is server-rendered with SSE for real-time delivery.

**Architecture:**

| Component | Implementation |
|---|---|
| Detection | Poll SEC via `_background_refresh`, compare `filing_date` against `seen_filing_dates` |
| Matching | `check_watchlist_matches()` — scan `changes` for CUSIPs/tickers in watchlist |
| Persistence | `notifications.py` — JSON file, atomic writes, capped at 200 notifications |
| Deduplication | Deterministic IDs: `{cik}-{filing_date}-{cusip}` — same notification never stored twice |
| Real-time delivery | SSE via FastAPI `StreamingResponse` (`/api/notifications/stream`) |
| Bell badge | `notification_bell.html` — HTMX polls every 60s, red badge with unread count |
| Toast popups | JavaScript `EventSource` listener in `base.html`, auto-dismiss after 10s |
| History page | `notifications.html` — full list with action badges, mark-read, mark-all-read |
| Scheduling | `_notification_poll_loop` — adaptive: 2h during filing season, 12h outside |
| First-run safety | `initialize_if_needed()` — marks all current filings as "seen" to prevent flood |
| Template global | `get_unread_count()` registered as Jinja2 global |

**Polling algorithm:**

```
_notification_poll_loop (runs as asyncio task):
  Loop forever:
    1. interval = get_poll_interval_seconds()
       - is_filing_season()? → 7,200s (2 hours)
       - otherwise → 43,200s (12 hours)
    2. await asyncio.sleep(interval)
    3. If not already refreshing → trigger _background_refresh()
```

Filing season is defined as ±15 days around SEC 13F deadlines:
Feb 14, May 15, Aug 14, Nov 14.

**New filing detection flow:**

```
_background_refresh (enhanced):
  For each superinvestor:
    1. Fetch fresh data via get_fund_summary(cik)
    2. Compare filing_date against seen_filing_dates[cik]
    3. If new filing detected:
       a. Load watchlist
       b. check_watchlist_matches() — scan changes for watchlist tickers/CUSIPs
       c. For each match: add_notification() + broadcast via SSE
       d. mark_filing_seen(cik, filing_date)
```

**Watchlist matching algorithm** (`check_watchlist_matches`):

```
1. Build cusip_set and ticker_set from watchlist items
2. Build ticker_by_cusip and pct_by_cusip lookup dicts from all_holdings
3. Iterate fund_data["changes"]:
   - If change cusip in cusip_set OR change issuer ticker in ticker_set → match
4. For each match:
   - Map status → action (NEW→"NEW BUY", INCREASED→"ADD", DECREASED→"REDUCE", CLOSED→"SOLD")
   - Look up pct_of_portfolio from all_holdings
   - Generate deterministic ID: {cik}-{filing_date}-{cusip}
   - Build notification dict with link to /stock/{ticker} or /stock/cusip/{cusip}
```

**SSE (Server-Sent Events) architecture:**

```
Server side:
  app.state.sse_clients: list[asyncio.Queue]

  /api/notifications/stream endpoint:
    1. Create asyncio.Queue for this client
    2. Add to sse_clients list
    3. StreamingResponse generator:
       - Await queue.get() indefinitely
       - Yield: "event: notification\ndata: {json}\n\n"
    4. On disconnect: remove queue from sse_clients

  _broadcast_sse(app, notif):
    For each queue in sse_clients:
      await queue.put(notif)

Client side (base.html inline JS):
  const evtSource = new EventSource("/api/notifications/stream");
  evtSource.addEventListener("notification", (e) => {
    const notif = JSON.parse(e.data);
    showToast(notif);           // Slide-in card, auto-dismiss 10s
    htmx.ajax("GET", ...);     // Refresh bell badge
  });
```

**Toast notification format:**
```
"Warren Buffett just disclosed a NEW BUY in $AAPL.
 It now makes up 22.7% of their portfolio."
```
- Green border for adds (NEW BUY, ADD)
- Red border for reduces (REDUCE, SOLD)
- Clickable → navigates to stock page

**Key design decisions:**
- SSE instead of WebSockets — simpler, no new dependencies, auto-reconnect built into `EventSource`
- HTMX bell poll (60s) as fallback if SSE events are missed or disconnected
- One `asyncio.Queue` per SSE client — no shared state, clean disconnect handling
- 200-notification cap prevents unbounded growth of JSON file
- `initialize_if_needed()` runs once on first startup to avoid flooding with historical notifications
- Adaptive polling (2h vs 12h) balances freshness with SEC rate limit courtesy
- Deterministic notification IDs prevent duplicates even if the same data is polled twice
- No browser push notifications (no Service Worker, no VAPID keys) — keeps architecture simple

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
- [x] In-app notification system (SEC poller + watchlist matching + SSE toasts + bell badge)
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
- [x] YouTube sync cron worker: YouTube Data API v3 → Supabase every 6h (11 channels, upcoming streams + uploads)
- [x] YouTube Calendar tab: channel filter strip, LIVE/VIDEO tags, duration badges, lazy-loaded
- [x] Retail page (`/retail`): Sentiment, Leaderboard (ApeWisdom), Calendar (YouTube sync)
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
- [x] PostHog analytics: stock_search, fund_viewed, youtube_video_click, retail_tab_switch, dark_mode_toggle
- [x] 13F sync worker: SEC EDGAR → Supabase every 12h with hot/cold archival, OOM prevention, content-hash change detection
- [x] Cold storage: Supabase Storage bucket for archived 13F quarterly data with circuit-breaker pattern
- [x] Egress optimization: content-hash delta detection on startup, skip-existing on all sync workers
- [ ] Custom donor fields: name + opt-in to feature on support page (Phase 2, requires FastAPI endpoint + Stripe Checkout Sessions)
- [ ] User-configurable superinvestor list (currently hardcoded in superinvestors.py)
- [ ] Export to CSV / PDF
- [ ] Comparison across multiple funds on the same page
- [ ] Email notification automation (extend notification system with email delivery)

### Technical Debt

- [ ] CLI should optionally read from cache instead of always hitting SEC API
- [x] `get_enriched_holdings()` bypassed with `get_enriched_holdings_from_cache()` —
      now reads from cached data instead of calling SEC API + compare_quarters()
- [ ] Add proper logging (currently all errors are silently caught with `pass`)
- [ ] Add error handling for malformed SEC data (corrupt DataFrames, missing columns)
- [ ] Unit tests (none exist currently)
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
| Server unresponsive after 84 superinvestor expansion | `save_cache()` called synchronously after every fund during background refresh (84× disk writes blocking event loop); `notifications.json` and `watchlist.json` read from disk on every HTTP request; 84 HTMX lazy-loads fired simultaneously | Batched cache writes (every 10 funds, via `asyncio.to_thread`); in-memory caching for notification/watchlist state; staggered HTMX lazy-loads (3 per second); fire-and-forget disk saves in fund_row endpoint |
| 13F fund data disappearing after TTL expiry | `load_cache_from_supabase()` called `get_cached()` which returns `None` when TTL expires, causing all fund data to vanish until the sync worker refreshes | Switched to `get_cached_with_stale()` which returns expired data as stale fallback; added L2 Supabase fallback to all fund web endpoints with L1 promotion |
| Insider trades "failed to load" on deploy | When L1 TTL expires and Supabase query fails (transient), `get_latest_insider_trades()` fell through to OpenInsider scrape which also failed, returning empty list | Added `_get_cached_with_stale()` to insider_trading.py; both global and per-ticker functions now use 4-tier fallback (L1 fresh → L2 Supabase → L3 scrape → L4 stale L1), never returning empty results if data was previously loaded |
| YouTube channel filter clicks do nothing | `<script>` tags injected via `innerHTML` don't execute per browser spec | Re-create script elements with `document.createElement('script')` in retail.html's `loadCalendar()` and `loadLeaderboard()` |
| Stale upcoming livestreams from 2017/2021 | `get_youtube_events()` had no date filter — returned any row with `event_type='upcoming'` regardless of age | Added `.gte("scheduled_at", cutoff)` where cutoff is 6 hours ago |
| 13F sync crash: "Bucket not found" | `paperpanda-archive` Supabase Storage bucket never created; 84 funds each tried to upload and failed, accumulating untrimmed data until OOM | Circuit-breaker in cold_storage.py: one-time check, then short-circuit all calls. Always trim to 2 quarters even on archive failure. |
| 13F sync OOM on Railway | `get_all_by_category("13f")` loaded all 84 fund JSONB blobs (~30MB) just to check CIK existence | Replaced with `get_cache_keys_by_category()` (fetches only key strings). Added `gc.collect()` + memory logging. |
| Supabase egress limit exceeded (8 GB / 5 GB) | Every deploy pulled all 84 fund blobs; sync workers re-uploaded identical data every run | Content-hash change detection for 13F; skip-existing for insider/YouTube; delta startup hydration |

---

## Reference Rule

> **When context drifts, re-read this file.**
>
> This file documents the system as of 2026-02-23 (YouTube calendar sync,
> cold storage, PostHog analytics, egress optimization, circuit-breaker patterns).
> If told "Context is drifting," the first action should be to re-read
> `/Users/Tevis_1/13F-project/README_DEV.md` and reconcile any discrepancies
> with the actual code.
