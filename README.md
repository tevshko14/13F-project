# PaperPanda — 13F Filing Tracker

A web dashboard for tracking SEC 13F institutional holdings filings from 84 superinvestors (Buffett, Ackman, Burry, Einhorn, and more). Live at [paperpanda.io](https://paperpanda.io).

## Features

### Homepage
- **Bento Grid Layout** - Glassmorphism card design with mesh gradient background
- **S&P 500 Heatmap** - Interactive treemap showing daily performance grouped by sector (ECharts), with gold borders on stocks held by superinvestors
- **Most Added by Superinvestors** - ECharts bar chart with gradient fills, analyst consensus tooltips, and 52-week range
- **Most Bought by Congress** - ECharts bar chart of trending congressional stock purchases
- **Trending with Smart Money** - Combined superinvestor + congress stacked bar chart with gradient fills
- **Retail Sentiment** - CNN Fear & Greed gauge widget with weekly/monthly/yearly comparison
- **Quick Access Cards** - Retail, Funds, and Insiders overview cards
- **Support Widget** - Panda Fund progress bar with Stripe Pricing Table embed for direct monthly subscriptions

### Congress Trading Page (`/congress`)
- **Congress Tab** - Chamber dot visualizations (Senate + House), each dot is a politician colored by party, with hover stats and click → profile
- **Holdings Tab** - Trending stocks bought by Congress, consensus leaders, recent buy/sell momentum, all-holdings table
- **Activity Tab** - Real-time trade filing feed with filter buttons (All/Buys/Sells/House/Senate)

### Politician Profile Pages (`/politician/{id}`)
- **Stats Grid** - Total trades, buys, sells, estimated net worth, last trade date
- **Portfolio Concentration** - ECharts donut chart (top 10 holdings by value)
- **Trade History** - Full sortable trade table

### Navigation
Top nav: **Home** | **Retail** | **Funds** | **Insiders** | **Congress** | **Earnings Calendar** | **Support the Panda** | **🔔 Notification Bell**

### Retail Page (`/retail`)
- **Sentiment Tab** - CNN Fear & Greed gauge, summary cards (Most Mentioned, Biggest Rank Mover, Top 5 Trending)
- **Leaderboard Tab** - ApeWisdom Reddit trending stocks table (sortable, expandable, lazy-loaded)
- **Calendar Tab** - Finance YouTuber schedules and topics

### Top Funds Page (`/funds`)
- **Funds Tab** - Overview of all 84 tracked institutional investors with HTMX lazy-loading
- **Holdings Tab** - Aggregated view of all superinvestor holdings ranked by conviction
- **Activity Tab** - Real-time buys, sells, and position changes across all 84 funds

### Insider Trading Page (`/insider-trading`)
- **Global Screener** - Latest insider buys/sells across all stocks with top-tickers chart
- **Per-Ticker View** - Full insider trade history (back to IPO) with quarterly grouping, insider summary cards, stacked bar chart, hover tooltips with quarterly breakdowns, and per-insider dropdown filter

### Earnings Calendar Page (`/earnings-calendar`)
- **Weekly View** — Mon–Fri grid showing upcoming earnings reports grouped by day, with BMO (Before Market Open) / AMC (After Market Close) timing badges
- **Monthly Heatmap** — Calendar grid color-coded by earnings density (more reports = deeper red), with mini company logo strips per day
- **Hover Cards** — Mouse over any company logo to see name, sector, EPS/revenue estimates, actuals (if reported), and beat/miss badges
- **Day Detail Panel** — Click any day to expand a full table with company, ticker, timing, estimates, actuals, and beat/miss results
- **Navigation** — Week/month toggle, prev/next arrows, "Today" button; all views load via HTMX without full page reload
- **Data Sources** — Finnhub `/calendar/earnings` (primary, free tier) with FMP `/earning_calendar` fallback (premium); deterministic mock data when no API keys configured
- **Feature Flag** — Controlled by `EARNINGS_CALENDAR_ENABLED` env var (default: enabled)

### Support Page (`/support`)
- **Panda Fund Dashboard** - Transparency page showing monthly funding progress, cost breakdown, and funding history chart (ECharts)
- **Stripe Donations** - One-time (Buy Button) and recurring (Pricing Table) support via embedded Stripe OOTB components
- **YouTube & Feedback CTAs** - Links to @funofinvesting and feedback form

### Search
- **Fuzzy Ticker Search** - Fuse.js-powered autocomplete with ~8,000 NYSE/NASDAQ listings, weighted by superinvestor holdings and S&P 500 membership
- **Fund Manager Search** - Search SEC EDGAR for any institutional investor by name

### Stock Pages (8 Tabs)
- **Overview** - Which superinvestors hold this stock, quarterly activity chart (Chart.js), star/watchlist button
- **Ownership** - Detailed holder list with position sizes and portfolio percentages
- **Analyst Ratings** - Wall Street consensus with firm-level upgrades/downgrades (Finnhub + yfinance)
- **Signals** - Unified alternative data tab combining sentiment, search interest, and web traffic:
  - *Sentiment* — Market mood (CNN Fear & Greed), news sentiment (Finnhub), Reddit buzz (ApeWisdom), NLP news (Alpha Vantage)
  - *Search Interest* — Google Trends keyword tracking (intent, product, comparison) with 3-month interest chart
  - *Web Traffic* — Domain popularity (Cloudflare Radar + Tranco), Wikipedia event detector with daily views chart
- **Vitals** - Employee headcount (People Data Labs), culture ratings (Glassdoor), App Store ratings (Apple iTunes)
- **SEC Filings** - Direct links to the company's SEC filings
- **Insider Trading** - Full insider trade history with quarterly groups, insider cards with hover tooltips, buy/sell timeline chart, per-insider filter, and SEC-resolved officer titles (CEO, CFO, etc.)
- **Congress** - Congressional stock trading activity (STOCK Act disclosures)

### Investor Pages
- **Holdings Tab** - Full portfolio with activity badges and percentage allocations
- **Compare Quarters** - Side-by-side quarterly diff with new buys, adds, reduces, and sells

### Cross-Site Features
- **Watchlist** - Star tickers from any stock page, persistent sidebar on all pages
- **Notification Bell** - Red dot indicator in the navbar with dropdown preview (latest 8) and full history page (`/notifications`). Four notification sources: 13F filing changes (new buys/sells from superinvestors), YouTube uploads from tracked finance channels, Reddit ticker velocity spikes, and congressional trade filings. Toast popups for new alerts (max 2 visible). Client-side dismiss state via `localStorage`, 120-second HTMX polling with 15-second server-side cache. Global notifications stored in Supabase with 48-hour retention.
- **Sortable Tables** - Click any column header to sort across all pages
- **Background Refresh** - Self-healing, request-triggered refresh for stale 13F data with per-fund TTL
- **Graceful Error Handling** - HTMX-aware inline error partials with Panda Fund CTA on rate limits
- **Dark Mode** - Light/dark theme toggle (sun/moon pill) in the navbar; persists via `localStorage`; respects system preference on first visit. ECharts axis/label colors auto-update on toggle via `pp-theme-changed` event; Stripe embeds invert via CSS `filter`
- **Glassmorphism UI** - Site-wide frosted glass card effects (`backdrop-filter: blur`) with theme-aware transparency
- **Gradient Bar Charts** - Consistent `LinearGradient` fills across all ECharts and Chart.js bar charts (blue, green, red, orange palettes)

### CLI
- Search managers, view holdings, and compare quarters from the terminal

## Tech Stack

| Layer | Technology |
|-------|------------|
| Language | Python 3.12+ |
| Package Manager | `uv` |
| Web Framework | FastAPI + Jinja2 + HTMX |
| CSS | Pico CSS v2 (classless, CDN) |
| Charts | Chart.js v4 + ECharts v5 (heatmap, bar charts, gradients) |
| Search | Fuse.js v7 (weighted fuzzy search) |
| CLI Output | Rich |
| SEC Data | `edgartools` (wraps EDGAR API) |
| Market Data | `yfinance` + NASDAQ Trader (all US listings) |
| Analyst Data | `yfinance` (free) + `finnhub-python` (optional) |
| Sentiment | CNN, Finnhub, ApeWisdom, Alpha Vantage |
| Search Interest | Google Trends (pytrends) |
| Web Traffic | Cloudflare Radar, Tranco, Wikipedia Page Views |
| Vitals | People Data Labs, Glassdoor (RapidAPI), Apple iTunes |
| Insider Data | OpenInsider (scraped, full history) + SEC Form 4 XML (title resolution) + Supabase `insider_trades` table |
| Congress Data | Capitol Trades (scraped, STOCK Act disclosures) + Supabase cold archive (~35K trades, 200+ politicians) |
| Caching | Supabase Postgres (L2, survives deploys) + disk JSON (L3 fallback) |
| Hosting | Railway (auto-deploy from main) |
| Domain | [paperpanda.io](https://paperpanda.io) |

## Quick Start

```bash
# Install dependencies
uv sync

# Run the web dashboard
uv run filings-web
# Open http://localhost:8000

# CLI commands
uv run filings search "Berkshire"
uv run filings holdings 1067983
uv run filings compare 1067983
```

## Deployment (Railway)

The project includes a Dockerfile configured for Railway deployment. Pushes to `main` trigger auto-deploy.

```bash
# Or manually via Railway CLI:
railway up
```

### Environment Variables

| Variable | Required | Free Tier | Description |
|----------|----------|-----------|-------------|
| `PORT` | Auto (Railway) | - | Port for the web server (default: 8000) |
| `SEC_IDENTITY` | No | - | SEC EDGAR identity string |
| `FINNHUB_API_KEY` | No | 60 calls/min | News sentiment + analyst ratings |
| `ALPHAVANTAGE_API_KEY` | No | 25 calls/day | NLP news sentiment analysis |
| `GLASSDOOR_RAPIDAPI_KEY` | No | 25 calls/month | Employee culture ratings |
| `PDL_API_KEY` | No | 100 calls/month | Employee headcount data |
| `SUPABASE_URL` | No | Free tier | Supabase project URL (persistent cache) |
| `SUPABASE_SERVICE_KEY` | No | Free tier | Supabase service role JWT |
| `SUPABASE_DB_PASSWORD` | No | Free tier | Supabase DB password (auto-migration) |
| `CACHE_DIR` | No | - | Cache directory (default: `~/.13f-cache/`) |
| `POSTHOG_API_KEY` | No | Free tier | Product analytics |
| `STRIPE_PUBLISHABLE_KEY` | No | - | Stripe publishable key (for donation embeds) |
| `STRIPE_BUY_BUTTON_ID` | No | - | Stripe Buy Button ID (one-time donations) |
| `STRIPE_PRICING_TABLE_ID` | No | - | Stripe Pricing Table ID (recurring subscriptions) |
| `PANDA_FUND_RAISED` | No | - | Current month's donation total in dollars |
| `FEEDBACK_LINK` | No | - | URL to feedback form (Notion, Google Form, etc.) |
| `FMP_API_KEY` | No | 250 calls/day | Financial Modeling Prep (earnings calendar fallback, earnings scorecard) |
| `EARNINGS_CALENDAR_ENABLED` | No | - | Enable/disable earnings calendar page (default: `1` = enabled) |
| `ENABLE_BACKGROUND_REFRESH` | No | - | Enable/disable background 13F refresh (default: `true`) |
| `WEB_WORKERS` | No | - | Gunicorn worker count (default: `2`) |
| `HEALTH_SECRET` | No | - | Secret for `/health/detail` endpoint |

> **Note:** The App Store ratings feature requires no API key (Apple's public iTunes API).
> Without Supabase env vars, the app works identically using disk-only cache.
> Without Stripe env vars, the support page shows "coming soon" placeholders gracefully.

## Project Structure

```
src/filings/
├── web.py              # FastAPI app (40+ routes, background refresh, Stripe/support)
├── cli.py              # CLI entry point
├── client.py           # SEC EDGAR data access layer
├── analysts.py         # Analyst ratings (Finnhub + yfinance)
├── market_data.py      # S&P 500 heatmap, most-added, ticker search (~8K listings)
├── sentiment.py        # Market sentiment (CNN, Finnhub, ApeWisdom, Alpha Vantage)
├── vitals.py           # Alternative data (Glassdoor, PDL, App Store) + Supabase persistence
├── cache.py            # Cache layer (3-tier: in-memory → Supabase → disk)
├── supabase_cache.py   # Supabase L2 persistent cache (survives deploys)
├── insider_trading.py  # Form 4 insider transaction data (Supabase-first + scrape fallback)
├── insider_sync.py     # Cron worker: scrape OpenInsider → upsert to Supabase (every 30 min)
├── congress_trading.py # STOCK Act: Capitol Trades scraper + display prep (chamber viz, trending, consensus, momentum, activity)
├── earnings.py         # Per-ticker earnings history (yfinance + Finnhub + FMP)
├── earnings_scorecard.py # Macro earnings season metrics (FMP + Supabase)
├── earnings_calendar.py  # Earnings calendar (Finnhub + FMP, week/month views)
├── models.py           # Dataclasses (data contracts)
├── watchlist.py        # Watchlist persistence
├── notifications.py    # Notification creators (13F, YouTube, Reddit, Congress) + filing season detection
├── sync_worker.py      # Cron worker: refresh 13F data + create notifications
├── youtube_sync.py     # Cron worker: sync YouTube events + create notifications
├── company_filings.py  # SEC filing links for stock pages
├── auth.py             # Authentication (sign-in, sessions)
├── superinvestors.py   # 84 hardcoded superinvestors
├── display.py          # CLI Rich formatters
├── static/             # Static assets
│   ├── logo-nav.png        # Light-mode navbar logo
│   └── logo-nav-dark.png   # Dark-mode navbar logo (transparent background, switched via CSS data-theme)
└── templates/          # Jinja2 HTML templates
    ├── base.html       # Master layout (nav: Home, Retail, Funds, Insiders, Support; dark mode toggle; CSS custom properties)
    ├── home.html       # Homepage: heatmap + most-added + cards + support widget
    ├── retail.html     # Retail page (Sentiment, Leaderboard, Calendar tabs)
    ├── grand_portfolio.html  # Top Funds page (Funds, Holdings, Activity tabs)
    ├── investor.html   # Individual fund page (Holdings + Compare tabs)
    ├── stock.html      # Stock page (7 tabs, lazy-loaded)
    ├── insider_trading.html  # Insider trading screener page
    ├── congress.html   # Congress Trading page (3 tabs: Congress, Holdings, Activity)
    ├── politician.html # Politician profile (stats, donut chart, trade history)
    ├── notifications.html # Notification history page
    ├── activity.html   # Cross-fund activity feed
    ├── support.html    # Panda Fund transparency dashboard + Stripe donations
    ├── earnings_calendar.html  # Earnings calendar page (weekly/monthly views)
    └── partials/       # HTMX / lazy-loaded partials
        ├── heatmap.html             # S&P 500 ECharts treemap
        ├── most_added.html          # Most-added-by-superinvestors table
        ├── ticker_search.html       # Nav autocomplete search input
        ├── analyst_ratings.html     # Analyst consensus + ratings table
        ├── sentiment.html           # Market/news sentiment cards
        ├── vitals.html              # Employee, culture, product cards
        ├── company_filings.html     # SEC filing links
        ├── insider_trades.html      # Insider trading table (global)
        ├── stock_insider_trades.html # Insider trades (per-ticker)
        ├── retail_leaderboard.html  # ApeWisdom Reddit leaderboard (lazy-loaded)
        ├── compare_content.html     # Compare quarters (lazy-loaded)
        ├── earnings_calendar_grid.html  # Earnings calendar grid/heatmap (HTMX partial)
        ├── earnings_calendar_day.html   # Earnings calendar day detail table (HTMX partial)
        └── data_error.html          # Reusable error partial (rate limits, HTMX-aware)
```

## Cron Jobs (Railway Cron Services)

| Service | Command | Schedule | Purpose |
|---------|---------|----------|---------|
| 13F Sync | `filings-sync` | Every 12h | Refresh stale superinvestor 13F holdings from SEC EDGAR |
| Insider Sync | `filings-insider-sync` | Every 30min | Scrape OpenInsider → upsert to Supabase |
| YouTube Sync | `filings-youtube-sync` | Periodic | Sync YouTube events + create notifications |
| Congress Sync | `python scripts/sync_congress_trades.py` | Every 24h | Incremental scrape of Capitol Trades → Supabase |

Each cron service shares the same Docker image. The `START_COMMAND` env var overrides the default gunicorn web server with the cron script.

#### Utility Commands

| Command | Purpose |
|---------|---------|
| `filings-migrate-cold` | Archive older 13F quarters to Supabase cold storage |
| `filings-insider-backfill` | Backfill historical insider purchase data into cold table |
| `filings-insider-returns` | Calculate forward returns (30d/90d/180d/365d) for insider trades |

## Caching Strategy

All endpoints are **cache-first** — data is always served from cache, and external APIs are only called when data is stale or missing. The cache uses a **stale-while-revalidate** pattern across multiple tiers:

#### 13F Fund Data (3-tier)

| Tier | Storage | Survives Deploy? | Speed |
|------|---------|-------------------|-------|
| L1 | In-memory (`app.state`) | No | Sub-ms |
| L2 | Supabase Postgres (`api_cache` table) | **Yes** | ~50ms |
| L3 | Disk JSON (`~/.13f-cache/`) | Only with volume mount | ~5ms |

On startup, L1 is hydrated from Supabase (one query for all ~100 funds) with a **30-second timeout** — if Supabase is slow or unavailable, the app falls back to disk rather than hanging. If Supabase is unavailable, falls back to disk. Every successful API fetch writes through to all tiers. Expired L2 data is returned as stale fallback (never dropped on TTL expiry).

#### Insider Trades (hot/cold architecture with 4-tier stale fallback)

| Tier | Storage | Description |
|------|---------|-------------|
| L1 | In-memory dict (5-10 min TTL) | Fast path, sub-ms |
| L2 | Supabase `insider_trades` table (hot, 30-day window) | Dedicated typed table, synced every 30 min |
| L2.5 | OpenInsider screener scrape (full history, paginated) | All-time history via `fd=0`/`td=0` params, up to 300 trades |
| L2.6 | Supabase `insider_purchases_history` table (cold, permanent) | Historical purchases, never deleted |
| L3 | OpenInsider scrape + backfill | Fallback when DB is empty/down |
| L4 | Stale L1 data | Last resort — never show errors to users |

Per-ticker data merges hot table → OpenInsider scrape → cold table, deduplicated by `sec_url`. The `insider_sync` cron worker scrapes OpenInsider every 30 minutes and upserts to Supabase. Title resolution fetches raw SEC Form 4 XML to replace "See Remarks" with real officer titles (CEO, CFO, etc.).

### What gets cached in Supabase

| Category | Data | TTL |
|----------|------|-----|
| `13f` | All ~100 superinvestor fund holdings, changes, quarterly history | 7 days / 12 hours (filing season) |
| `glassdoor` | Company culture ratings per ticker | 90 days |
| `glassdoor_quota` | Monthly API call counter | Never expires |
| `pdl` | People Data Labs employee data per ticker | 7 days |
| `pdl_quota` | Monthly PDL API call counter (100/month limit) | Never expires |
| `appstore` | Apple App Store ratings per ticker | 7 days |
| `insider_trades` | Insider trades (dedicated table, upsert-only) | No TTL (data persists) |
| `congress_members` | 200+ politician profiles (cold archive) | No TTL (upsert-refreshed) |
| `congress_trades` | ~35K STOCK Act disclosures (cold, write-once) | No TTL (append-only) |
| `congress_trades_prices` | Forward return data (+30/90/180/365d) | No TTL (refreshed as windows close) |
| `congress_sync_log` | Daily sync job health tracking | 90 days |
| `notifications` | 13F, YouTube, Reddit, Congress trade alerts | 48 hours |

### TTL by data type

| Data Type | TTL | Reason |
|-----------|-----|--------|
| 13F Fund Data | 7 days (off-season) / 12 hours (filing season) | 13F data changes quarterly |
| Glassdoor Ratings | 90 days | Employee ratings change very slowly |
| Insider Trades (L1) | 5-10 minutes | Form 4 filings update frequently |
| Insider Trades (L2) | No TTL (Supabase) | Sync worker upserts every 30 min |
| People Data Labs | 7 days | Headcount changes slowly |
| App Store Ratings | 7 days | App ratings change slowly |
| Finnhub Sentiment | 2 hours | News changes frequently |
| Earnings Calendar | 1 hour (in-memory) | Calendar events change intraday as companies confirm timing |
| Finnhub Raw Calendar | 1 hour (in-memory, shared) | Shared by earnings calendar + revenue enrichment |
| CNN Fear & Greed | 1 hour | Market-wide index, intraday updates |
| ApeWisdom Reddit | 1 hour | Reddit mentions aggregated hourly |
| Analyst Ratings | 5 minutes | Fresh data preferred |

Filing season is auto-detected (within 15 days of SEC 13F deadlines: Feb 14, May 15, Aug 14, Nov 14).

### Automatic Retention Cleanup

On every deploy, the app runs a background cleanup to stay within the Supabase free-tier row limits:

| Table | Retention |
|-------|-----------|
| `insider_trades` | 6 months |
| `youtube_events` | 30 days |
| `sync_logs` | 30 days |
| `congress_sync_log` | 90 days |
| `notifications` | 48 hours |
| `api_cache` | Expired entries only (by `expires_at`) |
| `congress_trades` | Permanent (cold, write-once) |
| `congress_members` | Permanent (refreshed on sync) |

## License

MIT
