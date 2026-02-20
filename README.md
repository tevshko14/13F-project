# PaperPanda — 13F Filing Tracker

A web dashboard for tracking SEC 13F institutional holdings filings from 84 superinvestors (Buffett, Ackman, Burry, Einhorn, and more). Live at [paperpanda.io](https://paperpanda.io).

## Features

### Homepage
- **S&P 500 Heatmap** - Interactive treemap showing daily performance grouped by sector (ECharts), with gold borders on stocks held by superinvestors
- **Most Added by Superinvestors** - Stack-ranked table of stocks most added this quarter, with analyst consensus and 52-week range
- **Quick Access Cards** - Retail, Funds, and Insiders overview cards

### Navigation
Top nav: **Home** | **Retail** | **Funds** | **Insiders**

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
- **Per-Ticker View** - Insider trades for individual companies

### Search
- **Fuzzy Ticker Search** - Fuse.js-powered autocomplete with ~8,000 NYSE/NASDAQ listings, weighted by superinvestor holdings and S&P 500 membership
- **Fund Manager Search** - Search SEC EDGAR for any institutional investor by name

### Stock Pages (7 Tabs)
- **Overview** - Which superinvestors hold this stock, quarterly activity chart (Chart.js), star/watchlist button
- **Ownership** - Detailed holder list with position sizes and portfolio percentages
- **Analyst Ratings** - Wall Street consensus with firm-level upgrades/downgrades (Finnhub + yfinance)
- **Sentiment** - Market mood (CNN Fear & Greed), news sentiment (Finnhub), Reddit buzz (ApeWisdom), NLP news (Alpha Vantage)
- **Vitals** - Employee headcount (People Data Labs), culture ratings (Glassdoor), App Store ratings (Apple iTunes)
- **SEC Filings** - Direct links to the company's SEC filings
- **Insider Trading** - Recent Form 4 insider transactions

### Investor Pages
- **Holdings Tab** - Full portfolio with activity badges and percentage allocations
- **Compare Quarters** - Side-by-side quarterly diff with new buys, adds, reduces, and sells

### Cross-Site Features
- **Watchlist** - Star tickers from any stock page, persistent sidebar on all pages
- **Notifications** - SSE-powered real-time alerts when new filings match your watchlist
- **Sortable Tables** - Click any column header to sort across all pages

### CLI
- Search managers, view holdings, and compare quarters from the terminal

## Tech Stack

| Layer | Technology |
|-------|------------|
| Language | Python 3.12+ |
| Package Manager | `uv` |
| Web Framework | FastAPI + Jinja2 + HTMX |
| CSS | Pico CSS v2 (classless, CDN) |
| Charts | Chart.js v4 + ECharts v5 (heatmap) |
| Search | Fuse.js v7 (weighted fuzzy search) |
| CLI Output | Rich |
| SEC Data | `edgartools` (wraps EDGAR API) |
| Market Data | `yfinance` + NASDAQ Trader (all US listings) |
| Analyst Data | `yfinance` (free) + `finnhub-python` (optional) |
| Sentiment | CNN, Finnhub, ApeWisdom, Alpha Vantage |
| Vitals | People Data Labs, Glassdoor (RapidAPI), Apple iTunes |
| Insider Data | OpenInsider (scraped) + Supabase `insider_trades` table |
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

> **Note:** The App Store ratings feature requires no API key (Apple's public iTunes API).
> Without Supabase env vars, the app works identically using disk-only cache.

## Project Structure

```
src/filings/
├── web.py              # FastAPI app (40+ routes)
├── cli.py              # CLI entry point
├── client.py           # SEC EDGAR data access layer
├── analysts.py         # Analyst ratings (Finnhub + yfinance)
├── market_data.py      # S&P 500 heatmap, most-added, ticker search (~8K listings)
├── sentiment.py        # Market sentiment (CNN, Finnhub, ApeWisdom, Alpha Vantage)
├── vitals.py           # Alternative data (Glassdoor, People Data Labs, App Store)
├── cache.py            # Cache layer (3-tier: in-memory → Supabase → disk)
├── supabase_cache.py   # Supabase L2 persistent cache (survives deploys)
├── insider_trading.py  # Form 4 insider transaction data (Supabase-first + scrape fallback)
├── insider_sync.py     # Cron worker: scrape OpenInsider → upsert to Supabase (every 30 min)
├── models.py           # Dataclasses (data contracts)
├── watchlist.py        # Watchlist persistence
├── notifications.py    # Filing notification engine + filing season detection
├── company_filings.py  # SEC filing links for stock pages
├── auth.py             # Authentication (sign-in, sessions)
├── superinvestors.py   # 84 hardcoded superinvestors
├── display.py          # CLI Rich formatters
└── templates/          # Jinja2 HTML templates
    ├── base.html       # Master layout (nav: Home, Retail, Funds, Insiders)
    ├── home.html       # Homepage: heatmap + most-added + cards
    ├── retail.html     # Retail page (Sentiment, Leaderboard, Calendar tabs)
    ├── grand_portfolio.html  # Top Funds page (Funds, Holdings, Activity tabs)
    ├── investor.html   # Individual fund page (Holdings + Compare tabs)
    ├── stock.html      # Stock page (7 tabs, lazy-loaded)
    ├── insider_trading.html  # Insider trading screener page
    ├── activity.html   # Cross-fund activity feed
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
        └── compare_content.html     # Compare quarters (lazy-loaded)
```

## Caching Strategy

All endpoints are **cache-first** — data is always served from cache, and external APIs are only called when data is stale or missing. The cache uses a **stale-while-revalidate** pattern across multiple tiers:

#### 13F Fund Data (3-tier)

| Tier | Storage | Survives Deploy? | Speed |
|------|---------|-------------------|-------|
| L1 | In-memory (`app.state`) | No | Sub-ms |
| L2 | Supabase Postgres (`api_cache` table) | **Yes** | ~50ms |
| L3 | Disk JSON (`~/.13f-cache/`) | Only with volume mount | ~5ms |

On startup, L1 is hydrated from Supabase (one query for all ~100 funds). If Supabase is unavailable, falls back to disk. Every successful API fetch writes through to all tiers. Expired L2 data is returned as stale fallback (never dropped on TTL expiry).

#### Insider Trades (4-tier with stale fallback)

| Tier | Storage | Description |
|------|---------|-------------|
| L1 | In-memory dict (5-10 min TTL) | Fast path, sub-ms |
| L2 | Supabase `insider_trades` table | Dedicated typed table, no TTL |
| L3 | OpenInsider scrape | Fallback when DB is empty/down |
| L4 | Stale L1 data | Last resort — never show errors to users |

The `insider_sync` cron worker scrapes OpenInsider every 30 minutes and upserts to Supabase. If all upstream sources fail, stale L1 data is returned instead of empty results.

### What gets cached in Supabase

| Category | Data | TTL |
|----------|------|-----|
| `13f` | All ~100 superinvestor fund holdings, changes, quarterly history | 7 days / 12 hours (filing season) |
| `glassdoor` | Company culture ratings per ticker | 90 days |
| `glassdoor_quota` | Monthly API call counter | Never expires |
| `insider_trades` | Insider trades (dedicated table, upsert-only) | No TTL (data persists) |

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
| CNN Fear & Greed | 1 hour | Market-wide index, intraday updates |
| ApeWisdom Reddit | 1 hour | Reddit mentions aggregated hourly |
| Analyst Ratings | 5 minutes | Fresh data preferred |

Filing season is auto-detected (within 15 days of SEC 13F deadlines: Feb 14, May 15, Aug 14, Nov 14).

## License

MIT
