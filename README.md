# PaperPanda — 13F Filing Tracker

A web dashboard for tracking SEC 13F institutional holdings filings from 84 superinvestors (Buffett, Ackman, Burry, Einhorn, and more). Live at [paperpanda.io](https://paperpanda.io).

## Features

### Homepage
- **S&P 500 Heatmap** - Interactive treemap showing daily performance grouped by sector (ECharts), with gold borders on stocks held by superinvestors
- **Most Added by Superinvestors** - Stack-ranked table of stocks most added this quarter, with analyst consensus and 52-week range
- **Superinvestor Cards** - Overview of all 84 tracked institutional investors with HTMX lazy-loading

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

### Cross-Fund Features
- **Activity Feed** - Real-time buys, sells, and position changes across all 84 funds
- **Grand Portfolio** - Aggregated view of all superinvestor holdings ranked by conviction
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
| Caching | JSON files at `~/.13f-cache/` with per-fund TTL |
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
| `CACHE_DIR` | No | - | Cache directory (default: `~/.13f-cache/`) |
| `POSTHOG_API_KEY` | No | Free tier | Product analytics |

> **Note:** The App Store ratings feature requires no API key (Apple's public iTunes API).

## Project Structure

```
src/filings/
├── web.py              # FastAPI app (30+ routes)
├── cli.py              # CLI entry point
├── client.py           # SEC EDGAR data access layer
├── analysts.py         # Analyst ratings (Finnhub + yfinance)
├── market_data.py      # S&P 500 heatmap, most-added, ticker search (~8K listings)
├── sentiment.py        # Market sentiment (CNN, Finnhub, ApeWisdom, Alpha Vantage)
├── vitals.py           # Alternative data (Glassdoor, People Data Labs, App Store)
├── cache.py            # Persistent cache (per-fund TTL: 7d/12h adaptive)
├── models.py           # Dataclasses (data contracts)
├── watchlist.py        # Watchlist persistence
├── notifications.py    # Filing notification engine + filing season detection
├── company_filings.py  # SEC filing links for stock pages
├── insider_trading.py  # Form 4 insider transaction data
├── superinvestors.py   # 84 hardcoded superinvestors
├── display.py          # CLI Rich formatters
└── templates/          # Jinja2 HTML templates
    ├── base.html       # Master layout + Fuse.js search + sortable tables
    ├── index.html      # Homepage: heatmap + most-added + investor cards
    ├── investor.html   # Superinvestor page (Holdings + Compare tabs)
    ├── stock.html      # Stock page (7 tabs, lazy-loaded)
    └── partials/       # HTMX / lazy-loaded partials
        ├── heatmap.html           # S&P 500 ECharts treemap
        ├── most_added.html        # Most-added-by-superinvestors table
        ├── ticker_search.html     # Nav autocomplete search input
        ├── analyst_ratings.html   # Analyst consensus + ratings table
        ├── sentiment.html         # Market/news sentiment cards
        ├── vitals.html            # Employee, culture, product cards
        ├── company_filings.html   # SEC filing links
        ├── insider_trades.html    # Insider trading table
        └── compare_content.html   # Compare quarters (lazy-loaded)
```

## Caching Strategy

The cache uses a **stale-while-revalidate** pattern:
1. Serve cached data immediately (never block on API calls)
2. Refresh stale funds in the background (per-fund TTL)
3. Keep old data on API failure (never lose data)

| Data Type | TTL | Reason |
|-----------|-----|--------|
| 13F Fund Data | 7 days (off-season) / 12 hours (filing season) | 13F data changes quarterly |
| Glassdoor Ratings | 30 days | Employee ratings change very slowly |
| People Data Labs | 7 days | Headcount changes slowly |
| App Store Ratings | 7 days | App ratings change slowly |
| Finnhub Sentiment | 2 hours | News changes frequently |
| CNN Fear & Greed | 1 hour | Market-wide index, intraday updates |
| Analyst Ratings | 5 minutes | Fresh data preferred |

Filing season is auto-detected (within 15 days of SEC 13F deadlines: Feb 14, May 15, Aug 14, Nov 14).

## License

MIT
