# 13F Filing Viewer

A web dashboard and CLI tool for tracking SEC 13F institutional holdings filings from 84 superinvestors (Buffett, Ackman, Burry, Einhorn, and more).

## Features

- **Superinvestor Tracking** - Monitor 84 top institutional investors' 13F filings from SEC EDGAR
- **Investor Pages** - Click any investor name to view a tabbed page with Holdings and Compare Quarters
- **Stock Detail Pages** - See which superinvestors hold a specific stock, with quarterly activity charts
- **Analyst Ratings** - View firm-level upgrade/downgrade ratings from Wall Street (via Finnhub + yfinance)
- **Sortable Tables** - Click any column header to sort tables across all pages
- **Activity Feed** - Track recent buys, sells, and position changes across all funds
- **Grand Portfolio** - Aggregated view of all superinvestor holdings ranked by conviction
- **Watchlist** - Star tickers you care about, with real-time notifications when superinvestors trade them
- **Notifications** - SSE-powered real-time alerts when new filings match your watchlist
- **CLI** - Search managers, view holdings, and compare quarters from the terminal

## Tech Stack

| Layer | Technology |
|-------|------------|
| Language | Python 3.12+ |
| Package Manager | `uv` |
| Web Framework | FastAPI + Jinja2 + HTMX |
| CSS | Pico CSS v2 (classless, CDN) |
| Charts | Chart.js v4 |
| CLI Output | Rich |
| SEC Data | `edgartools` (wraps EDGAR API) |
| Analyst Data | `yfinance` (free) + `finnhub-python` (free tier, optional API key) |
| Caching | JSON files at `~/.13f-cache/` |

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

The project includes a Dockerfile configured for Railway deployment:

```bash
# Railway auto-deploys from main branch
# Or manually via Railway CLI:
railway up
```

### Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `PORT` | Auto (Railway) | Port for the web server (default: 8000) |
| `SEC_IDENTITY` | No | SEC EDGAR identity string (default: generic) |
| `FINNHUB_API_KEY` | No | Finnhub API key for additional analyst data. If not set, only yfinance data is used. |

## Project Structure

```
src/filings/
├── web.py              # FastAPI app (20+ routes)
├── cli.py              # CLI entry point
├── client.py           # SEC EDGAR data access layer
├── analysts.py         # Analyst ratings (Finnhub + yfinance)
├── cache.py            # JSON file cache (6-hour TTL)
├── models.py           # Dataclasses (data contracts)
├── watchlist.py        # Watchlist persistence
├── notifications.py    # Filing notification engine
├── superinvestors.py   # 84 hardcoded superinvestors
├── display.py          # CLI Rich formatters
└── templates/          # Jinja2 HTML templates
    ├── base.html       # Master layout + sortable table engine
    ├── investor.html   # Superinvestor page (Holdings + Compare tabs)
    ├── stock.html      # Stock detail + analyst ratings tabs
    └── partials/       # HTMX partials
        ├── analyst_ratings.html   # Analyst consensus + ratings table
        └── compare_content.html   # Compare quarters (lazy-loaded)
```

## Analyst Ratings

The stock detail page includes an **Analyst Ratings** tab that shows:

- **Consensus Summary** - Buy/Hold/Sell breakdown with visual bar
- **Recent Actions** - Table of firm-level upgrades, downgrades, initiations, and reiterations

Data is fetched lazily (on tab click) from yfinance (free, no API key) and optionally Finnhub (free tier with API key). Results are cached in memory for 5 minutes.

> **Note:** Free data sources provide firm-level ratings (e.g., "JP Morgan: Overweight") but not individual analyst names. For per-analyst data, a paid API like Financial Modeling Prep ($22/mo) would be needed.

## License

MIT
