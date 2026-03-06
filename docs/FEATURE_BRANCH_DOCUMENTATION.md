# Feature Branch Documentation: Alternative Signals & Macro Dashboard

> **Branch:** `claude/investigate-stock-traffic-api-4onpt`
> **Scope:** 24 files changed, +5,290 lines added across 11 commits
> **Status:** Ready to merge into `main`

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Feature 1: Web Traffic Signals](#feature-1-web-traffic-signals)
3. [Feature 2: Google Trends Integration](#feature-2-google-trends-integration)
4. [Feature 3: Unified Signals Tab](#feature-3-unified-signals-tab)
5. [Feature 4: Macro Earnings Scorecard](#feature-4-macro-earnings-scorecard)
6. [Architecture Overview](#architecture-overview)
7. [Database Migrations](#database-migrations)
8. [Data Pipeline Scripts](#data-pipeline-scripts)
9. [API Endpoints](#api-endpoints)
10. [Frontend Components](#frontend-components)
11. [Configuration & Environment Variables](#configuration--environment-variables)
12. [Testing](#testing)
13. [Deployment Checklist](#deployment-checklist)

---

## Executive Summary

This branch introduces a comprehensive **alternative signals** platform to PaperPanda, adding three new data collection modules and a macro-level earnings dashboard. The goal is to give users non-traditional stock signals beyond standard 13F filings — web traffic patterns, search interest trends, and aggregated earnings season performance.

### What's New

| Feature | Description | Data Source(s) |
|---------|-------------|----------------|
| **Web Traffic Signals** | Domain popularity ranking + Wikipedia page views for digital-first stocks | Cloudflare Radar, Tranco List, Wikimedia API |
| **Google Trends** | Search interest for per-ticker keywords, macro sentiment, and trending searches | Google Trends (via `pytrends`) |
| **Unified Signals Tab** | Merged sentiment + web traffic + Google Trends into a single stock-level tab | All above + existing Reddit/ApeWisdom data |
| **Macro Earnings Scorecard** | Aggregated earnings season dashboard with beat rates, charts, and sortable table | Financial Modeling Prep (FMP) API |

### Commit History

| Commit | Description |
|--------|-------------|
| `b866415` | Add web traffic data investigation document |
| `05cc9a7` | Add web traffic tracking for digital-first stocks |
| `ec0f032` | Replace dead SimilarWeb API with Cloudflare Radar + Tranco List |
| `29ab907` | Fix source comment in web_traffic_history migration |
| `63f93b5` | Add historical backfill script, reframe Wikipedia as event detector |
| `f9da3ef` | Add Google Trends integration for alternative signals dashboard |
| `ec81ae4` | Add pytrends dependency, backfill script, and graceful error handling |
| `959c1da` | Update uv.lock for pytrends dependency |
| `a077a19` | Merge Sentiment + Web Traffic tabs into unified Signals tab |
| `6c5a0fd` | Update docs to reflect unified Signals tab |
| `f1d9462` | Add Macro page with Earnings Scorecard dashboard |

---

## Feature 1: Web Traffic Signals

### Overview

Tracks web traffic metrics for "digital-first" stocks — companies where website/app traffic is a meaningful revenue proxy (e-commerce, SaaS, fintech, ad-driven platforms). Uses **free, public data sources** after evaluating and rejecting paid options (see `docs/web-traffic-investigation.md` for the full analysis).

### Key Files

| File | Purpose |
|------|---------|
| `src/filings/web_traffic.py` (562 lines) | Core data collection module — Cloudflare Radar, Tranco List, Wikipedia page views |
| `scripts/sync_web_traffic.py` (228 lines) | Daily cron job for Supabase cold storage sync |
| `scripts/backfill_web_traffic.py` (462 lines) | Historical backfill for Wikipedia + Tranco data |
| `sql/005_web_traffic_history.sql` (52 lines) | Database migration |
| `src/filings/templates/partials/web_traffic.html` (365 lines) | Frontend partial with ECharts visualizations |

### Data Sources

#### 1. Cloudflare Radar API
- **What:** Domain popularity rank (1 = most popular globally)
- **Endpoint:** Cloudflare Radar API (free, CC BY-NC 4.0 license)
- **Cache TTL:** 24 hours
- **Limitation:** No historical data — collect-going-forward only

#### 2. Tranco List
- **What:** Aggregated domain ranking from 5 independent sources (Umbrella, Majestic, etc.)
- **Endpoint:** Daily CSV download from `tranco-list.eu`
- **Cache TTL:** 24 hours
- **Historical:** Available via dated download endpoint for backfills

#### 3. Wikipedia Page Views
- **What:** Daily article page view counts — acts as a public attention/event detector
- **Endpoint:** Wikimedia REST API (`/metrics/pageviews/per-article/`)
- **Cache TTL:** 24 hours
- **Historical:** Available back to July 2015

### Digital-First Stock Classification

The module uses **GICS sector + sub-industry overrides** to automatically determine which stocks are relevant for web traffic tracking. Sectors with high web-traffic-to-revenue correlation include:

- Online brokerages (HOOD, SOFI, COIN)
- E-commerce (AMZN, SHOP, ETSY)
- SaaS platforms (CRM, SNOW, DDOG)
- Ad-driven platforms (META, GOOGL, SNAP)
- Fintech (PYPL, SQ, AFRM)
- Online marketplaces (ABNB, UBER, DASH)

### Ticker-to-Domain Mapping

A maintained mapping converts stock tickers to their primary website domains:

| Ticker | Domain |
|--------|--------|
| SOFI | sofi.com |
| HOOD | robinhood.com |
| COIN | coinbase.com |
| PYPL | paypal.com |
| AMZN | amazon.com |
| NFLX | netflix.com |
| ... | (60+ tickers mapped) |

### Caching Strategy

- **In-memory cache** with thread-safe locking (`threading.Lock()`)
- `_RADAR_TTL`: 24 hours (Cloudflare Radar domain rank)
- `_TRANCO_TTL`: 24 hours (Tranco list rankings)
- `_WIKI_TTL`: 24 hours (Wikipedia page views)
- `_SECTOR_TTL`: 7 days (sector classifications don't change frequently)

---

## Feature 2: Google Trends Integration

### Overview

Provides three layers of search interest signals using Google Trends data, fetched via the `pytrends` library with rate-limiting and exponential backoff.

### Key Files

| File | Purpose |
|------|---------|
| `src/filings/google_trends.py` (719 lines) | Core module — per-ticker keywords, macro trends, trending-to-ticker mapping |
| `scripts/backfill_google_trends.py` (284 lines) | Historical backfill script |
| `sql/006_google_trends_history.sql` (39 lines) | Database migration |
| `src/filings/templates/partials/google_trends_ticker.html` (131 lines) | Per-stock trends partial |
| `src/filings/templates/partials/google_trends_macro.html` (56 lines) | Macro sentiment trends partial |
| `src/filings/templates/partials/google_trends_trending.html` (36 lines) | Trending searches partial |

### Signal Layers

#### Layer 1: Per-Ticker Keywords
Auto-generated search terms specific to each company, organized into three categories:

| Category | Example (Robinhood) | Signal |
|----------|-------------------|--------|
| **Intent** | "Robinhood sign up", "Robinhood download" | User acquisition signal |
| **Product** | "Robinhood Gold", "Robinhood crypto" | Product interest signal |
| **Comparison** | "Coinbase vs Robinhood", "Webull vs Robinhood" | Competitive positioning |

Keywords are **auto-generated** from company metadata (sector, industry, known products) — no manual curation required.

#### Layer 2: Macro Trends Dashboard
Generic market-sentiment keywords tracked over time across categories:

| Category | Keywords |
|----------|----------|
| Market Fear | "recession", "market crash", "bear market" |
| Crypto | "bitcoin", "ethereum", "crypto" |
| AI Hype | "best AI stock", "artificial intelligence investing" |
| Geopolitical | "war", "sanctions", "tariffs" |

#### Layer 3: Trending Searches → Ticker Mapping
Google's **daily trending searches** are automatically matched to known tickers/companies for real-time signal detection. If "Tesla recall" is trending, it gets tagged to TSLA.

### Rate Limiting

Google Trends rate-limits aggressively (~2 requests/minute). The module implements:
- 5-second delay between requests
- Exponential backoff on 429 responses
- Full backfill of ~60 tickers takes approximately 15–20 minutes

### Caching

- **In-memory:** `_INTEREST_TTL` = 24 hours
- **Cold storage:** Persisted to `google_trends_history` Supabase table

---

## Feature 3: Unified Signals Tab

### Overview

Previously, the stock detail page had separate "Sentiment" and "Web Traffic" tabs. This branch **merges them into a single "Signals" tab** that provides a unified view of all alternative data for a given ticker.

### Key Files

| File | Purpose |
|------|---------|
| `src/filings/templates/partials/signals.html` (640 lines) | Unified signals partial — combines sentiment, web traffic, and Google Trends |
| `src/filings/templates/stock.html` | Updated stock page to use new unified tab |
| `src/filings/templates/alternative_signals.html` (expanded) | Updated alternative signals overview page |

### Layout

The Signals tab is organized into collapsible sections with a responsive 2-column grid:

```
┌─────────────────────────────────────────────────┐
│ SENTIMENT                                        │
│ ┌──────────────────┐ ┌────────────────────────┐ │
│ │ Reddit/ApeWisdom │ │ Retail Sentiment Score │ │
│ └──────────────────┘ └────────────────────────┘ │
│                                                   │
│ WEB TRAFFIC                                       │
│ ┌──────────────────┐ ┌────────────────────────┐ │
│ │ Domain Rankings  │ │ Wikipedia Page Views    │ │
│ │ (Radar + Tranco) │ │ (event detector chart) │ │
│ └──────────────────┘ └────────────────────────┘ │
│                                                   │
│ SEARCH INTEREST                                   │
│ ┌──────────────────────────────────────────────┐ │
│ │ Google Trends — Intent / Product / Comparison │ │
│ │ (full-width ECharts time series)              │ │
│ └──────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────┘
```

### Responsive Design
- **Desktop (≥769px):** 2-column grid with full-width cards spanning both columns where appropriate
- **Mobile (<769px):** Single-column stack

---

## Feature 4: Macro Earnings Scorecard

### Overview

A new **top-level page** (`/macro`) that provides an aggregated view of earnings season performance. Shows beat/miss rates, revenue surprises, market reactions, and detailed per-company results for S&P 500 and NASDAQ 100 constituents.

### Key Files

| File | Purpose |
|------|---------|
| `src/filings/earnings.py` (492 lines) | Core earnings data module — FMP API integration + mock fallback |
| `src/filings/templates/macro.html` (222 lines) | Full page template with filters and HTMX loading |
| `src/filings/templates/partials/earnings_scorecard.html` (239 lines) | HTMX partial with KPIs, charts, and sortable table |
| `src/filings/templates/base.html` | Updated nav to include Macro link |
| `src/filings/web.py` | Route handlers for `/macro` and `/api/macro/scorecard` |

### Data Source: Financial Modeling Prep (FMP)

| Endpoint | Purpose |
|----------|---------|
| `/earnings-surprises` | EPS and revenue actuals vs. estimates |
| `/sp500_constituent` | S&P 500 member list for filtering |
| `/nasdaq_constituent` | NASDAQ 100 member list for filtering |

**API key:** Set via `FMP_API_KEY` environment variable. When not set, the module falls back to **deterministic mock data** (seeded with `random.seed(42)` for consistency).

### Page Structure

#### Filter Bar
- **Index toggle:** S&P 500 / NASDAQ 100 (tab-style buttons)
- **Quarter selector:** Last 8 quarters (dropdown)
- **Sector filter:** 11 GICS sectors (dropdown)

All filter changes trigger an **HTMX request** to `/api/macro/scorecard` which returns the partial HTML, avoiding full page reloads.

#### KPI Cards (4 metrics)

| Metric | Description | Example |
|--------|-------------|---------|
| **EPS Beat Rate** | % of companies that beat EPS estimates | 73.3% |
| **Revenue Beat Rate** | % of companies that beat revenue estimates | 66.7% |
| **Dual Beats** | Count of companies that beat both EPS and revenue | 15 |
| **Avg Market Reaction** | Average post-earnings stock price change | +2.52% |

Color coding: Green for positive values, red for negative.

#### Charts (powered by ECharts)

1. **Beat vs. Miss Distribution** — Dual donut chart showing EPS and revenue beat/miss/inline breakdown side by side
2. **Beat Rate vs. Market Reaction Trend** — Combo line+bar chart showing EPS beat rate (line, left axis) and average market reaction (bar, right axis) over the last 8 quarters

Both charts support dark/light theme switching and are responsive.

#### Sortable Results Table

| Column | Sortable | Color-Coded |
|--------|----------|-------------|
| Ticker | Yes | Links to `/stock/{ticker}` |
| Company | Yes | Muted text |
| Sector | Yes | — |
| Date | Yes | — |
| EPS | Yes | Green (beat) / Red (miss) |
| EPS Est. | Yes | — |
| EPS Surprise | Yes | Green (positive) / Red (negative) |
| Rev Surprise | Yes | Green (positive) / Red (negative) |
| Reaction | Yes | Green (positive) / Red (negative) |
| Guidance | Yes | Green (Raised) / Red (Lowered) / Muted (Maintained) |

Features:
- **Client-side search** — live filtering by ticker or company name
- **Client-side sorting** — click any column header to sort ascending/descending
- **Numeric-aware sorting** — detects numbers vs. strings automatically

### Caching

- **Thread-safe in-memory cache** (`threading.Lock()`)
- **TTL:** 1 hour (`_EARNINGS_TTL = 3600`)
- **Cache key format:** `{index}:{quarter}:{sector}` (e.g., `sp500:Q1 2026:Technology`)
- **Historical trend data** cached separately with key `history:{index}`

### Mock Data Fallback

When `FMP_API_KEY` is not configured:
- Returns **30 realistic mock companies** across all sectors
- Uses `random.seed(42)` for **deterministic output** (same mock data every time)
- Displays a yellow **warning banner** at the top of the scorecard
- Trend data uses `hashlib.md5` of quarter labels for deterministic per-quarter variation

---

## Architecture Overview

### Data Flow

```
External APIs                Module Layer              Cache Layer            Frontend
─────────────        ──────────────────────       ──────────────────     ──────────────

Cloudflare Radar ──→ web_traffic.py ───────────→ In-memory (24h) ─┐
Tranco List ────────→                             Supabase (cold)  │
Wikipedia API ──────→                                              │
                                                                   ├──→ signals.html
Google Trends ──────→ google_trends.py ────────→ In-memory (24h)  │    (per-stock)
                                                  Supabase (cold)  │
                                                                   │
ApeWisdom/Reddit ──→ (existing modules) ───────→ Existing cache ──┘

FMP API ────────────→ earnings.py ─────────────→ In-memory (1h) ──→ macro.html
                                                                      (top-level)
```

### Async Execution

Heavy/blocking API calls are offloaded from the FastAPI event loop using `_to_heavy()`, which runs synchronous functions in a thread pool executor. This prevents blocking the main async event loop while fetching data from external APIs.

### HTMX Pattern

All dynamic content uses the same HTMX pattern:
1. **Page route** returns the full HTML page with empty container + `hx-trigger="load"`
2. **API route** returns an HTMX partial with the actual data
3. Filter changes call `htmx.ajax()` to reload the partial without a full page reload

---

## Database Migrations

### `sql/005_web_traffic_history.sql`

```sql
CREATE TABLE IF NOT EXISTS web_traffic_history (
    id              BIGSERIAL PRIMARY KEY,
    ticker          TEXT NOT NULL,
    snapshot_date   DATE NOT NULL,
    source          TEXT NOT NULL,         -- 'cloudflare_radar', 'tranco', 'wikipedia'
    data            JSONB NOT NULL,        -- full response payload
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(ticker, snapshot_date, source)  -- one snapshot per source per day
);
```

**Indexes:** `(ticker, snapshot_date DESC)`, `(source, snapshot_date DESC)`
**Protection:** DELETE trigger prevents accidental data loss (cold/write-once table)

### `sql/006_google_trends_history.sql`

```sql
CREATE TABLE IF NOT EXISTS google_trends_history (
    id              BIGSERIAL PRIMARY KEY,
    ticker          TEXT,                        -- NULL for macro/trending entries
    category        TEXT NOT NULL,               -- 'intent', 'product', 'comparison', 'macro', 'trending'
    keywords        TEXT[] NOT NULL,             -- keywords queried
    snapshot_date   DATE NOT NULL,
    data            JSONB NOT NULL,              -- full response payload
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(ticker, category, snapshot_date)
);
```

**Indexes:** `(ticker, snapshot_date DESC)`, `(category, snapshot_date DESC)`, partial index for `category = 'trending'`
**Protection:** Same DELETE trigger pattern

Both tables follow the project's existing **cold storage pattern** — write-once with delete protection.

---

## Data Pipeline Scripts

### `scripts/sync_web_traffic.py`

**Purpose:** Daily cron job (designed for Railway Cron Service)

```bash
uv run python scripts/sync_web_traffic.py [--dry-run]
```

- Fetches Cloudflare Radar + Wikipedia data for all web-traffic-relevant tickers
- Idempotent: uses `ON CONFLICT ... DO UPDATE`
- Requires `SUPABASE_URL` + `SUPABASE_SERVICE_KEY`

### `scripts/backfill_web_traffic.py`

**Purpose:** One-time historical backfill

```bash
# Backfill 5 years of Wikipedia data
uv run python scripts/backfill_web_traffic.py --source wikipedia --years 5

# Dry run
uv run python scripts/backfill_web_traffic.py --dry-run

# Limit to 2 tickers for testing
uv run python scripts/backfill_web_traffic.py --source wikipedia --limit 2
```

Sources: Wikipedia (back to July 2015) and Tranco (daily historical lists).

### `scripts/backfill_google_trends.py`

**Purpose:** One-time Google Trends backfill

```bash
# Backfill everything
uv run python scripts/backfill_google_trends.py

# Just macro trends
uv run python scripts/backfill_google_trends.py --source macro

# First 5 tickers only
uv run python scripts/backfill_google_trends.py --source ticker --limit 5
```

**Warning:** Google Trends rate-limits at ~2 req/min. Full backfill takes ~15–20 minutes.

---

## API Endpoints

### New Routes

| Method | Path | Type | Description |
|--------|------|------|-------------|
| `GET` | `/macro` | HTML page | Macro Earnings Scorecard page |
| `GET` | `/api/macro/scorecard` | HTMX partial | Earnings scorecard data (filtered by index, quarter, sector) |
| `GET` | `/api/google-trends/ticker` | HTMX partial | Per-ticker Google Trends data |
| `GET` | `/api/google-trends/macro` | HTMX partial | Macro sentiment trends |
| `GET` | `/api/google-trends/trending` | HTMX partial | Trending searches → ticker mapping |

### Query Parameters

#### `/api/macro/scorecard`

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `index` | string | `"sp500"` | Index filter: `"sp500"` or `"nasdaq"` |
| `quarter` | string | `""` (current) | Fiscal quarter: `"Q1 2026"`, `"Q4 2025"`, etc. |
| `sector` | string | `""` (all) | GICS sector filter |

#### `/api/google-trends/ticker`

| Parameter | Type | Description |
|-----------|------|-------------|
| `ticker` | string | Stock ticker symbol |

#### `/api/google-trends/macro`

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `category` | string | `""` | Macro category filter |

---

## Frontend Components

### Templates Added/Modified

| Template | Type | Description |
|----------|------|-------------|
| `macro.html` | Full page | Earnings Scorecard with filters, HTMX container |
| `partials/earnings_scorecard.html` | HTMX partial | KPI cards + ECharts + sortable table |
| `partials/signals.html` | HTMX partial | Unified signals tab (sentiment + web traffic + trends) |
| `partials/web_traffic.html` | HTMX partial | Web traffic visualization cards |
| `partials/google_trends_ticker.html` | HTMX partial | Per-ticker search trends |
| `partials/google_trends_macro.html` | HTMX partial | Macro sentiment trends |
| `partials/google_trends_trending.html` | HTMX partial | Trending searches display |
| `base.html` | Modified | Added "Macro" link to sidebar navigation |
| `stock.html` | Modified | Replaced separate tabs with unified Signals tab |
| `alternative_signals.html` | Modified | Updated overview page for all signal types |

### Chart Library

All charts use **ECharts** (loaded lazily via `window.requireECharts()`). Charts include:
- Dual donut (beat/miss distribution)
- Combo line + bar (trend over time)
- Time series line charts (Google Trends interest)
- Area charts (Wikipedia page views)

All charts support **dark/light theme** via `data-theme` attribute detection.

### CSS Approach

Styles are **scoped per-component** using `<style>` blocks inside templates (no separate CSS files). This follows the project's existing convention. Key design tokens used:
- `--pp-surface`, `--pp-border`, `--pp-text`, `--pp-text-muted`
- `--pp-card-shadow-sm` for card elevation
- `--pp-overlay-8` for subtle backgrounds
- Responsive breakpoints: 600px (mobile), 769px (tablet)

---

## Configuration & Environment Variables

### New Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `FMP_API_KEY` | No | `""` | Financial Modeling Prep API key for live earnings data. When not set, mock data is served with a warning banner. |

### Existing Variables Used

| Variable | Used By | Description |
|----------|---------|-------------|
| `SUPABASE_URL` | sync/backfill scripts | Supabase project URL |
| `SUPABASE_SERVICE_KEY` | sync/backfill scripts | Supabase service role key |

### New Dependency

| Package | Version | Purpose |
|---------|---------|---------|
| `pytrends` | `>=4.9` | Google Trends data fetching |

---

## Testing

The existing test suite in `tests/test_web.py` covers general app health (200 responses, security headers, input validation). The new features degrade gracefully:

- **No FMP_API_KEY:** Earnings scorecard serves deterministic mock data
- **No `pytrends`:** Google Trends module checks `_PYTRENDS_AVAILABLE` flag and returns empty results
- **External API failures:** All modules catch exceptions and log errors without crashing
- **No Supabase:** Web traffic and trends modules work from in-memory cache only; cold storage writes are skipped

### Running Tests

```bash
uv run pytest tests/ -v
```

---

## Deployment Checklist

### Pre-merge

- [ ] Review all 24 changed files
- [ ] Run test suite: `uv run pytest tests/ -v`
- [ ] Verify app starts: `uv run uvicorn src.filings.web:app`

### Post-merge (Production)

1. **Run database migrations** (Supabase SQL Editor):
   ```
   sql/005_web_traffic_history.sql
   sql/006_google_trends_history.sql
   ```

2. **Set environment variables** (optional but recommended):
   - `FMP_API_KEY` — for live earnings data on the Macro page

3. **Set up cron jobs** (Railway or similar):
   - `uv run python scripts/sync_web_traffic.py` — daily
   - `uv run python scripts/backfill_google_trends.py --source trending` — daily

4. **Run initial backfills** (one-time):
   ```bash
   # Wikipedia historical data
   uv run python scripts/backfill_web_traffic.py --source wikipedia --years 3

   # Google Trends per-ticker + macro
   uv run python scripts/backfill_google_trends.py --source all
   ```

5. **Verify in production:**
   - `/macro` page loads with earnings data (mock or live)
   - Stock pages show the unified Signals tab
   - No errors in application logs

---

## Design Decisions & Trade-offs

### Why Free Data Sources?

After a thorough investigation (`docs/web-traffic-investigation.md`), paid options like SimilarWeb Stock Intelligence ($10K+/year) and SEMrush ($139/month) were rejected in favor of a **free composite approach** using Cloudflare Radar, Tranco List, Wikipedia page views, and Google Trends. This provides 80% of the signal at 0% of the cost.

### Why Mock Data Fallback?

The Macro Earnings Scorecard uses deterministic mock data (`random.seed(42)`) when no FMP API key is configured. This ensures:
- The page always renders correctly for development/demo purposes
- CI/CD tests pass without API keys
- A clear banner informs users they're seeing sample data

### Why HTMX Over Client-Side Rendering?

Consistent with the rest of the PaperPanda codebase, all dynamic content uses **server-side rendered HTMX partials** rather than client-side JavaScript frameworks. Benefits:
- Simpler architecture (no build step, no JS framework)
- SEO-friendly (initial HTML is server-rendered)
- Progressive enhancement (works without JS for initial load)

### Why Cold Tables With Delete Protection?

Web traffic and Google Trends data is **historical and append-only** — once a day's data is captured, it should never be modified or deleted. The DELETE trigger prevents accidental data loss while still allowing INSERT and UPDATE operations.
