# Web Traffic Data for Stocks — Investigation

## Goal
Fetch web traffic data (via SimilarWeb or alternatives) for stock-related companies
like SoFi (`sofi.com`) and Robinhood (`robinhood.com`), store in Supabase, and
visualize in a chart on the stock detail page.

---

## Option 1: SimilarWeb Official API (Stock Intelligence)

**What it is:** SimilarWeb has a dedicated "Stock Intelligence" product for investors.
It covers 300,000 domains and 200,000 apps linked to 60,000 companies across 144+
exchanges with 5 years of historical data.

**Data available:**
- Total visits, unique visits, visit duration, bounce rate (daily/weekly/monthly)
- Marketing channels: organic search, paid search, referrals, social, display, email
- Geographic traffic breakdown
- App downloads and engagement
- Revenue estimation models (backtested beat/miss signals)

**Pricing:** Custom/enterprise only. No public pricing. Requires contacting sales.
Based on market data, the Web Intelligence product starts at ~$199/month for Starter,
but Stock Intelligence and the API are enterprise-tier (likely $10K+/year based on
comparable offerings).

**Verdict:** Too expensive for this project. The API is geared toward hedge funds.

---

## Option 2: SimilarWeb DigitalRank API (Free Tier)

**What it is:** A free, official API from SimilarWeb that provides basic ranking data.

**Data available:**
- Global rank
- Country rank
- Category rank
- Top-ranking websites (leaderboard)

**Limits:** 100 free data credits/month. No traffic volume or engagement metrics —
just rank positions.

**Authentication:** Requires free SimilarWeb account + API key.

**Verdict:** Usable but limited. Rank data alone isn't very compelling for a stock
page. Could be a lightweight addition (e.g., "Global Web Rank: #1,234") but not
enough for a traffic chart.

---

## Option 3: Undocumented SimilarWeb Extension Endpoint (DEAD)

**What it was:** `https://data.similarweb.com/api/v1/data?domain=sofi.com`

The browser extension used to hit this endpoint and return rich data (traffic, bounce
rate, geo, traffic sources, etc.) without authentication.

**Status as of late 2025:** BLOCKED. Returns a CloudFront error. Different IPs don't
help. This option is no longer viable.

**Verdict:** Dead. Do not pursue.

---

## Option 4: RapidAPI / Apify Scrapers

**What they are:** Third-party scrapers that extract data from SimilarWeb's public
website pages.

**RapidAPI options:**
- `similarweb-api1` by oceanrock — wraps SimilarWeb data, pricing on RapidAPI
- Various other scraper APIs

**Apify option:**
- `curious_coder/similarweb-scraper` — extracts traffic, rank, bounce rate, visit
  duration, traffic sources. Returns JSON. Has smart retry and resume.

**Data available:** Traffic volume, global rank, bounce rate, visit duration, traffic
sources breakdown, geographic distribution.

**Pricing:** Pay-per-use. Apify is usage-based (~$5/1000 actor runs). RapidAPI
scrapers vary but typically offer 100-500 free calls/month on basic tiers.

**Risks:**
- Scrapers can break when SimilarWeb changes their frontend
- Terms of service concerns
- Data freshness/accuracy varies

**Verdict:** Most practical option for getting actual traffic data affordably. Apify
or a RapidAPI scraper could work well with a daily/weekly cron job fetching data for
tracked tickers.

---

## Option 5: SEMrush / Ahrefs Traffic Analytics

**SEMrush:** Starts at $139/month. Has Traffic Analytics with an API. Good data
quality but expensive for this use case.

**Ahrefs:** Starts at $129/month. Has traffic estimation data. Also has a free
"Traffic Checker" tool but it's behind captchas and not API-friendly.

**Verdict:** Overkill and expensive for a single feature.

---

## Option 6: Build Our Own Proxy Metric

Instead of paying for traffic data, we could approximate "web interest" using free
data sources we already have access to:

- **Google Trends API** (via `pytrends`): Free. Shows relative search interest over
  time for terms like "SoFi", "Robinhood app", etc.
- **Reddit mentions** (we already have ApeWisdom integration): Shows social buzz.
- **App Store rankings** (we already have Apple iTunes integration): Shows app
  popularity trends.
- **Wikipedia page views** (free API): Correlates with public interest surges.

**Verdict:** Free, reliable, and we already have some of the infrastructure. A
composite "web interest" chart combining Google Trends + Reddit mentions + app
rankings could be more compelling than raw SimilarWeb traffic numbers.

---

## Recommended Approach

### Phase 1: Free Composite "Web Interest" Chart (Recommended Starting Point)

Use free data sources to build a "Web Interest" tab/section on the stock page:

1. **Google Trends** via `pytrends` library (already in Python ecosystem)
   - Search interest over time for the company name/ticker
   - Compare multiple tickers (e.g., SOFI vs HOOD)
   - Daily/weekly granularity

2. **Wikipedia Page Views** via free Wikimedia API
   - `https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/en.wikipedia/all-access/all-agents/{article}/daily/{start}/{end}`
   - No auth needed, generous rate limits

3. **Existing integrations** (Reddit via ApeWisdom, App Store via iTunes)

**Implementation:**
- New Supabase table: `web_traffic_data` with columns for ticker, source, date, value
- New sync worker to fetch daily/weekly data
- New ECharts line chart on the stock detail page (new tab or within existing tabs)
- Cache in the 3-tier system like other data

### Phase 2: SimilarWeb via Scraper (If More Data Needed)

If the free composite approach isn't sufficient:

1. Use an Apify actor or RapidAPI scraper to fetch monthly SimilarWeb data
2. Store in Supabase with the same `web_traffic_data` table
3. Run weekly (SimilarWeb data updates monthly anyway)
4. Budget: ~$5-20/month depending on number of tickers tracked

---

## Supabase Schema (Proposed)

```sql
CREATE TABLE IF NOT EXISTS web_traffic_data (
    id BIGSERIAL PRIMARY KEY,
    ticker TEXT NOT NULL,
    domain TEXT,                          -- e.g., 'sofi.com', 'robinhood.com'
    source TEXT NOT NULL,                 -- 'google_trends', 'wikipedia', 'similarweb', 'reddit'
    metric TEXT NOT NULL,                 -- 'search_interest', 'page_views', 'total_visits', 'mentions'
    date DATE NOT NULL,
    value NUMERIC,
    metadata JSONB DEFAULT '{}',         -- extra context (e.g., traffic sources breakdown)
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(ticker, source, metric, date)
);

CREATE INDEX idx_web_traffic_ticker_date ON web_traffic_data(ticker, date DESC);
CREATE INDEX idx_web_traffic_source ON web_traffic_data(source);
```

## Visualization (Proposed)

- **Chart type:** Multi-line ECharts time series
- **Location:** New "Web Traffic" tab on stock detail page (`stock.html`)
- **Features:**
  - Toggle between data sources (Google Trends, Wikipedia, SimilarWeb)
  - Date range selector (1M, 3M, 6M, 1Y)
  - Overlay stock price for correlation analysis
  - Dark/light theme support (using existing design tokens)

---

## Ticker-to-Domain Mapping

For this to work, we need a mapping from stock tickers to website domains:

| Ticker | Company | Domain |
|--------|---------|--------|
| SOFI | SoFi Technologies | sofi.com |
| HOOD | Robinhood Markets | robinhood.com |
| COIN | Coinbase | coinbase.com |
| SQ | Block (Square) | squareup.com, cash.app |
| PYPL | PayPal | paypal.com |
| SHOP | Shopify | shopify.com |
| NFLX | Netflix | netflix.com |
| AMZN | Amazon | amazon.com |

This mapping could be stored in Supabase or as a config dict, and expanded over time.

---

## Sources

- [SimilarWeb Stock Intelligence](https://www.similarweb.com/corp/stocks/)
- [SimilarWeb Investors API](https://www.similarweb.com/corp/investors/api/)
- [SimilarWeb DigitalRank API](https://support.similarweb.com/hc/en-us/articles/4414317910929-Website-DigitalRank-API)
- [SimilarWeb Free API FAQ](https://support.similarweb.com/hc/en-us/articles/23714739955869-Does-Similarweb-have-a-Free-API)
- [Undocumented Free API (GitHub)](https://github.com/DaWe35/Similarweb-free-API)
- [Apify SimilarWeb Scraper](https://apify.com/curious_coder/similarweb-scraper)
- [RapidAPI SimilarWeb API](https://rapidapi.com/oceanrock/api/similarweb-api1)
- [SimilarWeb Pricing (Tekpon)](https://tekpon.com/software/similarweb/pricing/)
- [SimilarWeb Pricing (Vendr)](https://www.vendr.com/marketplace/similarweb)
- [Endpoint blocked discussion](https://www.blackhatworld.com/seo/data-similarweb-com-api-blocked-any-free-alternative-for-website-traffic.1770837/)
