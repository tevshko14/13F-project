# Francisco Fixes Branch

> **Temporary documentation.** This doc exists to share context on the
> `claude/francisco-fixes-aABrd` branch. Once all changes are merged into
> main and folded into README.md / README_DEV.md, this file should be
> deleted for cleanliness.

## Branch overview

**Branch:** `claude/francisco-fixes-aABrd`
**Base:** `main`
**Files changed:** 19
**Commits:** 14

This branch contains four categories of work:

1. **Earnings Calendar** — a new full-page feature (design, API, templates, refactoring, docs)
2. **/retail timeout fix** — a critical bug fix for the Signals product, plus a simplify pass and test suite
3. **x1000 value fix** — critical fix for SEC 13F portfolio values displayed 1000x too low, plus simplify pass and test suite
4. **Ticker correction** — fix broken/malformed ticker symbols from CUSIP mapping failures

---

## Change log (chronological)

### 1. Earnings Calendar feature (`b21445d`)

Added an interactive earnings calendar page at `/earnings-calendar` showing upcoming earnings reports in weekly and monthly views.

**New files:**
- `src/filings/earnings_calendar.py` — data layer (Finnhub + FMP APIs, caching, mock data)
- `src/filings/templates/earnings_calendar.html` — full page template
- `src/filings/templates/partials/earnings_calendar_day.html` — day-detail HTMX partial
- `src/filings/templates/partials/earnings_calendar_grid.html` — calendar grid HTMX partial

**Modified files:**
- `src/filings/web.py` — added `/earnings-calendar`, `/api/earnings-calendar/grid`, `/api/earnings-calendar/day` routes
- `src/filings/templates/base.html` — added nav link (gated by `EARNINGS_CALENDAR_ENABLED` env var)

**Key design decisions:**
- Uses HTMX partials for week/month switching and day-detail drill-down (no full page reloads)
- Graceful degradation: mock data when APIs are unavailable, `{% if %}` guards in all templates
- Feature-flagged via `EARNINGS_CALENDAR_ENABLED` environment variable

---

### 2. Earnings calendar refactoring (`801b251`)

Eliminated duplicated API fetches and helper functions across modules.

**What changed:**
- `earnings_calendar.py` no longer calls Finnhub/FMP APIs directly — delegates to shared functions
- `earnings.py` gained `fetch_finnhub_calendar_raw(start, end)` — a shared cached fetch (1h TTL)
- `earnings_scorecard.py` renamed `_fmp_get` → `fmp_get` (now public, imported cross-module)

**Shared functions matrix:**

| Function | Lives in | Used by |
|----------|----------|---------|
| `fetch_finnhub_calendar_raw()` | `earnings.py` | `earnings.py`, `earnings_calendar.py` |
| `fmp_get()` | `earnings_scorecard.py` | `earnings_scorecard.py`, `earnings_calendar.py` |
| `_build_company_lookup()` | `earnings_scorecard.py` | `earnings_scorecard.py`, `earnings_calendar.py` |
| `_fmt_revenue()` | `earnings.py` | `earnings.py`, `earnings_calendar.py` |

---

### 3. Simplify pass #1 (`b341ec4`)

- Made `fmp_get` public (removed underscore prefix)
- Added documentation comment explaining the dual Finnhub cache strategy:
  - `_finnhub_raw_cache` (1h TTL) — shared raw API data
  - `_finnhub_cal_cache` (6h TTL) — parsed/formatted calendar entries

---

### 4. Earnings calendar documentation (`8337e57`)

- **README.md** — added Earnings Calendar section (feature description, env vars, project structure entries, caching TTLs)
- **README_DEV.md** — added Section 5.17 (~150 lines): architecture diagram, module dependency graph, key functions table, normalized entry schema, caching strategy, HTMX interaction flow, template structure

---

### 5. /retail page timeout fix (`f15ed41`)

**Problem:** The `/retail` page timed out 100% of the time (30+ seconds), blocking 25% of the core Signals product. Root cause: `retail_page()` called `asyncio.gather()` with 3 external APIs and zero error handling. On cold start, ApeWisdom fetches 5 pages x 8s timeout = 40s worst case.

**Fix (3 parts):**

#### Part A — `web.py`: Per-source timeout + graceful degradation

- Added `_safe_fetch(coro, label, timeout=10)` — wraps any async call with a 10s timeout, returns `None` on failure
- `/retail` now fetches CNN Fear & Greed, ApeWisdom, and YouTube events via `_safe_fetch` in parallel — any failure returns `None`, never blocks the page
- Templates already guard all data with `{% if %}` checks, so missing sources render gracefully

#### Part B — `web.py`: Leaderboard endpoint hardening

- Both `/api/retail/leaderboard` and `/api/retail/leaderboard-data` wrapped with `asyncio.wait_for(timeout=10)`
- On timeout or error: `all_data=[], fear_greed=None` — leaderboard renders empty rather than hanging

#### Part C — `sentiment.py`: ApeWisdom fetch hardening

- `_fetch_apewisdom_pages()`: reduced per-page timeout from 8s → 6s
- Added `as_completed(timeout=8)` global timeout — collects whatever pages finished within 8s
- Partial results returned in page order (ranking stays consistent)

**Acceptance criteria met:**
- `/retail` never exceeds 15 seconds (Railway's 30s kill threshold)
- Each data source fails independently (one slow API can't block others)
- No user-visible errors — missing data renders as empty sections

---

### 6. Simplify pass #2 (`33af58e`)

Addressed code quality findings from review:

- **Extracted `_safe_fetch` to module level** — was recreated as a closure inside `retail_page()` on every request; now a reusable module-level async function
- **Created `_fetch_retail_data()` shared helper** — deduplicated identical 10-line try/except blocks in `retail_leaderboard_api` and `retail_leaderboard_data`
- **Fixed `page_results` scope** in `_fetch_apewisdom_pages()` — moved declaration before `try` block (was technically accessible due to Python scoping but fragile)
- **Deduplicated sorted-page assembly loop** — was duplicated in both `try` and `except TimeoutError` branches; now a single path after the try/except
- **Added future cancellation** — unfinished futures cancelled after `as_completed` timeout (prevents queued-but-not-started tasks from running)
- **Simplified exception handling** — `except (asyncio.TimeoutError, Exception)` → `except Exception` (TimeoutError is a subclass)

---

### 7. Test suite (`0fe9623`)

Added `tests/test_retail_timeout.py` — 15 test cases covering the timeout protection and graceful degradation.

**Test matrix:**

| Area | Tests | What's validated |
|------|-------|-----------------|
| `_safe_fetch` | 5 | Success passthrough, timeout → None, exception → None, completes in ~1s not 60s, None return is valid |
| `_fetch_retail_data` | 4 | Exception → ([], None), timeout → ([], None) within 15s, None → [] normalization, valid data passthrough |
| `_fetch_apewisdom_pages` | 6 | All-succeed page ordering, total failure → [], cache + index populated, malformed JSON graceful skip, all-hang completes in <12s, partial results with correct ordering |

**Run tests:**
```bash
PYTHONPATH=src python -m pytest tests/test_retail_timeout.py -v
```

Tests are designed to run without heavy dependencies (edgar, yfinance, supabase) by using stub modules and testing helper functions in isolation.

---

## Files changed (summary)

| File | Type | Description |
|------|------|-------------|
| `src/filings/earnings_calendar.py` | **New** | Earnings calendar data layer |
| `src/filings/templates/earnings_calendar.html` | **New** | Calendar full page |
| `src/filings/templates/partials/earnings_calendar_day.html` | **New** | Day-detail HTMX partial |
| `src/filings/templates/partials/earnings_calendar_grid.html` | **New** | Calendar grid HTMX partial |
| `tests/test_retail_timeout.py` | **New** | 15 test cases for timeout protection |
| `src/filings/web.py` | Modified | New routes + timeout protection + shared helpers |
| `src/filings/earnings.py` | Modified | Shared `fetch_finnhub_calendar_raw()` |
| `src/filings/earnings_scorecard.py` | Modified | `_fmp_get` → `fmp_get` (public) |
| `src/filings/sentiment.py` | Modified | ApeWisdom fetch hardening |
| `src/filings/templates/base.html` | Modified | Nav link for earnings calendar |
| `README.md` | Modified | Earnings calendar user docs |
| `README_DEV.md` | Modified | Earnings calendar developer docs |

---

### 8. Fix fund portfolio values wrong by 1000x (`be3e946`)

**Problem:** Several fund portfolio values were displayed wrong by ~1,000x. AKO Capital showed $6.5M instead of ~$6.5B. Baupost showed $5.2M instead of ~$5.2B. This was a credibility-destroying bug for a financial data product.

**Root Cause:** SEC 13F filings report all dollar values in **thousands**. The `edgartools` library returns these raw values without conversion (its own docstring says "Total value of holdings in thousands of dollars"). Our `client.py` was doing `int(tf.total_value)` and `int(row.Value)` without applying a x1000 multiplier. This affected **every single fund**.

**Fix (4 parts):**

#### Part A — `client.py`: Apply x1000 multiplier at all ingestion points

Added `_SEC_13F_VALUE_MULTIPLIER = 1000` constant and applied it at all 11 points where values are read from the `edgartools` library:

| Function | Lines fixed |
|----------|-----------|
| `get_holdings()` | `total_value`, `row.Value` |
| `_compare_two_filings()` | `curr_value`, `prev_value` |
| `compare_quarters()` | `tf_current.total_value`, `tf_previous.total_value` |
| `get_fund_summary()` | `tf.total_value`, `row.Value` (top_holdings), `row.Value` (all_holdings) |
| `get_enriched_holdings()` | `tf.total_value`, `row.Value` |

The fix is at the **ingestion boundary only** — all downstream code (cache, templates, grand portfolio, activity feed, deployment page) reads from the already-corrected values.

#### Part B — Template display formatting

Updated value display in 3 templates to use human-readable B/M/K suffixes:
- `grand_portfolio.html` — `/funds` page table
- `index.html` — homepage table
- `investor.html` — individual fund page header

Example: `$6,568,399,000` → `$6.6B`

#### Part C — Post-ingestion validation

Added `_validate_fund_values()` to `client.py` — called at the end of `get_fund_summary()`. Logs a WARNING if:
- A fund with 20+ holdings has total value under $10M (likely missing multiplier)
- Average value per holding is under $500K for funds with 5+ holdings

This prevents the bug from silently recurring.

#### Part D — Stale data indicator

Added a "stale" label in the Period column for funds whose `report_period` is older than 12 months (rolling threshold). Appears on both `/funds` and homepage tables with a tooltip: "This fund's data may be stale — last filing is over a year old."

Affected funds:
- Leon Cooperman / Omega Advisors (last filing: Dec 2018)
- Guy Spier / Aquamarine Capital (last filing: Jun 2022)
- David Einhorn / Greenlight Capital (last filing: Dec 2023)

**Re-processing requirement:** After deploying this fix, the sync worker must re-ingest all 84 funds so the cached values in Supabase get the corrected x1000 values. Until re-ingestion, existing cached data will still show 1000x-low values.

---

### 9. Test suite for x1000 fix (`be3e946`)

Added `tests/test_13f_value_multiplier.py` — 9 test cases.

| # | Test | Validates |
|---|------|-----------|
| 1 | `test_multiplier_constant_exists` | `_SEC_13F_VALUE_MULTIPLIER == 1000` |
| 2 | `test_multiplier_is_used_in_get_fund_summary` | total_value and holdings multiplied |
| 3 | `test_multiplier_is_used_in_get_holdings` | FundInfo and Holding values multiplied |
| 4 | `test_compare_two_filings_applies_multiplier` | current/previous values multiplied |
| 5 | `test_validate_flags_low_value_many_holdings` | Warns on 20+ holdings with <$10M |
| 6 | `test_validate_flags_low_avg_per_holding` | Warns on <$500K avg per holding |
| 7 | `test_validate_passes_normal_fund` | No warning for normal $6.5B fund |
| 8 | `test_validate_handles_zero_holdings` | No crash on zero holdings |
| 9 | `test_pct_of_portfolio_still_correct` | Percentages unaffected (70/30 stays 70/30) |

---

### 10. Simplify pass #3 — x1000 fix cleanup (`2deea6d`)

Addressed code quality and reuse findings from the x1000 fix:

#### A — Reusable `format_value` Jinja filter (`web.py`)

B/M/K formatting was duplicated ~40+ times across 4 templates with inline `{% if %}` chains. Extracted into a single `_format_value()` function registered as a Jinja filter:

```python
templates.env.filters["format_value"] = _format_value
```

Templates now use `{{ value | format_value }}` instead of 5-line inline conditionals.

#### B — Centralized multiplier helpers (`client.py`)

11 scattered `int(x) * _SEC_13F_VALUE_MULTIPLIER` call sites with inconsistent None guards → extracted into 2 helpers:

- `_filing_total_value(tf)` — converts `ThirteenF.total_value` with None guard
- `_row_value(row)` — converts a holdings DataFrame row's `Value`

#### C — Rolling stale threshold (`web.py`)

Hardcoded `"2024-06-01"` stale cutoff → rolling 12-month threshold computed at startup:

```python
_stale_cutoff = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
templates.env.globals["stale_cutoff"] = _stale_cutoff
```

#### D — Template consistency

- `fund_row.html` partial (HTMX lazy-loaded rows) now uses `| format_value` filter for consistency with the main tables
- `investor.html` was missing the K tier in its inline formatting — fixed by using the shared filter

#### E — Misleading comment fix (`aum_data.py`)

Fixed comment at line 632 that incorrectly said "13F total_value from edgartools is already in dollars" — updated to reflect that conversion happens at ingestion in `client.py`.

---

### 11. Fix broken and malformed ticker symbols (`1369209`)

**Problem:** Several fund holdings displayed malformed ticker symbols instead of proper stock tickers. Examples: "HILTON G", "CARDLYTI", "Compagni", "General" appeared as clickable ticker links, leading to broken `/stock/HILTON G` pages. This happened because when the CUSIP→ticker mapping failed, the system fell back to displaying the first 8 characters of the company name as if it were a ticker.

**Root Causes:**
1. **Missing CUSIPs** in edgartools' bundled `ct.pq` mapping file (e.g., Hilton Grand Vacations CUSIP `46321A104` not in ct.pq)
2. **Stale ticker mappings** (e.g., `FB` still mapped instead of `META`)
3. **Display fallback** in `web.py` used `h.get("ticker") or h.get("issuer", "?")[:8]` — truncating issuer names to 8 chars and displaying them as fake tickers with broken stock links

**Fix (4 parts):**

#### Part A — CUSIP override table (`client.py`)

Added `_CUSIP_OVERRIDES` dict for CUSIPs that edgartools can't resolve:

| CUSIP | Ticker | Company |
|-------|--------|---------|
| `46321A104` | HGV | Hilton Grand Vacations |
| `432848101` | HLT | Hilton Worldwide Holdings |
| `H25662105` | CFRUY | Compagnie Financière Richemont (ADR) |

CUSIP overrides are checked first in `_safe_ticker()` — highest priority.

#### Part B — Ticker validation (`client.py`)

Added `_is_valid_ticker()` function that rejects malformed tickers:
- Contains spaces (`"KKR & CO"`, `"HILTON G"`)
- Longer than 6 characters (`"CARDLYTI"`, `"Compagni"`)
- Contains non-alphanumeric characters (except dots for `BRK.A` etc.)

`_safe_ticker()` now validates the resolved ticker and returns `None` for invalid ones, instead of passing garbage through.

#### Part C — Display fallback fix (`web.py`)

Changed the `top_tickers` list comprehension from:
```python
h.get("ticker") or h.get("issuer", "?")[:8]  # OLD — truncated issuer name
```
to:
```python
h.get("ticker") for h in ... if h.get("ticker")  # NEW — skip if no ticker
```

Holdings without valid tickers are simply excluded from the top holdings tag list instead of showing garbage.

#### Part D — Post-ingestion ticker validation (`client.py`)

Added `_validate_tickers()` function called at the end of `get_fund_summary()`. Logs an INFO message listing all holdings without valid tickers, so new mapping gaps are visible in logs during sync.

**Re-processing requirement:** After deploying, the sync worker must re-ingest all funds so cached data gets corrected tickers.

---

### 12. Simplify pass #4 — ticker fix cleanup (`f56f320`)

- **Pre-compiled regex**: `_VALID_TICKER_RE` compiled at module level instead of per-call `re.match()`; removed redundant `_MAX_TICKER_LEN` constant (regex already enforces length)
- **Removed `cusip` parameter** from `_safe_ticker()` — now reads `row.Cusip` internally via `hasattr`, eliminating boilerplate at all 3 call sites
- **Extracted `_top_tickers(cached)`** helper in `web.py` to deduplicate identical list comprehension in fund_row and funds_page endpoints

---

### 13. Test suite for ticker corrections (`1369209`, expanded `PENDING_COMMIT`)

Added `tests/test_ticker_corrections.py` — 74 test cases.

| Area | Tests | What's validated |
|------|-------|-----------------|
| `_TICKER_CORRECTIONS` | 3 | FB→META, TWTR→X, BMNRD→BMNR |
| `_CUSIP_OVERRIDES` | 3 | HGV, HLT, CFRUY overrides exist |
| `_VALID_TICKER_RE` | 4 | Pre-compiled, matches standard, rejects invalid, case insensitive |
| `_is_valid_ticker` | 8 | Standard tickers, dots, ADRs, spaces, length, special chars, empty, lowercase names |
| `_is_valid_ticker` boundary | 10 | Exactly 6/7 chars, single char, digits-only, mixed case, dots at boundaries, whitespace, tab |
| `_safe_ticker` | 11 | Normal ticker, FB→META, CUSIP priority, NaN/None, malformed rejection, whitespace stripping |
| `_safe_ticker` edge cases | 9 | No Cusip attr, CUSIP not in overrides, correction after strip, NaN variants, all overrides end-to-end, all corrections end-to-end |
| `_validate_tickers` | 6 | Logs missing, no log when valid, truncates long lists, empty holdings, empty string ticker, log includes fund name/CIK |
| `_top_tickers` helper | 8 | Extract valid, skip None, n parameter, default n=5, empty, missing key, all None, missing ticker key |
| Bug report tickers | 9 | HILTON G, HGV override, CARDLYTI, Compagni, CFRUY override, General, KKR & CO, FB→META, TWTR→X |
| Display fallback | 3 | Filters None, old pattern produced garbage, all-valid passthrough |

---

## Files changed (summary)

| File | Type | Description |
|------|------|-------------|
| `src/filings/earnings_calendar.py` | **New** | Earnings calendar data layer |
| `src/filings/templates/earnings_calendar.html` | **New** | Calendar full page |
| `src/filings/templates/partials/earnings_calendar_day.html` | **New** | Day-detail HTMX partial |
| `src/filings/templates/partials/earnings_calendar_grid.html` | **New** | Calendar grid HTMX partial |
| `tests/test_retail_timeout.py` | **New** | 15 test cases for timeout protection |
| `tests/test_13f_value_multiplier.py` | **New** | 9 test cases for x1000 value fix |
| `tests/test_ticker_corrections.py` | **New** | 74 test cases for ticker correction |
| `src/filings/client.py` | Modified | CUSIP overrides + ticker validation + _safe_ticker rewrite |
| `src/filings/web.py` | Modified | Removed truncated-issuer fallback + _top_tickers helper |
| `src/filings/aum_data.py` | Modified | Fixed misleading comment about 13F values |
| `src/filings/earnings.py` | Modified | Shared `fetch_finnhub_calendar_raw()` |
| `src/filings/earnings_scorecard.py` | Modified | `_fmp_get` → `fmp_get` (public) |
| `src/filings/sentiment.py` | Modified | ApeWisdom fetch hardening |
| `src/filings/templates/base.html` | Modified | Nav link for earnings calendar |
| `src/filings/templates/grand_portfolio.html` | Modified | B/M/K filter + rolling stale indicator |
| `src/filings/templates/index.html` | Modified | B/M/K filter + rolling stale indicator |
| `src/filings/templates/investor.html` | Modified | B/M/K filter |
| `src/filings/templates/partials/fund_row.html` | Modified | B/M/K filter + stale indicator |
| `README.md` | Modified | Earnings calendar user docs |
| `README_DEV.md` | Modified | Earnings calendar developer docs |

---

## Environment variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `EARNINGS_CALENDAR_ENABLED` | `false` | Show/hide earnings calendar nav link |
| `FINNHUB_API_KEY` | — | Required for live earnings data |
| `FMP_API_KEY` | — | Required for company logos/details |
