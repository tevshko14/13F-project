# Francisco Fixes Branch

> **Temporary documentation.** This doc exists to share context on the
> `claude/francisco-fixes-aABrd` branch. Once all changes are merged into
> main and folded into README.md / README_DEV.md, this file should be
> deleted for cleanliness.

## Branch overview

**Branch:** `claude/francisco-fixes-aABrd`
**Base:** `main`
**Files changed:** 12 (net +2,200 lines)
**Commits:** 7

This branch contains two categories of work:

1. **Earnings Calendar** — a new full-page feature (design, API, templates, refactoring, docs)
2. **/retail timeout fix** — a critical bug fix for the Signals product, plus a simplify pass and test suite

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

## Environment variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `EARNINGS_CALENDAR_ENABLED` | `false` | Show/hide earnings calendar nav link |
| `FINNHUB_API_KEY` | — | Required for live earnings data |
| `FMP_API_KEY` | — | Required for company logos/details |
