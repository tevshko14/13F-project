# CLAUDE.md — PaperPanda Project Context

> This file is the source of truth for Claude Code sessions on this project.
> Read this FIRST before doing anything.

---

## Project Overview

**PaperPanda** is a production SEC 13F filing tracker and financial intelligence platform.
- **Live URL:** https://paperpanda.io
- **Stack:** Python 3.12+ / FastAPI / Jinja2 / HTMX / Pico CSS v2 / ECharts 5 / Chart.js 4
- **Database:** Supabase (Postgres + Storage)
- **Hosting:** Railway (auto-deploy from main branch)
- **Package manager:** `uv` (NOT pip)
- **Entry points:** `uv run filings-web` (web, port 8000), `uv run filings` (CLI)
- **Local dev:** Use `preview_start` with launch.json config `filings-web`

---

## Deployment Rules

- **NEVER push to main/prod unless I explicitly say "push to prod" or "deploy to prod"**
- Always test locally first — spin up via launch.json, verify in browser
- When I say "spin up local" or "load up local", start the preview server
- After deploy, verify the site is up and key pages load correctly
- Single branch workflow (main). No feature branches unless I ask for one
- Never amend existing commits — always create new ones

---

## SQL & Database Rules

- **Show SQL in chat, never write SQL to files** — I run migrations manually in Supabase dashboard
- Wait for my confirmation ("ok i ran it") before proceeding after SQL changes
- Historical data must NEVER be overwritten or deleted — use cold storage pattern
- Supabase MCP is available for read queries and migrations via `apply_migration`

---

## Architecture Patterns

### Caching (3-tier)
- **L1:** In-memory Python dict with TTL (typically 1h)
- **L2:** Supabase `api_cache` table (typically 24h) — survives restarts
- **L3:** Supabase Storage + disk JSON for cold archive (historical quarterly data)

### Heavy Operations
- Slow external API calls (SEC, yfinance, Finnhub) go through `_to_heavy()` thread pool
- This prevents starving health checks and fast endpoints
- Pattern: `data = await _to_heavy(module.function, args)`

### Template & Frontend
- Server-rendered HTML with Jinja2 — NO React, NO SPA
- HTMX for lazy-loading tab content via `fetch('/api/endpoint')` → inject HTML
- Each lazy tab tracks a `loaded` flag to prevent re-fetching
- Scripts in injected HTML must be re-activated via `activateScripts(container)`
- Charts: ECharts 5 (primary, lazy-loaded via `requireECharts()`) + Chart.js 4
- CSS: Pico CSS v2 base + custom teal theme (`#0f766e` / `#0d9488` / `#2dd4bf`)
- Dark mode: `data-theme="dark"` on `<html>`, listen for `pp-theme-changed` event
- Responsive resize: `window._ppRegisterResize(fn)` for chart resize handlers

### Route Pattern
```python
@app.get("/api/{feature}/{ticker}", response_class=HTMLResponse)
async def api_feature(request: Request, ticker: str):
    if not _valid_ticker(ticker):
        return PlainTextResponse("Invalid ticker", status_code=400)
    data = await _to_heavy(module.get_data, ticker)
    if not data:
        return HTMLResponse('<p class="text-muted">No data available.</p>')
    return templates.TemplateResponse("partials/feature.html", {"request": request, "data": data})
```

### Tab Lazy-Load Pattern (in stock.html switchTab)
```javascript
if (tab === 'feature' && !featureLoaded) {
    featureLoaded = true;
    fetch('/api/feature/{{ stock_info.ticker }}')
        .then(resp => resp.text())
        .then(html => { el.innerHTML = html; activateScripts(el); })
        .catch(() => { el.innerHTML = '...error...'; });
}
```

---

## Code Style

- Python: Follow existing patterns in `web.py` — async routes, `_to_heavy()` for blocking calls
- Templates: Jinja2 partials in `templates/partials/`, loaded via HTMX fetch
- CSS: Inline `<style>` blocks in templates, use existing CSS variables (`--pp-text`, `--pp-border-light`, etc.)
- JS: Vanilla JS only (no frameworks), IIFE pattern `(function() { ... })();` in `<script>` tags
- Naming: snake_case for Python, kebab-case for CSS classes, camelCase for JS variables
- Numbers: Use `fmt_num` macro pattern (B/M/K formatting) for financial values

---

## Communication Preferences

- Keep responses concise — don't over-explain
- Show options and let me pick rather than making assumptions
- Use bullet points for change summaries
- When I say "continue", just keep going without asking questions
- When I say "ok fix it" or "yes fix all", fix everything identified without asking per-item
- For commits, write descriptive messages (what + why)

---

## What I Handle Myself

- Running SQL in Supabase dashboard (show me the SQL, I'll run it)
- Adding environment variables on Railway
- Setting up cron jobs
- DNS management (Namecheap)
- Google Search Console / SEO indexing
- API key procurement
- Marketing / tweets (but draft them for me when I ask)

---

## Key Files

| File | Purpose |
|------|---------|
| `src/filings/web.py` | Main FastAPI app — all routes (~3100 lines) |
| `src/filings/templates/stock.html` | Stock page (8 tabs, all CSS) |
| `src/filings/templates/base.html` | Base template (nav, theme, search, auth) |
| `src/filings/templates/home.html` | Homepage (bento grid) |
| `src/filings/supabase_cache.py` | L2 cache layer (set/get with TTL + hash change detection) |
| `src/filings/market_data.py` | Real-time quotes (Tiingo → yfinance fallback) |
| `src/filings/fundamentals.py` | SEC XBRL financial statements |
| `README_DEV.md` | **Developer reference — source of truth for architecture** |

---

## Data Integrity

- I am extremely protective of historical data
- Never overwrite or delete historical records
- Use the cold storage pattern (Supabase Storage JSON) for archival
- When adding new data sources, always validate against 2+ external references
- Add explanatory tooltips so users never question data accuracy

---

## Testing

- No automated test suite — I test visually on local
- Always spin up the preview server after changes
- Verify both light and dark mode rendering
- Check mobile responsiveness for tables (horizontal scroll)
- For financial data, cross-reference against SEC EDGAR / CompaniesMarketCap / DataRoma
