# PaperPanda Homepage Design System (Modernization v1)

## Scope
Frontend-only modern refresh for local deployment and iteration. No backend or API contract changes.

## Keep (Existing Foundation)
- Theme system and base tokens in `/src/filings/templates/base.html` (`--pp-*` variables, light/dark support).
- Existing homepage data modules and endpoint wiring:
  - `/api/ticker-tape`
  - `/api/market-overview`
  - `/api/market-news`
  - `/api/heatmap` and `/api/heatmap-data`
  - `/api/retail-sentiment`
  - `/api/trending-combined`
- Existing ticker search behavior (`tickerSearchFilter`, `tickerSearchKeydown`) and DOM ids.
- Existing Stripe support flow and checkout session behavior.

## Add (Modernized Homepage Direction)
- Editorial hero with stronger typographic contrast and layered glass surface.
- New accent family for homepage shell:
  - `--home-accent: #0d9488`
  - `--home-accent-soft: rgba(13, 148, 136, 0.14)`
  - `--home-ink` and `--home-surface` for light/dark adaptive legibility.
- Aurora atmospheric background shapes for visual depth.
- Premium search bar treatment with elevated shadow, softer gradient CTA, and focus lift.
- Compact destination cards with descriptive copy and clear hover hierarchy.
- Section framing pattern:
  - Eyebrow label (`.section-eyebrow`)
  - Serif subheading (`.section-head h2`)
- Refined bento cards:
  - Larger radius, stronger layered shadow, subtle hover lift.
  - Staggered reveal animation on first paint.
- Updated support card visual language while preserving current Stripe actions.

## Typography
- Display: `Fraunces` (headlines/section titles).
- UI/body: `Space Grotesk`.
- Fallbacks remain safe and web-standard.

## Component Inventory (Homepage)
- `hero-wrap`
- `hero-search-bar`
- `hero-search-results` — dropdown positioned via CSS class (no inline styles)
- `home-card`
- `ticker-tape-shell`
- `section-head`
- `glass-card` (bento containers)
- `heatmap-toggle` / `heatmap-pill` — includes `role="group"`, `aria-label`, `aria-pressed`
- `pw-*` support module
- `checkout-overlay`

## Component Inventory (Funds + Subtabs)
- `funds-shell`
- `funds-hero`
- `gp-nav-shell` + `gp-subtab` — nav uses `-webkit-overflow-scrolling: touch` for iOS
- `gp-surface-card`
- `dash-card` (holdings charts)
- `gp-table-card` + `gp-table`
- `cache-info` + `sync-badge`
- `gp-panel` (Funds, Holdings, Activity, Capital Deployed) — opacity fade via `.visible` class added with double-`requestAnimationFrame` after `display:block`; initial active panel gets `.visible` immediately on load (no animation)
- `gp-expand-btn` / `gp-expand-arrow` — full-width teal-tinted footer button with 180° arrow rotation on expand
- `deployment-leaderboard` partial styling (badge + deployment bar palette alignment)

## Component Inventory (Insiders + 3 Tabs)
- `insider-shell`
- `insider-hero`
- `insider-nav-shell` + `insider-tab` (Latest, Purchases, Sales)
- `insider-period-bar` + `insider-period` (time filters)
- `insider-chart-card` + momentum toggle button
- `insider-results-card` (tab content container for `#insider-trades-content`)

## Component Inventory (Congress)
- `congress-shell`
- `congress-hero`
- `cg-nav-shell` + `cg-subtab` (Congress, Holdings, Activity)
- `cg-stats`
- `cg-card` (chart/data cards)
- `cg-table` + `cg-fade` overflow pattern
- `pol-shell` + `pol-header` (individual politician page)
- `pol-stat-card`, `pol-chart-card`, `pol-table`

## Component Inventory (Stock Detail + Subtabs)
- `stock-shell`
- `stock-hero` + `stock-kicker`
- `stock-nav-shell` + `stock-tab` — 5 top-level tabs: Overview, Ownership, Forecasts, Signals, Vitals
- `pp-pill-nav` + `pp-pill-btn` + `pp-pill-panel` — inner sub-nav used within tabs:
  - **Ownership pills**: Funds | Congress | Insiders | SEC Filings  (`own-panel-funds`, `own-panel-congress`, `own-panel-insider`, `own-panel-filings`)
  - **Forecasts pills**: Analysts | Earnings | Estimates (`forecast-analysts-panel`, `forecast-earnings-panel`, `forecast-estimates-panel`)
  - **Signals pills**: Sentiment | Short Interest (`signal-panel-sentiment`, `signal-panel-short-interest`)
- `seo-overview` — server-rendered SEO section (metrics grid, company description, holder summary, related stocks); glass treatment, visible to Googlebot
- overview modules:
  - `pp-ohlcv-chart` + `pp-period-btn`
  - ticker news cards/pagination (`tn-*`)
- `quarter-tab` + `quarter-panel` (ownership history within Funds panel)
- ownership helpers:
  - `holders-expand-btn`
  - chart toggle (`toggleChartView`)

## Component Inventory (Retail + Subtabs)
- `retail-shell`
- `retail-hero`
- `rt-nav-shell` + `rt-subtab` (Sentiment, Leaderboard, Calendar)
- `rt-card` summary tiles
- leaderboard surfaces:
  - `retail-treemap`
  - `retail-bubble-chart`
  - `retail-leaderboard-content`
- calendar surfaces:
  - `retail-calendar-content`
  - recent uploads grid + event list cards
- lazy panel wrappers:
  - `rt-panel` + `panel-sentiment|leaderboard|calendar`

## Performance Guardrails (Stock Page)
- Keep heavy pill panels lazy-loaded on first activation only (Analysts, Earnings, Estimates, Sentiment, Short Interest, Vitals).
- Do not prefetch all subtab or pill-panel payloads on page load.
- Chart.js loaded on-demand via `requireChartJS()` — do not add a `<script defer>` for Chart.js.
- Keep chart/theme updates incremental; avoid full page reloads.
- Prefer CSS-only visual enhancements over new runtime JS.
- Minimize new third-party dependencies and network calls.

## Performance Guardrails (Retail)
- Keep Leaderboard and Calendar lazy-loaded only when their tab is opened.
- Fetch chart JSON + table HTML in parallel only for active Leaderboard tab.
- Avoid eager loading YouTube calendar payload when Sentiment tab is active.
- Keep visual upgrades CSS-first; avoid extra polling/background intervals.

## Congress Color Rule
- Keep political affiliation colors unchanged:
  - Democrat: blue (`#2563eb`)
  - Republican: red (`#dc2626`)
  - Independent: neutral gray
- These party colors are semantic and should not be re-themed.

## Motion Rules
- Use restrained motion with purpose:
  - Card load-in (`bentoFadeIn`) once on entry.
  - Hover lift on cards/search/CTAs.
  - Progress fill pulse for support bar.
- Keep motion durations short (0.16s to 0.48s) and non-disruptive.

## Responsive Behavior
- Desktop (>1023px): 12-column bento grid, 4-col destination cards.
- Tablet (≤1023px): cards → 2-col, bento reflows to 6/6 then full-width.
- Mobile (≤767px): single-column bento, tighter hero and CTA spacing.
- Narrow phone (≤480px): destination cards → single column.

## Accessibility
- `prefers-reduced-motion: reduce` disables all animations and hover transforms — applies to homepage, Funds page, Insiders page, Congress page, Politician page, Stock page, and Retail page.
- Heatmap toggle uses `role="group"`, `aria-label`, and `aria-pressed` for screen readers.
- Aurora decorative elements are `aria-hidden="true"`.
- Checkout close button has `aria-label="Close"`.

## Insiders Page Notes
- Chart.js loaded via `<script defer>` with `<link rel="preload">` for faster first chart paint.
- `insider-nav-list` uses `-webkit-overflow-scrolling: touch` for iOS momentum scroll + `padding: 0 0 0 0.5rem` to match Funds nav alignment.
- `insider-period-bar` `flex-wrap: wrap` keeps period pills accessible at any width.
- Chart toggle button (`insider-chart-toggle-btn`) switches simple/detailed view; JS clears `lastViewType` to force full Chart.js recreate on view change.
- Theme change listener (`pp-theme-changed`) forces full chart recreate so axis/grid colors update correctly.

## Congress Page Notes
- Space Grotesk + Fraunces loaded via `extra_head_scripts` alongside ECharts preload.
- `cg-nav-list` uses `-webkit-overflow-scrolling: touch` for iOS momentum scroll + `padding: 0 0 0 0.5rem` to match Funds/Insiders nav alignment.
- Political affiliation colors (`#2563eb` Dem, `#dc2626` Rep, `#6b7280` Ind) are semantic — used in badge, dot, and ECharts bar gradient; never re-themed.
- Trade filter pills on politician page (`.insider-filter.active`) use teal gradient (`#0f766e → #0d9488`) matching the rest of the design system — not the old `#ef4444` red.
- ECharts loaded via `requireECharts()` lazy helper (already in base.html); chamber dot charts stored in `window._cgCharts` for resize on tab switch.

## Retail Page Notes
- Space Grotesk + Fraunces loaded via `extra_head_scripts` only. Chart.js loaded on-demand via `requireChartJS()` callback inside `renderBubbleChart` — no `<script defer>` tag; works with hx-boost navigation.
- `rt-nav-list` uses `-webkit-overflow-scrolling: touch` for iOS momentum scroll + `padding: 0 0 0 0.5rem` to match all other page nav alignment.
- Leaderboard and Calendar panels lazy-loaded on first tab activation only (`leaderboardLoaded` / `calendarLoaded` flags).
- `renderRetailTreemap` uses `requireECharts()` lazy helper; chart stored and resized via `_ppRegisterResize`.
- `renderBubbleChart` checks `typeof Chart === 'undefined'` and defers via `requireChartJS()` if not ready.
- `pp-theme-changed` listener re-renders the bubble chart with current guru-filter state on theme switch.
- `.retail-charts-row` is the semantic class for the charts grid — CSS targets `#panel-leaderboard .retail-charts-row > article` (not a fragile inline-style attribute selector).
- Fear & Greed inline color values use `var(--pp-text-muted)` (not hardcoded `#666`/`#888`/`#999`) so they adapt to dark mode.
- `.calendar-filter-pill.active` uses teal gradient (`#0f766e → #0d9488`) matching design system — not the legacy `var(--pp-text)` black.
- `loadCalendar` exposed as `window.loadCalendar` so the Calendar panel's Retry button can call it.
- `posthog.capture('retail_tab_switch', ...)` fires on every tab switch for analytics.

## Stock Page Notes
- Space Grotesk + Fraunces loaded via `extra_head_scripts` only. Chart.js loaded on-demand via `requireChartJS()` callback — preserves hx-boost navigation compatibility; no `<script defer>` tag.
- `stock-nav-list` uses `-webkit-overflow-scrolling: touch` for iOS momentum scroll + `padding: 0 0 0 0.5rem` to match all other page nav alignment.
- Tab structure: **5 top-level tabs** (Overview, Ownership, Forecasts, Signals, Vitals) with inner `pp-pill-nav` sub-navigation inside Ownership, Forecasts, and Signals tabs.
- `switchForecastPill(pill)` and `switchSignalPill(pill)` handle inner pill switching within Forecasts and Signals tabs; ownership pills use event delegation on `.pp-pill-btn[data-own-tab]`.
- `.pp-pill-btn.active` uses teal gradient (`linear-gradient(120deg, #0f766e, #0d9488)`) with `box-shadow: 0 4px 10px rgba(13, 148, 136, 0.22)`.
- `.quarter-tab.active` uses teal (`#0d9488`) underline + text color — not the legacy `#ef4444` red.
- `.pp-period-btn.active` uses teal gradient (`#0f766e → #0d9488`) to match the design system; hover uses `#0d9488` border/text.
- ECharts OHLCV candlestick chart (`#pp-ohlcv-chart`) loaded via `requireECharts()` lazy helper; period changes via `ppChartPeriod(period)`.
- Heavy content (Analysts, Earnings, Estimates, Sentiment, Short Interest, and Vitals panels) lazy-loaded on first activation only — no prefetch.
- `seo-overview` section rendered server-side at the bottom of the page (below all tab content) for Googlebot indexing; glass treatment with metrics grid, `long_business_summary`, holder summary, and related stocks.
- JSON-LD includes both BreadcrumbList and Dataset schemas; Dataset uses `variableMeasured` for holder count and combined value.
- Theme change listener (`pp-theme-changed`) re-renders both the OHLCV chart and the ownership activity Chart.js chart with updated axis/grid colors.
- Live price display (`#pp-header-price`) updates on each OHLCV fetch; hidden until data arrives.

## Component Inventory (Alt Signals)
- `alt-signals-shell`
- `alt-signals-hero` + `alt-signals-kicker`
- `alt-signals-aurora-one` / `alt-signals-aurora-two` — teal + orange radials (matches teal/orange design tokens)
- `alt-signals-nav-shell` + `alt-signals-nav-list` + `alt-tab` — pill tabs with teal active gradient (`#0f766e → #0d9488`); iOS `-webkit-overflow-scrolling: touch`
- `alt-panel` (Trending Now / Macro Trends / Ticker Lookup) — `display:none` / `display:block` toggle via `.active`
- `alt-card` — glass card container (`linear-gradient(165deg, …)`, `border-radius: 18px`, shadow)
- `gt-card` + `gt-card-header` — inner card surfaces for Google Trends data
- `gt-macro-grid` + `gt-macro-card` — responsive auto-fill grid for macro keyword categories
- `gt-pill` / `gt-pill-intent` / `gt-pill-product` / `gt-pill-comparison` — keyword classification badges
- `gt-trending-row` + `gt-trending-query` + `gt-trending-ticker` — trending search row layout
- `gt-lookup-controls` + `gt-lookup-input` + `gt-lookup-btn` — ticker search form; focus ring uses teal (`rgba(13,148,136,0.16)`)
- `gt-quick-wrap` + `gt-quick-ticker` — quick-access ticker pill buttons
- `gt-ticker-result` — HTMX swap target for per-ticker keyword results
- `gt-loading` + `gt-loading-spinner` — loading state with teal border-top spinner
- Interaction hooks preserved: `hx-get` on trending/macro panels, `htmx.ajax` in `lookupTicker()`, `gt-ticker-input`, `gt-lookup-btn`, `gt-ticker-result`, `.gt-quick-ticker[data-ticker]`

## Component Inventory (Support)
- `support-shell`
- `support-hero` + `support-kicker` + `.hero-sub`
- `support-aurora-one` / `support-aurora-two` — teal + orange radials (standard design tokens)
- `panda-progress` — glass progress module; `.progress-fill` uses green gradient (`#15803d → #22c55e`) preserving funding semantics
- `goal-reached-badge` — green pill badge shown when goal is met
- `donate-widget` — glass card wrapper for the donation flow
- `donate-nav-shell` + `donate-nav-list` + `donate-tab` — pill tab nav for Monthly/One-time; active state uses green gradient (semantically correct for funding); backdrop-filter glass shell
- `donate-panel` (Monthly / One-time) — `display:none` / `display:block` via `.active`
- `tier-grid` / `tier-grid-single` + `tier-card` — responsive 3-col or single-col tier layout
- `tier-emoji` / `tier-name` / `tier-price` / `tier-desc` / `tier-btn` — tier card internals; price uses Fraunces font
- `checkout-overlay` + `checkout-overlay-inner` + `checkout-close` + `checkout-loading` + `checkout-mount` — Stripe Embedded Checkout modal (fully intact)
- `thank-you-banner` — post-checkout success banner (conditionally rendered)
- `where-it-goes` + `cost-tiles` + `cost-tile` — cost breakdown section; tiles use glass card treatment with teal hover
- `funding-history` + `chart-container` (`#funding-chart`) — ECharts bar chart of monthly funding; loaded via `requireECharts()`
- `roadmap-section` + `roadmap-tiles` + `roadmap-tile` — what's-next grid; hover uses teal border/shadow (replaces old blue)
- `other-support` + `.yt-link` — YouTube CTA card
- `feedback-section` + `.btn-feedback` — feedback CTA; hover uses teal gradient (replaces old green)
- `feedback-modal-overlay` + `feedback-modal` + `feedback-modal-header` + `feedback-modal-body` + `feedback-loading` — feedback iframe modal (fully intact)
- Interaction hooks preserved: `openCheckout()`, `closeCheckout()`, `switchDonateTab()`, `openFeedbackModal()`, `closeFeedbackModal()`, `_stripeInstance`, `_checkoutInstance`, `_feedbackLoaded`, `#checkout-overlay`, `#checkout-mount`, `#checkout-loading`, `#tab-monthly`, `#tab-onetime`, `#panel-monthly`, `#panel-onetime`, `#funding-chart`, `#feedback-overlay`, `#feedback-modal-body`, `#feedback-loading`

## Alt Signals Page Notes
- Space Grotesk + Fraunces loaded via `extra_head_scripts`. No extra JS dependencies.
- Aurora colors updated to standard teal/orange tokens (`rgba(45,212,191,…)` + `rgba(251,146,60,…)`) — previously used sky blue (`rgba(14,165,233,…)`).
- Tab nav migrated to `alt-signals-nav-shell` + `alt-signals-nav-list` structure (matches `rt-nav-shell`, `stock-nav-shell` pattern) with `-webkit-overflow-scrolling: touch` for iOS.
- Tab active state updated to teal gradient (`#0f766e → #0d9488`) with `box-shadow: 0 8px 18px rgba(13,148,136,0.28)` — previously used blue sky (`#0369a1 → #0ea5e9`).
- Tab hover, input focus ring, and lookup button all updated to teal tokens.
- `gt-card-header h3` uses Fraunces for stronger typographic hierarchy inside panels.
- Dark mode tab nav shell uses `rgba(15,23,42,0.72)` bg + `rgba(45,212,191,0.2)` border; tab hover uses teal radial.
- `@media (prefers-reduced-motion: reduce)` block added covering aurora, tabs, cards, and lookup button.
- All HTMX attrs (`hx-get`, `hx-trigger="load"`, `hx-trigger="revealed"`, `hx-swap="innerHTML"`) and JS behavior fully preserved.

## Support Page Notes
- Space Grotesk + Fraunces loaded via `extra_head_scripts`. Stripe JS loaded async inline (preserved as-is).
- Aurora colors updated to standard teal/orange tokens — previously used green/sky blue.
- `donate-tabs` div replaced with `donate-nav-shell` + `donate-nav-list` `<ul>` pattern for structural consistency with other pages; tab IDs and onclick handlers unchanged.
- Donate tab active state remains green (`#15803d → #22c55e`) — intentional funding semantic color override.
- `progress-fill` updated to gradient (`#15803d → #22c55e`) for visual polish while preserving green semantics.
- `where-it-goes h2`, `roadmap-section h2`, `funding-history h3`, `other-support h3`, `feedback-section h3`, `feedback-modal-header h3`, `thank-you-banner h2`, and `tier-price` now use Fraunces for stronger typographic hierarchy.
- `roadmap-tile:hover` updated to teal border/shadow (`rgba(13,148,136,0.3)`) — previously used blue (`#1976d2`).
- `cost-tile` elevated from bare `var(--pp-surface)` to glass card treatment with teal hover; `cost-tile:hover` updated to teal from green.
- `feedback-section .btn-feedback:hover` updated to teal gradient — previously used green.
- `other-support` and `feedback-section` added explicit `box-shadow` in dark mode (previously missing).
- `@media (prefers-reduced-motion: reduce)` block added covering aurora, tabs, tier cards, roadmap tiles, cost tiles, progress fill, and modals.
- All Stripe checkout flow functions (`openCheckout`, `closeCheckout`, `_stripeInstance`, `_checkoutInstance`), modal functions (`openFeedbackModal`, `closeFeedbackModal`), and ECharts chart logic fully preserved.
- `posthog` not used on this page (no analytics events to preserve).

## Guardrails For Next Pages
- Reuse current accent family and typographic pairing.
- Keep high-contrast content blocks over atmospheric backgrounds.
- Prefer meaningful depth and spacing over extra ornament.
- Maintain existing data interaction patterns before adding new frontend state.

## File Locations
- Homepage: `/src/filings/templates/home.html`
- Funds page: `/src/filings/templates/grand_portfolio.html`
- Deployment partial: `/src/filings/templates/partials/deployment_leaderboard.html`
- Insiders page: `/src/filings/templates/insider_trading.html`
- Congress page: `/src/filings/templates/congress.html`
- Politician page: `/src/filings/templates/politician.html`
- Stock page: `/src/filings/templates/stock.html`
- Retail page: `/src/filings/templates/retail.html`
- Retail leaderboard partial: `/src/filings/templates/partials/retail_leaderboard_v2.html`
- Retail calendar partial: `/src/filings/templates/partials/retail_calendar.html`
- Design system: `/docs/homepage-design-system.md`
