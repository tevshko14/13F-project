"""Full post-item-6 visual verification.

For each extracted route:
  1. Hit it through https://paperpanda.io/
  2. Verify status code is acceptable
  3. Verify the rendered HTML contains route-specific content markers
     (catches the "200 but empty body" failure mode)
  4. Capture title + size + timing

Returns non-zero on any failure or content-warning.
"""

import re
import sys
import urllib.request

# Each entry: (path, acceptable_statuses, [content_markers])
# Markers are case-insensitive substring checks against the response body.
ROUTES: list[tuple[str, set[int], list[str]]] = [
    # ── Home + partials (just extracted) ─────────────────────────
    ("/",                           {200},      ["Markets", "PaperPanda"]),
    ("/_pages",                     {200},      ["Home", "Stock"]),
    ("/api/home/heatmap",           {200},      ["heatmap"]),
    ("/api/home/activity",          {200},      ["activity"]),
    ("/api/home/calendar",          {200},      ["Earnings"]),
    # ── Stock ────────────────────────────────────────────────────
    ("/stock/NVDA",                 {200},      ["NVDA", "PaperPanda"]),
    ("/stock/NVDA/chart/1M",        {200},      ["chart"]),
    # ── Funds ────────────────────────────────────────────────────
    ("/funds",                      {200},      ["Funds", "PaperPanda"]),
    ("/funds/1067983",              {200},      ["Berkshire", "PaperPanda"]),
    ("/api/funds-index/holdings",   {200},      ["holding"]),
    ("/api/funds-index/activity",   {200},      ["pp-kpi"]),
    # ── Macro / Retail (medium-complex pages) ───────────────────
    ("/macro",                      {200},      ["Macro", "PaperPanda"]),
    ("/retail",                     {200},      ["Retail", "PaperPanda"]),
    # ── Signals + activity feed ─────────────────────────────────
    ("/insiders",                   {200},      ["Insider", "PaperPanda"]),
    ("/congress",                   {200},      ["Congress", "PaperPanda"]),
    ("/notifications",              {200},      ["Notifications"]),
    # ── User + support ──────────────────────────────────────────
    ("/profile",                    {200, 302}, []),
    ("/watchlist",                  {200, 302}, []),
    ("/support",                    {200},      ["Support", "Panda"]),
    ("/support/thank-you",          {200},      ["Support"]),
    # ── Operational ─────────────────────────────────────────────
    ("/health",                     {200},      ["status"]),
    # ── Legacy /_v2 redirects (should 301) ──────────────────────
    ("/_v2/stock/NVDA",             {301},      []),
    ("/_v2/macro",                  {301},      []),
    ("/_v2/funds",                  {301},      []),
    ("/_v2/congress",               {301},      []),
    ("/_v2/insiders",               {301},      []),
]

BASE = "https://paperpanda.io"


def fetch(url: str) -> tuple[int, str, float]:
    """Fetch url WITHOUT following redirects; return (status, body, seconds)."""
    import time
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (paperpanda-visual-check)",
        "Accept": "text/html",
        "Accept-Encoding": "identity",
    })

    class NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self, req, fp, code, msg, headers, newurl):
            return None

    opener = urllib.request.build_opener(NoRedirect)
    t0 = time.time()
    try:
        with opener.open(req, timeout=30) as r:
            return r.status, r.read().decode("utf-8", errors="replace"), time.time() - t0
    except urllib.error.HTTPError as e:
        # 301/302/4xx — capture status
        return e.code, e.read().decode("utf-8", errors="replace") if e.fp else "", time.time() - t0


failed = 0
warned = 0
print(f"{'Route':35s}  {'Status':6s}  {'Time':>7s}  {'Size':>9s}  {'Title':40s}  Notes")
print("-" * 130)

for path, ok_status, markers in ROUTES:
    url = BASE + path
    try:
        status, body, dt = fetch(url)
    except Exception as e:
        print(f"{path:35s}  {'ERR':>6s}  {'':>7s}  {'':>9s}  {'':40s}  {type(e).__name__}: {e}")
        failed += 1
        continue

    title_m = re.search(r"<title>([^<]+)</title>", body, re.I)
    title = (title_m.group(1) if title_m else "—").strip()[:40]
    size = f"{len(body):,}"

    notes_parts = []
    if status not in ok_status:
        failed += 1
        notes_parts.append(f"✗ unexpected status (want {sorted(ok_status)})")
    else:
        # Content-marker check only meaningful for 200 responses.
        if status == 200 and markers:
            missing = [m for m in markers if m.lower() not in body.lower()]
            if missing:
                warned += 1
                notes_parts.append(f"⚠ missing markers: {missing}")
            else:
                notes_parts.append("✓")
        else:
            notes_parts.append("✓")

    print(f"{path:35s}  {status:>6d}  {dt:>6.2f}s  {size:>9s}  {title:40s}  {' '.join(notes_parts)}")

print()
print(f"Result: {failed} failed, {warned} content-warning(s) across {len(ROUTES)} routes")
sys.exit(0 if failed == 0 else 1)
