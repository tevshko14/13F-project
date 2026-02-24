#!/usr/bin/env bash
# warm_cache.sh — Hit top pages after deploy to pre-warm server-side caches.
#
# Usage:
#   ./scripts/warm_cache.sh                  # defaults to https://paperpanda.io
#   ./scripts/warm_cache.sh http://localhost:8000
#
# Warms: homepage, funds list, retail (+ leaderboard & calendar APIs),
#        insider trading, and the ticker search index.

set -euo pipefail

BASE="${1:-https://paperpanda.io}"
BASE="${BASE%/}"   # strip trailing slash

GREEN='\033[0;32m'
RED='\033[0;31m'
DIM='\033[0;90m'
NC='\033[0m'

# Portable millisecond timer (works on macOS + Linux)
now_ms() { python3 -c "import time; print(int(time.time()*1000))"; }

# Wait for the health endpoint to come up (max 90s)
echo "⏳ Waiting for ${BASE}/health ..."
for i in $(seq 1 30); do
    status=$(curl -s -o /dev/null -w "%{http_code}" "${BASE}/health" 2>/dev/null || echo "000")
    if [ "$status" = "200" ]; then
        echo -e "${GREEN}✓${NC} Health check passed"
        break
    fi
    if [ "$i" = "30" ]; then
        echo -e "${RED}✗${NC} Health check failed after 90s — aborting"
        exit 1
    fi
    sleep 3
done

# Pages to warm (order matters — homepage warms the fund cache,
# retail warms ApeWisdom + CNN, etc.)
URLS=(
    "/"                          # Homepage — warms fund cache + S&P 500 data
    "/funds"                     # Top Funds list
    "/retail"                    # Retail hub — warms ApeWisdom, CNN, YouTube
    "/api/retail/leaderboard"    # Leaderboard partial (lazy-loaded by retail)
    "/api/retail/calendar"       # Calendar partial (lazy-loaded by retail)
    "/insider-trading"           # Insider trading screener
    "/api/ticker-search-index"   # Search autocomplete index
)

echo ""
echo "🔥 Warming ${#URLS[@]} pages on ${BASE} ..."
echo ""

ok=0
fail=0

for path in "${URLS[@]}"; do
    url="${BASE}${path}"
    start=$(now_ms)
    status=$(curl -s -o /dev/null -w "%{http_code}" --max-time 60 "$url" 2>/dev/null || echo "000")
    end=$(now_ms)
    ms=$(( end - start ))

    if [ "$status" = "200" ]; then
        echo -e "  ${GREEN}✓${NC} ${path}  ${DIM}${status} ${ms}ms${NC}"
        ok=$((ok + 1))
    else
        echo -e "  ${RED}✗${NC} ${path}  ${RED}${status} ${ms}ms${NC}"
        fail=$((fail + 1))
    fi
done

echo ""
echo -e "Done: ${GREEN}${ok} ok${NC}, ${RED}${fail} failed${NC}"
[ "$fail" -eq 0 ] || exit 1
