"""Scrape individual analyst data from TipRanks and price targets from FinViz.

TipRanks (primary):
  - Individual analyst names, firm, headshot URL, star rating, success rate
  - Per-analyst price targets and rating history for a given ticker
  - Accessed via curl_cffi browser impersonation (already installed via yfinance)

FinViz (fallback):
  - Per-event price targets embedded in JSON inside <script type="application/json">
  - Works without browser impersonation (standard httpx)
  - Provides firm-level (not individual analyst) data

All functions are synchronous — call via asyncio.to_thread() from async routes.
"""

from __future__ import annotations

import base64
import json
import logging
import re
import time
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# ── TipRanks constants ────────────────────────────────────────────────

_TR_ANALYSIS_URL = (
    "https://www.tipranks.com/api/stocks/getStockAnalysis/?name={ticker}"
)
_TR_PHOTO_URL = "https://cdn.tipranks.com/expert-pictures/{uid}_tsqr.jpg"

# TipRanks consensus → grade string
_TR_CONSENSUS_MAP = {
    1: "Buy",
    2: "Outperform",
    3: "Hold",
    4: "Underperform",
    5: "Sell",
}

# TipRanks actionId → action string
_TR_ACTION_MAP = {
    1: "upgrade",
    2: "downgrade",
    3: "init",
    4: "reiterate",
    5: "maintain",
}

# ── Firm → Clearbit logo domain mapping ──────────────────────────────

FIRM_DOMAINS: dict[str, str] = {
    "goldman sachs": "gs.com",
    "jp morgan": "jpmorgan.com",
    "jpmorgan": "jpmorgan.com",
    "j.p. morgan": "jpmorgan.com",
    "morgan stanley": "morganstanley.com",
    "bank of america": "bankofamerica.com",
    "bofa securities": "bankofamerica.com",
    "citigroup": "citi.com",
    "citi": "citi.com",
    "citibank": "citi.com",
    "wells fargo": "wellsfargo.com",
    "barclays": "barclays.com",
    "ubs": "ubs.com",
    "deutsche bank": "db.com",
    "jefferies": "jefferies.com",
    "raymond james": "raymondjames.com",
    "piper sandler": "pipersandler.com",
    "wedbush": "wedbush.com",
    "wedbush securities": "wedbush.com",
    "oppenheimer": "oppenheimer.com",
    "mizuho": "mizuhogroup.com",
    "mizuho securities": "mizuhogroup.com",
    "hsbc": "hsbc.com",
    "stifel": "stifel.com",
    "stifel nicolaus": "stifel.com",
    "truist": "truist.com",
    "truist securities": "truist.com",
    "keybanc": "key.com",
    "keycorp": "key.com",
    "rbc capital": "rbccm.com",
    "rbc capital markets": "rbccm.com",
    "bmo capital": "bmo.com",
    "bmo capital markets": "bmo.com",
    "td cowen": "tdcowen.com",
    "cowen": "cowen.com",
    "wolfe research": "wolferesearch.com",
    "bernstein": "bernstein.com",
    "evercore": "evercore.com",
    "evercore isi": "evercore.com",
    "guggenheim": "guggenheimpartners.com",
    "guggenheim securities": "guggenheimpartners.com",
    "rosenblatt": "rblt.com",
    "rosenblatt securities": "rblt.com",
    "maxim group": "maximgrp.com",
    "needham": "needham.com",
    "canaccord": "canaccordgenuity.com",
    "canaccord genuity": "canaccordgenuity.com",
    "da davidson": "dadavidson.com",
    "d.a. davidson": "dadavidson.com",
    "benchmark": "benchmarkcompany.com",
    "loop capital": "loopcapital.com",
    "baird": "rwbaird.com",
    "william blair": "williamblair.com",
    "craig hallum": "craighallum.com",
    "chardan": "chardanml.com",
    "h.c. wainwright": "hcwainwright.com",
    "ladenburg thalmann": "ladenburg.com",
    "b. riley": "brileyfin.com",
    "b riley": "brileyfin.com",
    "northland securities": "northlandsecurities.com",
    "roth capital": "rothcapitalpartners.com",
    "roth mkm": "rothcapitalpartners.com",
    "susquehanna": "susquehanna.net",
    "janney montgomery": "janney.com",
    "atlantic equities": "atlanticequities.com",
    "macquarie": "macquarie.com",
    "scotia capital": "scotiabank.com",
    "scotiabank": "scotiabank.com",
    "societe generale": "socgen.com",
    "credit suisse": "credit-suisse.com",
    "bnp paribas": "bnpparibas.com",
    "natixis": "natixis.com",
    "daiwa": "daiwa-grp.jp",
    "nomura": "nomura.com",
    "sanford bernstein": "bernstein.com",
    "alliance bernstein": "bernstein.com",
    "seaport global": "seaportglobal.com",
    "moffettnathanson": "moffettnathanson.com",
    "new street research": "newstreetresearch.com",
    "monness crespi": "mncw.com",
    "melius research": "meliusresearch.com",
    "redburn atlantic": "redburnatlantic.com",
    "td securities": "tdcowen.com",
}


def get_firm_logo_url(firm: str) -> str | None:
    """Return Clearbit logo URL for a known analyst firm, or None."""
    key = firm.lower().strip()
    # Try exact match first
    domain = FIRM_DOMAINS.get(key)
    if not domain:
        # Try prefix match (e.g. "Goldman Sachs & Co." → "Goldman Sachs")
        for known, d in FIRM_DOMAINS.items():
            if key.startswith(known) or known.startswith(key[:12]):
                domain = d
                break
    return f"https://logo.clearbit.com/{domain}" if domain else None


# ── TipRanks scraper ─────────────────────────────────────────────────


def _safe_float(v) -> float | None:
    """Convert value to float, returning None for NaN/None/invalid."""
    if v is None:
        return None
    try:
        f = float(v)
        return f if f == f else None  # NaN check
    except (TypeError, ValueError):
        return None


def _parse_tipranks_response(data: dict, ticker: str) -> list[dict]:
    """Parse TipRanks getStockAnalysis JSON → list of analyst dicts.

    Each dict has:
      analyst_id, name, firm, photo_url, star_rating, success_rate,
      total_ratings, tipranks_rank, ratings (list of rating events)
    """
    ticker = ticker.upper()
    analysts_raw = data.get("analysts") or data.get("expertList") or []

    result: list[dict] = []
    for a in analysts_raw:
        analyst_id = str(a.get("expertUID") or a.get("uid") or "").strip()
        if not analyst_id:
            continue

        name = str(a.get("name") or a.get("fullName") or "").strip()
        firm = str(a.get("firmName") or a.get("firm") or "").strip()
        if not name or not firm:
            continue

        # Photo URL from TipRanks CDN
        photo_url = _TR_PHOTO_URL.format(uid=analyst_id)

        # Ratings / price target history for this stock
        ratings_raw = a.get("ratings") or a.get("pricetargets") or []
        ratings: list[dict] = []
        for r in ratings_raw:
            date_str = ""
            raw_date = r.get("date") or r.get("d") or ""
            if raw_date:
                try:
                    # TipRanks may return epoch ms or ISO string
                    if isinstance(raw_date, (int, float)):
                        dt = datetime.fromtimestamp(raw_date / 1000, tz=timezone.utc)
                        date_str = dt.strftime("%Y-%m-%d")
                    else:
                        date_str = str(raw_date)[:10]
                except Exception:
                    date_str = str(raw_date)[:10]

            consensus_id = r.get("consensus") or r.get("newRatingId")
            action_id = r.get("actionId") or r.get("ratingActionId")

            to_grade = _TR_CONSENSUS_MAP.get(consensus_id, "")
            action = _TR_ACTION_MAP.get(action_id, "reiterate")

            pt = _safe_float(r.get("priceTarget") or r.get("p"))
            prior_pt = _safe_float(r.get("priorPriceTarget") or r.get("prevP"))

            if date_str:
                ratings.append(
                    {
                        "grade_date": date_str,
                        "action": action,
                        "to_grade": to_grade,
                        "from_grade": "",  # TipRanks doesn't always provide prior grade
                        "price_target": pt,
                        "prior_price_target": prior_pt,
                    }
                )

        result.append(
            {
                "analyst_id": analyst_id,
                "name": name,
                "firm": firm,
                "photo_url": photo_url,
                "star_rating": _safe_float(a.get("stars") or a.get("numOfStars")),
                "success_rate": _safe_float(
                    a.get("successRate") or a.get("success_rate")
                ),
                "total_ratings": int(a.get("numOfRatings") or a.get("total_ratings") or 0),
                "tipranks_rank": int(a.get("rank") or 0),
                "ratings": ratings,
            }
        )

    return result


def fetch_tipranks_analysts(ticker: str) -> list[dict]:
    """Fetch per-analyst ratings from TipRanks via curl_cffi browser impersonation.

    Returns list of analyst dicts (see _parse_tipranks_response).
    Returns [] on any failure.
    """
    try:
        from curl_cffi import requests as cffi_requests
    except ImportError:
        logger.warning("curl_cffi not available — TipRanks fetch skipped")
        return []

    url = _TR_ANALYSIS_URL.format(ticker=ticker.upper())
    try:
        session = cffi_requests.Session(impersonate="chrome124")
        resp = session.get(url, timeout=20)
        if resp.status_code != 200:
            logger.info(
                "TipRanks returned %d for %s (Cloudflare or auth required)",
                resp.status_code,
                ticker,
            )
            return []
        data = resp.json()
        analysts = _parse_tipranks_response(data, ticker)
        logger.info("TipRanks: fetched %d analysts for %s", len(analysts), ticker)
        return analysts
    except Exception as exc:
        logger.warning("TipRanks fetch failed for %s: %s", ticker, exc)
        return []


def download_analyst_photo(analyst_id: str) -> bytes | None:
    """Download analyst headshot from TipRanks CDN.

    Returns JPEG bytes if successful (>500 bytes), None otherwise.
    """
    try:
        from curl_cffi import requests as cffi_requests

        session = cffi_requests.Session(impersonate="chrome124")
        url = _TR_PHOTO_URL.format(uid=analyst_id)
        resp = session.get(url, timeout=10)
        if resp.status_code == 200 and len(resp.content) > 500:
            return resp.content
    except Exception as exc:
        logger.debug("Photo download failed for %s: %s", analyst_id, exc)
    return None


def download_analyst_photos_batch(
    analyst_ids: list[str],
    delay: float = 0.3,
) -> dict[str, bytes]:
    """Download multiple analyst headshots with polite delay.

    Returns dict of analyst_id → JPEG bytes for successful downloads.
    """
    try:
        from curl_cffi import requests as cffi_requests

        session = cffi_requests.Session(impersonate="chrome124")
    except ImportError:
        return {}

    results: dict[str, bytes] = {}
    for analyst_id in analyst_ids:
        try:
            url = _TR_PHOTO_URL.format(uid=analyst_id)
            resp = session.get(url, timeout=10)
            if resp.status_code == 200 and len(resp.content) > 500:
                results[analyst_id] = resp.content
        except Exception as exc:
            logger.debug("Photo download failed for %s: %s", analyst_id, exc)
        if delay > 0:
            time.sleep(delay)

    return results


# ── FinViz fallback (price targets only) ─────────────────────────────


def _parse_target_price(raw: str) -> tuple[float | None, float | None]:
    """Parse FinViz targetPrice string into (prior, current).

    Examples:
      "$315 → $325"  → (315.0, 325.0)
      "$300"         → (None, 300.0)
      ""             → (None, None)
    """
    raw = raw.strip().replace("&rarr;", "→").replace("→", "→")
    # Remove HTML entities
    raw = re.sub(r"&[a-z]+;", " ", raw)
    raw = re.sub(r"\s+", " ", raw).strip()

    # Split on → arrow
    if "→" in raw:
        parts = raw.split("→")
        prior_str = re.sub(r"[^\d.]", "", parts[0])
        curr_str = re.sub(r"[^\d.]", "", parts[1])
        return _safe_float(prior_str), _safe_float(curr_str)
    else:
        curr_str = re.sub(r"[^\d.]", "", raw)
        return None, _safe_float(curr_str)


def _parse_rating_change(raw: str) -> tuple[str, str]:
    """Parse FinViz rating string into (from_grade, to_grade).

    Examples:
      "Hold → Buy"   → ("Hold", "Buy")
      "Overweight"   → ("", "Overweight")
    """
    raw = raw.strip().replace("&rarr;", "→")
    if "→" in raw:
        parts = raw.split("→", 1)
        return parts[0].strip(), parts[1].strip()
    return "", raw.strip()


def fetch_finviz_targets(ticker: str) -> list[dict]:
    """Fetch per-event price targets from FinViz embedded JSON.

    FinViz embeds chartEvents JSON in <script type="application/json">.
    Each event: {dateTimestamp, eventType, ratings: [{action, analyst, rating, targetPrice}]}

    Returns list of dicts with firm-level data:
      {firm, action, from_grade, to_grade, price_target, prior_price_target, grade_date}
    """
    try:
        import httpx
    except ImportError:
        logger.warning("httpx not available — FinViz fetch skipped")
        return []

    url = f"https://finviz.com/quote.ashx?t={ticker.upper()}"
    try:
        resp = httpx.get(
            url,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
            },
            timeout=15,
            follow_redirects=True,
        )
        if resp.status_code != 200:
            logger.info("FinViz returned %d for %s", resp.status_code, ticker)
            return []
        html = resp.text
    except Exception as exc:
        logger.warning("FinViz fetch failed for %s: %s", ticker, exc)
        return []

    # Find all <script type="application/json"> blocks
    script_blocks = re.findall(
        r'<script[^>]+type=["\']application/json["\'][^>]*>(.*?)</script>',
        html,
        re.DOTALL,
    )

    results: list[dict] = []
    seen: set[tuple[str, str]] = set()

    for block in script_blocks:
        try:
            data = json.loads(block)
        except json.JSONDecodeError:
            continue

        chart_events = data.get("chartEvents") or []
        for event in chart_events:
            if event.get("eventType") != "chartEvent/ratings":
                continue

            ts = event.get("dateTimestamp")
            if ts:
                try:
                    date_str = datetime.fromtimestamp(ts, tz=timezone.utc).strftime(
                        "%Y-%m-%d"
                    )
                except Exception:
                    date_str = ""
            else:
                date_str = ""

            for r in event.get("ratings") or []:
                firm = str(r.get("analyst") or "").strip()
                if not firm or not date_str:
                    continue

                action_raw = str(r.get("action") or "").lower()
                action_map = {
                    "upgrade": "upgrade",
                    "downgrade": "downgrade",
                    "initiated": "init",
                    "initiation": "init",
                    "reiterated": "reiterate",
                    "maintained": "maintain",
                    "resumed": "reiterate",
                    "assumed": "reiterate",
                    "lowered": "downgrade",
                    "raised": "upgrade",
                }
                action = action_map.get(action_raw, action_raw)

                from_grade, to_grade = _parse_rating_change(
                    str(r.get("rating") or "")
                )
                prior_pt, curr_pt = _parse_target_price(
                    str(r.get("targetPrice") or "")
                )

                key = (firm.lower(), date_str)
                if key in seen:
                    continue
                seen.add(key)

                results.append(
                    {
                        "firm": firm,
                        "action": action,
                        "from_grade": from_grade,
                        "to_grade": to_grade,
                        "price_target": curr_pt,
                        "prior_price_target": prior_pt,
                        "grade_date": date_str,
                    }
                )

    # Sort by date descending
    results.sort(key=lambda x: x.get("grade_date") or "0000-00-00", reverse=True)
    logger.info("FinViz: found %d rating events for %s", len(results), ticker)
    return results


# ── Supabase DB helpers ───────────────────────────────────────────────


def upsert_analyst_profiles(
    analysts_data: list[dict],
    client,
) -> None:
    """Upsert analyst profile rows (no photo_b64 — photos stored separately)."""
    if not analysts_data or client is None:
        return

    rows = []
    now_iso = datetime.now(timezone.utc).isoformat()
    for a in analysts_data:
        if not a.get("analyst_id") or not a.get("name"):
            continue
        rows.append(
            {
                "analyst_id": a["analyst_id"],
                "name": a["name"],
                "firm": a.get("firm") or "",
                "star_rating": a.get("star_rating"),
                "success_rate": a.get("success_rate"),
                "total_ratings": a.get("total_ratings"),
                "tipranks_rank": a.get("tipranks_rank"),
                "fetched_at": now_iso,
            }
        )

    if not rows:
        return

    # Batch in chunks of 500
    for i in range(0, len(rows), 500):
        chunk = rows[i : i + 500]
        try:
            client.table("analyst_profiles").upsert(
                chunk, on_conflict="analyst_id"
            ).execute()
        except Exception as exc:
            logger.warning("upsert_analyst_profiles chunk %d failed: %s", i, exc)


def upsert_analyst_stock_ratings(
    ticker: str,
    analysts_data: list[dict],
    client,
) -> None:
    """Upsert per-analyst-per-ticker ratings to analyst_stock_ratings."""
    if not analysts_data or client is None:
        return

    ticker = ticker.upper()
    now_iso = datetime.now(timezone.utc).isoformat()
    rows = []

    for a in analysts_data:
        analyst_id = a.get("analyst_id")
        if not analyst_id:
            continue
        for r in a.get("ratings") or []:
            if not r.get("grade_date"):
                continue
            rows.append(
                {
                    "ticker": ticker,
                    "analyst_id": analyst_id,
                    "firm": a.get("firm") or "",
                    "analyst_name": a.get("name") or "",
                    "action": r.get("action") or "",
                    "to_grade": r.get("to_grade") or "",
                    "from_grade": r.get("from_grade") or "",
                    "price_target": r.get("price_target"),
                    "prior_price_target": r.get("prior_price_target"),
                    "grade_date": r.get("grade_date"),
                    "fetched_at": now_iso,
                }
            )

    if not rows:
        return

    for i in range(0, len(rows), 500):
        chunk = rows[i : i + 500]
        try:
            client.table("analyst_stock_ratings").upsert(
                chunk, on_conflict="ticker,analyst_id,grade_date"
            ).execute()
        except Exception as exc:
            logger.warning("upsert_analyst_stock_ratings chunk %d failed: %s", i, exc)

    logger.debug(
        "Upserted %d analyst stock ratings for %s", len(rows), ticker
    )


def get_all_analyst_photos(client) -> list[dict]:
    """Bulk-read all analyst_profiles rows that have photo_b64 set.

    Returns list of {analyst_id, photo_b64, content_type}.
    Used at startup to populate in-memory photo cache.
    """
    if client is None:
        return []

    result = []
    page_size = 1000
    offset = 0

    while True:
        try:
            resp = (
                client.table("analyst_profiles")
                .select("analyst_id,photo_b64,content_type")
                .not_.is_("photo_b64", "null")
                .neq("photo_b64", "")
                .range(offset, offset + page_size - 1)
                .execute()
            )
            rows = resp.data or []
            result.extend(rows)
            if len(rows) < page_size:
                break
            offset += page_size
        except Exception as exc:
            logger.warning("get_all_analyst_photos failed: %s", exc)
            break

    return result


def get_profiles_missing_photos(client, limit: int = 200) -> list[dict]:
    """Return analyst_profiles rows that have no photo yet.

    Returns list of {analyst_id, name, firm}.
    """
    if client is None:
        return []
    try:
        resp = (
            client.table("analyst_profiles")
            .select("analyst_id,name,firm")
            .is_("photo_b64", "null")
            .limit(limit)
            .execute()
        )
        return resp.data or []
    except Exception as exc:
        logger.warning("get_profiles_missing_photos failed: %s", exc)
        return []


def save_analyst_photos(photos: dict[str, bytes], client) -> int:
    """Upsert analyst photos into analyst_profiles (insert-only for photo field).

    photos: dict of analyst_id → JPEG bytes
    Returns number of photos saved.
    """
    if not photos or client is None:
        return 0

    saved = 0
    for analyst_id, photo_bytes in photos.items():
        try:
            b64 = base64.b64encode(photo_bytes).decode()
            client.table("analyst_profiles").update(
                {"photo_b64": b64, "content_type": "image/jpeg"}
            ).eq("analyst_id", analyst_id).is_("photo_b64", "null").execute()
            saved += 1
        except Exception as exc:
            logger.debug("save_analyst_photo failed for %s: %s", analyst_id, exc)

    logger.info("Saved %d analyst photos to DB", saved)
    return saved
