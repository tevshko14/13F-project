"""Slow-changing company "About" facts — CEO / employees / HQ / IPO / website
/ logo / blurb.

Backed by the Supabase ``company_about`` table.  Refreshed in bulk every
~6 months by ``scripts/backfill_about.py``; per-page reads never hit
yfinance.  Falls through to a live yfinance + Finnhub merge when the row
is missing, and writes the result back so the next reader is fast.

Storage shape mirrors the data sources directly (raw values, no display
formatting) so the backfill script and the read-through path produce
identical rows.  Display formatting lives in the redesign router where
it can stay coupled to the template's column layout.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

# 6 months — company facts change slowly; CEO turnover, employee count
# revisions, and HQ moves are the main churn vectors.
FRESH_SECONDS = 180 * 24 * 3_600


# ── Supabase I/O ───────────────────────────────────────────────────────


def _get_client():
    """Lazy import — keeps Supabase client init off the hot path at import."""
    from filings import supabase_cache
    return supabase_cache._get_client()


def get_row(ticker: str) -> dict | None:
    """Read one ticker's row.  Returns ``None`` if missing or on error.

    Caller checks ``is_fresh`` to decide whether to refresh — we always
    return whatever's in the table, even stale, so a flaky network
    doesn't cause a render regression.
    """
    client = _get_client()
    if client is None:
        return None
    try:
        resp = (
            client.table("company_about")
            .select("*")
            .eq("ticker", ticker.upper())
            .maybe_single()
            .execute()
        )
        return resp.data if resp else None
    except Exception as exc:
        logger.debug("company_about get_row failed for %s: %s", ticker, exc)
        return None


def upsert_row(row: dict) -> bool:
    """Upsert a single ``company_about`` row.  Stamps ``fetched_at`` with now.

    ``row`` MUST include ``ticker``.  ``None`` values are dropped before
    upsert so we don't blast existing data with nulls when a refetch
    only resolved a subset of fields.
    """
    if "ticker" not in row:
        logger.warning("upsert_row missing ticker key: %s", row.keys())
        return False
    client = _get_client()
    if client is None:
        return False
    payload = {k: v for k, v in row.items() if v is not None}
    payload["ticker"] = row["ticker"].upper()
    payload["fetched_at"] = datetime.now(timezone.utc).isoformat()
    try:
        client.table("company_about").upsert(
            payload, on_conflict="ticker"
        ).execute()
        return True
    except Exception as exc:
        logger.warning("company_about upsert failed for %s: %s",
                       payload.get("ticker"), exc)
        return False


def is_fresh(row: dict | None, *, cutoff_seconds: int = FRESH_SECONDS) -> bool:
    """True iff *row* exists and ``fetched_at`` is within the freshness window."""
    if not row or not row.get("fetched_at"):
        return False
    try:
        ts = datetime.fromisoformat(str(row["fetched_at"]).replace("Z", "+00:00"))
        return (datetime.now(timezone.utc) - ts).total_seconds() < cutoff_seconds
    except (TypeError, ValueError):
        return False


def get_stale_or_missing(
    tickers: list[str], *, cutoff_seconds: int = FRESH_SECONDS,
    chunk_size: int = 200,
) -> list[str]:
    """From a universe, return only tickers whose row is missing or stale.

    Chunks the ``in_()`` query at *chunk_size* per request — Postgres URL
    length blows up around ~6k tickers if we send them all at once, and
    silently truncates rather than erroring (which would mark every
    ticker as "missing" and trigger a wasteful full refetch).
    """
    universe = [t.upper() for t in tickers]
    client = _get_client()
    if client is None:
        return universe

    existing: dict[str, str] = {}
    for i in range(0, len(universe), chunk_size):
        chunk = universe[i:i + chunk_size]
        try:
            resp = (
                client.table("company_about")
                .select("ticker, fetched_at")
                .in_("ticker", chunk)
                .execute()
            )
        except Exception as exc:
            logger.warning("get_stale_or_missing chunk %d-%d failed: %s",
                           i, i + len(chunk), exc)
            continue
        for r in resp.data or []:
            existing[r["ticker"]] = r["fetched_at"]

    cutoff = datetime.now(timezone.utc).timestamp() - cutoff_seconds
    out: list[str] = []
    for t in universe:
        ts_str = existing.get(t)
        if not ts_str:
            out.append(t)
            continue
        try:
            ts_secs = datetime.fromisoformat(ts_str.replace("Z", "+00:00")).timestamp()
            if ts_secs < cutoff:
                out.append(t)
        except (TypeError, ValueError):
            out.append(t)
    return out


# ── Live fetch ─────────────────────────────────────────────────────────


def _domain_from_url(url: str | None) -> str | None:
    if not url:
        return None
    parsed = urlparse(url if "://" in url else f"https://{url}")
    domain = parsed.netloc or parsed.path
    if domain.startswith("www."):
        domain = domain[4:]
    return domain or None


def _extract_ceo(info: dict) -> str | None:
    officers = info.get("companyOfficers") or []
    for o in officers:
        title = (o.get("title") or "").lower()
        if "chief executive" in title or "ceo" in title:
            return o.get("name") or None
    if officers and officers[0].get("name"):
        return officers[0]["name"]
    return None


def fetch_live(ticker: str) -> dict:
    """Live fetch from yfinance + Finnhub.  Returns a row dict ready to upsert.

    Always returns a dict with ``ticker`` + ``source``; other fields are
    populated only when the upstream actually delivered them, so a
    rate-limited yfinance call still yields a partial Finnhub-only row
    rather than overwriting good data with nulls (the caller's
    ``upsert_row`` drops ``None`` values to make this safe).

    ``source`` reflects which APIs contributed: ``"yfinance"`` /
    ``"finnhub"`` / ``"mixed"`` / ``"empty"``.
    """
    t_up = ticker.upper()
    row: dict[str, Any] = {"ticker": t_up}
    sources: list[str] = []

    # ── yfinance .info (cached 1h via filings.client) ──
    info: dict = {}
    try:
        from filings import client
        info = client.get_yfinance_info(t_up) or {}
    except Exception as exc:
        logger.debug("yfinance fetch failed for %s: %s", ticker, exc)

    if info:
        sources.append("yfinance")
        row["name"] = info.get("longName") or info.get("shortName") or None
        row["ceo"] = _extract_ceo(info)
        if info.get("fullTimeEmployees"):
            try:
                row["employees"] = int(info["fullTimeEmployees"])
            except (TypeError, ValueError):
                pass
        row["hq_city"] = info.get("city") or None
        row["hq_state"] = info.get("state") or None
        row["hq_country"] = info.get("country") or None

        epoch = (info.get("firstTradeDateEpochUtc")
                 or info.get("firstTradeDateMilliseconds"))
        if epoch:
            try:
                ts = float(epoch)
                if ts > 1e11:
                    ts /= 1000
                row["ipo_date"] = (
                    datetime.fromtimestamp(ts, tz=timezone.utc)
                    .strftime("%Y-%m-%d")
                )
            except (TypeError, ValueError, OSError):
                pass

        domain = _domain_from_url(info.get("website"))
        if domain:
            row["website"] = domain
            row["logo_url"] = f"https://logo.clearbit.com/{domain}"

        if info.get("longBusinessSummary"):
            row["blurb"] = info["longBusinessSummary"].strip()

    # ── Finnhub /stock/profile2 (sync httpx — backfill is sync; live
    #    read-through uses to_heavy in the router) ──
    fh: dict = {}
    fh_key = os.environ.get("FINNHUB_API_KEY", "").strip()
    if fh_key:
        try:
            import httpx
            r = httpx.get(
                "https://finnhub.io/api/v1/stock/profile2",
                params={"symbol": t_up, "token": fh_key},
                timeout=10,
            )
            if r.status_code == 200:
                fh = r.json() or {}
        except Exception as exc:
            logger.debug("Finnhub profile2 failed for %s: %s", ticker, exc)

    if fh:
        sources.append("finnhub")
        # Fill gaps yfinance couldn't.  Never overwrite a populated field.
        if not row.get("name"):
            row["name"] = fh.get("name") or None
        if not row.get("hq_country"):
            row["hq_country"] = fh.get("country") or None
        if not row.get("ipo_date") and fh.get("ipo"):
            row["ipo_date"] = fh["ipo"]
        if not row.get("website"):
            domain = _domain_from_url(fh.get("weburl"))
            if domain:
                row["website"] = domain
        if not row.get("logo_url") and fh.get("logo"):
            row["logo_url"] = fh["logo"]

    if len(sources) > 1:
        row["source"] = "mixed"
    elif sources:
        row["source"] = sources[0]
    else:
        row["source"] = "empty"
    return row


# ── Display formatting (used by the redesign stock router) ────────────


def format_for_template(row: dict | None) -> dict:
    """Map a stored ``company_about`` row to the about_* keys the template
    expects (e.g. ``"Apr 22, 1999"``, ``"Santa Clara, CA, US"``).

    Returns em-dashes for missing fields so the panel never blanks out.
    """
    if not row:
        row = {}

    # IPO: "YYYY-MM-DD" → "Apr 22, 1999"
    ipo = "—"
    if row.get("ipo_date"):
        try:
            ipo = (
                datetime.strptime(str(row["ipo_date"])[:10], "%Y-%m-%d")
                .strftime("%b %-d, %Y")
            )
        except (TypeError, ValueError):
            ipo = str(row["ipo_date"])

    # HQ: comma-joined parts; blank → em-dash
    parts = [row.get(k) for k in ("hq_city", "hq_state", "hq_country")]
    hq = ", ".join([p for p in parts if p]) or "—"

    # Employees: int → "29,600"
    emp = "—"
    if row.get("employees"):
        try:
            emp = f"{int(row['employees']):,}"
        except (TypeError, ValueError):
            pass

    # Blurb: trim to 280 chars at word boundary so the about panel's
    # fixed height stays predictable across tickers.
    blurb = row.get("blurb") or ""
    if blurb and len(blurb) > 280:
        blurb = blurb[:280].rsplit(" ", 1)[0] + "…"

    return {
        "about_blurb":     blurb,
        "about_ceo":       row.get("ceo") or "—",
        "about_employees": emp,
        "about_hq":        hq,
        "about_ipo":       ipo,
        "about_website":   row.get("website") or "—",
        "logo_url":        row.get("logo_url") or "",
        "company_name":    row.get("name") or "",
    }
