"""Notifications page (v2 redesign).

Chronological feed of recent platform notifications.  Mirrors the v1
/notifications page but in the v2 design language: filter chips at the
top + inbox-style row list.  Reads from ``supabase_cache``.

Visiting the page writes a ``pp-notif-seen`` cookie so the bell badge
collapses for subsequent renders -- see ``helpers._shell_context`` for
the read side of that contract.
"""

from __future__ import annotations

import logging
from collections import OrderedDict
from datetime import datetime, timezone

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from filings.app_state import templates
from filings.concurrency import to_supabase
from filings.routers._redesign.helpers import (
    _bounded_call,
    _SHELL_NOTIF_COOKIE,
    _SHELL_NOTIF_COOKIE_MAX_AGE,
    _shell_context,
    _time_ago_iso,
    GracefulRoute,
)

logger = logging.getLogger(__name__)

router = APIRouter(route_class=GracefulRoute)


_NOTIF_PAGE_TYPE_META: dict[str, dict[str, str]] = {
    "13f_change":      {"label": "13F",        "color": "#2563eb"},
    "youtube":         {"label": "YouTube",    "color": "#dc2626"},
    "reddit_velocity": {"label": "Reddit",     "color": "#f97316"},
    "congress_trade":  {"label": "Congress",   "color": "#059669"},
    "insider_trade":   {"label": "Insider",    "color": "#7c3aed"},
    "feature_release": {"label": "New feature","color": "#0ea5e9"},
}


@router.get("/notifications", response_class=HTMLResponse)
async def preview_notifications(request: Request, types: str = "", page: int = 1):
    """Notifications activity feed — chronological list with type filters."""
    valid_types = list(_NOTIF_PAGE_TYPE_META.keys())
    active_types: list[str] = []
    if types.strip():
        active_types = [t.strip() for t in types.split(",") if t.strip() in valid_types]

    page = max(int(page or 1), 1)
    per_page = 30
    offset = (page - 1) * per_page

    # Supabase-direct call -- to_supabase keeps it off the heavy pool
    # so yfinance saturation can't queue it.  Wrapped in _bounded_call
    # because to_supabase raises on hang; ungated would crash the page.
    from filings import supabase_cache
    notifs = await _bounded_call(
        to_supabase(
            supabase_cache.get_recent_notifications,
            per_page + 1, active_types or None, offset,
        ),
        timeout=6.0, fallback=[], name="notifications",
    )

    has_next = len(notifs) > per_page
    notifs = notifs[:per_page]

    rows: list[dict] = []
    for n in notifs:
        ntype = n.get("type", "")
        meta = _NOTIF_PAGE_TYPE_META.get(ntype, {"label": ntype.title(), "color": "var(--pp-dim)"})
        ticker = ((n.get("metadata") or {}).get("ticker") or "").upper()
        rows.append({
            "id":        n.get("id"),
            "type":      ntype,
            "type_label": meta["label"],
            "type_color": meta["color"],
            "title":     n.get("title", ""),
            "message":   n.get("message", ""),
            "icon":      n.get("icon", "•"),
            "link":      n.get("link", ""),
            "ticker":    ticker,
            "time_ago":  _time_ago_iso(n.get("created_at", "")),
            "created_at": n.get("created_at", ""),
        })

    # Group by day for the timeline rail.
    by_day: "OrderedDict[str, list[dict]]" = OrderedDict()
    for r in rows:
        d_label = "Earlier"
        try:
            d = datetime.fromisoformat((r["created_at"] or "").replace("Z", "+00:00"))
            now = datetime.now(d.tzinfo) if d.tzinfo else datetime.now()
            delta_days = (now.date() - d.date()).days
            if delta_days == 0:
                d_label = "Today"
            elif delta_days == 1:
                d_label = "Yesterday"
            else:
                d_label = d.strftime("%a, %b %-d")
        except Exception:
            pass
        by_day.setdefault(d_label, []).append(r)

    shell = await _shell_context(request, "Notifications")
    # Visiting this page IS the "I've seen these" event — clear the badge
    # for the current render so the user doesn't see a stale count next
    # to the page they're already on.
    shell["notif_unread"] = 0

    ctx = {
        "request":         request,
        **shell,
        "notif_types":     [
            {"key": k, "label": _NOTIF_PAGE_TYPE_META[k]["label"], "color": _NOTIF_PAGE_TYPE_META[k]["color"]}
            for k in valid_types
        ],
        "notif_active_types": active_types,
        "notif_groups":    list(by_day.items()),
        "notif_total":     len(rows),
        "notif_page":      page,
        "notif_has_next":  has_next,
        "notif_has_prev":  page > 1,
    }
    response = templates.TemplateResponse("_redesign/notifications.html", ctx)
    # Persist the visit so subsequent page renders count only notifications
    # that arrive AFTER this moment.  Cookie is per-browser, no auth needed.
    response.set_cookie(
        _SHELL_NOTIF_COOKIE,
        datetime.now(timezone.utc).isoformat(),
        max_age=_SHELL_NOTIF_COOKIE_MAX_AGE,
        path="/",
        samesite="lax",
    )
    return response
