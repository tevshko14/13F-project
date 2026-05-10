"""Authentication + admin router — user-lifecycle pages + admin dashboard.

Moved out of web.py during audit-sprint-6.  Groups:
  * Auth pages: /login, /signup, /profile, /logout (thin template renders)
  * Clerk webhook: /api/webhooks/clerk (HMAC-verified user sync)
  * Admin panel: /admin, /admin/user/{user_id} (404 for non-admin users)

The admin routes depend on ``request.state.user`` being populated by the
Clerk auth middleware registered in web.py, the same way the watchlist
router does.  ``_check_admin`` keeps a 5-min in-memory cache so every
admin page load doesn't round-trip to Supabase.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json as _json
import logging
import os
import time

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from filings import supabase_cache
from filings.app_state import templates

logger = logging.getLogger(__name__)

router = APIRouter()


# ── Clerk webhook config ──────────────────────────────────────────────
# Env vars are read at router-load time (same as the rest of web.py).
_CLERK_WEBHOOK_SECRET = os.environ.get("CLERK_WEBHOOK_SECRET", "")


# ── Admin check cache ─────────────────────────────────────────────────
# In-memory 5-min TTL so /admin page loads don't hit Supabase per request.
# Connection failures are NOT cached — only definitive True/False.
_admin_cache: dict[str, tuple[float, bool]] = {}
_ADMIN_CACHE_TTL = 300  # 5 minutes


def _check_admin(user_id: str) -> bool:
    """Return True if user_id is in admin_users table (cached)."""
    now = time.monotonic()
    cached = _admin_cache.get(user_id)
    if cached and (now - cached[0]) < _ADMIN_CACHE_TTL:
        return cached[1]
    result = supabase_cache.is_admin_user(user_id)
    if result is None:
        return False
    _admin_cache[user_id] = (now, result)
    return result


# ── Auth pages ────────────────────────────────────────────────────────


# `/profile` is owned by the v2 redesign router
# (`filings.routers.redesign_preview.preview_profile`) which renders
# `_redesign/profile.html`.  The v1 `profile.html` template is
# unreached in production -- left in the repo only for fallback /
# rollback reference until the v2 page is fully fleshed out and we
# can delete the v1 template.


@router.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    """Render page that auto-opens Clerk sign-in modal."""
    return templates.TemplateResponse("login.html", {"request": request})


@router.get("/signup", response_class=HTMLResponse)
async def signup_page(request: Request):
    """Render page that auto-opens Clerk sign-up modal."""
    return templates.TemplateResponse("signup.html", {"request": request})


@router.get("/logout")
async def logout(request: Request):
    response = RedirectResponse(url="/", status_code=302)
    response.delete_cookie("__session", path="/")
    return response


# ── Clerk user-lifecycle webhook ──────────────────────────────────────


@router.post("/api/webhooks/clerk")
async def clerk_webhook(request: Request):
    """Handle Clerk user lifecycle webhooks (user.created/updated/deleted).

    Verifies svix HMAC-SHA256 signature, then upserts/deletes the profiles
    table row.  No external dependency required — svix uses standard HMAC.
    """
    if not _CLERK_WEBHOOK_SECRET:
        return JSONResponse({"error": "Webhook secret not configured"}, status_code=500)

    svix_id = request.headers.get("svix-id")
    svix_timestamp = request.headers.get("svix-timestamp")
    svix_signature = request.headers.get("svix-signature")

    if not svix_id or not svix_timestamp or not svix_signature:
        return JSONResponse({"error": "Missing svix headers"}, status_code=400)

    body = await request.body()

    # Verify HMAC-SHA256 signature (svix protocol)
    try:
        secret = _CLERK_WEBHOOK_SECRET
        if secret.startswith("whsec_"):
            secret = secret[6:]
        secret_bytes = base64.b64decode(secret)
        to_sign = f"{svix_id}.{svix_timestamp}.{body.decode()}".encode()
        expected = base64.b64encode(
            hmac.new(secret_bytes, to_sign, hashlib.sha256).digest()
        ).decode()
        # svix-signature may contain multiple sigs like "v1,<sig1> v1,<sig2>"
        sigs = [s.split(",", 1)[1] for s in svix_signature.split(" ") if "," in s]
        if not any(hmac.compare_digest(expected, s) for s in sigs):
            raise ValueError("No matching signature")
    except Exception as exc:
        logger.warning("Clerk webhook verification failed: %s", exc)
        return JSONResponse({"error": "Invalid signature"}, status_code=400)

    payload = _json.loads(body)
    event_type = payload.get("type", "")
    data = payload.get("data", {})

    if event_type in ("user.created", "user.updated"):
        user_id = data.get("id")
        email_addresses = data.get("email_addresses", [])
        email = email_addresses[0]["email_address"] if email_addresses else None
        first_name = data.get("first_name") or ""
        last_name = data.get("last_name") or ""
        display_name = " ".join(filter(None, [first_name, last_name])) or None
        avatar_url = data.get("image_url")

        try:
            client = supabase_cache._get_client()
            if client:
                row = {
                    "id": user_id,
                    "email": email,
                    "display_name": display_name,
                    "avatar_url": avatar_url,
                }
                await asyncio.to_thread(
                    lambda: client.table("profiles").upsert(row, on_conflict="id").execute()
                )
        except Exception as exc:
            logger.error("Profile upsert failed: %s", exc)
            return JSONResponse({"error": "Database error"}, status_code=500)

    elif event_type == "user.deleted":
        user_id = data.get("id")
        if user_id:
            try:
                client = supabase_cache._get_client()
                if client:
                    await asyncio.to_thread(
                        lambda: client.table("profiles").delete().eq("id", user_id).execute()
                    )
            except Exception as exc:
                logger.error("Profile delete failed: %s", exc)
                return JSONResponse({"error": "Database error"}, status_code=500)

    return JSONResponse({"status": "ok"}, status_code=200)


# ── Admin panel (private — returns 404 for non-admin users) ───────────


@router.get("/admin", response_class=HTMLResponse)
async def admin_dashboard(request: Request):
    """Admin dashboard — returns 404 for non-admin users."""
    if not request.state.user:
        raise HTTPException(status_code=404)
    user_id = request.state.user.get("sub", "")
    is_admin = await asyncio.to_thread(_check_admin, user_id)
    if not is_admin:
        raise HTTPException(status_code=404)

    # Fetch all admin data in parallel
    summary, leaderboard, recent, prefs_stats, users, digest = await asyncio.gather(
        asyncio.to_thread(supabase_cache.admin_watchlist_summary),
        asyncio.to_thread(supabase_cache.admin_watchlist_leaderboard, 50),
        asyncio.to_thread(supabase_cache.admin_recent_hearts, 100),
        asyncio.to_thread(supabase_cache.admin_notification_prefs_stats),
        asyncio.to_thread(supabase_cache.admin_user_list),
        asyncio.to_thread(supabase_cache.admin_digest_stats),
    )

    return templates.TemplateResponse("admin.html", {
        "request": request,
        "summary": summary,
        "leaderboard": leaderboard,
        "recent_hearts": recent,
        "prefs_stats": prefs_stats,
        "users": users,
        "digest": digest,
        "is_admin": True,
    })


@router.get("/admin/user/{user_id}", response_class=HTMLResponse)
async def admin_user_detail_page(request: Request, user_id: str):
    """Admin user detail view — returns 404 for non-admin users."""
    if not request.state.user:
        raise HTTPException(status_code=404)
    admin_id = request.state.user.get("sub", "")
    is_admin = await asyncio.to_thread(_check_admin, admin_id)
    if not is_admin:
        raise HTTPException(status_code=404)

    detail = await asyncio.to_thread(supabase_cache.admin_user_detail, user_id)
    if not detail:
        raise HTTPException(status_code=404)

    return templates.TemplateResponse("admin_user.html", {
        "request": request,
        "detail": detail,
        "is_admin": True,
    })
