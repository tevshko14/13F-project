"""Authentication helpers — Supabase JWT validation for FastAPI.

Provides middleware that extracts and validates Supabase JWTs from
cookies on every request, attaching ``request.state.user`` (JWT claims)
and ``request.state.profile`` (profiles table row) for downstream use.

When ``SUPABASE_JWT_SECRET`` is not set the middleware is never added
and the app behaves exactly as before (anonymous-only).
"""

from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)

# ── Configuration (read once at import time) ────────────────────────
JWT_SECRET: str = os.environ.get("SUPABASE_JWT_SECRET", "")
SUPABASE_URL: str = os.environ.get("SUPABASE_URL", "")
SUPABASE_ANON_KEY: str = os.environ.get("SUPABASE_ANON_KEY", "")

# Paths where we skip JWT decoding entirely (performance)
_SKIP_PREFIXES = ("/health", "/static", "/favicon.ico")


# ── JWT helpers ─────────────────────────────────────────────────────

def decode_token(token: str) -> dict | None:
    """Decode and validate a Supabase JWT.

    Returns the claims dict on success, or ``None`` on any failure.
    """
    if not JWT_SECRET:
        return None
    try:
        import jwt  # PyJWT

        return jwt.decode(
            token,
            JWT_SECRET,
            algorithms=["HS256"],
            audience="authenticated",
        )
    except Exception:
        # Lazy-import may fail if PyJWT not installed; token may be
        # expired or malformed.  All cases → treat as unauthenticated.
        return None


def get_user_from_request(request) -> dict | None:
    """Return the user claims dict attached by AuthMiddleware, or None."""
    return getattr(request.state, "user", None)


def get_profile(user_id: str) -> dict | None:
    """Fetch the ``profiles`` row for *user_id* from Supabase.

    Returns ``None`` when Supabase is unavailable or the profile
    doesn't exist.
    """
    try:
        from filings.supabase_cache import _get_client

        client = _get_client()
        if not client:
            return None
        resp = (
            client.table("profiles")
            .select("*")
            .eq("id", user_id)
            .single()
            .execute()
        )
        return resp.data
    except Exception as exc:
        logger.debug("Profile fetch failed for %s: %s", user_id, exc)
        return None


# ── Middleware ───────────────────────────────────────────────────────

def _build_auth_middleware():
    """Return the AuthMiddleware class.

    Defined inside a factory to avoid importing Starlette at module
    level when auth is not configured.
    """
    from starlette.middleware.base import BaseHTTPMiddleware

    class AuthMiddleware(BaseHTTPMiddleware):
        """Attach ``request.state.user`` and ``.profile`` from cookie JWT."""

        async def dispatch(self, request, call_next):
            request.state.user = None
            request.state.profile = None

            # Skip static/health paths
            path = request.url.path
            if not any(path.startswith(p) for p in _SKIP_PREFIXES):
                token = request.cookies.get("sb-access-token")
                if token:
                    claims = decode_token(token)
                    if claims:
                        request.state.user = claims
                        request.state.profile = get_profile(claims.get("sub", ""))

            return await call_next(request)

    return AuthMiddleware
