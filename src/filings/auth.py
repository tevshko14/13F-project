"""Authentication helpers -- Clerk JWT validation for FastAPI.

Provides middleware that extracts and validates JWTs from cookies on every
request, attaching ``request.state.user`` (JWT claims) and
``request.state.profile`` (profiles table row) for downstream use.

Uses **Clerk** RS256 JWTs verified via JWKS from Clerk's domain.
Token read from ``__session`` cookie or ``Authorization: Bearer`` header.

When ``CLERK_DOMAIN`` is not set the middleware is never added and the
app behaves as anonymous-only.
"""

from __future__ import annotations

import asyncio
import logging
import os
import threading
import time

logger = logging.getLogger(__name__)

# ── Configuration (read once at import time) ────────────────────────
SUPABASE_URL: str = os.environ.get("SUPABASE_URL", "")
SUPABASE_ANON_KEY: str = os.environ.get("SUPABASE_ANON_KEY", "")

# Clerk configuration
CLERK_DOMAIN: str = os.environ.get("CLERK_DOMAIN", "")
_CLERK_JWKS_URL: str = (
    f"https://{CLERK_DOMAIN}/.well-known/jwks.json" if CLERK_DOMAIN else ""
)

# Local dev bypass -- when set, mock user/profile is injected on every
# request that has no real Clerk session.  Lets you work on
# auth-gated pages without signing in.  NEVER set in production.
LOCAL_DEV_USER: str = os.environ.get("LOCAL_DEV_USER", "").strip()

_DEV_USER_CLAIMS = {
    "sub": "dev_user_local",
    "email": "dev@localhost",
    "name": "Dev User",
}
_DEV_USER_PROFILE = {
    "id": "dev_user_local",
    "email": "dev@localhost",
    "display_name": "Dev User",
    "user_role": "admin",
}

# Paths where we skip JWT decoding entirely (performance)
_SKIP_PREFIXES = ("/health", "/static", "/favicon.ico")

# ── JWKS cache (Clerk RS256 public keys) ────────────────────────────
_jwks_lock = threading.Lock()
_jwks_keys: list[dict] = []
_jwks_fetched_at: float = 0
_JWKS_TTL = 3600  # 1 hour

# ── Profile cache (avoid Supabase HTTP round-trip on every request) ──
from filings.caching import TTLCache as _TTLCache, MISS as _MISS
_PROFILE_TTL = 60  # seconds
_profile_cache = _TTLCache(ttl=_PROFILE_TTL, max_size=200)


# ── JWKS helpers ────────────────────────────────────────────────────


def _fetch_jwks() -> list[dict]:
    """Fetch Clerk's JWKS and return the list of key dicts.

    Cached for ``_JWKS_TTL`` seconds. Thread-safe.
    """
    global _jwks_keys, _jwks_fetched_at
    now = time.time()

    with _jwks_lock:
        if _jwks_keys and (now - _jwks_fetched_at) < _JWKS_TTL:
            return _jwks_keys

    if not _CLERK_JWKS_URL:
        return []

    try:
        import httpx

        resp = httpx.get(_CLERK_JWKS_URL, timeout=10)
        resp.raise_for_status()
        keys = resp.json().get("keys", [])
    except Exception as exc:
        logger.warning("JWKS fetch failed: %s", exc)
        # Return stale keys if available
        with _jwks_lock:
            return _jwks_keys

    with _jwks_lock:
        _jwks_keys = keys
        _jwks_fetched_at = time.time()

    logger.info("Fetched %d JWKS key(s) from %s", len(keys), CLERK_DOMAIN)
    return keys


def _get_rsa_key(kid: str) -> dict | None:
    """Find the JWK matching the given ``kid`` from the cached JWKS."""
    keys = _fetch_jwks()
    for key in keys:
        if key.get("kid") == kid:
            return key
    return None


# ── JWT helpers ─────────────────────────────────────────────────────


def decode_clerk_token(token: str) -> dict | None:
    """Decode and validate a Clerk JWT (RS256 via JWKS).

    Returns the claims dict on success, or ``None`` on any failure.
    """
    if not CLERK_DOMAIN:
        return None
    try:
        import jwt  # PyJWT
        from jwt import PyJWKClient

        # Get the signing key from Clerk's JWKS
        jwks_client = PyJWKClient(_CLERK_JWKS_URL)
        signing_key = jwks_client.get_signing_key_from_jwt(token)

        return jwt.decode(
            token,
            signing_key.key,
            algorithms=["RS256"],
            options={
                "verify_exp": True,
                "verify_nbf": True,
                "verify_iss": True,
            },
            issuer=f"https://{CLERK_DOMAIN}",
        )
    except Exception as exc:
        logger.debug("Clerk token decode failed: %s", exc)
        return None


def get_user_from_request(request) -> dict | None:
    """Return the user claims dict attached by AuthMiddleware, or None."""
    return getattr(request.state, "user", None)


def get_profile(user_id: str) -> dict | None:
    """Fetch the ``profiles`` row for *user_id* from Supabase.

    Results are cached in-memory for 60 seconds to avoid a Supabase
    HTTP round-trip on every authenticated request.

    Returns ``None`` when Supabase is unavailable or the profile
    doesn't exist.
    """
    # ── L1: check in-memory cache ──
    cached = _profile_cache.get(user_id, _MISS)
    if cached is not _MISS:
        return cached  # may be None — negative-cached profile

    # ── Cache miss: query Supabase ──
    try:
        from filings.supabase_cache import _get_client

        client = _get_client()
        if not client:
            return None
        resp = (
            client.table("profiles")
            .select("id,email,display_name,user_role")
            .eq("id", user_id)
            .single()
            .execute()
        )
        profile = resp.data
    except Exception as exc:
        logger.debug("Profile fetch failed for %s: %s", user_id, exc)
        profile = None

    _profile_cache.set(user_id, profile)
    return profile


def _extract_bearer(request) -> str | None:
    """Extract JWT from Authorization: Bearer header."""
    auth_header = request.headers.get("authorization", "")
    if auth_header.startswith("Bearer "):
        return auth_header[7:]
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
                token = (
                    request.cookies.get("__session")
                    or _extract_bearer(request)
                )
                if token and CLERK_DOMAIN:
                    claims = decode_clerk_token(token)
                    if claims:
                        request.state.user = claims
                        request.state.profile = await asyncio.to_thread(
                            get_profile, claims.get("sub", "")
                        )

                # Local dev bypass -- inject mock user when no real
                # session resolved AND the env var is set.  NEVER on
                # in prod (env var is unset there).
                if request.state.user is None and LOCAL_DEV_USER:
                    request.state.user = _DEV_USER_CLAIMS
                    request.state.profile = _DEV_USER_PROFILE

            return await call_next(request)

    return AuthMiddleware
