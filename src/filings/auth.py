"""Authentication helpers -- Clerk + legacy Supabase JWT validation for FastAPI.

Provides middleware that extracts and validates JWTs from cookies on every
request, attaching ``request.state.user`` (JWT claims) and
``request.state.profile`` (profiles table row) for downstream use.

Supports two providers:
  1. **Clerk** (primary): RS256 JWTs verified via JWKS from Clerk's domain.
     Token read from ``__session`` cookie or ``Authorization: Bearer`` header.
  2. **Supabase** (legacy): HS256 JWTs verified with a shared secret.
     Token read from ``sb-access-token`` cookie. Kept for migration period.

When neither ``CLERK_DOMAIN`` nor ``SUPABASE_JWT_SECRET`` is set the
middleware is never added and the app behaves as anonymous-only.
"""

from __future__ import annotations

import asyncio
import logging
import os
import threading
import time

logger = logging.getLogger(__name__)

# ── Configuration (read once at import time) ────────────────────────
JWT_SECRET: str = os.environ.get("SUPABASE_JWT_SECRET", "")
SUPABASE_URL: str = os.environ.get("SUPABASE_URL", "")
SUPABASE_ANON_KEY: str = os.environ.get("SUPABASE_ANON_KEY", "")

# Clerk configuration
CLERK_DOMAIN: str = os.environ.get("CLERK_DOMAIN", "")
_CLERK_JWKS_URL: str = (
    f"https://{CLERK_DOMAIN}/.well-known/jwks.json" if CLERK_DOMAIN else ""
)

# Paths where we skip JWT decoding entirely (performance)
_SKIP_PREFIXES = ("/health", "/static", "/favicon.ico")

# ── JWKS cache (Clerk RS256 public keys) ────────────────────────────
_jwks_lock = threading.Lock()
_jwks_keys: list[dict] = []
_jwks_fetched_at: float = 0
_JWKS_TTL = 3600  # 1 hour

# ── Profile cache (avoid Supabase HTTP round-trip on every request) ──
_profile_lock = threading.Lock()
_profile_cache: dict[str, tuple[float, dict | None]] = {}
_PROFILE_TTL = 60  # seconds
_MAX_PROFILE_CACHE = 500  # LRU eviction threshold


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


def decode_supabase_token(token: str) -> dict | None:
    """Decode and validate a legacy Supabase JWT (HS256).

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
        return None


def decode_token(token: str) -> dict | None:
    """Decode a JWT -- tries Clerk (RS256) first, then legacy Supabase (HS256).

    Returns the claims dict on success, or ``None`` on any failure.
    """
    # Try Clerk first (primary provider)
    if CLERK_DOMAIN:
        result = decode_clerk_token(token)
        if result:
            return result

    # Fall back to legacy Supabase
    if JWT_SECRET:
        result = decode_supabase_token(token)
        if result:
            return result

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
    now = time.time()

    # ── L1: check in-memory cache ──
    with _profile_lock:
        if user_id in _profile_cache:
            ts, data = _profile_cache[user_id]
            if now - ts < _PROFILE_TTL:
                return data

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

    # ── Store in cache (with LRU eviction) ──
    with _profile_lock:
        _profile_cache[user_id] = (now, profile)
        if len(_profile_cache) > _MAX_PROFILE_CACHE:
            sorted_keys = sorted(
                _profile_cache,
                key=lambda k: _profile_cache[k][0],
            )
            for k in sorted_keys[: len(_profile_cache) - _MAX_PROFILE_CACHE]:
                del _profile_cache[k]

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
                # Try Clerk __session cookie first, then legacy sb-access-token,
                # then Authorization: Bearer header
                token = (
                    request.cookies.get("__session")
                    or request.cookies.get("sb-access-token")
                    or _extract_bearer(request)
                )
                if token:
                    claims = decode_token(token)
                    if claims:
                        request.state.user = claims
                        request.state.profile = await asyncio.to_thread(
                            get_profile, claims.get("sub", "")
                        )

            return await call_next(request)

    return AuthMiddleware
