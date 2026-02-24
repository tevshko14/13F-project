"""Cold storage layer using Supabase Storage (S3-compatible).

Provides archive/retrieve for historical 13F quarterly data.
Hot data (current + previous quarter) stays in Supabase Postgres;
older quarters are archived here as JSON files.

Bucket structure:
    paperpanda-archive/
        13f/{cik}/quarterly/{period}.json
        e.g. 13f/1067983/quarterly/Q3_2024.json

All functions are fault-tolerant: return None/False on failure, never raise.

Circuit-breaker: if the bucket cannot be created/verified, all upload
calls are short-circuited for the rest of the process lifetime to avoid
84× failing HTTP calls that slow down the sync worker.
"""

from __future__ import annotations

import json
import logging

logger = logging.getLogger(__name__)

BUCKET_NAME = "paperpanda-archive"

# ── Circuit-breaker state ─────────────────────────────────────────
# _bucket_status tracks whether the bucket is usable:
#   None  = not yet checked
#   True  = bucket exists, uploads allowed
#   False = bucket missing & couldn't be created, skip uploads
_bucket_status: bool | None = None


def _get_storage():
    """Return the Supabase Storage client, or None if unavailable."""
    from filings.supabase_cache import _get_client

    client = _get_client()
    if client is None:
        return None
    try:
        return client.storage
    except Exception as exc:
        logger.debug("Could not access Supabase Storage: %s", exc)
        return None


def is_available() -> bool:
    """Return True if cold storage bucket is confirmed usable.

    Performs a one-time lazy check on first call.  Once the bucket is
    confirmed missing (and creation fails), returns False for the rest
    of the process lifetime so callers can skip archival immediately.
    """
    global _bucket_status
    if _bucket_status is not None:
        return _bucket_status
    # First call — try to ensure the bucket exists
    _bucket_status = ensure_bucket()
    return _bucket_status


def ensure_bucket() -> bool:
    """Create the archive bucket if it doesn't exist.

    Called once during auto-migration and lazily on first upload.
    Idempotent and non-fatal.
    Returns True if bucket exists/was created, False on failure.
    """
    global _bucket_status
    storage = _get_storage()
    if storage is None:
        _bucket_status = False
        return False

    try:
        storage.create_bucket(BUCKET_NAME, options={"public": False})
        logger.info("Created Supabase Storage bucket: %s", BUCKET_NAME)
        _bucket_status = True
        return True
    except Exception as exc:
        err = str(exc).lower()
        if "already exists" in err or "duplicate" in err or "409" in err:
            logger.debug("Bucket %s already exists", BUCKET_NAME)
            _bucket_status = True
            return True
        logger.warning("Failed to create bucket %s: %s", BUCKET_NAME, exc)
        _bucket_status = False
        return False


def upload_json(path: str, data: dict | list) -> bool:
    """Upload a JSON-serializable object to Supabase Storage.

    Args:
        path: Object path within the bucket,
              e.g. ``"13f/1067983/quarterly/Q3_2024.json"``
        data: dict or list to serialize as JSON

    Uses upsert semantics (overwrite if exists) for idempotent re-runs.
    Returns True on success, False on failure.

    Short-circuits immediately if the bucket is known to be unavailable
    (circuit-breaker pattern).
    """
    # Circuit-breaker: skip if bucket is confirmed missing
    if not is_available():
        return False

    storage = _get_storage()
    if storage is None:
        return False

    try:
        payload = json.dumps(data, separators=(",", ":")).encode("utf-8")
        storage.from_(BUCKET_NAME).upload(
            path,
            payload,
            file_options={"content-type": "application/json", "upsert": "true"},
        )
        return True
    except Exception as exc:
        err = str(exc).lower()
        # If bucket disappeared mid-run, flip the circuit-breaker
        if "bucket not found" in err or "404" in err:
            logger.warning(
                "Bucket %s not found — disabling cold storage for this run", BUCKET_NAME
            )
            _bucket_status = False
            return False
        logger.warning("upload_json(%s) failed: %s", path, exc)
        return False


def download_json(path: str) -> dict | list | None:
    """Download and parse a JSON file from Supabase Storage.

    Returns parsed JSON on success, None on miss/error.
    """
    if _bucket_status is False:
        return None

    storage = _get_storage()
    if storage is None:
        return None

    try:
        data = storage.from_(BUCKET_NAME).download(path)
        if data is None:
            return None
        return json.loads(data)
    except Exception as exc:
        err = str(exc).lower()
        if "not found" in err or "404" in err:
            return None
        logger.warning("download_json(%s) failed: %s", path, exc)
        return None


def list_files(prefix: str) -> list[str]:
    """List file names under a prefix in the archive bucket.

    Args:
        prefix: e.g. ``"13f/1067983/quarterly"``

    Returns list of file names (not full paths), e.g. ``["Q1_2024.json", "Q2_2024.json"]``.
    Returns empty list on failure.
    """
    if _bucket_status is False:
        return []

    storage = _get_storage()
    if storage is None:
        return []

    try:
        results = storage.from_(BUCKET_NAME).list(prefix)
        if not results:
            return []
        # results is a list of dicts with "name" key
        return [f["name"] for f in results if isinstance(f, dict) and f.get("name")]
    except Exception as exc:
        logger.warning("list_files(%s) failed: %s", prefix, exc)
        return []


def delete_files(paths: list[str]) -> bool:
    """Delete files from the archive bucket.

    Args:
        paths: Full paths within the bucket, e.g.
               ``["13f/1067983/quarterly/Q1_2023.json"]``

    Returns True on success, False on failure.
    """
    storage = _get_storage()
    if storage is None:
        return False

    if not paths:
        return True

    try:
        storage.from_(BUCKET_NAME).remove(paths)
        return True
    except Exception as exc:
        logger.warning("delete_files failed: %s", exc)
        return False
