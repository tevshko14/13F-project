"""Cold storage layer using Supabase Storage (S3-compatible).

Provides archive/retrieve for historical 13F quarterly data.
Hot data (current + previous quarter) stays in Supabase Postgres;
older quarters are archived here as JSON files.

Bucket structure:
    paperpanda-archive/
        13f/{cik}/quarterly/{period}.json
        e.g. 13f/1067983/quarterly/Q3_2024.json

All functions are fault-tolerant: return None/False on failure, never raise.
"""

from __future__ import annotations

import json
import logging

logger = logging.getLogger(__name__)

BUCKET_NAME = "paperpanda-archive"


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


def ensure_bucket() -> bool:
    """Create the archive bucket if it doesn't exist.

    Called once during auto-migration. Idempotent and non-fatal.
    Returns True if bucket exists/was created, False on failure.
    """
    storage = _get_storage()
    if storage is None:
        return False

    try:
        storage.create_bucket(BUCKET_NAME, options={"public": False})
        logger.info("Created Supabase Storage bucket: %s", BUCKET_NAME)
        return True
    except Exception as exc:
        err = str(exc).lower()
        if "already exists" in err or "duplicate" in err or "409" in err:
            logger.debug("Bucket %s already exists", BUCKET_NAME)
            return True
        logger.warning("Failed to create bucket %s: %s", BUCKET_NAME, exc)
        return False


def upload_json(path: str, data: dict | list) -> bool:
    """Upload a JSON-serializable object to Supabase Storage.

    Args:
        path: Object path within the bucket,
              e.g. ``"13f/1067983/quarterly/Q3_2024.json"``
        data: dict or list to serialize as JSON

    Uses upsert semantics (overwrite if exists) for idempotent re-runs.
    Returns True on success, False on failure.
    """
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
        logger.warning("upload_json(%s) failed: %s", path, exc)
        return False


def download_json(path: str) -> dict | list | None:
    """Download and parse a JSON file from Supabase Storage.

    Returns parsed JSON on success, None on miss/error.
    """
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
