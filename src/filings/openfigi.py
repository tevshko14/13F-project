"""OpenFIGI CUSIP → Ticker mapping utility.

Free tier: 25 requests/min, 100 IDs per request, no API key required.
With API key: 250 requests/min.
Source: https://www.openfigi.com/api

Used to resolve CUSIPs from SEC 13F filings to tickers/names.
"""

import json
import logging
import os
import time
import urllib.request

from filings.caching import TTLCache

logger = logging.getLogger(__name__)

_FIGI_URL = "https://api.openfigi.com/v3/mapping"
_cusip_cache = TTLCache(ttl=3600 * 24 * 7, max_size=500)  # 7 days — CUSIP mappings rarely change


def _get_api_key() -> str:
    return os.environ.get("OPENFIGI_API_KEY", "")


def resolve_cusips(cusips: list[str]) -> dict[str, dict]:
    """Resolve a batch of CUSIPs to ticker/name/exchange info.

    Args:
        cusips: List of 9-digit CUSIP strings

    Returns:
        Dict keyed by CUSIP → {ticker, name, exchange, figi, security_type}
    """
    if not cusips:
        return {}

    # Check L1 cache first
    uncached = []
    result = {}
    for cusip in cusips:
        cached = _cusip_cache.get(cusip)
        if cached is not None:
            result[cusip] = cached
        else:
            uncached.append(cusip)

    if not uncached:
        return result

    # Check L2 for remaining
    still_uncached = []
    try:
        from filings import supabase_cache
        for cusip in uncached:
            l2 = supabase_cache.get_cached(f"figi:{cusip}")
            if l2 and isinstance(l2, dict):
                result[cusip] = l2
                _cusip_cache.set(cusip, l2)
            else:
                still_uncached.append(cusip)
    except Exception:
        still_uncached = uncached

    if not still_uncached:
        return result

    # L3: Batch API call (max 100 per request)
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "PaperPanda/1.0",
    }
    api_key = _get_api_key()
    if api_key:
        headers["X-OPENFIGI-APIKEY"] = api_key

    # Process in batches of 100
    for batch_start in range(0, len(still_uncached), 100):
        batch = still_uncached[batch_start:batch_start + 100]
        payload = [{"idType": "ID_CUSIP", "idValue": cusip} for cusip in batch]

        try:
            req_data = json.dumps(payload).encode("utf-8")
            req = urllib.request.Request(_FIGI_URL, data=req_data, headers=headers)
            with urllib.request.urlopen(req, timeout=20) as resp:
                data = json.loads(resp.read())

            for i, item in enumerate(data):
                cusip = batch[i]
                if "data" in item and item["data"]:
                    figi_data = item["data"][0]
                    mapped = {
                        "ticker": figi_data.get("ticker", ""),
                        "name": figi_data.get("name", ""),
                        "exchange": figi_data.get("exchCode", ""),
                        "figi": figi_data.get("figi", ""),
                        "security_type": figi_data.get("securityType", ""),
                        "market_sector": figi_data.get("marketSector", ""),
                    }
                    result[cusip] = mapped
                    _cusip_cache.set(cusip, mapped)
                    # L2 cache
                    try:
                        from filings import supabase_cache
                        supabase_cache.set_cached(f"figi:{cusip}", "openfigi", mapped, 3600 * 168)  # 7 days
                    except Exception:
                        pass
                else:
                    # No match found
                    empty = {"ticker": "", "name": "", "exchange": "", "figi": "",
                             "security_type": "", "market_sector": ""}
                    result[cusip] = empty

        except Exception as exc:
            logger.warning("OpenFIGI batch resolve failed: %s", exc)
            # Rate limit — wait and retry
            if "429" in str(exc):
                time.sleep(2)

    return result


def resolve_cusip(cusip: str) -> dict | None:
    """Resolve a single CUSIP. Returns ticker info dict or None."""
    results = resolve_cusips([cusip])
    mapped = results.get(cusip)
    if mapped and mapped.get("ticker"):
        return mapped
    return None
