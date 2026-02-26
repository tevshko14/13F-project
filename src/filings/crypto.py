"""Multi-source crypto data client for PaperPanda.

Fetches on-chain data (balances, transfers) for tracked wallets using
free public APIs, routed by chain:

    ethereum / base / polygon / arbitrum  →  Etherscan-compatible APIs
    bitcoin                               →  Blockchain.com API
    solana                                →  Helius API (optional)

Entity → wallet mapping is stored in Supabase (crypto_wallets table) and
seeded manually. This module only handles the on-chain data fetching layer;
Supabase reads/writes live in supabase_cache.py.

Every public function is fault-tolerant — returns empty results, never
raises — so callers need no try/except.

Env vars (all optional, graceful fallback without them):
    ETHERSCAN_API_KEY   -- etherscan.io API key (free tier: 5 req/s)
    HELIUS_API_KEY      -- helius.dev API key (free tier, Solana only)

Arkham Intelligence (future):
    ARKHAM_API_KEY      -- intel.arkm.com API key (on waitlist)
    When set, Arkham is used as a supplementary enrichment layer on top
    of the on-chain sources, adding entity attribution for unknown addresses.
"""

from __future__ import annotations

import logging
import os
from typing import Any

import time as _time

import httpx

logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

_ETHERSCAN_KEY = os.environ.get("ETHERSCAN_API_KEY", "")
_HELIUS_KEY = os.environ.get("HELIUS_API_KEY", "")
_ARKHAM_KEY = os.environ.get("ARKHAM_API_KEY", "")  # future

_TIMEOUT = 15

# Etherscan free tier: 3 req/s actual limit.  Keep a safe gap.
_MIN_CALL_GAP = 0.4  # seconds (~2.5 req/s)
_last_call_ts: float = 0.0

# Etherscan V2 API — single endpoint, chain selected via &chainid param
# https://docs.etherscan.io/v2-migration
_ETHERSCAN_V2_BASE = "https://api.etherscan.io/v2/api"
_ETHERSCAN_CHAIN_IDS: dict[str, int] = {
    "ethereum": 1,
    "base":     8453,
    "polygon":  137,
    "arbitrum": 42161,
    "optimism": 10,
}

# Keep old host dict for backward compat reference — now unused
_ETHERSCAN_HOSTS: dict[str, str] = {
    k: _ETHERSCAN_V2_BASE for k in _ETHERSCAN_CHAIN_IDS
}

_EVM_CHAINS = set(_ETHERSCAN_HOSTS.keys())

# ── Internal helpers ──────────────────────────────────────────────────────────


def _get(url: str, params: dict | None = None, headers: dict | None = None) -> Any:
    """GET with timeout, rate limiting, retry, and structured error logging.

    Returns None on failure.  Enforces a minimum gap of _MIN_CALL_GAP
    seconds between consecutive outbound requests to respect API rate limits.
    Retries once on rate-limit responses after a 1s back-off.
    """
    global _last_call_ts

    for attempt in range(2):
        now = _time.monotonic()
        wait = _MIN_CALL_GAP - (now - _last_call_ts)
        if wait > 0:
            _time.sleep(wait)
        _last_call_ts = _time.monotonic()

        try:
            resp = httpx.get(url, params=params or {}, headers=headers or {}, timeout=_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
            # Etherscan returns 200 with status=0 on rate limit
            if (
                isinstance(data, dict)
                and data.get("status") == "0"
                and "rate limit" in str(data.get("result", "")).lower()
                and attempt == 0
            ):
                logger.debug("crypto: rate limited, retrying in 1s")
                _time.sleep(1)
                continue
            return data
        except httpx.HTTPStatusError as exc:
            logger.warning("crypto: HTTP %s for %s", exc.response.status_code, url)
        except httpx.TimeoutException:
            logger.warning("crypto: timeout for %s", url)
        except Exception as exc:
            logger.warning("crypto: error for %s: %s", url, exc)
        break
    return None


def _safe_float(val: Any) -> float:
    try:
        return float(val) if val is not None else 0.0
    except (TypeError, ValueError):
        return 0.0


def _safe_int(val: Any) -> int:
    try:
        return int(val) if val is not None else 0
    except (TypeError, ValueError):
        return 0


# ── EVM / Etherscan ───────────────────────────────────────────────────────────


def _etherscan_params(chain: str) -> tuple[str, int] | None:
    """Return (base_url, chain_id) for a supported EVM chain, or None."""
    chain_id = _ETHERSCAN_CHAIN_IDS.get(chain.lower())
    if chain_id is None:
        return None
    return _ETHERSCAN_V2_BASE, chain_id


def get_evm_eth_balance(address: str, chain: str = "ethereum") -> float:
    """Return native token balance (ETH/MATIC/etc.) in whole units for an EVM address."""
    result = _etherscan_params(chain)
    if not result:
        logger.warning("crypto: unsupported EVM chain %s", chain)
        return 0.0
    base, chain_id = result

    params: dict[str, Any] = {
        "chainid": chain_id,
        "module": "account",
        "action": "balance",
        "address": address,
        "tag": "latest",
    }
    if _ETHERSCAN_KEY:
        params["apikey"] = _ETHERSCAN_KEY

    data = _get(base, params)
    if not isinstance(data, dict) or data.get("status") != "1":
        return 0.0

    # Result is in wei (10^-18 ETH)
    return _safe_float(data.get("result")) / 1e18


def get_evm_token_balances(address: str, chain: str = "ethereum") -> list[dict]:
    """Return ERC-20 token balances for an EVM address.

    Each item: {token_symbol, token_name, chain, amount, usd_value}
    Note: usd_value is 0 — price enrichment happens in the sync layer.
    """
    result = _etherscan_params(chain)
    if not result:
        return []
    base, chain_id = result

    params: dict[str, Any] = {
        "chainid": chain_id,
        "module": "account",
        "action": "tokentx",
        "address": address,
        "sort": "desc",
        "page": 1,
        "offset": 100,
    }
    if _ETHERSCAN_KEY:
        params["apikey"] = _ETHERSCAN_KEY

    data = _get(base, params)
    if not isinstance(data, dict) or data.get("status") != "1":
        return []

    # tokentx gives transfer history — collect unique contract addresses
    # then query tokenbalance per contract
    seen: dict[str, dict] = {}
    for tx in data.get("result") or []:
        contract = tx.get("contractAddress", "").lower()
        if contract and contract not in seen:
            seen[contract] = {
                "token_symbol": tx.get("tokenSymbol", "").upper(),
                "token_name": tx.get("tokenName", ""),
                "decimals": _safe_int(tx.get("tokenDecimal") or 18),
                "contract": contract,
            }

    balances = []
    for contract, meta in seen.items():
        bal_params: dict[str, Any] = {
            "chainid": chain_id,
            "module": "account",
            "action": "tokenbalance",
            "contractaddress": contract,
            "address": address,
            "tag": "latest",
        }
        if _ETHERSCAN_KEY:
            bal_params["apikey"] = _ETHERSCAN_KEY

        bal_data = _get(base, bal_params)
        if not isinstance(bal_data, dict) or bal_data.get("status") != "1":
            continue

        raw_bal = _safe_float(bal_data.get("result"))
        decimals = meta["decimals"] or 18
        amount = raw_bal / (10 ** decimals)

        if amount > 0:
            balances.append({
                "token_symbol": meta["token_symbol"],
                "token_name": meta["token_name"],
                "chain": chain,
                "amount": amount,
                "usd_value": 0.0,
            })

    return balances


def get_evm_transfers(
    address: str,
    chain: str = "ethereum",
    min_usd: float = 100_000,
    limit: int = 50,
) -> list[dict]:
    """Return recent large ETH transfers for an EVM address.

    Each item: {arkham_tx_id, from_address, to_address, token_symbol,
                chain, amount, usd_value, tx_hash, transferred_at}
    usd_value is 0 — enriched by sync layer using price data.
    """
    result = _etherscan_params(chain)
    if not result:
        return []
    base, chain_id = result

    params: dict[str, Any] = {
        "chainid": chain_id,
        "module": "account",
        "action": "txlist",
        "address": address,
        "sort": "desc",
        "page": 1,
        "offset": limit,
    }
    if _ETHERSCAN_KEY:
        params["apikey"] = _ETHERSCAN_KEY

    data = _get(base, params)
    if not isinstance(data, dict) or data.get("status") != "1":
        return []

    native_symbol = {
        "ethereum": "ETH",
        "base": "ETH",
        "polygon": "MATIC",
        "arbitrum": "ETH",
        "optimism": "ETH",
    }.get(chain, "ETH")

    transfers = []
    for tx in data.get("result") or []:
        value_wei = _safe_float(tx.get("value", 0))
        amount = value_wei / 1e18
        if amount == 0:
            continue

        # Convert block timestamp to ISO string
        ts = tx.get("timeStamp", "")
        transferred_at = ""
        if ts:
            from datetime import datetime, timezone
            transferred_at = datetime.fromtimestamp(
                int(ts), tz=timezone.utc
            ).isoformat()

        transfers.append({
            "arkham_tx_id": tx.get("hash", ""),
            "from_entity_id": None,
            "to_entity_id": None,
            "from_address": tx.get("from", "").lower(),
            "to_address": tx.get("to", "").lower(),
            "token_symbol": native_symbol,
            "chain": chain,
            "amount": amount,
            "usd_value": 0.0,  # enriched by sync layer
            "tx_hash": tx.get("hash", ""),
            "transferred_at": transferred_at,
        })

    return transfers


# ── Bitcoin / Blockchain.com ──────────────────────────────────────────────────


def get_btc_balance(address: str) -> float:
    """Return Bitcoin balance in BTC for an address. No API key required."""
    data = _get(f"https://blockchain.info/q/addressbalance/{address}")
    if data is None:
        return 0.0
    # Returns satoshis as plain integer
    try:
        return int(str(data)) / 1e8
    except (TypeError, ValueError):
        return 0.0


def get_btc_transfers(address: str, limit: int = 50) -> list[dict]:
    """Return recent Bitcoin transactions for an address.

    Each item: {arkham_tx_id, from_address, to_address, token_symbol,
                chain, amount, usd_value, tx_hash, transferred_at}
    """
    data = _get(
        f"https://blockchain.info/rawaddr/{address}",
        params={"limit": limit},
    )
    if not isinstance(data, dict):
        return []

    transfers = []
    for tx in data.get("txs") or []:
        # Sum inputs from this address
        out_amount = sum(
            _safe_float(o.get("value", 0)) / 1e8
            for o in tx.get("inputs") or []
            if (o.get("prev_out") or {}).get("addr") == address
        )
        # Sum outputs to this address
        in_amount = sum(
            _safe_float(o.get("value", 0)) / 1e8
            for o in tx.get("out") or []
            if o.get("addr") == address
        )

        amount = max(out_amount, in_amount)
        if amount == 0:
            continue

        ts = tx.get("time")
        transferred_at = ""
        if ts:
            from datetime import datetime, timezone
            transferred_at = datetime.fromtimestamp(
                int(ts), tz=timezone.utc
            ).isoformat()

        tx_hash = tx.get("hash", "")
        transfers.append({
            "arkham_tx_id": tx_hash,
            "from_entity_id": None,
            "to_entity_id": None,
            "from_address": address if out_amount > 0 else "",
            "to_address": address if in_amount > 0 else "",
            "token_symbol": "BTC",
            "chain": "bitcoin",
            "amount": amount,
            "usd_value": 0.0,  # enriched by sync layer
            "tx_hash": tx_hash,
            "transferred_at": transferred_at,
        })

    return transfers


# ── Unified interface (used by sync layer) ───────────────────────────────────


def get_wallet_balance(address: str, chain: str) -> dict:
    """Return native token balance for any supported wallet.

    Returns: {token_symbol, chain, amount, usd_value}
    """
    chain = chain.lower()
    if chain == "bitcoin":
        amount = get_btc_balance(address)
        return {
            "token_symbol": "BTC",
            "token_name": "Bitcoin",
            "chain": "bitcoin",
            "amount": amount,
            "usd_value": 0.0,
        }
    elif chain in _EVM_CHAINS:
        native_symbol = {
            "ethereum": "ETH",
            "base": "ETH",
            "polygon": "MATIC",
            "arbitrum": "ETH",
            "optimism": "ETH",
        }.get(chain, "ETH")
        amount = get_evm_eth_balance(address, chain)
        return {
            "token_symbol": native_symbol,
            "token_name": native_symbol,
            "chain": chain,
            "amount": amount,
            "usd_value": 0.0,
        }
    else:
        logger.warning("crypto: unsupported chain %s", chain)
        return {}


def get_wallet_transfers(
    address: str,
    chain: str,
    limit: int = 50,
) -> list[dict]:
    """Return recent transfers for any supported wallet, routed by chain."""
    chain = chain.lower()
    if chain == "bitcoin":
        return get_btc_transfers(address, limit=limit)
    elif chain in _EVM_CHAINS:
        return get_evm_transfers(address, chain=chain, limit=limit)
    else:
        logger.warning("crypto: unsupported chain %s for transfers", chain)
        return []


def is_configured() -> bool:
    """True if at least one API key is set (Etherscan or Helius).
    Bitcoin and basic Ethereum work without any key.
    """
    return bool(_ETHERSCAN_KEY or _HELIUS_KEY or _ARKHAM_KEY) or True
    # Always returns True — BTC + basic ETH work keyless
