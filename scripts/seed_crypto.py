"""Seed crypto_entities and crypto_wallets tables in Supabase.

Run once to populate initial tracking data:
    uv run python scripts/seed_crypto.py

Idempotent — re-running skips existing entities (ON CONFLICT DO NOTHING).
"""

import os
import sys

# Load .env file if present
from pathlib import Path
env_path = Path(__file__).resolve().parent.parent / ".env"
if env_path.exists():
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, _, val = line.partition("=")
            os.environ.setdefault(key.strip(), val.strip())

from supabase import create_client

url = os.environ.get("SUPABASE_URL", "")
key = os.environ.get("SUPABASE_SERVICE_KEY", "")
if not url or not key:
    print("ERROR: SUPABASE_URL and SUPABASE_SERVICE_KEY must be set")
    sys.exit(1)

sb = create_client(url, key)

# ── Entities to seed ──────────────────────────────────────────────────────────

ENTITIES = [
    {
        "name": "Vitalik Buterin",
        "slug": "vitalik-buterin",
        "entity_type": "individual",
        "description": "Co-founder of Ethereum. Publicly known wallet addresses tracked on-chain.",
        "website": "https://vitalik.eth.limo",
        "twitter": "VitalikButerin",
        "ticker": "",
        "logo_url": "",
    },
    {
        "name": "Ethereum Foundation",
        "slug": "ethereum-foundation",
        "entity_type": "fund",
        "description": "Non-profit supporting Ethereum protocol development and ecosystem growth.",
        "website": "https://ethereum.org",
        "twitter": "ethereumfndn",
        "ticker": "",
        "logo_url": "",
    },
    {
        "name": "MicroStrategy",
        "slug": "microstrategy",
        "entity_type": "fund",
        "description": "Largest corporate Bitcoin holder. Led by Michael Saylor, holds 717K+ BTC. Holdings are in institutional custody (Fidelity/Coinbase) — not trackable at single addresses.",
        "website": "https://www.strategy.com",
        "twitter": "Strategy",
        "ticker": "MSTR",
        "logo_url": "",
    },
    {
        "name": "Galaxy Digital",
        "slug": "galaxy-digital",
        "entity_type": "fund",
        "description": "Crypto-focused financial services firm founded by Mike Novogratz.",
        "website": "https://www.galaxy.com",
        "twitter": "galaxyhq",
        "ticker": "GLXY",
        "logo_url": "",
    },
    {
        "name": "Justin Sun",
        "slug": "justin-sun",
        "entity_type": "individual",
        "description": "Founder of Tron blockchain. One of the largest on-chain whale wallets.",
        "website": "https://tron.network",
        "twitter": "justinsuntron",
        "ticker": "",
        "logo_url": "",
    },
]

# ── Wallets per entity (slug → list of wallets) ──────────────────────────────

WALLETS: dict[str, list[dict]] = {
    # ── Vitalik Buterin ──────────────────────────────────────────────────
    # Sources: Etherscan public labels, Arkham Intelligence
    # Total known: ~240K ETH (~$467M)
    "vitalik-buterin": [
        {
            "address": "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045",
            "chain": "ethereum",
            "label": "vitalik.eth (primary, 73K+ txns)",
        },
        {
            "address": "0x220866B1A2219f40e72f5c628B65D54268cA3a9D",
            "chain": "ethereum",
            "label": "Vb 3 (Gnosis Safe, largest ~$461M)",
        },
        {
            "address": "0xAb5801a7D398351b8bE11C439e05C5B3259aeC9B",
            "chain": "ethereum",
            "label": "Vb (original known wallet, ~$635K)",
        },
        {
            "address": "0x1db3439a222c519ab44bb1144fc28167b4fa6ee6",
            "chain": "ethereum",
            "label": "Vb 2 (~$155K, 1.7K txns)",
        },
    ],

    # ── Ethereum Foundation ──────────────────────────────────────────────
    # Sources: Etherscan labels, Hsiao-Wei Wang (EF core dev) tweets,
    #          Arkham Intelligence, GlobeNewsWire
    # Treasury migrated to Safe multisig in Oct 2025 (~$650M+)
    "ethereum-foundation": [
        {
            "address": "0xde0B295669a9FD93d5F28D9Ec85E40f4cb697BAe",
            "chain": "ethereum",
            "label": "EthDev main treasury (original multisig)",
        },
        {
            "address": "0x5eD8Cee6b63b1c6AFce3AD7c92f4fD7E1B8fAd9F",
            "chain": "ethereum",
            "label": "EF 1 (EOA)",
        },
        {
            "address": "0x9fC3dc011b461664c835F2527fffb1169b3C213e",
            "chain": "ethereum",
            "label": "EF DeFi Multisig (3-of-5 Safe, 50K+ ETH, Jan 2025)",
        },
    ],

    # ── MicroStrategy / Strategy ─────────────────────────────────────────
    # BTC held in institutional custody (Fidelity omnibus + Coinbase Prime).
    # No single on-chain address represents their 717K+ BTC.
    # Best tracked via SEC filings / 13F data.
    "microstrategy": [],

    # ── Galaxy Digital ───────────────────────────────────────────────────
    # Sources: Etherscan labels (hildobby compilation), Arkham Intelligence,
    #          on-chain transfer alerts (Lookonchain, Onchain Lens)
    # Note: Galaxy frequently creates new wallets for large transfers;
    #       these are the two persistently labeled addresses.
    "galaxy-digital": [
        {
            "address": "0x15abb66ba754f05cbc0165a64a11cded1543de48",
            "chain": "ethereum",
            "label": "Galaxy Digital main (~$84M, 4K txns)",
        },
        {
            "address": "0x33566c9d8be6cf0b23795e0d380e112be9d75836",
            "chain": "ethereum",
            "label": "Galaxy Digital OTC (~$7M, 25K txns)",
        },
    ],

    # ── Justin Sun ───────────────────────────────────────────────────────
    # Sources: Etherscan public labels (Sun 1-4), Arkham Intelligence
    # Arkham identifies 142+ wallets; these are the Etherscan-labeled ones.
    # Sun 2 & Sun 3 are drained/empty — omitted.
    # Sun 4 also labeled "Poloniex 9" (Sun owns Poloniex).
    # Holdings: ~$2.1B total, heavy stETH/wstETH + USDT positions
    "justin-sun": [
        {
            "address": "0x3DdfA8eC3052539b6C9549F12cEA2C295cfF5296",
            "chain": "ethereum",
            "label": "Justin Sun (primary labeled, ~$540K)",
        },
        {
            "address": "0x176F3DAb24a159341c0509bB36B833E7fDD0a132",
            "chain": "ethereum",
            "label": "Justin Sun 4 / Poloniex 9 (~$720M, stETH+USDT)",
        },
    ],
}

# ── Seed logic ────────────────────────────────────────────────────────────────


def seed():
    print("Seeding crypto entities...")

    for entity in ENTITIES:
        slug = entity["slug"]
        try:
            resp = sb.table("crypto_entities").upsert(
                entity, on_conflict="slug"
            ).execute()
            print(f"  ✓ {entity['name']} ({slug})")
        except Exception as e:
            print(f"  ✗ {entity['name']}: {e}")
            continue

    print("\nSeeding crypto wallets...")

    for slug, wallets in WALLETS.items():
        # Look up entity_id by slug
        resp = sb.table("crypto_entities").select("id").eq("slug", slug).execute()
        rows = resp.data or []
        if not rows:
            print(f"  ✗ Entity '{slug}' not found — skipping wallets")
            continue
        entity_id = rows[0]["id"]

        for wallet in wallets:
            row = {
                "entity_id": entity_id,
                "address": wallet["address"],
                "chain": wallet["chain"],
                "label": wallet["label"],
                "is_active": True,
            }
            try:
                sb.table("crypto_wallets").upsert(
                    row, on_conflict="address,chain"
                ).execute()
                print(f"  ✓ {slug} → {wallet['chain']} {wallet['address'][:12]}… ({wallet['label']})")
            except Exception as e:
                print(f"  ✗ {slug} → {wallet['address'][:12]}…: {e}")

    print("\nDone! Visit http://localhost:8000/crypto to see the entities.")


if __name__ == "__main__":
    seed()
