"""
Look up CRD numbers for investment adviser firms using the SEC IAPD search API.

For each CIK/fund name pair, queries:
  https://api.adviserinfo.sec.gov/search/firm?query={fund_name}&hl=true&nrows=5&start=0&r=25&sort=score+desc&wt=json

The response JSON has hits.hits[]._source.firm_source_id (CRD number)
and hits.hits[]._source.firm_name.

Outputs a Python dict mapping CIK -> CRD number.
"""

import json
import time
import urllib.request
import urllib.parse
import ssl

try:
    import certifi
    SSL_CTX = ssl.create_default_context(cafile=certifi.where())
except ImportError:
    SSL_CTX = ssl.create_default_context()

# CIK -> fund name mapping
FUNDS = {
    1376879: "AKO Capital",
    1112520: "Akre Capital Management",
    1063296: "Atlantic Investment Mgmt",
    1631014: "AltaRock Partners",
    936753: "Ariel Investments",
    1061768: "Baupost Group",
    1067983: "Berkshire Hathaway",
    1166559: "Gates Foundation Trust",
    1336528: "Pershing Square",
    1135778: "Miller Value Partners",
    813917: "Harris Associates",
    1553733: "Brave Warrior Advisors",
    1350694: "Bridgewater Associates",
    1056831: "Fairholme Capital",
    1657335: "Oakcliff Capital",
    1279936: "Cantillon Capital Mgmt",
    1697591: "CAS Investment Partners",
    1165797: "Causeway Capital Management",
    1389403: "Chou Associates",
    1423053: "Citadel Advisors",
    1135730: "Coatue Management",
    1773994: "Conifer Management",
    1549575: "Dalal Street LLC",
    1036325: "Davis Selected Advisers",
    200217: "Dodge & Cox",
    1671657: "Dorsey Asset Management",
    1536411: "Duquesne Family Office",
    1798849: "Durable Capital Partners",
    1581811: "Egerton Capital",
    1559771: "Engaged Capital",
    915191: "Fairfax Financial Holdings",
    1325447: "First Eagle Investment Mgmt",
    1377581: "First Pacific Advisors",
    1327055: "Bragg Financial Advisors",
    1569205: "Fundsmith",
    860643: "Gardner Russo & Quinn",
    1641864: "Giverny Capital",
    1079114: "Greenlight Capital",
    846222: "Greenhaven Associates",
    1766504: "Greenlea Lane Capital",
    1404599: "Aquamarine Capital",
    1759760: "H&H International Investment",
    1314620: "Hillman Capital Management",
    1709323: "Himalaya Capital",
    921669: "Icahn Capital",
    1106129: "Jensen Investment Management",
    1039565: "Kahn Brothers Group",
    1484150: "Lindsell Train",
    1061165: "Lone Pine Capital",
    1070134: "Mairs & Power",
    1540866: "Makaira Partners",
    1096343: "Markel Group",
    1016287: "Matrix Asset Advisors",
    934639: "Maverick Capital",
    949509: "Oaktree Capital",
    947996: "Olstein Capital Management",
    898202: "Omega Advisors",
    1854794: "Patient Capital Management",
    1034524: "Polen Capital Management",
    1631664: "Punch Card Management",
    1027796: "Pzena Investment Management",
    1037389: "Renaissance Technologies",
    1766596: "RV Capital",
    1649339: "Scion Asset Mgmt",
    1115373: "Semper Augustus",
    1766908: "ShawSpring Partners",
    820124: "Sound Shore Management",
    807985: "Southeastern Asset Mgmt",
    1647251: "TCI Fund Management",
    1099281: "Third Avenue Management",
    1040273: "Third Point",
    1167483: "Tiger Global",
    98758: "Torray Investment Partners",
    1345471: "Trian Fund Management",
    1454502: "Triple Frond Partners",
    732905: "Tweedy Browne Co.",
    1697868: "Valley Forge Capital Mgmt",
    1418814: "ValueAct Holdings",
    1103804: "Viking Global",
    859804: "Wedgewood Partners",
    883965: "Weitz Investment Management",
    905567: "Yacktman Asset Management",
    1656456: "Appaloosa LP",
    1358706: "Abrams Capital Management",
}

# Some firms are publicly traded (file 10-K/10-Q), not investment advisers
PUBLICLY_TRADED = {
    1067983: "Berkshire Hathaway",
    1096343: "Markel Group",
    915191: "Fairfax Financial Holdings",
}

# Manual CRD overrides for firms where automated matching picks the wrong result,
# or the firm is not findable via search. Verified by hand on adviserinfo.sec.gov.
MANUAL_CRD = {
    # Harris Associates -> "Harris | Oakmark" (CRD 106960), not "Harris & Associates"
    813917: 106960,
    # Pershing Square Capital Management (active, CRD 132982), not Pershing Square GP (inactive)
    1336528: 132982,
    # Miller Value Partners (active, CRD 110632), not the inactive entity (269945)
    1135778: 110632,
    # Oaktree Capital Management LP (active, CRD 106793), not Oaktree Capital Corp (inactive 114075)
    949509: 106793,
    # Polen Capital Management (CRD 106093), not Polen Capital Credit (108468)
    1034524: 106093,
    # Dalal Street LLC (CIK 1549575) is Mohnish Pabrai's entity - maps to Pabrai Investment Funds
    1549575: 161471,
    # ValueAct Capital Management (CRD 154249) - 13F filed under ValueAct Holdings
    1418814: 154249,
    # Kahn Brothers Advisors LLC (CRD 144368) - the IA entity
    1039565: 144368,
    # Punch Card Management (CRD 162939)
    1631664: 162939,
    # RV Capital (CRD 173008) - the active entity
    1766596: 173008,
    # Greenlight Capital Inc (CRD 157083) - David Einhorn's firm
    1079114: 157083,
    # Oakcliff Capital -> Oakcliff Partners LLC (CRD 162325) is correct mapping
    1657335: 162325,
    # Egerton Capital (UK) LLP (CRD 156384) is the main adviser entity
    1581811: 156384,
    # Cantillon Capital Management LLC (CRD 137895) is the US entity
    1279936: 137895,
    # Omega Advisors Inc (CRD 106867) - Leon Cooperman's firm (now inactive/wound down)
    898202: 106867,
    # Appaloosa Management LP (CRD 132306) - David Tepper's firm, not Appaloosa Wealth Mgmt
    1656456: 132306,
}

# Firms with no IAPD registration (not registered investment advisers, or foreign/exempt)
NO_CRD_EXPECTED = {
    1166559: "Gates Foundation Trust (not an IA firm, it's a trust/13F filer only)",
    1389403: "Chou Associates (Canadian firm, not SEC-registered as IA)",
    1536411: "Duquesne Family Office (family office, exempt from IA registration)",
    921669: "Icahn Capital (files 13F but Carl Icahn entities are broker-dealer/holding co)",
    1454502: "Triple Frond Partners (exempt reporting adviser or not registered)",
}

# Some names need tweaking for better search results
SEARCH_OVERRIDES = {
    1063296: "Atlantic Investment Management",
    1279936: "Cantillon Capital Management",
    1325447: "First Eagle Investment Management",
    807985: "Southeastern Asset Management",
    1649339: "Scion Asset Management",
    1697868: "Valley Forge Capital Management",
    732905: "Tweedy Browne",
    1569205: "Fundsmith LLP",
    1656456: "Appaloosa Management",
    1404599: "Aquamarine Capital Management",
    1766504: "Greenlea Lane Capital Management",
}

SEARCH_URL = "https://api.adviserinfo.sec.gov/search/firm"


def search_iapd(fund_name: str) -> list[dict]:
    """Search the IAPD firm search API. Returns list of hit source dicts."""
    params = urllib.parse.urlencode({
        "query": fund_name,
        "hl": "true",
        "nrows": 5,
        "start": 0,
        "r": 25,
        "sort": "score+desc",
        "wt": "json",
    })
    url = f"{SEARCH_URL}?{params}"

    req = urllib.request.Request(url)
    req.add_header(
        "User-Agent",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36",
    )
    req.add_header("Accept", "application/json")
    req.add_header("Referer", "https://adviserinfo.sec.gov/")

    try:
        with urllib.request.urlopen(req, context=SSL_CTX, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            hits = data.get("hits", {}).get("hits", [])
            return [h.get("_source", {}) for h in hits]
    except Exception as e:
        print(f"  ERROR querying IAPD for '{fund_name}': {e}")
        return []


def pick_best_match(fund_name: str, results: list[dict]) -> dict | None:
    """Pick the best matching result from the IAPD search results.
    Prefers ACTIVE firms over INACTIVE ones when scores are close."""
    if not results:
        return None

    fund_lower = fund_name.lower().replace("&", "and")

    # Build search tokens from the fund name (ignore very short words)
    tokens = [t for t in fund_lower.split() if len(t) > 2]
    # Core tokens: the distinctive words
    ignore_words = {
        "llc", "lp", "inc", "ltd", "mgmt", "management", "capital",
        "partners", "advisors", "advisers", "associates", "group",
        "fund", "investment", "investments", "holdings", "corp",
        "co.", "the", "and", "asset", "family", "office", "trust",
    }
    core_tokens = [t for t in tokens if t not in ignore_words]

    best = None
    best_score = -1

    for r in results:
        name = (r.get("firm_name") or "").lower()
        other_names = r.get("firm_other_names", []) or []
        combined = name + " " + " ".join(n.lower() for n in other_names)

        # Score: how many of our core tokens appear in the result name
        score = sum(1 for t in core_tokens if t in combined)
        # Bonus for matching common tokens too
        score += 0.3 * sum(1 for t in tokens if t in combined)
        # Bonus for ACTIVE status
        if r.get("firm_ia_scope") == "ACTIVE":
            score += 0.2

        if score > best_score:
            best_score = score
            best = r

    # Only return if we matched enough core tokens
    min_score = max(0.8, len(core_tokens) * 0.4)
    if best_score >= min_score:
        return best
    return None


def main():
    results_map = {}  # CIK -> CRD
    not_found = []
    found_details = []

    total = len(FUNDS)
    for i, (cik, fund_name) in enumerate(FUNDS.items(), 1):
        print(f"[{i}/{total}] Searching for: {fund_name} (CIK {cik})...")

        # Skip publicly traded companies
        if cik in PUBLICLY_TRADED:
            print(f"  -> Publicly traded company (files 10-K/10-Q), skipping IAPD lookup")
            not_found.append((cik, fund_name, "publicly traded"))
            time.sleep(0.2)
            continue

        # Use manual CRD if we have one (verified by hand)
        if cik in MANUAL_CRD:
            crd = MANUAL_CRD[cik]
            results_map[cik] = crd
            found_details.append((cik, fund_name, "(manual override)", crd))
            print(f"  -> MANUAL CRD: {crd}")
            time.sleep(0.2)
            continue

        # Skip firms known to have no CRD
        if cik in NO_CRD_EXPECTED:
            reason = NO_CRD_EXPECTED[cik]
            print(f"  -> No CRD expected: {reason}")
            not_found.append((cik, fund_name, reason))
            time.sleep(0.2)
            continue

        # Use search override if available
        search_name = SEARCH_OVERRIDES.get(cik, fund_name)
        if search_name != fund_name:
            print(f"  (searching as: {search_name})")

        results = search_iapd(search_name)
        if results:
            # Show what we got
            for r in results[:3]:
                org_name = r.get("firm_name", "?")
                crd = r.get("firm_source_id", "?")
                sec_num = r.get("firm_ia_full_sec_number", "?")
                scope = r.get("firm_ia_scope", "?")
                print(f"    Result: {org_name} (CRD: {crd}, SEC#: {sec_num}, {scope})")

            match = pick_best_match(search_name, results)
            if match:
                crd_num = match.get("firm_source_id")
                org_name = match.get("firm_name", "?")
                if crd_num:
                    results_map[cik] = int(crd_num)
                    found_details.append((cik, fund_name, org_name, crd_num))
                    print(f"  -> MATCHED: {org_name} -> CRD {crd_num}")
                else:
                    not_found.append((cik, fund_name, "no CRD in result"))
                    print(f"  -> No CRD number in match")
            else:
                not_found.append((cik, fund_name, "no good match"))
                print(f"  -> No good match found in results")
        else:
            not_found.append((cik, fund_name, "no results"))
            print(f"  -> No results from IAPD")

        time.sleep(0.5)

    # Print results
    print("\n" + "=" * 70)
    print("RESULTS: CIK -> CRD mapping")
    print("=" * 70)
    print()
    print("CIK_TO_CRD = {")
    for cik, crd in sorted(results_map.items()):
        fund_name = FUNDS[cik]
        print(f"    {cik}: {crd},  # {fund_name}")
    print("}")

    print(f"\nFound CRDs for {len(results_map)} out of {total} firms.")

    if not_found:
        print(f"\nNot found / not applicable ({len(not_found)}):")
        for cik, name, reason in not_found:
            print(f"  CIK {cik}: {name} ({reason})")

    print("\n" + "=" * 70)
    print("PUBLICLY TRADED COMPANIES (file 10-K/10-Q, not investment advisers):")
    print("=" * 70)
    for cik, name in PUBLICLY_TRADED.items():
        print(f"  CIK {cik}: {name}")

    print("\n" + "=" * 70)
    print("FIRMS WITH NO CRD (exempt, foreign, or non-IA):")
    print("=" * 70)
    for cik, reason in NO_CRD_EXPECTED.items():
        print(f"  CIK {cik}: {reason}")


if __name__ == "__main__":
    main()
