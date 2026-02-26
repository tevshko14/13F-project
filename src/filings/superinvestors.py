from dataclasses import dataclass, field


@dataclass
class SuperinvestorInfo:
    cik: str
    display_name: str
    fund_name: str
    crd_number: str | None = None  # SEC IAPD CRD# for Form ADV RAUM lookups
    is_public_company: bool = False  # If True, XBRL cash data available from 10-K/10-Q


# All CIKs verified against SEC EDGAR with active 13F-HR filings
# Source list: https://www.dataroma.com/m/managers.php
# CRD numbers sourced from SEC IAPD (api.adviserinfo.sec.gov) — Feb 2026
SUPERINVESTORS: list[SuperinvestorInfo] = [
    # A
    SuperinvestorInfo("1376879", "AKO Capital", "AKO Capital", crd_number="160379"),
    SuperinvestorInfo("1112520", "Chuck Akre", "Akre Capital Management", crd_number="109242"),
    SuperinvestorInfo("1063296", "Alex Roepers", "Atlantic Investment Mgmt", crd_number="137698"),
    SuperinvestorInfo("1631014", "AltaRock Partners", "AltaRock Partners", crd_number="161005"),
    SuperinvestorInfo("936753", "John Rogers", "Ariel Investments", crd_number="108211"),
    # B
    SuperinvestorInfo("1061768", "Seth Klarman", "Baupost Group", crd_number="109530"),
    SuperinvestorInfo("1067983", "Warren Buffett", "Berkshire Hathaway", is_public_company=True),
    SuperinvestorInfo(
        "1166559", "Bill & Melinda Gates Foundation", "Gates Foundation Trust"
    ),  # Trust, not an IA — no CRD
    SuperinvestorInfo("1336528", "Bill Ackman", "Pershing Square", crd_number="132982"),
    SuperinvestorInfo("1135778", "Bill Miller", "Miller Value Partners", crd_number="110632"),
    SuperinvestorInfo("813917", "Bill Nygren", "Harris Associates", crd_number="106960"),
    SuperinvestorInfo("1553733", "Glenn Greenberg", "Brave Warrior Advisors", crd_number="108894"),
    SuperinvestorInfo("1350694", "Ray Dalio", "Bridgewater Associates", crd_number="105129"),
    SuperinvestorInfo("1056831", "Bruce Berkowitz", "Fairholme Capital", crd_number="107987"),
    SuperinvestorInfo("1657335", "Bryan Lawrence", "Oakcliff Capital", crd_number="162325"),
    # C
    SuperinvestorInfo("1279936", "William Von Mueffling", "Cantillon Capital Mgmt", crd_number="137895"),
    SuperinvestorInfo("1697591", "Clifford Sosin", "CAS Investment Partners", crd_number="170071"),
    SuperinvestorInfo("1165797", "Sarah Ketterer", "Causeway Capital Management", crd_number="113308"),
    SuperinvestorInfo("1389403", "Francis Chou", "Chou Associates"),  # Canadian, not SEC-registered IA
    SuperinvestorInfo("1423053", "Ken Griffin", "Citadel Advisors", crd_number="148826"),
    SuperinvestorInfo("1135730", "Philippe Laffont", "Coatue Management", crd_number="157910"),
    SuperinvestorInfo("1773994", "Greg Alexander", "Conifer Management", crd_number="283492"),
    # D
    SuperinvestorInfo("1549575", "Mohnish Pabrai", "Dalal Street LLC", crd_number="161471"),
    SuperinvestorInfo("1036325", "Christopher Davis", "Davis Selected Advisers", crd_number="108674"),
    SuperinvestorInfo("200217", "Dodge & Cox", "Dodge & Cox", crd_number="104596"),
    SuperinvestorInfo("1671657", "Pat Dorsey", "Dorsey Asset Management", crd_number="169056"),
    SuperinvestorInfo("1536411", "Stanley Druckenmiller", "Duquesne Family Office"),  # Family office, exempt
    SuperinvestorInfo("1798849", "Henry Ellenbogen", "Durable Capital Partners", crd_number="305221"),
    # E
    SuperinvestorInfo("1581811", "John Armitage", "Egerton Capital", crd_number="156384"),
    SuperinvestorInfo("1559771", "Glenn Welling", "Engaged Capital", crd_number="161251"),
    # F
    SuperinvestorInfo("915191", "Prem Watsa", "Fairfax Financial Holdings", is_public_company=True),
    SuperinvestorInfo(
        "1325447", "First Eagle Investment Mgmt", "First Eagle Investment Mgmt", crd_number="108260"
    ),
    SuperinvestorInfo("1377581", "Steven Romick", "First Pacific Advisors", crd_number="141823"),
    SuperinvestorInfo("1327055", "FPA Queens Road", "Bragg Financial Advisors", crd_number="108780"),
    SuperinvestorInfo("1569205", "Terry Smith", "Fundsmith", crd_number="160365"),
    # G
    SuperinvestorInfo("860643", "Thomas Russo", "Gardner Russo & Quinn", crd_number="106114"),
    SuperinvestorInfo("1641864", "Francois Rochon", "Giverny Capital", crd_number="130640"),
    SuperinvestorInfo("1079114", "David Einhorn", "Greenlight Capital"),  # CRD 157083 terminated 3/2024
    SuperinvestorInfo("846222", "Greenhaven Associates", "Greenhaven Associates", crd_number="104729"),
    SuperinvestorInfo("1766504", "Josh Tarasoff", "Greenlea Lane Capital", crd_number="162012"),
    SuperinvestorInfo("1404599", "Guy Spier", "Aquamarine Capital"),  # CRD 170931 terminated 12/2022
    # H
    SuperinvestorInfo("1759760", "Duan Yongping", "H&H International Investment", crd_number="292451"),
    SuperinvestorInfo("1314620", "Hillman Capital", "Hillman Capital Management", crd_number="110096"),
    SuperinvestorInfo("1709323", "Li Lu", "Himalaya Capital", crd_number="157594"),
    # I
    SuperinvestorInfo("921669", "Carl Icahn", "Icahn Capital"),  # Holding company, not registered IA
    # J
    SuperinvestorInfo(
        "1106129", "Jensen Investment Mgmt", "Jensen Investment Management", crd_number="105281"
    ),
    # K
    SuperinvestorInfo("1039565", "Kahn Brothers", "Kahn Brothers Group", crd_number="144368"),
    # L
    SuperinvestorInfo("1484150", "Lindsell Train", "Lindsell Train", crd_number="158323"),
    SuperinvestorInfo("1061165", "Steve Mandel", "Lone Pine Capital", crd_number="156602"),
    # M
    SuperinvestorInfo("1070134", "Mairs & Power", "Mairs & Power", crd_number="110351"),
    SuperinvestorInfo("1540866", "Tom Bancroft", "Makaira Partners", crd_number="153729"),
    SuperinvestorInfo("1096343", "Tom Gayner", "Markel Group", is_public_company=True),
    SuperinvestorInfo("1016287", "David Katz", "Matrix Asset Advisors", crd_number="107408"),
    SuperinvestorInfo("934639", "Lee Ainslie", "Maverick Capital", crd_number="108262"),
    # N–O
    SuperinvestorInfo("949509", "Howard Marks", "Oaktree Capital", crd_number="106793"),
    SuperinvestorInfo("947996", "Robert Olstein", "Olstein Capital Management", crd_number="38474"),
    SuperinvestorInfo("898202", "Leon Cooperman", "Omega Advisors"),  # CRD 106867 terminated 1/2019
    # P
    SuperinvestorInfo("1854794", "Samantha McLemore", "Patient Capital Management", crd_number="307336"),
    SuperinvestorInfo("1034524", "Polen Capital", "Polen Capital Management", crd_number="106093"),
    SuperinvestorInfo("1631664", "Norbert Lou", "Punch Card Management", crd_number="162939"),
    SuperinvestorInfo("1027796", "Richard Pzena", "Pzena Investment Management", crd_number="106847"),
    # R
    SuperinvestorInfo("1037389", "Jim Simons", "Renaissance Technologies", crd_number="106661"),
    SuperinvestorInfo("1766596", "Robert Vinall", "RV Capital", crd_number="173008"),
    # S
    SuperinvestorInfo("1649339", "Michael Burry", "Scion Asset Mgmt"),  # CRD 167772 terminated 11/2025
    SuperinvestorInfo("1115373", "Christopher Bloomstran", "Semper Augustus", crd_number="108153"),
    SuperinvestorInfo("1766908", "Dennis Hong", "ShawSpring Partners", crd_number="172766"),
    SuperinvestorInfo("820124", "Harry Burn", "Sound Shore Management", crd_number="112379"),
    SuperinvestorInfo("807985", "Mason Hawkins", "Southeastern Asset Mgmt", crd_number="105276"),
    # T
    SuperinvestorInfo("1647251", "Chris Hohn", "TCI Fund Management", crd_number="269954"),
    SuperinvestorInfo("1099281", "Third Avenue Management", "Third Avenue Management", crd_number="107545"),
    SuperinvestorInfo("1040273", "Dan Loeb", "Third Point", crd_number="137927"),
    SuperinvestorInfo("1167483", "Chase Coleman", "Tiger Global", crd_number="160318"),
    SuperinvestorInfo("98758", "Torray LLC", "Torray Investment Partners", crd_number="105818"),
    SuperinvestorInfo("1345471", "Nelson Peltz", "Trian Fund Management", crd_number="154172"),
    SuperinvestorInfo("1454502", "Triple Frond Partners", "Triple Frond Partners"),  # ERA, no CRD
    SuperinvestorInfo("732905", "Tweedy Browne", "Tweedy Browne Co.", crd_number="6857"),
    # V
    SuperinvestorInfo("1697868", "Valley Forge Capital", "Valley Forge Capital Mgmt", crd_number="162953"),
    SuperinvestorInfo("1418814", "ValueAct Capital", "ValueAct Holdings", crd_number="154249"),
    SuperinvestorInfo("1103804", "Andreas Halvorsen", "Viking Global", crd_number="132272"),
    # W
    SuperinvestorInfo("859804", "David Rolfe", "Wedgewood Partners", crd_number="21923"),
    SuperinvestorInfo("883965", "Wallace Weitz", "Weitz Investment Management", crd_number="105088"),
    # Y
    SuperinvestorInfo("905567", "Yacktman Asset Mgmt", "Yacktman Asset Management", crd_number="164420"),
    # Additional (not on Dataroma but notable)
    SuperinvestorInfo("1656456", "David Tepper", "Appaloosa LP", crd_number="281909"),  # was 132306 (old terminated entity)
    SuperinvestorInfo("1358706", "David Abrams", "Abrams Capital Management", crd_number="155729"),
]

SUPERINVESTORS_BY_CIK: dict[str, SuperinvestorInfo] = {s.cik: s for s in SUPERINVESTORS}
