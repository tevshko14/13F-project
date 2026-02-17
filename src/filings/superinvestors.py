from dataclasses import dataclass


@dataclass
class SuperinvestorInfo:
    cik: str
    display_name: str
    fund_name: str


# All CIKs verified against SEC EDGAR with active 13F-HR filings
# Source list: https://www.dataroma.com/m/managers.php
SUPERINVESTORS: list[SuperinvestorInfo] = [
    # A
    SuperinvestorInfo("1376879", "AKO Capital", "AKO Capital"),
    SuperinvestorInfo("1112520", "Chuck Akre", "Akre Capital Management"),
    SuperinvestorInfo("1063296", "Alex Roepers", "Atlantic Investment Mgmt"),
    SuperinvestorInfo("1631014", "AltaRock Partners", "AltaRock Partners"),
    SuperinvestorInfo("936753", "John Rogers", "Ariel Investments"),
    # B
    SuperinvestorInfo("1061768", "Seth Klarman", "Baupost Group"),
    SuperinvestorInfo("1067983", "Warren Buffett", "Berkshire Hathaway"),
    SuperinvestorInfo("1166559", "Bill & Melinda Gates Foundation", "Gates Foundation Trust"),
    SuperinvestorInfo("1336528", "Bill Ackman", "Pershing Square"),
    SuperinvestorInfo("1135778", "Bill Miller", "Miller Value Partners"),
    SuperinvestorInfo("813917", "Bill Nygren", "Harris Associates"),
    SuperinvestorInfo("1553733", "Glenn Greenberg", "Brave Warrior Advisors"),
    SuperinvestorInfo("1350694", "Ray Dalio", "Bridgewater Associates"),
    SuperinvestorInfo("1056831", "Bruce Berkowitz", "Fairholme Capital"),
    SuperinvestorInfo("1657335", "Bryan Lawrence", "Oakcliff Capital"),
    # C
    SuperinvestorInfo("1279936", "William Von Mueffling", "Cantillon Capital Mgmt"),
    SuperinvestorInfo("1697591", "Clifford Sosin", "CAS Investment Partners"),
    SuperinvestorInfo("1165797", "Sarah Ketterer", "Causeway Capital Management"),
    SuperinvestorInfo("1389403", "Francis Chou", "Chou Associates"),
    SuperinvestorInfo("1423053", "Ken Griffin", "Citadel Advisors"),
    SuperinvestorInfo("1135730", "Philippe Laffont", "Coatue Management"),
    SuperinvestorInfo("1773994", "Greg Alexander", "Conifer Management"),
    # D
    SuperinvestorInfo("1549575", "Mohnish Pabrai", "Dalal Street LLC"),
    SuperinvestorInfo("1036325", "Christopher Davis", "Davis Selected Advisers"),
    SuperinvestorInfo("200217", "Dodge & Cox", "Dodge & Cox"),
    SuperinvestorInfo("1671657", "Pat Dorsey", "Dorsey Asset Management"),
    SuperinvestorInfo("1536411", "Stanley Druckenmiller", "Duquesne Family Office"),
    SuperinvestorInfo("1798849", "Henry Ellenbogen", "Durable Capital Partners"),
    # E
    SuperinvestorInfo("1581811", "John Armitage", "Egerton Capital"),
    SuperinvestorInfo("1559771", "Glenn Welling", "Engaged Capital"),
    # F
    SuperinvestorInfo("915191", "Prem Watsa", "Fairfax Financial Holdings"),
    SuperinvestorInfo("1325447", "First Eagle Investment Mgmt", "First Eagle Investment Mgmt"),
    SuperinvestorInfo("1377581", "Steven Romick", "First Pacific Advisors"),
    SuperinvestorInfo("1327055", "FPA Queens Road", "Bragg Financial Advisors"),
    SuperinvestorInfo("1569205", "Terry Smith", "Fundsmith"),
    # G
    SuperinvestorInfo("860643", "Thomas Russo", "Gardner Russo & Quinn"),
    SuperinvestorInfo("1641864", "Francois Rochon", "Giverny Capital"),
    SuperinvestorInfo("1079114", "David Einhorn", "Greenlight Capital"),
    SuperinvestorInfo("846222", "Greenhaven Associates", "Greenhaven Associates"),
    SuperinvestorInfo("1766504", "Josh Tarasoff", "Greenlea Lane Capital"),
    SuperinvestorInfo("1404599", "Guy Spier", "Aquamarine Capital"),
    # H
    SuperinvestorInfo("1759760", "Duan Yongping", "H&H International Investment"),
    SuperinvestorInfo("1314620", "Hillman Capital", "Hillman Capital Management"),
    SuperinvestorInfo("1709323", "Li Lu", "Himalaya Capital"),
    # I
    SuperinvestorInfo("921669", "Carl Icahn", "Icahn Capital"),
    # J
    SuperinvestorInfo("1106129", "Jensen Investment Mgmt", "Jensen Investment Management"),
    # K
    SuperinvestorInfo("1039565", "Kahn Brothers", "Kahn Brothers Group"),
    # L
    SuperinvestorInfo("1484150", "Lindsell Train", "Lindsell Train"),
    SuperinvestorInfo("1061165", "Steve Mandel", "Lone Pine Capital"),
    # M
    SuperinvestorInfo("1070134", "Mairs & Power", "Mairs & Power"),
    SuperinvestorInfo("1540866", "Tom Bancroft", "Makaira Partners"),
    SuperinvestorInfo("1096343", "Tom Gayner", "Markel Group"),
    SuperinvestorInfo("1016287", "David Katz", "Matrix Asset Advisors"),
    SuperinvestorInfo("934639", "Lee Ainslie", "Maverick Capital"),
    # N–O
    SuperinvestorInfo("949509", "Howard Marks", "Oaktree Capital"),
    SuperinvestorInfo("947996", "Robert Olstein", "Olstein Capital Management"),
    SuperinvestorInfo("898202", "Leon Cooperman", "Omega Advisors"),
    # P
    SuperinvestorInfo("1854794", "Samantha McLemore", "Patient Capital Management"),
    SuperinvestorInfo("1034524", "Polen Capital", "Polen Capital Management"),
    SuperinvestorInfo("1631664", "Norbert Lou", "Punch Card Management"),
    SuperinvestorInfo("1027796", "Richard Pzena", "Pzena Investment Management"),
    # R
    SuperinvestorInfo("1037389", "Jim Simons", "Renaissance Technologies"),
    SuperinvestorInfo("1766596", "Robert Vinall", "RV Capital"),
    # S
    SuperinvestorInfo("1649339", "Michael Burry", "Scion Asset Mgmt"),
    SuperinvestorInfo("1115373", "Christopher Bloomstran", "Semper Augustus"),
    SuperinvestorInfo("1766908", "Dennis Hong", "ShawSpring Partners"),
    SuperinvestorInfo("820124", "Harry Burn", "Sound Shore Management"),
    SuperinvestorInfo("807985", "Mason Hawkins", "Southeastern Asset Mgmt"),
    # T
    SuperinvestorInfo("1647251", "Chris Hohn", "TCI Fund Management"),
    SuperinvestorInfo("1099281", "Third Avenue Management", "Third Avenue Management"),
    SuperinvestorInfo("1040273", "Dan Loeb", "Third Point"),
    SuperinvestorInfo("1167483", "Chase Coleman", "Tiger Global"),
    SuperinvestorInfo("98758", "Torray LLC", "Torray Investment Partners"),
    SuperinvestorInfo("1345471", "Nelson Peltz", "Trian Fund Management"),
    SuperinvestorInfo("1454502", "Triple Frond Partners", "Triple Frond Partners"),
    SuperinvestorInfo("732905", "Tweedy Browne", "Tweedy Browne Co."),
    # V
    SuperinvestorInfo("1697868", "Valley Forge Capital", "Valley Forge Capital Mgmt"),
    SuperinvestorInfo("1418814", "ValueAct Capital", "ValueAct Holdings"),
    SuperinvestorInfo("1103804", "Andreas Halvorsen", "Viking Global"),
    # W
    SuperinvestorInfo("859804", "David Rolfe", "Wedgewood Partners"),
    SuperinvestorInfo("883965", "Wallace Weitz", "Weitz Investment Management"),
    # Y
    SuperinvestorInfo("905567", "Yacktman Asset Mgmt", "Yacktman Asset Management"),
    # Additional (not on Dataroma but notable)
    SuperinvestorInfo("1656456", "David Tepper", "Appaloosa LP"),
    SuperinvestorInfo("1358706", "David Abrams", "Abrams Capital Management"),
]

SUPERINVESTORS_BY_CIK: dict[str, SuperinvestorInfo] = {
    s.cik: s for s in SUPERINVESTORS
}
