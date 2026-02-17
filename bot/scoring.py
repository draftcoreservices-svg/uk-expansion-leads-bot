from typing import Dict, List, Tuple
from .utils import norm_upper, is_uk_country
from .config import Config

PRIORITY_COUNTRIES = {
    "US", "USA", "UNITED STATES", "CANADA", "UAE", "UNITED ARAB EMIRATES", "INDIA", "AUSTRALIA",
    "GERMANY", "FRANCE", "NETHERLANDS", "SPAIN", "ITALY", "IRELAND", "SWEDEN", "DENMARK", "NORWAY",
    "FINLAND", "BELGIUM", "SWITZERLAND", "AUSTRIA", "POLAND", "CZECHIA", "PORTUGAL", "GREECE",
    "ROMANIA", "BULGARIA", "HUNGARY",
}


def bucket_from_score(cfg: Config, score: int) -> str:
    if score >= cfg.score_hot:
        return "HOT"
    if score >= cfg.score_medium:
        return "MEDIUM"
    return "WATCH"


def score_mailbox_penalty(cfg: Config, reg_address: str) -> Tuple[int, List[str]]:
    addr_u = (reg_address or "").upper()
    penalties = 0
    reasons = []
    for phrase in cfg.mailbox_penalty_phrases:
        if phrase in addr_u:
            penalties += 10
            reasons.append(f"Mailbox/serviced office indicator: {phrase.title()}")
    return penalties, reasons


def score_sic(cfg: Config, sic_list: List[str]) -> Tuple[int, List[str]]:
    score = 0
    reasons = []
    joined = " ".join([str(s).lower() for s in (sic_list or [])])
    for k in cfg.sic_boost_keywords:
        if k.lower() in joined:
            score += 6
            reasons.append(f"SIC/sector boost: '{k}'")
            break
    for k in cfg.sic_penalty_keywords:
        if k.lower() in joined:
            score -= 8
            reasons.append(f"SIC/sector penalty: '{k}'")
            break
    return score, reasons


def overseas_signal_from_officers(officers: List[Dict]) -> Tuple[int, List[str], List[str]]:
    non_uk = 0
    countries = []
    for o in officers or []:
        addr = o.get("address") or {}
        c = addr.get("country") or ""
        if c:
            countries.append(c.title())
        if c and not is_uk_country(c):
            non_uk += 1

    score = 0
    reasons = []
    if non_uk >= 1:
        score += 20
        reasons.append(f"{non_uk} officer(s) show non-UK address country")
    if non_uk >= 2:
        score += 8
        reasons.append("Multiple non-UK officers (stronger overseas signal)")
    if any(norm_upper(c) in PRIORITY_COUNTRIES for c in countries):
        score += 5
        reasons.append("Priority country detected in officer addresses")
    return score, reasons, sorted(set(countries))


def overseas_signal_from_registered_office(country: str) -> Tuple[int, List[str], List[str]]:
    if country and not is_uk_country(country):
        return 18, [f"Registered office country is non-UK ({country})"], [country.title()]
    return 0, [], []


def psc_signal(psc_items: List[Dict]) -> Tuple[int, List[str]]:
    score = 0
    reasons = []
    if not psc_items:
        return 0, []
    corporate = 0
    non_uk = 0
    for p in psc_items:
        kind = (p.get("kind") or "").lower()
        if "corporate" in kind:
            corporate += 1
        addr = (p.get("address") or {})
        c = addr.get("country") or ""
        if c and not is_uk_country(c):
            non_uk += 1
    if corporate:
        score += 15
        reasons.append(f"Corporate PSC present ({corporate})")
    if non_uk:
        score += 15
        reasons.append(f"Non-UK PSC address country present ({non_uk})")
    return score, reasons


def structure_signal(officers: List[Dict]) -> Tuple[int, List[str]]:
    n = len(officers or [])
    if n >= 3:
        return 6, ["3+ officers (more structured organisation)"]
    if n == 2:
        return 3, ["2 officers (some structure)"]
    return 0, []


def base_company_filters(profile: Dict) -> Tuple[bool, List[str]]:
    status = (profile.get("company_status") or "").lower()
    ctype = (profile.get("type") or "").lower()
    if status and status != "active":
        return False, [f"Dropped: company_status={status}"]
    allowed = {"ltd", "plc", "private-limited-guarant-nsc", "private-limited-shares-section-30-exemption"}
    if ctype and ctype not in allowed:
        return False, [f"Dropped: company_type={ctype}"]
    return True, []


def compute_score(cfg: Config, *, source: str, sponsor_route: str, profile: Dict, officers: List[Dict], psc_items: List[Dict]) -> Tuple[int, List[str], List[str]]:
    score = 0
    reasons: List[str] = []
    countries: List[str] = []

    if source == "SPONSOR_REGISTER":
        if "UK Expansion Worker" in (sponsor_route or ""):
            score += 35
            reasons.append("Sponsor route includes UK Expansion Worker")
        elif "Senior or Specialist Worker" in (sponsor_route or ""):
            score += 28
            reasons.append("Sponsor route includes GBM Senior/Specialist Worker")
        elif "Skilled Worker" in (sponsor_route or ""):
            score += 18
            reasons.append("Sponsor route includes Skilled Worker")

    ro_country = ((profile.get("registered_office_address") or {}).get("country") or "")
    s_ro, r_ro, c_ro = overseas_signal_from_registered_office(ro_country)
    score += s_ro
    reasons += r_ro
    countries += c_ro

    s_off, r_off, c_off = overseas_signal_from_officers(officers)
    score += s_off
    reasons += r_off
    countries += c_off

    s_psc, r_psc = psc_signal(psc_items)
    score += s_psc
    reasons += r_psc

    sic = profile.get("sic_codes") or []
    s_sic, r_sic = score_sic(cfg, sic)
    score += s_sic
    reasons += r_sic

    s_struct, r_struct = structure_signal(officers)
    score += s_struct
    reasons += r_struct

    ro = profile.get("registered_office_address") or {}
    reg_addr = " ".join([
        str(ro.get(k, ""))
        for k in ["address_line_1", "address_line_2", "locality", "region", "postal_code", "country"]
        if ro.get(k)
    ])
    p_mail, r_mail = score_mailbox_penalty(cfg, reg_addr)
    score -= p_mail
    reasons += r_mail

    score = max(0, min(100, score))
    return score, reasons, sorted(set(countries))


def classify_case_type(*, source: str, sponsor_route: str, score: int, countries: List[str], psc_items: List[Dict]) -> str:
    route = sponsor_route or ""
    if source == "SPONSOR_REGISTER":
        if "UK Expansion Worker" in route:
            return "D — UK Expansion Worker (new sponsor listing)"
        if "Senior or Specialist Worker" in route:
            return "C — GBM Senior/Specialist (new sponsor listing)"
        if "Skilled Worker" in route:
            return "A — New Sponsor (Skilled Worker / compliance / CoS usage)"
        return "A — New Sponsor (worker routes / compliance)"

    corporate_psc = any("corporate" in (p.get("kind", "").lower()) for p in (psc_items or []))
    non_uk_psc = any((p.get("address") or {}).get("country") and not is_uk_country((p.get("address") or {}).get("country", "")) for p in (psc_items or []))
    if corporate_psc or non_uk_psc:
        return "C — Likely GBM (overseas group/PSC signal)"
    if score >= 80 and countries:
        return "D — Likely UK Expansion Worker (strong overseas signal)"
    if score >= 55:
        return "B — Likely Sponsor Licence Applicant"
    return "E — Watchlist (low signal)"
