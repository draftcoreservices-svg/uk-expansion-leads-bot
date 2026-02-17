import re
from typing import List
from .config import CFG
from .leads import Lead


def _contains_any(text: str, phrases: List[str]) -> List[str]:
    t = text.lower()
    hits = []
    for p in phrases:
        if p.lower() in t:
            hits.append(p)
    return hits


def _count_roles_heuristic(text: str) -> int:
    """
    crude count based on repeated patterns
    """
    t = text.lower()
    # common ATS tokens
    tokens = ["apply", "job", "role", "position", "engineering", "software", "manager"]
    score = sum(t.count(tok) for tok in tokens)
    # map to rough roles count buckets
    if score > 80:
        return 15
    if score > 50:
        return 10
    if score > 25:
        return 5
    if score > 10:
        return 2
    return 0


def score_lead(lead: Lead) -> Lead:
    text = (lead.page_text or "") + " " + (lead.snippet or "")
    tlow = text.lower()

    # Hard excludes by obvious org type in title/company
    name_upper = (lead.company_or_person or lead.title).upper()
    for kw in CFG.name_exclude_keywords:
        if kw in name_upper:
            lead.score = 0
            lead.reasons.append(f"Excluded keyword match: {kw}")
            return lead

    if lead.lead_type == "sponsor_licence":
        score_sponsor(lead, tlow)
    elif lead.lead_type == "global_mobility":
        score_mobility(lead, tlow)
    elif lead.lead_type == "global_talent":
        score_talent(lead, tlow)

    # Sponsor register bump (if known)
    if lead.lead_type == "sponsor_licence" and lead.sponsor_register is False:
        lead.score += 15
        lead.reasons.append("Not on sponsor register (known)")

    # Clamp
    lead.score = max(0, min(100, lead.score))
    return lead


def score_sponsor(lead: Lead, tlow: str) -> None:
    s = 0
    hits = _contains_any(tlow, CFG.sponsor_phrases)
    if hits:
        s += 40
        lead.reasons.append(f"Sponsorship language found: {', '.join(hits[:3])}")

    uk_hits = _contains_any(tlow, CFG.uk_hiring_phrases)
    if uk_hits:
        s += 15
        lead.reasons.append("UK hiring/location indicators present")

    roles = _count_roles_heuristic(tlow)
    if roles >= 5:
        s += 20
        lead.reasons.append(f"Multiple openings likely (heuristic ≈ {roles})")
    elif roles >= 2:
        s += 10
        lead.reasons.append(f"Some openings likely (heuristic ≈ {roles})")

    # sector-ish keywords
    sector_keys = ["engineer", "developer", "data", "ai", "scientist", "architect", "cloud", "security", "devops"]
    sector_hits = sum(1 for k in sector_keys if k in tlow)
    if sector_hits >= 3:
        s += 10
        lead.reasons.append("Skilled-role keywords suggest sponsorship suitability")

    lead.score = s


def score_mobility(lead: Lead, tlow: str) -> None:
    s = 0
    exp_hits = _contains_any(tlow, CFG.expansion_phrases)
    if exp_hits:
        s += 40
        lead.reasons.append(f"Expansion language found: {', '.join(exp_hits[:2])}")

    if any(x in tlow for x in ["emea", "apac", "north america", "global offices", "subsidiary", "group"]):
        s += 15
        lead.reasons.append("Group/international structure language present")

    # overseas HQ hints (best-effort)
    if any(p in tlow for p in CFG.overseas_hq_phrases) and any(c in tlow for c in ["usa", "india", "uae", "germany", "france", "singapore", "australia", "canada"]):
        s += 20
        lead.reasons.append("Overseas HQ suggested by text")

    # Hiring UK people while expansion is mentioned
    if any(x in tlow for x in ["we are hiring", "open roles", "careers", "jobs"]) and any(x in tlow for x in ["london", "united kingdom", "uk"]):
        s += 15
        lead.reasons.append("Hiring signal + UK location found")

    lead.score = s


def score_talent(lead: Lead, tlow: str) -> None:
    s = 0
    gt_hits = _contains_any(tlow, CFG.global_talent_phrases)
    if gt_hits:
        s += 45
        lead.reasons.append(f"Global Talent/Promise language found: {', '.join(gt_hits[:3])}")

    # profile strength hints
    strength_keys = ["award", "speaker", "keynote", "publication", "journal", "patent", "open source", "github", "conference"]
    strength_hits = sum(1 for k in strength_keys if k in tlow)
    if strength_hits >= 3:
        s += 20
        lead.reasons.append("Public-profile strength indicators present")

    # UK move intent
    if any(x in tlow for x in ["moving to the uk", "relocating to the uk", "based in london", "work in the uk"]):
        s += 15
        lead.reasons.append("UK move/relocation intent detected")

    lead.score = s
