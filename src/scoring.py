from typing import List
from datetime import datetime
import re
from .config import CFG
from .leads import Lead


def _contains_any(text: str, phrases: List[str]) -> List[str]:
    t = (text or "").lower()
    hits = []
    for p in phrases:
        if p.lower() in t:
            hits.append(p)
    return hits


def _count_roles_heuristic(text: str) -> int:
    """
    Rough heuristic based on common ATS/job-page tokens.
    """
    t = (text or "").lower()
    tokens = ["apply", "job", "role", "position", "responsibilities", "requirements", "location"]
    count = sum(t.count(tok) for tok in tokens)

    if count > 120:
        return 20
    if count > 80:
        return 15
    if count > 50:
        return 10
    if count > 25:
        return 5
    if count > 12:
        return 2
    return 0


def hard_excluded(name_or_title: str) -> str | None:
    upper = (name_or_title or "").upper()
    for kw in CFG.name_exclude_keywords:
        if kw in upper:
            return kw
    return None


def score_heuristic(lead: Lead) -> Lead:
    """
    Produces a baseline score + reasons. OpenAI can later refine/override.
    """
    text = f"{lead.title} {lead.snippet} {lead.page_text}"
    tlow = text.lower()

    bad_kw = hard_excluded(lead.company_or_person or lead.title)
    if bad_kw:
        lead.score = 0
        lead.reasons = [f"Excluded keyword match: {bad_kw}"]
        return lead

    # ------------------------------------------------
    # SPONSOR LICENCE LEADS
    # ------------------------------------------------
    if lead.lead_type == "sponsor_licence":
        s = 0

        hits = _contains_any(tlow, CFG.sponsor_phrases)
        if hits:
            s += 40
            lead.reasons.append(f"Sponsorship language found: {', '.join(hits[:3])}")

        uk_hits = _contains_any(tlow, CFG.uk_location_phrases)
        if uk_hits:
            s += 15
            lead.reasons.append("UK location/hiring indicators present")

        roles = _count_roles_heuristic(tlow)
        if roles >= 5:
            s += 20
            lead.reasons.append(f"Multiple openings likely (heuristic ≈ {roles})")
        elif roles >= 2:
            s += 10
            lead.reasons.append(f"Some openings likely (heuristic ≈ {roles})")

        skilled_keys = ["engineer", "developer", "data", "ai", "scientist", "architect", "cloud", "security", "devops"]
        skilled_hits = sum(1 for k in skilled_keys if k in tlow)
        if skilled_hits >= 3:
            s += 10
            lead.reasons.append("Skilled-role keywords present")

        if lead.on_sponsor_register is False:
            s += 15
            lead.reasons.append("Not on sponsor register")
        elif lead.on_sponsor_register is True:
            s -= 40
            lead.reasons.append("Already on sponsor register (likely not a sponsor-setup lead)")

        lead.score = s

    # ------------------------------------------------
    # GLOBAL MOBILITY LEADS
    # ------------------------------------------------
    elif lead.lead_type == "global_mobility":
        s = 0

        exp_hits = _contains_any(tlow, CFG.expansion_phrases)
        if exp_hits:
            s += 40
            lead.reasons.append(f"Expansion language found: {', '.join(exp_hits[:2])}")

        intl_keys = ["emea", "apac", "north america", "global offices", "subsidiary", "group", "international"]
        if any(k in tlow for k in intl_keys):
            s += 15
            lead.reasons.append("International/group language present")

        # Defensive: only use overseas_hq_phrases if defined in Config
        overseas_phrases = getattr(CFG, "overseas_hq_phrases", [])
        hq_countries = ["usa", "india", "uae", "germany", "france", "singapore", "australia", "canada", "netherlands"]

        if overseas_phrases and any(p in tlow for p in overseas_phrases) and any(c in tlow for c in hq_countries):
            s += 20
            lead.reasons.append("Overseas HQ suggested by text")

        if any(x in tlow for x in ["we are hiring", "open roles", "careers", "jobs"]) and any(
            x in tlow for x in ["london", "united kingdom", "uk"]
        ):
            s += 15
            lead.reasons.append("Hiring signal + UK location present")

        # Recency safeguard: heavily penalise old announcements.
        # (We only need coarse detection: if the page loudly shows a year that is far in the past.)
        years = [int(y) for y in re.findall(r"\b(19\d{2}|20\d{2})\b", tlow)]
        if years:
            newest = max(years)
            current_year = datetime.utcnow().year
            if newest <= current_year - 5:
                s -= 60
                lead.reasons.append(f"Appears outdated (year mentioned: {newest})")
            elif newest <= current_year - 3:
                s -= 30
                lead.reasons.append(f"Possibly outdated (year mentioned: {newest})")

        lead.score = s

    # ------------------------------------------------
    # GLOBAL TALENT LEADS
    # ------------------------------------------------
    else:  # global_talent
        s = 0

        gt_hits = _contains_any(tlow, CFG.global_talent_phrases)
        if gt_hits:
            s += 45
            lead.reasons.append(f"Global Talent/Promise language found: {', '.join(gt_hits[:3])}")

        strength_keys = ["award", "speaker", "keynote", "publication", "journal", "patent", "open source", "github", "conference"]
        strength_hits = sum(1 for k in strength_keys if k in tlow)
        if strength_hits >= 3:
            s += 20
            lead.reasons.append("Public-profile strength indicators present")

        if any(x in tlow for x in ["moving to the uk", "relocating to the uk", "based in london", "work in the uk"]):
            s += 15
            lead.reasons.append("UK move/relocation intent detected")

        lead.score = s

    lead.score = max(0, min(100, lead.score))
    return lead
