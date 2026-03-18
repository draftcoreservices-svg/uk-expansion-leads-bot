from __future__ import annotations

from dataclasses import dataclass
from typing import List, Tuple

ALLOWLIST = {
    "58290","62012","62020","62030","62090","63110","63120",
    "21100","21200","72110","72190","46460",
    "25620","26110","26200","26309","26511","26600","27110","27900","28110","28290","28990",
    "46190","46510","46520","46900",
    "64110","64191","64999","66190",
    "86210","86220","86900",
}

DENYLIST = {
    "68100","68209","68320",
    "41100","41201","41202","43310","43320","43330","43390","43999",
    "56101","56103","56302",
    "96020","96090",
    "81210","81299",
    "47190","47290","47710","47799","47890",
    "82990",
}

@dataclass
class Signals:
    age_days: int
    corporate_psc: bool
    foreign_psc_hub: bool
    corporate_director: bool
    foreign_director_hub: bool
    directors_count: int
    psc_count: int
    uk_in_name_and_corp_psc: bool
    allowlist_hit: bool
    denylist_hits: int

def score(signals: Signals) -> Tuple[int, List[str]]:
    reasons: List[str] = []
    s = 0

    # Age weighting: 0–3 lower, 3–9 good, 9–12 good
    if signals.age_days < 90:
        s += 4
        reasons.append("Incorporated 0–3 months ago (early-stage)")
    elif signals.age_days < 270:
        s += 10
        reasons.append("Incorporated 3–9 months ago (prime window)")
    elif signals.age_days <= 365:
        s += 10
        reasons.append("Incorporated 9–12 months ago (prime window)")

    if signals.corporate_psc:
        s += 18
        reasons.append("Corporate PSC present (strong group/structure signal)")
    if signals.foreign_psc_hub:
        s += 14
        reasons.append("Foreign PSC from approved trading hub")
    if signals.corporate_director:
        s += 10
        reasons.append("Corporate director present")
    if signals.foreign_director_hub:
        s += 10
        reasons.append("Foreign director from approved trading hub")

    if signals.directors_count > 1:
        s += 6
        reasons.append(f"{signals.directors_count} active directors")
    if signals.psc_count > 1:
        s += 5
        reasons.append(f"{signals.psc_count} active PSCs")

    if signals.uk_in_name_and_corp_psc:
        s += 4
        reasons.append("Company name contains 'UK' + corporate PSC (bonus)")

    # SIC allowlist is NOT strict (per your rule) but affects score
    if signals.allowlist_hit:
        s += 3
        reasons.append("SIC allowlist hit (target sector)")
    else:
        s -= 2
        reasons.append("No SIC allowlist hit (small penalty)")

    if signals.denylist_hits:
        s -= min(6, 2 * signals.denylist_hits)
        reasons.append(f"SIC denylist present ({signals.denylist_hits} code(s)) (penalty)")

    return s, reasons
