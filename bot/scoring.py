from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from .utils import norm_upper, is_uk_country, looks_like_subsidiary_name


@dataclass
class ScoreResult:
    score: int
    bucket: str
    why: str


def bucket_from_score(score: int) -> str:
    if score >= 70:
        return 'HOT'
    if score >= 45:
        return 'MEDIUM'
    return 'WATCH'


def recency_points(incorporated: str) -> int:
    if not incorporated:
        return 0
    try:
        d = datetime.fromisoformat(incorporated).date()
    except Exception:
        return 0
    days = (datetime.now(timezone.utc).date() - d).days
    if days <= 14:
        return 10
    if days <= 30:
        return 6
    if days <= 60:
        return 3
    return 0


def sponsor_points(route: str) -> int:
    r = route or ''
    if 'UK Expansion Worker' in r:
        return 25
    if 'Senior or Specialist Worker' in r:
        return 18
    if 'Skilled Worker' in r:
        return 12
    return 0


def sector_points(sic_codes_csv: str) -> int:
    # lightweight heuristic; you can replace with a proper SIC mapping later.
    sic = (sic_codes_csv or '').replace(' ', '')
    if not sic:
        return 0

    # hard deprioritise typical low-value BD targets (customise freely)
    deprioritise_prefix = {'87','88','49','56','55'}  # care/residential/social; land transport; food; accommodation
    if any(sic.startswith(p) for p in deprioritise_prefix):
        return -20

    # boost for tech / prof services / engineering-ish (coarse)
    boost_prefix = {'62','63','70','71','72','73','74','64','65','66','46','47','28','29','30','32'}
    if any(sic.startswith(p) for p in boost_prefix):
        return 10

    return 0


def website_points(level: str, score_0_10: int | None) -> int:
    lvl = (level or '').upper()
    if lvl == 'VERIFIED':
        return 10
    if lvl == 'PLAUSIBLE':
        return 4
    return 0


def compute_score(*,
                  incorporated: str,
                  sponsor_route: str,
                  sic_codes: str,
                  has_foreign_psc: bool,
                  foreign_officer_residence: int,
                  foreign_officer_nationality: int,
                  name: str,
                  website_level: str = '',
                  website_score: int | None = None) -> ScoreResult:

    score = 0
    why_parts: list[str] = []

    # Overseas expansion signals
    if has_foreign_psc:
        score += 25
        why_parts.append('Foreign corporate PSC detected')

    if foreign_officer_residence >= 1:
        score += 15
        why_parts.append('Officer country of residence non-UK')

    if foreign_officer_nationality >= 1:
        score += 10
        why_parts.append('Officer nationality non-UK')

    if looks_like_subsidiary_name(name):
        score += 5
        why_parts.append('Name suggests UK subsidiary')

    # Sponsor leverage
    sp = sponsor_points(sponsor_route)
    if sp:
        score += sp
        why_parts.append(f'Sponsor route: {sponsor_route}')

    # Recency
    rp = recency_points(incorporated)
    if rp:
        score += rp
        why_parts.append('Recently incorporated')

    # Sector
    sec = sector_points(sic_codes)
    if sec:
        score += sec
        why_parts.append('Sector weighting applied')

    # Web presence
    wp = website_points(website_level, website_score)
    if wp:
        score += wp
        why_parts.append(f'Website {website_level.lower()}')

    score = max(0, min(100, score))
    bucket = bucket_from_score(score)
    why = '; '.join(why_parts)[:240]
    return ScoreResult(score=score, bucket=bucket, why=why)


def visa_hint(source: str, sponsor_route: str, score: int) -> str:
    if source == 'SPONSOR_REGISTER':
        if 'UK Expansion Worker' in (sponsor_route or ''):
            return 'Likely UK Expansion Worker / sponsor compliance'
        if 'Senior or Specialist Worker' in (sponsor_route or ''):
            return 'GBM Senior/Specialist Worker route'
        if 'Skilled Worker' in (sponsor_route or ''):
            return 'Skilled Worker sponsor / compliance'
        return 'Sponsor compliance / worker routes'

    # Companies House
    if score >= 70:
        return 'Strong overseas-linked incorporation (Expansion Worker likely)'
    if score >= 45:
        return 'Possible overseas-linked incorporation (review)'
    return 'Watchlist'
