from __future__ import annotations

import requests

from .utils import name_variants, token_similarity, norm_upper
from .sources import companies_house as ch


def best_ch_match_for_sponsor(session: requests.Session, sponsor_name: str, town: str, timeout: int = 20) -> tuple[str, int]:
    """Return (company_number, match_score 0..100)."""
    town_u = norm_upper(town)
    best_num = ''
    best_score = 0

    queries = name_variants(sponsor_name)
    # Add a town-assisted query as a last resort (tends to widen results)
    if town and queries:
        queries.append(f"{queries[0]} {town}")

    seen_numbers: set[str] = set()

    for q in queries[:4]:
        items = ch.search_companies(session, q, items_per_page=12, timeout=timeout)
        for it in items:
            title = it.get('title') or ''
            num = it.get('company_number') or ''
            if not title or not num or num in seen_numbers:
                continue
            seen_numbers.add(num)

            sim = token_similarity(sponsor_name, title)

            snippet = norm_upper(it.get('address_snippet') or '')
            if town_u and town_u in snippet:
                sim = min(100, sim + 8)

            status = (it.get('company_status') or '').lower()
            if status == 'active':
                sim = min(100, sim + 3)

            if sim > best_score:
                best_score = sim
                best_num = num

    return best_num, best_score
