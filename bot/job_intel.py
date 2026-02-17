import time
import re
from typing import Dict, List, Tuple, Any

from .config import Config
from .enrichment import serp_search, url_domain


def _text_blob(item: Any) -> str:
    """
    Build a lowercase text blob from either:
      - a SerpAPI organic result dict, OR
      - a plain string URL
    """
    if isinstance(item, dict):
        return " ".join([
            str(item.get("title") or ""),
            str(item.get("snippet") or ""),
            str(item.get("link") or item.get("url") or ""),
        ]).lower()

    # Fallback: string URL or other primitive
    return str(item or "").lower()


def _extract_link(item: Any) -> str:
    """Extract a URL from either dict result or string."""
    if isinstance(item, dict):
        return str(item.get("link") or item.get("url") or "").strip()
    return str(item or "").strip()


def score_hiring_intent(
    session,
    cfg: Config,
    serp_key: str,
    company_name: str,
    website: str = "",
    serp_sleep: float = 1.2,
    serp_budget: Dict = None,
) -> Tuple[int, List[str], List[str]]:
    """Lightweight hiring/intent detection using SerpAPI snippets.

    We avoid scraping job boards. We only use Google result titles/snippets/URLs.

    Returns: (score 0..40-ish, reasons, source_urls)
    """
    if not serp_key:
        return 0, [], []

    if serp_budget is None:
        serp_budget = {"calls": 0, "cap": 0}

    # Respect budget if a cap is set (>0)
    if serp_budget.get("cap", 0) and serp_budget["calls"] >= serp_budget["cap"]:
        return 0, [], []

    dom = url_domain(website) if website else ""
    q_base = f'"{company_name}"'

    queries = [
        f"{q_base} hiring jobs careers",
        f"{q_base} visa sponsorship",
    ]
    if dom:
        queries.insert(0, f"site:{dom} (careers OR jobs OR vacancies OR hiring)")

    pos_hits = 0
    neg_hits = 0
    role_hits = 0
    urls: List[str] = []

    for q in queries[:3]:
        if serp_budget.get("cap", 0) and serp_budget["calls"] >= serp_budget["cap"]:
            break

        # NOTE: serp_search wrapper will increment budget internally via _serpapi_search;
        # but for safety we keep this in sync with the legacy pattern too.
        serp_budget["calls"] = serp_budget.get("calls", 0) + 1

        results = serp_search(
            session=session,
            query=q,
            serp_key=serp_key,
            num=cfg.serp_jobs_results_per_query,
            serp_budget=serp_budget,
            serp_sleep=serp_sleep,
        )

        # Throttle between queries
        if serp_sleep and serp_sleep > 0:
            time.sleep(float(serp_sleep))

        for item in results:
            link = _extract_link(item)
            if link and link.startswith("http"):
                urls.append(link)

            blob = _text_blob(item)

            if any(k in blob for k in cfg.job_negative_keywords):
                neg_hits += 1
            if any(k in blob for k in cfg.job_positive_keywords):
                pos_hits += 1
            if any(k in blob for k in cfg.sponsor_role_keywords):
                role_hits += 1

    # Scoring: prioritise evidence of hiring + sponsorship language.
    score = 0
    reasons: List[str] = []

    if role_hits:
        score += min(12, 4 * role_hits)
        reasons.append(f"Hiring signal: sponsor-heavy roles mentioned ({role_hits})")

    if pos_hits:
        score += min(22, 8 * pos_hits)
        reasons.append(f"Visa/sponsorship language appears in public job/search snippets ({pos_hits})")

    if neg_hits:
        score -= min(18, 9 * neg_hits)
        reasons.append(f"Negative signal: 'no sponsorship/right to work required' appears ({neg_hits})")

    # If we found *any* jobs/careers pages (even without keywords), give small intent bump.
    if urls:
        likely_jobs = [
            u for u in urls
            if re.search(r"(careers|jobs|vacan|recruit|indeed|workday|greenhouse|lever|smartrecruiters)", u, flags=re.I)
        ]
        if likely_jobs:
            score += 4
            reasons.append("Careers/jobs pages discovered")
            urls = likely_jobs + [u for u in urls if u not in likely_jobs]

    score = max(0, min(40, int(score)))
    return score, reasons[:6], urls[:6]
