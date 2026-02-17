import os
import re
import requests
from typing import Optional, Dict, Any, List, Tuple

CH_ENDPOINT = "https://api.company-information.service.gov.uk"


def _norm(s: str) -> str:
    s = (s or "").lower()
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return " ".join(s.split()).strip()


def _token_set(s: str) -> set:
    return set(_norm(s).split())


def _jaccard(a: set, b: set) -> float:
    if not a or not b:
        return 0.0
    inter = len(a & b)
    union = len(a | b)
    return inter / union if union else 0.0


class CompaniesHouseClient:
    """Thin Companies House search client with conservative matching.

    This avoids dangerous 'first result wins' behaviour which produced nonsense matches.
    """

    def __init__(self, api_key: Optional[str] = None, timeout_s: float = 30.0):
        self.api_key = api_key or os.environ.get("COMPANIES_HOUSE_API_KEY")
        if not self.api_key:
            raise RuntimeError("Missing COMPANIES_HOUSE_API_KEY")
        self.timeout_s = timeout_s

    def search_company(self, name: str) -> Optional[Dict[str, Any]]:
        name = (name or "").strip()
        if not name:
            return None

        url = f"{CH_ENDPOINT}/search/companies"
        r = requests.get(
            url,
            params={"q": name, "items_per_page": 10},
            auth=(self.api_key, ""),
            timeout=self.timeout_s,
        )
        r.raise_for_status()
        data = r.json()

        items = data.get("items", []) or []
        if not items:
            return None

        # Score candidates by token overlap.
        target = _token_set(name)
        scored: List[Tuple[float, Dict[str, Any]]] = []
        for it in items:
            title = it.get("title") or ""
            score = _jaccard(target, _token_set(title))
            scored.append((score, it))

        scored.sort(key=lambda x: x[0], reverse=True)
        best_score, best = scored[0]

        # Conservative threshold: if we aren't sure, return None instead of garbage.
        if best_score < 0.80:
            return None

        return {
            "company_name": best.get("title") or "",
            "company_number": best.get("company_number") or "",
            "company_status": best.get("company_status") or "",
            "company_type": best.get("company_type") or "",
            "address_snippet": (best.get("address_snippet") or "").strip(),
            "date_of_creation": best.get("date_of_creation") or "",
            "score": best_score,
        }
