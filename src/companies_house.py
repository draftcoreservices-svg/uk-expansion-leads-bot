import os
import requests
from typing import Optional, Dict, Any

CH_ENDPOINT = "https://api.company-information.service.gov.uk"


class CompaniesHouseClient:
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.environ.get("COMPANIES_HOUSE_API_KEY")
        if not self.api_key:
            raise RuntimeError("Missing COMPANIES_HOUSE_API_KEY")

    def search_company(self, name: str) -> Optional[Dict[str, Any]]:
        name = (name or "").strip()
        if not name:
            return None

        url = f"{CH_ENDPOINT}/search/companies"
        r = requests.get(
            url,
            params={"q": name, "items_per_page": 5},
            auth=(self.api_key, ""),
            timeout=30,
        )
        r.raise_for_status()
        data = r.json()
        items = data.get("items", []) or []
        if not items:
            return None

        def _norm_entity(s: str) -> str:
            s = "".join(ch.lower() if ch.isalnum() else " " for ch in (s or ""))
            s = " ".join(s.split())
            suffixes = {
                "limited", "ltd", "plc", "llp", "lp", "limited liability partnership",
                "uk", "holdings", "holding", "group", "international", "int",
                "services", "service", "company", "co"
            }
            toks = [t for t in s.split() if t not in suffixes]
            return " ".join(toks)

        target = _norm_entity(name)
        tgt_set = set(target.split())

        best = None
        best_score = 0.0
        for it in items:
            title = it.get("title") or ""
            cand = _norm_entity(title)
            cand_set = set(cand.split())
            if not cand_set or not tgt_set:
                continue
            inter = len(tgt_set & cand_set)
            union = len(tgt_set | cand_set)
            score = inter / union if union else 0.0
            if score > best_score:
                best_score = score
                best = it

        # If no decent match, don't guess.
        if best is None or best_score < 0.80:
            return None

        return {
            "company_number": best.get("company_number"),
            "company_name": best.get("title"),
            "company_status": best.get("company_status"),
            "address_snippet": best.get("address_snippet"),
            "date_of_creation": best.get("date_of_creation"),
            "match_confidence": round(best_score, 2),
        }
