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
        items = data.get("items", [])
        if not items:
            return None

        best = items[0]
        return {
            "company_number": best.get("company_number"),
            "company_name": best.get("title"),
            "company_status": best.get("company_status"),
            "address_snippet": best.get("address_snippet"),
            "date_of_creation": best.get("date_of_creation"),
        }
