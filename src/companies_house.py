from __future__ import annotations

import base64
import time
from dataclasses import dataclass
from typing import Any, Dict

import requests

API_BASE = "https://api.company-information.service.gov.uk"


@dataclass
class CHClient:
    api_key: str
    timeout: int = 30
    sleep: float = 0.2

    def _auth_header(self) -> Dict[str, str]:
        token = base64.b64encode(f"{self.api_key}:".encode("utf-8")).decode("ascii")
        return {"Authorization": f"Basic {token}"}

    def get(self, path: str, params: Dict[str, Any] | None = None) -> Dict[str, Any]:
        url = f"{API_BASE}{path}"
        r = requests.get(url, headers=self._auth_header(), params=params, timeout=self.timeout)
        if r.status_code != 200:
            raise RuntimeError(f"Companies House API error {r.status_code} for {path}: {r.text[:300]}")
        time.sleep(self.sleep)
        return r.json()

    def advanced_search(
        self,
        *,
        incorporated_from: str,
        incorporated_to: str,
        sic_codes: str | None,
        company_status: str = "active",
        start_index: int = 0,
        size: int = 200,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {
            "incorporated_from": incorporated_from,
            "incorporated_to": incorporated_to,
            "company_status": company_status,
            "start_index": start_index,
            "size": size,
        }
        if sic_codes:
            params["sic_codes"] = sic_codes
        return self.get("/advanced-search/companies", params=params)

    def company_profile(self, company_number: str) -> Dict[str, Any]:
        return self.get(f"/company/{company_number}")

    def psc_list(self, company_number: str, items_per_page: int = 100, start_index: int = 0) -> Dict[str, Any]:
        return self.get(
            f"/company/{company_number}/persons-with-significant-control",
            params={"items_per_page": items_per_page, "start_index": start_index},
        )

    def officers_list(self, company_number: str, items_per_page: int = 100, start_index: int = 0) -> Dict[str, Any]:
        return self.get(
            f"/company/{company_number}/officers",
            params={"items_per_page": items_per_page, "start_index": start_index},
        )

    def officer_appointment(self, company_number: str, appointment_id: str) -> Dict[str, Any]:
        return self.get(f"/company/{company_number}/appointments/{appointment_id}")
