from __future__ import annotations

import csv
import io
import re
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Dict, List, Tuple

import requests

from .normalize import norm_company_name, norm_text

GOVUK_WORKERS_PUBLICATION = "https://www.gov.uk/government/publications/register-of-licensed-sponsors-workers"
_ASSET_CSV_RE = re.compile(
    r"https://assets\.publishing\.service\.gov\.uk/media/[^\"']+Worker_and_Temporary_Worker\.csv"
)


def _discover_latest_workers_csv_url() -> str:
    html = requests.get(GOVUK_WORKERS_PUBLICATION, timeout=30).text
    m = _ASSET_CSV_RE.search(html)
    if m:
        return m.group(0)
    any_csv = re.findall(r"https://assets\.publishing\.service\.gov\.uk/media/[^\"']+\.csv", html)
    for u in any_csv:
        if "Worker" in u and "Temporary" in u:
            return u
    raise RuntimeError(
        "Could not discover Sponsor Register CSV URL from GOV.UK page. Set SPONSOR_REGISTER_URL env var."
    )


def _seq_ratio(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()


@dataclass
class SponsorRegister:
    names_to_towns: Dict[str, List[str]]

    @classmethod
    def load(cls, direct_url: str | None = None) -> "SponsorRegister":
        url = direct_url or _discover_latest_workers_csv_url()
        resp = requests.get(url, timeout=60)
        if resp.status_code != 200:
            raise RuntimeError(f"Failed to download sponsor register CSV: {resp.status_code} {url}")

        buf = io.StringIO(resp.content.decode("utf-8", errors="replace"))
        reader = csv.DictReader(buf)

        names_to_towns: Dict[str, List[str]] = {}
        for row in reader:
            name = row.get("Organisation Name") or row.get("Sponsor Name") or row.get("Sponsor") or ""
            town = row.get("Town/City") or row.get("Town") or row.get("City") or ""
            nname = norm_company_name(name)
            if not nname:
                continue
            ntown = norm_text(town)
            names_to_towns.setdefault(nname, [])
            if ntown and ntown not in names_to_towns[nname]:
                names_to_towns[nname].append(ntown)

        return cls(names_to_towns=names_to_towns)

    def is_licensed(self, company_name: str, town: str | None) -> Tuple[bool, str]:
        """
        Return (is_licensed, reason).
        Bias: exclude only when confident (avoid false positives).
        """
        n = norm_company_name(company_name)
        t = norm_text(town or "")

        if not n:
            return False, "No company name"

        # Exact match: only confident if town matches (when available)
        if n in self.names_to_towns:
            towns = self.names_to_towns[n]
            if not towns:
                return True, "Exact name match (no town in register)"
            if t and t in towns:
                return True, "Exact name + town match"
            return False, "Exact name match but town mismatch/unknown (not confident)"

        # Fuzzy match: only exclude at high confidence
        best = 0.0
        best_name = ""
        best_towns: List[str] = []
        for reg_name, towns in self.names_to_towns.items():
            r = _seq_ratio(n, reg_name)
            if r > best:
                best = r
                best_name = reg_name
                best_towns = towns

        if best >= 0.92:
            return True, f"Fuzzy name match {best:.2f} to sponsor '{best_name}'"
        if t and best >= 0.88 and (not best_towns or t in best_towns):
            return True, f"Fuzzy name+town match {best:.2f} to sponsor '{best_name}'"

        return False, f"Not found (best fuzzy {best:.2f})"
