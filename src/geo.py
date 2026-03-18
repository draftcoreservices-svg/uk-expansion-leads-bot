from __future__ import annotations

import re
from typing import Tuple

from .normalize import norm_text

UK_POSTCODE_RE = re.compile(r"^[A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2}$", re.I)

SCOTLAND_PREFIXES = ("AB","DD","DG","EH","FK","G","HS","IV","KA","KW","KY","ML","PA","PH","TD","ZE")
WALES_PREFIXES = ("CF","LD","LL","NP","SA")
NI_PREFIXES = ("BT",)

def infer_gb_nation(country: str | None, postal_code: str | None) -> Tuple[bool, str]:
    """
    Returns (allowed, inferred_country_label).
    Rules:
    - Allow: England/Wales/Scotland
    - Exclude: Northern Ireland, Ireland
    - If country ambiguous, infer from postcode; exclude BT.
    """
    c = norm_text(country)
    pc2 = (postal_code or "").strip().upper()

    if c in {"england","wales","scotland"}:
        return True, c.title()
    if c in {"northern ireland","ireland","republic of ireland"}:
        return False, c.title()

    if not postal_code:
        return False, "Unknown"

    prefix2 = pc2.split()[0][:2].upper() if pc2 else ""
    prefix1 = pc2.split()[0][:1].upper() if pc2 else ""

    if prefix2 in NI_PREFIXES:
        return False, "Northern Ireland"
    if prefix2 in WALES_PREFIXES:
        return True, "Wales"
    if prefix2 in SCOTLAND_PREFIXES or prefix1 in ("G",):
        return True, "Scotland"

    if UK_POSTCODE_RE.match(pc2):
        return True, "England"

    return False, "Unknown"
