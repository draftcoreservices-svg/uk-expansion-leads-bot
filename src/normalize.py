from __future__ import annotations

import re

_NON_ALNUM = re.compile(r"[^a-z0-9]+", re.I)

LTD_TOKENS = {
    "ltd",
    "limited",
    "plc",
    "llp",
    "lp",
    "company",
    "co",
    "the",
    "and",
    "&",
    "uk",
    "(uk)",
    "u",
    "k",
}

UK_VALUES = {
    "british",
    "uk",
    "united kingdom",
    "england",
    "scotland",
    "wales",
}

COUNTRY_VARIANTS = {
    "united states": {"usa", "u.s.a", "united states of america", "us", "u.s"},
    "united arab emirates": {"uae", "u.a.e"},
    "south korea": {
        "republic of korea",
        "korea, republic of",
        "korea republic of",
        "korea (republic of)",
        "korea",
    },
    "hong kong": {"hong kong sar", "hong kong, china"},
}

APPROVED_HUBS_CANON = [
    "india",
    "united states",
    "china",
    "united arab emirates",
    "australia",
    "japan",
    "south korea",
    "canada",
    "singapore",
    "hong kong",
    "switzerland",
    "germany",
    "france",
    "netherlands",
    "ireland",
    "luxembourg",
    "saudi arabia",
    "qatar",
    "israel",
    "taiwan",
    "new zealand",
]


def norm_text(s: str | None) -> str:
    if not s:
        return ""
    s = s.strip().lower()
    s = _NON_ALNUM.sub(" ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def norm_company_name(name: str) -> str:
    t = norm_text(name)
    toks = [x for x in t.split() if x and x not in LTD_TOKENS]
    return " ".join(toks)


def canon_country(raw: str | None) -> str:
    t = norm_text(raw)
    if not t:
        return ""
    for canon, variants in COUNTRY_VARIANTS.items():
        if t == canon or t in variants:
            return canon
    return t


def is_uk_value(raw: str | None) -> bool:
    return norm_text(raw) in UK_VALUES


def approved_hub(raw: str | None) -> bool:
    c = canon_country(raw)
    if not c:
        return False
    if c in APPROVED_HUBS_CANON:
        return True
    for canon, _variants in COUNTRY_VARIANTS.items():
        if c == canon and canon in APPROVED_HUBS_CANON:
            return True
    return False
