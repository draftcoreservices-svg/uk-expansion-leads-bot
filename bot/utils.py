import re
from typing import List
from rapidfuzz import fuzz

UK_COUNTRIES = {"UNITED KINGDOM","UK","ENGLAND","SCOTLAND","WALES","NORTHERN IRELAND"}

_ORG_SUFFIXES = [
    " LIMITED",
    " LTD",
    " LTD.",
    " PLC",
    " LLP",
    " L.L.P.",
]


def norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def norm_upper(s: str) -> str:
    return norm(s).upper()


def clean_display_name(name: str) -> str:
    n = norm(name)
    n = re.sub(r"^[\s\"\'\`\*\@\[\]\(\)\{\}\<\>\#\!\$\%\^\&\=\+\;\:\,\.\-\/\\]+", "", n)
    n = re.sub(r"\s+", " ", n).strip()
    return n


def non_alnum_ratio(s: str) -> float:
    if not s:
        return 1.0
    non = sum(1 for ch in s if not ch.isalnum() and ch != " ")
    return non / max(len(s), 1)


def token_similarity(a: str, b: str) -> int:
    return int(fuzz.token_set_ratio((a or "").upper(), (b or "").upper()))


def extract_emails(text: str) -> List[str]:
    if not text:
        return []
    emails = set(re.findall(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", text, flags=re.I))
    return sorted(set(emails))[:10]


def extract_phones(text: str) -> List[str]:
    if not text:
        return []
    cands = re.findall(r"(\+?\d[\d\-\s().]{8,}\d)", text)
    cleaned = []
    for c in cands:
        digits = re.sub(r"\D", "", c)
        if len(digits) < 9:
            continue
        cleaned.append(c.strip())
    out, seen = [], set()
    for c in cleaned:
        key = re.sub(r"\D", "", c)
        if key in seen:
            continue
        seen.add(key)
        out.append(c)
    return out[:10]


def is_uk_country(country: str) -> bool:
    return norm_upper(country) in UK_COUNTRIES


def org_key(name: str) -> str:
    """Normalise organisation names for loose matching (e.g., sponsor register exclusion)."""
    n = norm_upper(clean_display_name(name))
    for suf in _ORG_SUFFIXES:
        if n.endswith(suf):
            n = n[: -len(suf)].strip()
    n = re.sub(r"[^A-Z0-9 ]+", " ", n)
    n = re.sub(r"\s+", " ", n).strip()
    return n


def safe_join(parts):
    return ", ".join([p for p in parts if p]).strip(", ")
