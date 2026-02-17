import re
from typing import List, Tuple

EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.I)
PHONE_RE = re.compile(r"(\+?\d[\d\s().-]{8,}\d)")


def extract_contacts(text: str) -> Tuple[List[str], List[str]]:
    emails = sorted(set(EMAIL_RE.findall(text or "")))[:6]

    phones: List[str] = []
    for m in PHONE_RE.findall(text or ""):
        m2 = re.sub(r"\s+", " ", m).strip()
        if len(m2) >= 10:
            phones.append(m2)
    phones = sorted(set(phones))[:4]
    return emails, phones
