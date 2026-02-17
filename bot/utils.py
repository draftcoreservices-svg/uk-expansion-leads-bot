import re
from datetime import datetime, timezone
from urllib.parse import urlparse

from rapidfuzz import fuzz

UK_COUNTRIES = {'UNITED KINGDOM','UK','ENGLAND','SCOTLAND','WALES','NORTHERN IRELAND','GREAT BRITAIN'}


def utc_now():
    return datetime.now(timezone.utc).replace(microsecond=0)


def norm(s: str) -> str:
    return re.sub(r'\s+', ' ', (s or '')).strip()


def norm_upper(s: str) -> str:
    return norm(s).upper()


def clean_display_name(name: str) -> str:
    n = norm(name)
    n = re.sub(r"^[\s\"\'\`\*\@\[\]\(\)\{\}\<\>\#\!\$\%\^\&\=\+\;\:\,\.\-\/\\]+", "", n)
    n = re.sub(r'\s+', ' ', n).strip()
    return n


def non_alnum_ratio(s: str) -> float:
    if not s:
        return 1.0
    non = sum(1 for ch in s if not ch.isalnum() and ch != ' ')
    return non / max(len(s), 1)


_SUFFIXES = [
    ' LIMITED', ' LTD', ' L.T.D', ' LLP', ' PLC', ' LIMITED.', ' LTD.',
    ' UK', ' (UK)', ' GROUP', ' HOLDINGS', ' HOLDING', ' INTERNATIONAL', ' INTL',
]


def normalize_for_match(name: str) -> str:
    s = norm_upper(name)
    s = re.sub(r'[^A-Z0-9 ]+', ' ', s)
    s = re.sub(r'\s+', ' ', s).strip()
    for suf in _SUFFIXES:
        s = s.replace(suf, '')
    s = re.sub(r'\s+', ' ', s).strip()
    s = s.replace('THE ', '') if s.startswith('THE ') else s
    return s


def name_variants(name: str) -> list[str]:
    raw = clean_display_name(name)
    a = raw
    b = normalize_for_match(raw)
    # keep a few variants, in preference order
    out = []
    for v in [a, b, b.replace(' AND ', ' & '), b.replace(' & ', ' AND ')]:
        v = norm(v)
        if v and v not in out:
            out.append(v)
    return out[:4]


def token_similarity(a: str, b: str) -> int:
    return int(fuzz.token_set_ratio(norm_upper(a), norm_upper(b)))


def is_uk_country(country: str) -> bool:
    c = norm_upper(country)
    return (not c) or (c in UK_COUNTRIES)


def url_domain(u: str) -> str:
    if not u:
        return ''
    u = u.strip()
    if not u.startswith('http'):
        u = 'https://' + u
    try:
        return urlparse(u).netloc.lower()
    except Exception:
        return ''


def extract_emails(text: str) -> list[str]:
    if not text:
        return []
    emails = set(re.findall(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", text, flags=re.I))
    bad_domains = {'example.com', 'domain.com'}
    out = []
    for e in emails:
        el = e.lower()
        if any(el.endswith(d) for d in bad_domains):
            continue
        out.append(e)
    return sorted(set(out))


def rank_emails(emails: list[str], website_domain: str | None = None) -> list[str]:
    if not emails:
        return []
    wd = (website_domain or '').lower()
    prefs = ['immigration@','globalmobility@','mobility@','hr@','people@','talent@','recruitment@','legal@','admin@','office@','info@']

    def score(e: str) -> tuple[int,int,str]:
        el = e.lower()
        s = 0
        for i, p in enumerate(prefs):
            if el.startswith(p):
                s += 50 - i
                break
        if wd and el.endswith('@' + wd):
            s += 25
        # penalise obvious personal free email domains
        if any(el.endswith('@' + d) for d in ['gmail.com','yahoo.com','hotmail.com','outlook.com','icloud.com']):
            s -= 10
        return (-s, len(el), el)

    return sorted(set(emails), key=score)[:5]


def extract_phones(text: str) -> list[str]:
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
    return out[:5]


def looks_like_subsidiary_name(name: str) -> bool:
    n = norm_upper(name)
    patterns = ['(UK', ' UK ', ' EUROPE ', ' INTERNATIONAL ', ' GLOBAL ', ' HOLDINGS ', ' GROUP ']
    return any(p in n for p in patterns)
