# bot/enrichment.py
# Drop-in replacement: adds strong denylist filtering + better “official homepage” selection.
# Keeps the same function signatures used by bot/main.py.

import re
import time
from typing import Dict, List, Tuple, Optional
from urllib.parse import urlparse, urlunparse

from bs4 import BeautifulSoup


# Domains we do NOT want to treat as the company’s official website
DENY_DOMAINS = {
    # UK gov / corporate registries / company databases
    "find-and-update.company-information.service.gov.uk",
    "companieshouse.gov.uk",
    "opencorporates.com",
    "duedil.com",
    "endole.co.uk",
    "northdata.com",
    "companycheck.co.uk",
    "corporationwiki.com",
    "bizapedia.com",
    "dnb.com",
    "dnb.co.uk",

    # Social / profiles (not the official site)
    "linkedin.com",
    "facebook.com",
    "instagram.com",
    "twitter.com",
    "x.com",
    "youtube.com",
    "tiktok.com",

    # General business directories / aggregators
    "crunchbase.com",
    "bloomberg.com",
    "zoominfo.com",
    "signalhire.com",
    "rocketreach.co",
}


def _norm_domain(host: str) -> str:
    host = (host or "").lower().strip()
    host = host.split(":")[0]
    if host.startswith("www."):
        host = host[4:]
    return host


def _get_domain(url: str) -> str:
    try:
        return _norm_domain(urlparse(url).netloc)
    except Exception:
        return ""


def _base_url(url: str) -> str:
    """Return scheme://netloc (no path/query)"""
    try:
        p = urlparse(url)
        scheme = p.scheme or "https"
        netloc = p.netloc
        if not netloc:
            return ""
        return urlunparse((scheme, netloc, "", "", "", ""))
    except Exception:
        return ""


def _is_denied(url: str) -> bool:
    d = _get_domain(url)
    if not d:
        return True
    if d in DENY_DOMAINS:
        return True
    # Also deny subdomains of denied domains (e.g. uk.linkedin.com)
    for bad in DENY_DOMAINS:
        if d.endswith("." + bad):
            return True
    return False


def _clean_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def _serpapi_search(
    session,
    serp_key: str,
    query: str,
    budget: Dict[str, int],
    sleep_s: float,
    location: str = "United Kingdom",
    num: int = 10,
) -> Dict:
    """
    Minimal SerpAPI call using the existing requests session.
    Budget shape: {"calls": int, "cap": int}
    """
    if budget["calls"] >= budget["cap"]:
        return {}

    params = {
        "engine": "google",
        "q": query,
        "api_key": serp_key,
        "google_domain": "google.co.uk",
        "hl": "en",
        "gl": "gb",
        "num": num,
        "location": location,
    }

    # Light throttling
    if sleep_s and sleep_s > 0:
        time.sleep(float(sleep_s))

    r = session.get("https://serpapi.com/search.json", params=params, timeout=25)
    budget["calls"] += 1
    if not r.ok:
        return {}
    try:
        return r.json()
    except Exception:
        return {}


def _score_homepage_candidate(url: str, title: str, snippet: str, company_name: str) -> int:
    """
    Heuristic score to prefer likely official sites.
    Higher is better.
    """
    score = 0
    d = _get_domain(url)
    t = (title or "").lower()
    s = (snippet or "").lower()
    n = (company_name or "").lower()

    if not d:
        return -999
    if _is_denied(url):
        return -999

    # Prefer non-free blog platforms (weak signal)
    if any(x in d for x in ["wordpress.com", "blogspot.", "wixsite."]):
        score -= 10

    # Prefer typical corporate signals
    if any(x in url.lower() for x in ["/careers", "/jobs", "/contact", "/about"]):
        score += 8

    # Title/snippet contains company name (strong)
    if n and n[:6] in t:
        score += 12
    if n and n[:6] in s:
        score += 6
    if n and n in t:
        score += 10
    if n and n in s:
        score += 5

    # Penalise obvious directory language
    if any(x in t for x in ["company profile", "company information", "companies house", "director", "filings"]):
        score -= 25
    if any(x in s for x in ["company profile", "company information", "companies house", "director", "filings"]):
        score -= 15

    # Prefer UK-ish domains slightly (tiny nudge)
    if d.endswith(".co.uk") or d.endswith(".uk"):
        score += 3

    return score


def find_official_homepage(
    session,
    serp_key: str,
    company_name: str,
    reg_postcode: str = "",
    town: str = "",
    serp_sleep: float = 1.2,
    serp_budget: Optional[Dict[str, int]] = None,
) -> List[str]:
    """
    Returns a list of base URLs (scheme://domain) that look like the official company website.
    Filters out directories/aggregators/social aggressively.
    """
    if not serp_key:
        return []
    if serp_budget is None:
        serp_budget = {"calls": 0, "cap": 999999}

    name = _clean_text(company_name)
    if not name:
        return []

    # Query tries to bias towards “official site”
    q = f'"{name}" official website'
    if town:
        q += f" {town}"
    if reg_postcode:
        q += f" {reg_postcode}"

    data = _serpapi_search(
        session=session,
        serp_key=serp_key,
        query=q,
        budget=serp_budget,
        sleep_s=serp_sleep,
        num=10,
    )

    organic = data.get("organic_results") or []
    scored: List[Tuple[int, str]] = []

    for r in organic:
        link = r.get("link") or ""
        title = r.get("title") or ""
        snippet = r.get("snippet") or ""

        if not link:
            continue
        if _is_denied(link):
            continue

        b = _base_url(link)
        if not b or _is_denied(b):
            continue

        sc = _score_homepage_candidate(link, title, snippet, name)
        if sc <= -100:
            continue
        scored.append((sc, b))

    # Dedup while keeping best score
    best: Dict[str, int] = {}
    for sc, b in scored:
        if b not in best or sc > best[b]:
            best[b] = sc

    ranked = sorted(best.items(), key=lambda x: x[1], reverse=True)
    return [b for b, _sc in ranked[:5]]


_EMAIL_RE = re.compile(r"([a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,})")
_PHONE_RE = re.compile(r"(\+?\d[\d\s().\-]{7,}\d)")


def _extract_contacts(html: str) -> Tuple[str, str]:
    emails = sorted(set(m.group(1) for m in _EMAIL_RE.finditer(html or "")))
    phones = sorted(set(_clean_text(m.group(1)) for m in _PHONE_RE.finditer(html or "")))
    # Basic cleanup
    emails = [e for e in emails if len(e) <= 120]
    phones = [p for p in phones if 8 <= len(p) <= 30]
    return ", ".join(emails[:8]), ", ".join(phones[:6])


def _verification_score(
    soup: BeautifulSoup,
    company_name: str,
    reg_postcode: str,
    base_url: str,
) -> int:
    score = 0
    text = _clean_text(soup.get_text(" ", strip=True)).lower()
    title = _clean_text(soup.title.get_text(" ", strip=True) if soup.title else "").lower()

    name = (company_name or "").lower().strip()
    pc = (reg_postcode or "").lower().replace(" ", "")

    if not _is_denied(base_url):
        score += 2

    if name and name in title:
        score += 4
    if name and name[:10] in title:
        score += 2
    if name and name in text:
        score += 2

    if pc and pc in text.replace(" ", ""):
        score += 3

    # Contact/about links are a positive sign
    links = [(_clean_text(a.get("href") or ""), _clean_text(a.get_text(" ", strip=True)).lower()) for a in soup.find_all("a")]
    if any("contact" in (h.lower() + " " + t) for h, t in links):
        score += 2
    if any("about" in (h.lower() + " " + t) for h, t in links):
        score += 1
    if any("careers" in (h.lower() + " " + t) or "jobs" in (h.lower() + " " + t) for h, t in links):
        score += 2

    return score


def scrape_verified_contacts(
    session,
    cfg,
    company_name: str,
    company_number: str,
    reg_postcode: str,
    base_url: str,
) -> Tuple[str, int, str, str, str]:
    """
    Fetch a candidate base URL, attempt to verify it looks like the official site,
    and extract public contact details. Returns:
    (website, confidence_score, emails, phones, source_url)
    """
    if not base_url:
        return ("", 0, "", "", "")

    # Deny obvious aggregator domains up-front
    if _is_denied(base_url):
        return ("", 0, "", "", base_url)

    try:
        r = session.get(base_url, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
        if not r.ok or not (r.text or "").strip():
            return ("", 0, "", "", base_url)
    except Exception:
        return ("", 0, "", "", base_url)

    soup = BeautifulSoup(r.text, "html.parser")
    conf = _verification_score(soup, company_name=company_name, reg_postcode=reg_postcode, base_url=base_url)

    # Extract contacts from homepage
    emails, phones = _extract_contacts(r.text)

    # If no contacts on homepage, try one contact-ish page (light touch)
    if not emails and not phones:
        contact_href = ""
        for a in soup.find_all("a"):
            href = a.get("href") or ""
            txt = (a.get_text(" ", strip=True) or "").lower()
            if "contact" in href.lower() or "contact" in txt:
                contact_href = href
                break
        if contact_href:
            # Make absolute if needed
            if contact_href.startswith("/"):
                contact_url = base_url.rstrip("/") + contact_href
            elif contact_href.startswith("http"):
                contact_url = contact_href
            else:
                contact_url = base_url.rstrip("/") + "/" + contact_href.lstrip("/")

            if not _is_denied(contact_url):
                try:
                    rc = session.get(contact_url, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
                    if rc.ok and (rc.text or "").strip():
                        e2, p2 = _extract_contacts(rc.text)
                        if e2:
                            emails = e2
                            conf += 1
                        if p2:
                            phones = p2
                            conf += 1
                except Exception:
                    pass

    website = base_url
    return (website, int(conf), emails, phones, base_url)

def url_domain(url: str) -> str:
    """Backwards-compatible helper expected by job_intel.py"""
    return _get_domain(url)


def serp_search(
    session,
    serp_key: str,
    query: str,
    serp_budget: Dict[str, int],
    serp_sleep: float = 1.2,
    num: int = 10,
) -> Dict:
    """
    Backwards-compatible SerpAPI search helper expected by job_intel.py.
    """
    return _serpapi_search(
        session=session,
        serp_key=serp_key,
        query=query,
        budget=serp_budget,
        sleep_s=serp_sleep,
        num=num,
    )
