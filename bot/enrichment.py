import time
import re
from typing import Dict, List, Tuple
from bs4 import BeautifulSoup
from .utils import extract_emails, extract_phones, token_similarity


def serp_search(session, query: str, api_key: str, num: int = 5) -> List[Dict]:
    params = {"engine": "google", "q": query, "api_key": api_key, "num": num}
    r = session.get("https://serpapi.com/search.json", params=params, timeout=60)
    r.raise_for_status()
    return (r.json().get("organic_results") or [])


def url_domain(u: str) -> str:
    if not u:
        return ""
    u = re.sub(r"^https?://", "", u.strip(), flags=re.I)
    return u.split("/")[0].lower()


def get_url_text(session, url: str, timeout: int = 20) -> str:
    try:
        r = session.get(url, timeout=timeout, headers={"User-Agent": "Mozilla/5.0 (compatible; CWLeadsBot/2.0)"})
        if r.status_code >= 400:
            return ""
        return (r.text or "")[:700000]
    except Exception:
        return ""


def find_contact_links(html: str, base_url: str) -> List[str]:
    soup = BeautifulSoup(html, "lxml")
    links = []
    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        label = (a.get_text(" ") or "").strip().lower()
        if any(k in href.lower() for k in ["/contact", "contact-us", "contactus"]) or "contact" in label:
            links.append(href)
        if any(k in href.lower() for k in ["/privacy", "/terms", "/legal", "/imprint", "privacy", "terms"]):
            links.append(href)

    abs_links = []
    for h in links:
        if h.startswith("http"):
            abs_links.append(h)
        elif h.startswith("/"):
            abs_links.append(base_url.rstrip("/") + h)
        else:
            abs_links.append(base_url.rstrip("/") + "/" + h)

    out, seen = [], set()
    for l in abs_links:
        d = l.split("#")[0]
        if d in seen:
            continue
        seen.add(d)
        out.append(d)
    return out[:8]


def verification_signals(company_name: str, company_number: str, reg_postcode: str, page_text: str) -> Tuple[int, List[str]]:
    ev = []
    score = 0
    t_upper = (page_text or "").upper()

    has_number = False
    has_postcode = False
    has_name = False

    if company_number and re.search(r"\b" + re.escape(company_number) + r"\b", t_upper):
        score += 6
        has_number = True
        ev.append("Company number found on site")

    if reg_postcode:
        pc = reg_postcode.replace(" ", "").upper()
        if pc and pc in t_upper.replace(" ", ""):
            score += 3
            has_postcode = True
            ev.append("Registered postcode found on site")

    if company_name:
        snippet = re.sub(r"\s+", " ", (page_text or "")[:20000])
        sim = token_similarity(company_name, snippet)
        if sim >= 75:
            score += 2
            has_name = True
            ev.append(f"Name similarity strong ({sim})")
        elif sim >= 60:
            score += 1
            has_name = True
            ev.append(f"Name similarity moderate ({sim})")

    if sum([has_number, has_postcode, has_name]) >= 2:
        score = min(10, score + 1)
        ev.append("2-of-3 verification signals met")
    return min(score, 10), ev


def find_official_homepage(session, serp_key: str, company_name: str, reg_postcode: str, town: str, serp_sleep: float, serp_budget: Dict) -> List[str]:
    if serp_budget["calls"] >= serp_budget["cap"]:
        return []
    q = f"\"{company_name}\" {reg_postcode}".strip() if reg_postcode else f"\"{company_name}\" {town} contact".strip()
    serp_budget["calls"] += 1
    results = serp_search(session, q, serp_key, num=6)
    time.sleep(serp_sleep)

    candidates = []
    for r in results:
        link = r.get("link") or ""
        if not link.startswith("http"):
            continue
        d = url_domain(link)
        if not d:
            continue
        if any(x in d for x in [
            "companieshouse.gov.uk", "gov.uk", "linkedin.com", "facebook.com", "yell.com", "endole.co.uk",
            "opencorporates.com", "find-and-update.company-information.service.gov.uk",
            "bloomberg.com", "dnb.com", "zoominfo.com", "crunchbase.com", "glassdoor.",
        ]):
            continue
        candidates.append("https://" + d)

    out, seen = [], set()
    for c in candidates:
        if c in seen:
            continue
        seen.add(c)
        out.append(c)
    return out[:3]


def scrape_verified_contacts(session, cfg, company_name: str, company_number: str, reg_postcode: str, base_url: str) -> Tuple[str, int, str, str, str]:
    home_html = get_url_text(session, base_url)
    if not home_html:
        return "", 0, "", "", ""

    best_score, _ = verification_signals(company_name, company_number, reg_postcode, home_html)
    best_source = base_url

    links = find_contact_links(home_html, base_url)
    for l in links[:6]:
        extra = get_url_text(session, l)
        if not extra:
            continue
        s2, _ = verification_signals(company_name, company_number, reg_postcode, extra)
        if s2 > best_score:
            best_score = s2
            best_source = l

    if best_score < cfg.verify_min_score:
        return base_url, best_score, "", "", best_source

    combined = BeautifulSoup(home_html, "lxml").get_text(" ", strip=True)
    for l in links[:5]:
        extra = get_url_text(session, l)
        if extra:
            combined += " " + BeautifulSoup(extra, "lxml").get_text(" ", strip=True)

    emails = extract_emails(combined)
    phones = extract_phones(combined)

    kept = []
    for e in emails:
        local = (e.split("@")[0] or "").lower()
        if cfg.allow_personal_emails:
            kept.append(e)
            continue
        if local in cfg.allowed_email_prefixes:
            kept.append(e)

    emails = kept[:5]
    phones = phones[:5]
    return base_url, best_score, ", ".join(emails), ", ".join(phones), best_source
