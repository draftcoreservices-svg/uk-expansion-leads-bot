from __future__ import annotations

import re
import time
from datetime import datetime, timezone, timedelta

import requests
from bs4 import BeautifulSoup

from .utils import url_domain, extract_emails, extract_phones, rank_emails, token_similarity


DIRECTORY_BLOCKLIST = [
    'companieshouse.gov.uk','gov.uk','linkedin.com','facebook.com','yell.com','endole.co.uk',
    'opencorporates.com','find-and-update.company-information.service.gov.uk','bloomberg.com','dnb.com',
    'zoominfo.com','crunchbase.com'
]


def serp_search(session: requests.Session, query: str, api_key: str, num: int = 6) -> list[dict]:
    params = {'engine': 'google', 'q': query, 'api_key': api_key, 'num': num}
    r = session.get('https://serpapi.com/search.json', params=params, timeout=60)
    r.raise_for_status()
    return (r.json().get('organic_results') or [])


def get_url_text(session: requests.Session, url: str, timeout: int = 20) -> str:
    try:
        r = session.get(url, timeout=timeout, headers={'User-Agent': 'Mozilla/5.0 (compatible; CWLeadsBot/2.0)'})
        if r.status_code >= 400:
            return ''
        return (r.text or '')[:600000]
    except Exception:
        return ''


def find_contact_links(html: str, base_url: str) -> list[str]:
    soup = BeautifulSoup(html, 'lxml')
    links = []
    for a in soup.select('a[href]'):
        href = (a.get('href') or '').strip()
        if not href:
            continue
        label = (a.get_text(' ') or '').strip().lower()

        if any(k in href.lower() for k in ['/contact', 'contact-us', 'contactus']) or 'contact' in label:
            links.append(href)
        if any(k in href.lower() for k in ['/privacy', '/terms', '/legal', '/imprint', 'privacy', 'terms']):
            links.append(href)

    abs_links = []
    for h in links:
        if h.startswith('http'):
            abs_links.append(h)
        elif h.startswith('/'):
            abs_links.append(base_url.rstrip('/') + h)
        else:
            abs_links.append(base_url.rstrip('/') + '/' + h)

    out, seen = [], set()
    for l in abs_links:
        d = l.split('#')[0]
        if d in seen:
            continue
        seen.add(d)
        out.append(d)
    return out[:8]


def _hard_verify(company_name: str, company_number: str, reg_postcode: str, page_text: str) -> tuple[int, list[str]]:
    ev = []
    score = 0
    t_upper = (page_text or '').upper()

    if company_number and re.search(r'\b' + re.escape(company_number) + r'\b', t_upper):
        score += 6
        ev.append('Company number found')

    if reg_postcode:
        pc = reg_postcode.replace(' ', '').upper()
        if pc and pc in t_upper.replace(' ', ''):
            score += 3
            ev.append('Registered postcode found')

    if company_name:
        sim = token_similarity(company_name, (page_text or '')[:20000])
        if sim >= 75:
            score += 2
            ev.append(f'Name similarity strong ({sim})')
        elif sim >= 60:
            score += 1
            ev.append(f'Name similarity moderate ({sim})')

    return min(score, 10), ev


def _plausibility(company_name: str, page_text: str) -> tuple[int, list[str]]:
    ev = []
    score = 0
    txt = (page_text or '')
    if not txt:
        return 0, []

    sim = token_similarity(company_name, txt[:20000])
    if sim >= 70:
        score += 5
        ev.append(f'Name appears on site ({sim})')
    elif sim >= 55:
        score += 3
        ev.append(f'Name partially matches ({sim})')

    upper = txt.upper()
    if any(k in upper for k in ['REGISTERED IN ENGLAND', 'COMPANY NUMBER', 'REGISTERED OFFICE']):
        score += 2
        ev.append('UK legal footer patterns found')

    if any(k in upper for k in ['LIMITED', 'LTD', 'LLP', 'PLC']):
        score += 1

    return min(score, 10), ev


def enrich(http: requests.Session, *, company_name: str, company_number: str, reg_postcode: str, reg_town: str,
           serp_key: str, serp_budget: dict, sleep_s: float, verify_min: int) -> dict:

    if serp_budget['calls'] >= serp_budget['cap']:
        return {
            'website': '',
            'website_level': '',
            'website_score': None,
            'verification_evidence': '',
            'emails': '',
            'phones': '',
            'enrich_status': 'Skipped (Serp budget cap)'
        }

    q = f'"{company_name}" {reg_postcode}'.strip() if reg_postcode else f'"{company_name}" {reg_town} contact'.strip()

    serp_budget['calls'] += 1
    results = serp_search(http, q, serp_key, num=6)
    time.sleep(sleep_s)

    candidates = []
    for r in results:
        link = r.get('link') or ''
        if not link.startswith('http'):
            continue
        d = url_domain(link)
        if not d:
            continue
        if any(x in d for x in DIRECTORY_BLOCKLIST):
            continue
        candidates.append('https://' + d)

    # dedupe
    deduped = []
    seen = set()
    for c in candidates:
        if c not in seen:
            seen.add(c)
            deduped.append(c)
    candidates = deduped[:3]

    best = {'hard_score': -1, 'plaus_score': -1, 'base_url': '', 'ev': []}

    for base_url in candidates:
        home_html = get_url_text(http, base_url)
        if not home_html:
            continue

        plaus, evp = _plausibility(company_name, home_html)
        hard, evh = _hard_verify(company_name, company_number, reg_postcode, home_html)
        score = max(hard, plaus - 1)  # never let plaus beat hard, but allow it to rank candidates
        ev = list(dict.fromkeys(evp + evh))

        links = find_contact_links(home_html, base_url)
        for l in links:
            # keep going until hard verify is maxed out
            if hard >= 9:
                break
            extra = get_url_text(http, l)
            if not extra:
                continue
            p2, evp2 = _plausibility(company_name, extra)
            h2, evh2 = _hard_verify(company_name, company_number, reg_postcode, extra)
            if h2 > hard:
                hard = h2
            if p2 > plaus:
                plaus = p2
            ev = list(dict.fromkeys(ev + evp2 + evh2))

        if hard > best['hard_score'] or (hard == best['hard_score'] and plaus > best['plaus_score']):
            best = {'hard_score': hard, 'plaus_score': plaus, 'base_url': base_url, 'ev': ev}

    website = best['base_url']
    hard_score = best['hard_score'] if best['hard_score'] >= 0 else None
    plaus_score = best['plaus_score'] if best['plaus_score'] >= 0 else None

    level = ''
    if hard_score is not None and hard_score >= verify_min:
        level = 'VERIFIED'
    elif plaus_score is not None and plaus_score >= 6:
        level = 'PLAUSIBLE'

    evidence = '; '.join(best['ev'])[:240] if best['ev'] else ''

    if level != 'VERIFIED':
        status = 'Manual verify needed' if website else 'No website found'
        return {
            'website': website,
            'website_level': level,
            'website_score': hard_score if hard_score is not None else plaus_score,
            'verification_evidence': evidence,
            'emails': '',
            'phones': '',
            'enrich_status': status
        }

    # Scrape contacts (verified only)
    combined = ''
    home_html = get_url_text(http, website)
    combined += ' ' + BeautifulSoup(home_html, 'lxml').get_text(' ', strip=True)

    for l in find_contact_links(home_html, website)[:6]:
        extra = get_url_text(http, l)
        if extra:
            combined += ' ' + BeautifulSoup(extra, 'lxml').get_text(' ', strip=True)

    emails = rank_emails(extract_emails(combined), website_domain=url_domain(website))
    phones = extract_phones(combined)

    return {
        'website': website,
        'website_level': level,
        'website_score': hard_score,
        'verification_evidence': evidence,
        'emails': ', '.join(emails[:5]),
        'phones': ', '.join(phones[:5]),
        'enrich_status': 'Verified & scraped' if (emails or phones) else 'Verified (no contacts found)'
    }
