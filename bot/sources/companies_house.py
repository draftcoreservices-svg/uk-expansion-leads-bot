import requests
from datetime import date

from ..utils import norm, norm_upper

CH_BASE = 'https://api.company-information.service.gov.uk'


def ch_auth() -> tuple[str, str]:
    import os
    return (os.environ['COMPANIES_HOUSE_API_KEY'], '')


def advanced_incorporated(session: requests.Session, inc_from: str, inc_to: str, size: int, max_total: int) -> list[dict]:
    out: list[dict] = []
    start_index = 0
    while True:
        params = {
            'incorporated_from': inc_from,
            'incorporated_to': inc_to,
            'size': size,
            'start_index': start_index,
        }
        r = session.get(f'{CH_BASE}/advanced-search/companies', params=params, auth=ch_auth(), timeout=25)
        r.raise_for_status()
        data = r.json()
        items = data.get('items') or []
        out.extend(items)
        if len(items) < size:
            break
        start_index += size
        if start_index >= max_total:
            break
    return out


def search_companies(session: requests.Session, query: str, items_per_page: int = 10, timeout: int = 20) -> list[dict]:
    r = session.get(
        f'{CH_BASE}/search/companies',
        params={'q': query, 'items_per_page': items_per_page},
        auth=ch_auth(),
        timeout=timeout,
    )
    r.raise_for_status()
    return r.json().get('items') or []


def company_profile(session: requests.Session, company_number: str, timeout: int = 20) -> dict:
    r = session.get(f'{CH_BASE}/company/{company_number}', auth=ch_auth(), timeout=timeout)
    r.raise_for_status()
    return r.json()


def company_officers(session: requests.Session, company_number: str, timeout: int = 15) -> list[dict]:
    r = session.get(
        f'{CH_BASE}/company/{company_number}/officers',
        params={'items_per_page': 100},
        auth=ch_auth(),
        timeout=timeout,
    )
    r.raise_for_status()
    return r.json().get('items') or []


def company_psc(session: requests.Session, company_number: str, timeout: int = 15) -> list[dict]:
    # PSC endpoint may 404/401 for some companies; handle upstream
    r = session.get(
        f'{CH_BASE}/company/{company_number}/persons-with-significant-control',
        params={'items_per_page': 100},
        auth=ch_auth(),
        timeout=timeout,
    )
    if r.status_code == 404:
        return []
    r.raise_for_status()
    return r.json().get('items') or []


def flatten_reg_address(ro: dict) -> tuple[str, str, str, str]:
    ro = ro or {}
    address = ', '.join([x for x in [
        ro.get('address_line_1',''),
        ro.get('address_line_2',''),
        ro.get('locality',''),
        ro.get('region',''),
        ro.get('postal_code',''),
        ro.get('country',''),
    ] if x]).strip(', ')
    postcode = norm(ro.get('postal_code',''))
    town = norm(ro.get('locality','') or ro.get('post_town','') or '')
    country = norm(ro.get('country',''))
    return address, postcode, town, country


def sic_codes_from_profile(profile: dict) -> str:
    sic = profile.get('sic_codes') or []
    if isinstance(sic, list):
        return ','.join([str(x) for x in sic if x])
    return str(sic or '')
