from typing import List, Dict, Tuple
from .utils import norm, norm_upper, token_similarity, safe_join

CH_BASE = "https://api.company-information.service.gov.uk"


def ch_auth(api_key: str):
    return (api_key, "")


def advanced_incorporated(session, api_key: str, inc_from: str, inc_to: str, size: int = 100, max_total: int = 1000, timeout: int = 20) -> List[Dict]:
    out = []
    start_index = 0
    while True:
        params = {
            "incorporated_from": inc_from,
            "incorporated_to": inc_to,
            "size": size,
            "start_index": start_index,
        }
        r = session.get(f"{CH_BASE}/advanced-search/companies", params=params, auth=ch_auth(api_key), timeout=timeout)
        r.raise_for_status()
        data = r.json()
        items = data.get("items") or []
        out.extend(items)
        if len(items) < size:
            break
        start_index += size
        if start_index >= max_total:
            break
    return out


def company_profile(session, api_key: str, company_number: str, timeout: int = 20) -> Dict:
    r = session.get(f"{CH_BASE}/company/{company_number}", auth=ch_auth(api_key), timeout=timeout)
    r.raise_for_status()
    return r.json()


def company_officers(session, api_key: str, company_number: str, timeout: int = 15) -> List[Dict]:
    r = session.get(
        f"{CH_BASE}/company/{company_number}/officers",
        params={"items_per_page": 100},
        auth=ch_auth(api_key),
        timeout=timeout,
    )
    r.raise_for_status()
    return r.json().get("items") or []


def company_psc(session, api_key: str, company_number: str, timeout: int = 15) -> List[Dict]:
    r = session.get(
        f"{CH_BASE}/company/{company_number}/persons-with-significant-control",
        params={"items_per_page": 100},
        auth=ch_auth(api_key),
        timeout=timeout,
    )
    if r.status_code == 404:
        return []
    r.raise_for_status()
    return r.json().get("items") or []


def search_companies(session, api_key: str, query: str, items_per_page: int = 10, timeout: int = 20) -> List[Dict]:
    r = session.get(
        f"{CH_BASE}/search/companies",
        params={"q": query, "items_per_page": items_per_page},
        auth=ch_auth(api_key),
        timeout=timeout,
    )
    r.raise_for_status()
    return r.json().get("items") or []


def best_match_for_name(session, api_key: str, name: str, town: str = "", timeout: int = 20) -> Tuple[str, int]:
    items = search_companies(session, api_key, name, items_per_page=10, timeout=timeout)
    if not items:
        return "", 0
    best_num, best_score = "", 0
    town_u = norm_upper(town)
    for it in items:
        title = it.get("title") or ""
        num = it.get("company_number") or ""
        if not title or not num:
            continue
        sim = token_similarity(name, title)
        snippet = norm_upper(it.get("address_snippet") or "")
        if town_u and town_u in snippet:
            sim = min(100, sim + 6)
        if sim > best_score:
            best_score, best_num = sim, num
    return best_num, best_score


def normalize_registered_office(ro: Dict) -> Tuple[str, str, str, str]:
    address = safe_join([
        ro.get("address_line_1", ""),
        ro.get("address_line_2", ""),
        ro.get("locality", "") or ro.get("post_town", "") or "",
        ro.get("region", ""),
        ro.get("postal_code", ""),
        ro.get("country", ""),
    ])
    postcode = norm(ro.get("postal_code", ""))
    town = norm(ro.get("locality", "") or ro.get("post_town", "") or "")
    country = norm(ro.get("country", ""))
    return address, postcode, town, country
