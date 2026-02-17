import io
import re
import pandas as pd
from bs4 import BeautifulSoup
import requests

from ..utils import norm, norm_upper, clean_display_name, non_alnum_ratio

SPONSOR_PAGE = 'https://www.gov.uk/government/publications/register-of-licensed-sponsors-workers'


def _find_latest_csv_url(html: str) -> str:
    soup = BeautifulSoup(html, 'lxml')
    links = []
    for a in soup.select('a[href]'):
        href = a.get('href')
        if href and '.csv' in href.lower():
            links.append(href)
    if not links:
        raise RuntimeError('Could not find CSV link on sponsor register page.')
    assets = [h for h in links if 'assets.publishing.service.gov.uk' in h]
    chosen = assets[0] if assets else links[0]
    if chosen.startswith('/'):
        chosen = 'https://www.gov.uk' + chosen
    return chosen


def fetch_df(session: requests.Session) -> pd.DataFrame:
    page = session.get(SPONSOR_PAGE, timeout=30)
    page.raise_for_status()
    csv_url = _find_latest_csv_url(page.text)
    r = session.get(csv_url, timeout=60)
    r.raise_for_status()
    df = pd.read_csv(io.BytesIO(r.content), dtype=str).fillna('')
    df.columns = [c.strip() for c in df.columns]
    return df


def row_key(row: dict) -> str:
    name = norm_upper(row.get('Organisation Name') or row.get('Organization Name') or '')
    town = norm_upper(row.get('Town/City') or row.get('Town') or '')
    route = norm_upper(row.get('Route') or '')
    sub = norm_upper(row.get('Sub Route') or '')
    # normalise punctuation to reduce noisy "new" rows
    name = re.sub(r'[^A-Z0-9 ]+', ' ', name)
    name = re.sub(r'\s+', ' ', name).strip()
    town = re.sub(r'\s+', ' ', town).strip()
    return f'SPONSOR::{name}::{town}::{route}::{sub}'


def row_fields(row: dict) -> dict:
    raw_name = row.get('Organisation Name') or row.get('Organization Name') or ''
    name = clean_display_name(raw_name)
    town = norm(row.get('Town/City') or row.get('Town') or '')
    county = norm(row.get('County') or '')
    route = norm(row.get('Route') or '')
    sub = norm(row.get('Sub Route') or '')
    addr = ', '.join([x for x in [town, county] if x]).strip(', ')
    return {
        'name': name,
        'town': town,
        'county': county,
        'address': addr,
        'route': route,
        'subroute': sub,
        'raw_name': raw_name,
    }


def is_noise(name: str, min_len: int, max_non_alnum: float) -> bool:
    n = clean_display_name(name)
    if len(n) < min_len:
        return True
    if non_alnum_ratio(n) > max_non_alnum:
        return True
    return False
