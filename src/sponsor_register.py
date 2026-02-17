import re
import io
import csv
from typing import Optional, Tuple

import requests
import pandas as pd
from bs4 import BeautifulSoup

from .config import SPONSOR_REGISTER_PAGE
from .storage import Storage

DEFAULT_TIMEOUT = (10, 60)


def _absolute_govuk(href: str) -> str:
    href = (href or "").strip()
    if href.startswith("/"):
        return "https://www.gov.uk" + href
    return href


def _find_attachment_url_from_govuk_html(html: str) -> Optional[str]:
    """
    GOV.UK publication pages link to an attachment on assets.publishing.service.gov.uk.
    Prefer XLSX; fall back to CSV then ODS.
    """
    soup = BeautifulSoup(html, "lxml")
    links = []
    for a in soup.find_all("a", href=True):
        href = _absolute_govuk(a["href"])
        if "assets.publishing.service.gov.uk" in href:
            links.append(href)

    for ext in (".xlsx", ".csv", ".ods"):
        for href in links:
            if href.lower().split("?")[0].endswith(ext):
                return href
    return None


def _source_date_from_url(url: str) -> str:
    """
    Best-effort: sometimes the asset URL contains YYYY-MM-DD.
    """
    m = re.search(r"(\d{4}-\d{2}-\d{2})", url or "")
    return m.group(1) if m else ""


def _download_register_attachment_url() -> str:
    r = requests.get(
        SPONSOR_REGISTER_PAGE,
        timeout=DEFAULT_TIMEOUT,
        headers={"User-Agent": "Mozilla/5.0 (CWLeadsBot/1.0)"},
    )
    r.raise_for_status()
    url = _find_attachment_url_from_govuk_html(r.text)
    if not url:
        raise RuntimeError(f"Could not locate sponsor register attachment on page: {SPONSOR_REGISTER_PAGE}")
    return url


def refresh_sponsor_register(storage: Storage) -> Tuple[bool, str]:
    """
    Download + load sponsor register into sqlite table: sponsor_register
    Returns (updated, source_date_str)
    """
    attachment_url = _download_register_attachment_url()
    source_date = _source_date_from_url(attachment_url) or ""

    prev_url = storage.get_meta("sponsor_register_url")
    prev_loaded = storage.get_meta("sponsor_register_loaded")

    # Skip if same URL and marked loaded
    if prev_url == attachment_url and prev_loaded == "1":
        return False, (storage.get_meta("sponsor_register_source_date") or source_date or "")

    r = requests.get(
        attachment_url,
        timeout=DEFAULT_TIMEOUT,
        headers={"User-Agent": "Mozilla/5.0 (CWLeadsBot/1.0)"},
    )
    r.raise_for_status()

    # Clear table
    table = storage.db["sponsor_register"]
    table.delete_where("1=1")

    lower = attachment_url.lower().split("?")[0]

    if lower.endswith(".csv"):
        content = r.content.decode("utf-8", errors="replace")
        reader = csv.DictReader(io.StringIO(content))
        for row in reader:
            org = (
                (row.get("Organisation Name") or "")
                or (row.get("Organisation name") or "")
                or (row.get("Organisation") or "")
                or (row.get("Name") or "")
            ).strip()
            if not org:
                continue

            name_norm = storage.normalize_name(org)
            if not name_norm:
                continue

            table.insert(
                {
                    "name_norm": name_norm,
                    "org_name": org,
                    "town": (row.get("Town/City") or row.get("Town") or row.get("City") or "").strip(),
                    "county": (row.get("County") or "").strip(),
                    "type_rating": (row.get("Type & Rating") or row.get("Type and Rating") or "").strip(),
                    "route": (row.get("Route") or "").strip(),
                    "source_date": source_date,
                },
                pk="name_norm",
                replace=True,
            )

    else:
        # XLSX / ODS
        df = pd.read_excel(io.BytesIO(r.content))
        df.columns = [str(c).strip() for c in df.columns]

        # Find org/name column
        name_col = None
        for c in df.columns:
            if c.lower() in ("organisation name", "organization name", "sponsor name", "name"):
                name_col = c
                break
        if not name_col:
            for c in df.columns:
                if "name" in c.lower():
                    name_col = c
                    break
        if not name_col:
            raise RuntimeError(f"Could not identify organisation name column. Columns={df.columns.tolist()}")

        # Optional columns
        town_col = next((c for c in df.columns if c.lower() in ("town/city", "town", "city")), None)
        county_col = next((c for c in df.columns if c.lower() == "county"), None)
        type_col = next((c for c in df.columns if c.lower() in ("type & rating", "type and rating")), None)
        route_col = next((c for c in df.columns if c.lower() == "route"), None)

        for _, row in df.iterrows():
            org = str(row.get(name_col, "")).strip()
            if not org or org.lower() == "nan":
                continue

            name_norm = storage.normalize_name(org)
            if not name_norm:
                continue

            town = str(row.get(town_col, "")).strip() if town_col else ""
            county = str(row.get(county_col, "")).strip() if county_col else ""
            type_rating = str(row.get(type_col, "")).strip() if type_col else ""
            route = str(row.get(route_col, "")).strip() if route_col else ""

            if town.lower() == "nan":
                town = ""
            if county.lower() == "nan":
                county = ""
            if type_rating.lower() == "nan":
                type_rating = ""
            if route.lower() == "nan":
                route = ""

            table.insert(
                {
                    "name_norm": name_norm,
                    "org_name": org,
                    "town": town,
                    "county": county,
                    "type_rating": type_rating,
                    "route": route,
                    "source_date": source_date,
                },
                pk="name_norm",
                replace=True,
            )

    storage.upsert_meta("sponsor_register_url", attachment_url)
    storage.upsert_meta("sponsor_register_loaded", "1")
    storage.upsert_meta("sponsor_register_source_date", source_date)
    return True, source_date


def is_on_sponsor_register(storage: Storage, org_name: str) -> bool:
    """
    Uses Storage helpers if present.
    """
    try:
        if storage.sponsor_lookup(org_name) is not None:
            return True
        if storage.sponsor_lookup_fuzzy(org_name) is not None:
            return True
        return False
    except Exception:
        # Fallback: exact normalized lookup in sponsor_register table
        name_norm = storage.normalize_name(org_name or "")
        if not name_norm:
            return False
        row = storage.db["sponsor_register"].get(name_norm, default=None)
        return row is not None
