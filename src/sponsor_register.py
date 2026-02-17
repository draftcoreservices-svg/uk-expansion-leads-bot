import re
import hashlib
from datetime import datetime
from typing import Tuple, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup

from .config import SPONSOR_REGISTER_PAGE

DEFAULT_TIMEOUT = (10, 60)  # connect, read


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _extract_register_file_url_from_govuk(page_url: str) -> Optional[str]:
    """
    GOV.UK publication pages usually contain a direct link to an attachment (xlsx/csv/ods).
    We pick the first .xlsx if present, otherwise first csv/ods.
    """
    r = requests.get(page_url, timeout=DEFAULT_TIMEOUT, headers={"User-Agent": "Mozilla/5.0 (CWLeadsBot/1.0)"})
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "lxml")
    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not href:
            continue
        if href.startswith("/"):
            href = "https://www.gov.uk" + href
        links.append(href)

    # Prefer XLSX, then CSV, then ODS
    for ext in (".xlsx", ".csv", ".ods"):
        for href in links:
            if href.lower().split("?")[0].endswith(ext):
                return href

    return None


def _download_register_bytes() -> Tuple[bytes, str]:
    file_url = _extract_register_file_url_from_govuk(SPONSOR_REGISTER_PAGE)
    if not file_url:
        raise RuntimeError(f"Could not find sponsor register attachment link on: {SPONSOR_REGISTER_PAGE}")

    r = requests.get(file_url, timeout=DEFAULT_TIMEOUT, headers={"User-Agent": "Mozilla/5.0 (CWLeadsBot/1.0)"})
    r.raise_for_status()
    return r.content, file_url


def _load_register_df(content: bytes, file_url: str) -> pd.DataFrame:
    lower = file_url.lower().split("?")[0]
    if lower.endswith(".csv"):
        df = pd.read_csv(pd.io.common.BytesIO(content))
    else:
        # xlsx/ods
        df = pd.read_excel(pd.io.common.BytesIO(content))

    # Normalize column names
    df.columns = [str(c).strip().lower() for c in df.columns]
    return df


def _normalize_name(name: str) -> str:
    s = (name or "").lower()
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return " ".join(s.split()).strip()


def refresh_sponsor_register(storage) -> Tuple[bool, str]:
    """
    Downloads sponsor register and stores names in sqlite via Storage.
    Returns (updated, src_date_str).
    """
    content, file_url = _download_register_bytes()
    digest = _sha256_bytes(content)

    # If unchanged, skip
    existing = storage.get_meta("sponsor_register_sha256")
    if existing and existing == digest:
        # Still return a "date" for subject line – use today if unchanged
        return False, datetime.utcnow().strftime("%Y-%m-%d")

    df = _load_register_df(content, file_url)

    # Heuristic: find best column containing organisation name
    # Typical columns include: "organisation name", "name", "sponsor name"
    name_col = None
    for c in df.columns:
        if c in ("organisation name", "organization name", "name", "sponsor name"):
            name_col = c
            break
    if not name_col:
        # fallback: first column containing "name"
        for c in df.columns:
            if "name" in c:
                name_col = c
                break
    if not name_col:
        raise RuntimeError(f"Could not identify sponsor name column in register file. Columns: {df.columns.tolist()}")

    names = []
    for raw in df[name_col].astype(str).tolist():
        n = _normalize_name(raw)
        if n:
            names.append(n)

    names = sorted(set(names))

    storage.replace_sponsor_register(names)
    storage.set_meta("sponsor_register_sha256", digest)
    storage.set_meta("sponsor_register_source_url", file_url)

    # Best-effort source date: use today (publication attachment dates can be inconsistent)
    src_date = datetime.utcnow().strftime("%Y-%m-%d")
    return True, src_date


def is_on_sponsor_register(storage, company_name: str) -> bool:
    """
    Exact match against normalized sponsor names stored in sqlite.
    (Fuzzy matching happens higher up / elsewhere.)
    """
    if not company_name:
        return False
    return storage.is_on_sponsor_register(_normalize_name(company_name))
