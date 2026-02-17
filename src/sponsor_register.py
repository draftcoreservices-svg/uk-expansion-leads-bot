import re
import csv
import io
import requests
from bs4 import BeautifulSoup
from typing import Optional, Tuple
from .config import SPONSOR_REGISTER_PAGE
from .storage import Storage

def _find_latest_csv_url(html: str) -> Optional[str]:
    soup = BeautifulSoup(html, "lxml")
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "assets.publishing.service.gov.uk" in href and href.lower().endswith(".csv"):
            return href
    return None

def get_latest_register_csv_url() -> str:
    r = requests.get(SPONSOR_REGISTER_PAGE, timeout=30, headers={"User-Agent":"Mozilla/5.0"})
    r.raise_for_status()
    url = _find_latest_csv_url(r.text)
    if not url:
        raise RuntimeError("Could not locate sponsor register CSV URL on GOV.UK page")
    return url

def refresh_sponsor_register(storage: Storage) -> Tuple[bool, str]:
    csv_url = get_latest_register_csv_url()

    prev_url = storage.get_meta("sponsor_register_csv_url")
    if prev_url == csv_url and storage.get_meta("sponsor_register_loaded") == "1":
        return (False, storage.get_meta("sponsor_register_source_date") or "")

    r = requests.get(csv_url, timeout=60, headers={"User-Agent":"Mozilla/5.0"})
    r.raise_for_status()

    m = re.search(r"(\d{4}-\d{2}-\d{2})", csv_url)
    source_date = m.group(1) if m else ""

    content = r.content.decode("utf-8", errors="replace")
    reader = csv.DictReader(io.StringIO(content))
    table = storage.db["sponsor_register"]
    table.delete_where("1=1")

    for row in reader:
        org = (row.get("Organisation Name") or row.get("Organisation name") or row.get("Organisation") or "").strip()
        if not org:
            continue
        name_norm = storage.normalize_name(org)
        table.insert({
            "name_norm": name_norm,
            "org_name": org,
            "town": (row.get("Town/City") or row.get("Town") or "").strip(),
            "county": (row.get("County") or "").strip(),
            "type_rating": (row.get("Type & Rating") or row.get("Type and Rating") or "").strip(),
            "route": (row.get("Route") or "").strip(),
            "source_date": source_date,
        }, pk="name_norm", replace=True)

    storage.upsert_meta("sponsor_register_csv_url", csv_url)
    storage.upsert_meta("sponsor_register_loaded", "1")
    storage.upsert_meta("sponsor_register_source_date", source_date)
    return (True, source_date)

def is_on_sponsor_register(storage: Storage, org_name: str) -> bool:
    return storage.sponsor_lookup(org_name) is not None
