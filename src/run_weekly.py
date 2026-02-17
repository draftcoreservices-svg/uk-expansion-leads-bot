from datetime import datetime
from urllib.parse import urlparse

from .config import CFG
from .storage import Storage
from .serp import SerpClient
from .text_extract import fetch_page_text
from .extract import extract_contacts
from .leads import Lead
from .companies_house import CompaniesHouseClient
from .sponsor_register import refresh_sponsor_register, is_on_sponsor_register
from .scoring import score_heuristic
from .openai_classifier import classify_lead, enabled as openai_enabled
from .email_report import build_email
from .emailer import send_email


def _denied(url: str) -> bool:
    try:
        u = urlparse(url)
        host = (u.netloc or "").lower()

        # deny TLDs
        if any(host.endswith(tld) for tld in CFG.deny_tlds):
            return True

        # deny domains
        for d in CFG.deny_domains:
            if host == d or host.endswith("." + d):
                return True

        return False
    except Exception:
        return True


def _label_from_title(title: str) -> str:
    # best-effort: strip common separators
    if not title:
        return ""
    for sep in ["|", " - ", " — ", " · "]:
        if sep in title:
            title = title.split(sep)[0]
            break
    return title.strip()[:120]


def _build_leads(serp: SerpClient, lead_type: str, queries: list[str]) -> list[Lead]:
    out: list[Lead] = []
    for q in queries:
        for r in serp.search(q, num=CFG.max_results_per_query):
            url = (r.get("link") or "").strip()
            if not url or _denied(url):
                continue
            out.append(
                Lead(
                    lead_type=lead_type,
                    title=r.get("title", ""),
                    url=url,
                    snippet=r.get("snippet", ""),
                    query=r.get("query", ""),
                )
            )
    return out


def main():
    storage = Storage("cache.sqlite")
    serp = SerpClient()
    ch = CompaniesHouseClient()

    # 1) Refresh sponsor register (GOV.UK CSV)
    try:
        updated, src_date = refresh_sponsor_register(storage)
    except Exception:
        updated, src_date = (False, "")

    now_iso = datetime.utcnow().isoformat(timespec="seconds") + "Z"

    # 2) Gather raw candidates
    raw_sponsor = _build_leads(serp, "sponsor_licence", CFG.sponsor_queries)
    raw_mobility = _build_leads(serp, "global_mobility", CFG.mobility_queries)
    raw_talent = _build_leads(serp, "global_talent", CFG.talent_queries)

    all_raw = raw_sponsor + raw_mobility + raw_talent

    sponsor_strong: list[Lead] = []
    mobility_strong: list[Lead] = []
    talent_strong: list[Lead] = []

    pages_fetched = 0
    openai_calls = 0

    for lead in all_raw:
        lead_id = storage.lead_id(lead.lead_type, lead.url, lead.title)
        if storage.seen_before(lead_id):
            continue

        if pages_fetched >= CFG.max_pages_to_fetch:
            break

        # 3) Fetch page text
        try:
            final_url, text = fetch_page_text(lead.url, max_chars=CFG.page_text_max_chars)
            lead.final_url = final_url
            lead.page_text = text
            pages_fetched += 1
        except Exception:
            storage.mark_seen(lead_id, lead.lead_type, lead.title, lead.url, now_iso)
            continue

        # 4) Extract contacts
        emails, phones = extract_contacts(lead.page_text)
        lead.contact_emails = emails
        lead.contact_phones = phones

        # 5) Label
        lead.company_or_person = _label_from_title(lead.title)

        # 6) Sponsor register check (only relevant for sponsor bucket)
        if lead.lead_type == "sponsor_licence":
            try:
                lead.on_sponsor_register = is_on_sponsor_register(storage, lead.company_or_person)
            except Exception:
                lead.on_sponsor_register = None

        # 7) Companies House enrichment for org buckets
        if lead.lead_type in ("sponsor_licence", "global_mobility"):
            try:
                lead.companies_house = ch.search_company(lead.company_or_person)
            except Excep
