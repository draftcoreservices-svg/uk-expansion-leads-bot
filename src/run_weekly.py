from .ats_company_lookup import extract_company_website_from_ats
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


FREE_EMAIL_DOMAINS = {
    "gmail.com", "hotmail.com", "outlook.com", "live.com", "yahoo.com", "icloud.com",
    "proton.me", "protonmail.com", "aol.com", "gmx.com"
}


def _denied(url: str) -> bool:
    try:
        u = urlparse(url)
        host = (u.netloc or "").lower()

        if any(host.endswith(tld) for tld in CFG.deny_tlds):
            return True

        for d in CFG.deny_domains:
            if host == d or host.endswith("." + d):
                return True

        return False
    except Exception:
        return True


def _content_excluded(title: str, snippet: str) -> str | None:
    t = f"{title} {snippet}".lower()
    for p in CFG.content_exclude_phrases:
        if p.lower() in t:
            return p
    return None


def _label_from_title(title: str) -> str:
    if not title:
        return ""
    for sep in ["|", " - ", " — ", " · "]:
        if sep in title:
            title = title.split(sep)[0]
            break
    return title.strip()[:120]


def _extract_employer_from_ats(url: str) -> str | None:
    """
    Greenhouse/Lever/Workable: derive employer slug from URL.
    Examples:
      https://job-boards.greenhouse.io/monzo/jobs/...
      https://jobs.lever.co/spotify/...
      https://apply.workable.com/costello-medical/j/...
    """
    try:
        u = urlparse(url)
        host = (u.netloc or "").lower()
        parts = [p for p in (u.path or "").split("/") if p]

        if host in {"job-boards.greenhouse.io", "boards.greenhouse.io"}:
            # /<company>/jobs/<id>
            if len(parts) >= 1:
                return parts[0]

        if host == "jobs.lever.co":
            # /<company>/<jobId>
            if len(parts) >= 1:
                return parts[0]

        if host == "apply.workable.com":
            # /<company>/j/<jobId>
            if len(parts) >= 1:
                return parts[0]

        return None
    except Exception:
        return None


def _has_company_signal(lead: Lead) -> bool:
    # Good if we have a non-free email domain
    for e in (lead.contact_emails or []):
        e = (e or "").strip().lower()
        if "@" in e:
            dom = e.split("@", 1)[1]
            if dom and dom not in FREE_EMAIL_DOMAINS:
                return True

    # Or if the final URL is on a company domain (not ATS host)
    try:
        u = urlparse(lead.final_url or lead.url)
        host = (u.netloc or "").lower()
        if host and host not in CFG.ats_hosts and host not in CFG.deny_domains:
            # If it's a normal company domain, count it as signal.
            return True
    except Exception:
        pass

    return False


def _build_leads(serp: SerpClient, lead_type: str, queries: list[str]) -> list[Lead]:
    out: list[Lead] = []
    print(f"[SERP] Building leads for {lead_type} — queries={len(queries)}")
    for i, q in enumerate(queries, start=1):
        print(f"[SERP] ({lead_type}) Query {i}/{len(queries)}: {q}")
        results = serp.search(q, num=CFG.max_results_per_query)
        print(f"[SERP] ({lead_type}) Got {len(results)} results")
        for r in results:
            url = (r.get("link") or "").strip()
            if not url:
                continue
            if _denied(url):
                continue

            title = r.get("title", "") or ""
            snippet = r.get("snippet", "") or ""

            # Kill obvious job-board listing pages/guides early
            bad_phrase = _content_excluded(title, snippet)
            if bad_phrase:
                continue

            out.append(
                Lead(
                    lead_type=lead_type,
                    title=title,
                    url=url,
                    snippet=snippet,
                    query=r.get("query", ""),
                )
            )
    print(f"[SERP] {lead_type} candidates after deny-filter/content-filter: {len(out)}")
    return out


def main():
    start = datetime.utcnow()
    print(f"[START] CW weekly run @ {start.isoformat()}Z")

    storage = Storage("cache.sqlite")
    print("[OK] Storage initialised")

    serp = SerpClient()
    print("[OK] SerpClient initialised")

    ch = CompaniesHouseClient()
    print("[OK] CompaniesHouseClient initialised")

    # 1) Refresh sponsor register
    try:
        updated, src_date = refresh_sponsor_register(storage)
        print(f"[SPONSOR-REG] refreshed={updated} source_date={src_date}")
    except Exception as e:
        print(f"[SPONSOR-REG] refresh failed (continuing): {repr(e)}")
        updated, src_date = (False, "")

    now_iso = datetime.utcnow().isoformat(timespec="seconds") + "Z"

    # 2) Gather candidates
    raw_sponsor = _build_leads(serp, "sponsor_licence", CFG.sponsor_queries)
    raw_mobility = _build_leads(serp, "global_mobility", CFG.mobility_queries)
    raw_talent = _build_leads(serp, "global_talent", CFG.talent_queries)

    all_raw = raw_sponsor + raw_mobility + raw_talent
    print(f"[TOTAL] total raw candidates: {len(all_raw)}")

    sponsor_strong: list[Lead] = []
    mobility_strong: list[Lead] = []
    talent_strong: list[Lead] = []

    pages_fetched = 0
    openai_calls = 0
    processed = 0
    skipped_seen = 0

    for lead in all_raw:
        lead_id = storage.lead_id(lead.lead_type, lead.url, lead.title)
        if storage.seen_before(lead_id):
            skipped_seen += 1
            continue

        if pages_fetched >= CFG.max_pages_to_fetch:
            print("[STOP] max_pages_to_fetch reached")
            break

        processed += 1

        # Fetch page text
        try:
            final_url, text = fetch_page_text(lead.url, max_chars=CFG.page_text_max_chars)
            lead.final_url = final_url
            lead.page_text = text
            pages_fetched += 1
        except Exception:
            storage.mark_seen(lead_id, lead.lead_type, lead.title, lead.url, now_iso)
            continue

        # Extract contacts
        emails, phones = extract_contacts(lead.page_text)
        lead.contact_emails = emails
        lead.contact_phones = phones

        # Label: prefer ATS employer extraction if present
        employer = _extract_employer_from_ats(lead.final_url or lead.url)
        if employer:
            lead.company_or_person = employer
        else:
            lead.company_or_person = _label_from_title(lead.title)

        # Sponsor register check
        if lead.lead_type == "sponsor_licence":
            try:
                lead.on_sponsor_register = is_on_sponsor_register(storage, lead.company_or_person)
            except Exception:
                lead.on_sponsor_register = None

        # Companies House enrichment (only if label looks sane)
        if lead.lead_type in ("sponsor_licence", "global_mobility"):
            try:
                # Avoid looking up nonsense like "Visa Sponsorship Jobs in London"
                if len(lead.company_or_person) >= 3:
                    lead.companies_house = ch.search_company(lead.company_or_person)
            except Exception:
                lead.companies_house = None

        # Heuristic scoring
        lead = score_heuristic(lead)

        # OpenAI refinement
        if openai_enabled() and lead.score >= CFG.medium_threshold and openai_calls < CFG.max_openai_calls:
            ai = classify_lead(
                lead_type_hint=lead.lead_type,
                label=lead.company_or_person,
                url=lead.final_url or lead.url,
                title=lead.title,
                snippet=lead.snippet,
                page_text=lead.page_text,
                model="gpt-5",
            )
            if ai:
                bucket = ai.get("bucket")
                if bucket in ("sponsor_licence", "global_mobility", "global_talent", "none"):
                    if bucket == "none":
                        lead.score = 0
                        lead.reasons = ["AI triage: not a relevant immigration lead"]
                    else:
                        lead.lead_type = bucket
                        lead.score = int(ai.get("score", lead.score))
                        lead.reasons = ai.get("reasons", lead.reasons) or lead.reasons

                lead.ai_summary = ai.get("summary", "") or ""
                lead.ai_outreach_angle = ai.get("outreach_angle", "") or ""
                lead.ai_quote = ai.get("sponsorship_signal_quote", "") or ""
                openai_calls += 1

        # Mark seen
        storage.mark_seen(lead_id, lead.lead_type, lead.title, lead.url, now_iso)

        # Only allow “strong” leads if we have a real company signal
        if lead.score >= CFG.strong_threshold:
            if CFG.require_company_signal_for_strong and lead.lead_type in ("sponsor_licence", "global_mobility"):
                if not _has_company_signal(lead):
                    continue

            if lead.lead_type == "sponsor_licence":
                sponsor_strong.append(lead)
            elif lead.lead_type == "global_mobility":
                mobility_strong.append(lead)
            elif lead.lead_type == "global_talent":
                talent_strong.append(lead)

    sponsor_strong.sort(key=lambda x: x.score, reverse=True)
    mobility_strong.sort(key=lambda x: x.score, reverse=True)
    talent_strong.sort(key=lambda x: x.score, reverse=True)

    sponsor_strong = sponsor_strong[:CFG.max_strong_per_bucket]
    mobility_strong = mobility_strong[:CFG.max_strong_per_bucket]
    talent_strong = talent_strong[:CFG.max_strong_per_bucket]

    print(f"[STATS] processed={processed} skipped_seen={skipped_seen} pages_fetched={pages_fetched} openai_calls={openai_calls}")
    print(f"[LEADS] sponsor_strong={len(sponsor_strong)} mobility_strong={len(mobility_strong)} talent_strong={len(talent_strong)}")

    subject = f"CW Weekly Leads — {datetime.utcnow().strftime('%Y-%m-%d')}"
    if src_date:
        subject += f" (SponsorReg {src_date})"

    html = build_email(sponsor_strong, mobility_strong, talent_strong)

    print("[EMAIL] sending…")
    send_email(subject, html)
    print("[EMAIL] sent")

    end = datetime.utcnow()
    print(f"[DONE] finished @ {end.isoformat()}Z (duration {(end-start).total_seconds():.1f}s)")


if __name__ == "__main__":
    main()
