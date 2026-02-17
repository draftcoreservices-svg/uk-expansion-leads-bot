from datetime import datetime
from urllib.parse import urlparse

import threading
import time

from .ats_company_lookup import extract_company_website_from_ats
from .companies_house import CompaniesHouseClient
from .config import CFG
from .email_report import build_email
from .emailer import send_email
from .extract import extract_contacts
from .leads import Lead
from .openai_classifier import classify_lead, enabled as openai_enabled
from .scoring import score_heuristic
from .serp import SerpClient
from .sponsor_register import refresh_sponsor_register, is_on_sponsor_register
from .storage import Storage
from .text_extract import fetch_page_text


FREE_EMAIL_DOMAINS = {
    "gmail.com", "hotmail.com", "outlook.com", "live.com", "yahoo.com", "icloud.com",
    "proton.me", "protonmail.com", "aol.com", "gmx.com"
}

_current_step = "boot"

def _set_step(s: str) -> None:
    global _current_step
    _current_step = s
    print(f"[STEP] {_current_step}", flush=True)

def _watchdog(interval_s: int = 30) -> None:
    # Periodically emits a heartbeat so GitHub Actions shows progress even if a network call blocks.
    while True:
        time.sleep(interval_s)
        print(f"[HEARTBEAT] still running | step={_current_step}", flush=True)


def _denied(url: str) -> bool:
    try:
        u = urlparse(url)
        host = (u.netloc or "").lower()

        deny_tlds = getattr(CFG, "deny_tlds", set())
        deny_domains = getattr(CFG, "deny_domains", set())

        if any(host.endswith(tld) for tld in deny_tlds):
            return True

        for d in deny_domains:
            if host == d or host.endswith("." + d):
                return True

        return False
    except Exception:
        return True


def _content_excluded(title: str, snippet: str) -> str | None:
    phrases = getattr(CFG, "content_exclude_phrases", [])
    t = f"{title} {snippet}".lower()
    for p in phrases:
        if (p or "").lower() in t:
            return p
    return None


def _label_from_title(title: str) -> str:
    if not title:
        return ""
    t = title.strip()
    t = t.replace("–", "-").replace("—", "-")
    # keep first segment if looks like “Company - Job title”
    if " - " in t:
        return t.split(" - ", 1)[0].strip()
    if " | " in t:
        return t.split(" | ", 1)[0].strip()
    return t[:80]


def _safe_company_domain(email: str) -> bool:
    try:
        dom = (email.split("@", 1)[1] or "").lower().strip()
        return dom and dom not in FREE_EMAIL_DOMAINS
    except Exception:
        return False


def main():
    # Start watchdog early to avoid "no output" hangs in GitHub Actions.
    t = threading.Thread(target=_watchdog, daemon=True)
    t.start()

    start = datetime.utcnow()
    _set_step(f"start @ {start.isoformat()}Z")
    print(f"[START] CW weekly run @ {start.isoformat()}Z", flush=True)
    print(
        f"[ENV] SERP key set={bool(__import__('os').environ.get('SERPAPI_API_KEY') or __import__('os').environ.get('SERPAPI_KEY') or __import__('os').environ.get('SERP_API_KEY'))}",
        flush=True,
    )
    print(f"[ENV] OPENAI_API_KEY set={bool(__import__('os').environ.get('OPENAI_API_KEY'))}", flush=True)
    print(f"[ENV] COMPANIES_HOUSE_API_KEY set={bool(__import__('os').environ.get('COMPANIES_HOUSE_API_KEY'))}", flush=True)

    _set_step("init storage")
    storage = Storage("cache.sqlite")
    print("[OK] Storage initialised", flush=True)

    _set_step("init serp client")
    serp = SerpClient()
    print("[OK] SerpClient initialised", flush=True)

    _set_step("init companies house client")
    ch = CompaniesHouseClient()
    print("[OK] CompaniesHouseClient initialised", flush=True)

    _set_step("refresh sponsor register")

    # 1) Refresh sponsor register
    try:
        updated, src_date = refresh_sponsor_register(storage)
        print(f"[OK] Sponsor Register refreshed updated={updated} src_date={src_date}", flush=True)
    except Exception as e:
        print(f"[WARN] Sponsor Register refresh failed: {e}", flush=True)
        src_date = ""

    # 2) Serp searches
    _set_step("load queries")
    queries = getattr(CFG, "queries", {})
    leads: list[Lead] = []

    _set_step("run serp searches")
    for bucket, qlist in queries.items():
        for q in qlist:
            q = (q or "").strip()
            if not q:
                continue
            print(f"[SEARCH] bucket={bucket} q={q}", flush=True)
            try:
                items = serp.search(q, num=getattr(CFG, "serp_num", 10))
            except Exception as e:
                print(f"[WARN] Serp search failed q={q}: {e}", flush=True)
                continue

            for it in items:
                title = it.get("title", "")
                link = it.get("link", "")
                snippet = it.get("snippet", "")
                if not link or _denied(link):
                    continue

                excl = _content_excluded(title, snippet)
                if excl:
                    continue

                label = _label_from_title(title)
                leads.append(
                    Lead(
                        bucket=bucket,
                        label=label,
                        url=link,
                        title=title,
                        snippet=snippet,
                        score=0,
                    )
                )

    print(f"[STATS] serp_leads={len(leads)}", flush=True)

    # Deduplicate by URL
    seen = set()
    deduped: list[Lead] = []
    for l in leads:
        if l.url in seen:
            continue
        seen.add(l.url)
        deduped.append(l)
    leads = deduped
    print(f"[STATS] deduped_leads={len(leads)}", flush=True)

    # 3) Enrichment
    enriched: list[Lead] = []
    for lead in leads:
        url = lead.url
        print(f"[ENRICH] {lead.bucket} | {lead.label} | {url}", flush=True)

        try:
            _set_step(f"fetch page {url}")
            final_url, page_text = fetch_page_text(url)
            lead.final_url = final_url
            lead.page_text = page_text
        except Exception as e:
            print(f"[WARN] fetch failed url={url}: {e}", flush=True)
            lead.page_text = ""
            lead.final_url = url

        # ATS site extraction
        try:
            site = extract_company_website_from_ats(lead.final_url or url, lead.page_text or "")
            if site and not _denied(site):
                lead.company_website = site
        except Exception:
            pass

        # Contacts
        try:
            contacts = extract_contacts((lead.company_website or lead.final_url or url), lead.page_text or "")
            lead.emails = sorted(set(contacts.get("emails", []) or []))
            lead.phones = sorted(set(contacts.get("phones", []) or []))
        except Exception:
            lead.emails = lead.emails or []
            lead.phones = lead.phones or []

        # Sponsor register check (only meaningful for sponsor bucket, but safe to store)
        try:
            on_reg = False
            if lead.label:
                on_reg = is_on_sponsor_register(storage, lead.label)
            lead.on_sponsor_register = bool(on_reg)
        except Exception:
            lead.on_sponsor_register = None

        # Companies House (best-effort; conservative match returns None if unsure)
        try:
            if lead.label:
                ch_hit = ch.search_company(lead.label)
                lead.companies_house = ch_hit
        except Exception as e:
            print(f"[WARN] Companies House lookup failed label={lead.label}: {e}", flush=True)
            lead.companies_house = None

        # Heuristic score
        try:
            lead.score = score_heuristic(lead)
        except Exception:
            lead.score = 0

        # Optional AI refinement
        if openai_enabled():
            try:
                ai = classify_lead(
                    lead_type_hint=lead.bucket,
                    label=lead.label or "",
                    url=lead.final_url or lead.url,
                    title=lead.title or "",
                    snippet=lead.snippet or "",
                    page_text=lead.page_text or "",
                )
                lead.ai = ai or None
            except Exception:
                lead.ai = None

        enriched.append(lead)

    leads = enriched
    _set_step("scoring and filtering")
    print(f"[STATS] total_leads={len(leads)}", flush=True)

    # 4) Strong lead selection
    sponsor_strong = [l for l in leads if l.bucket == "sponsor" and (l.score or 0) >= getattr(CFG, "min_score_sponsor", 70)]
    mobility_strong = [l for l in leads if l.bucket == "mobility" and (l.score or 0) >= getattr(CFG, "min_score_mobility", 70)]
    talent_strong = [l for l in leads if l.bucket == "talent" and (l.score or 0) >= getattr(CFG, "min_score_talent", 75)]

    # Prefer corporate emails (if present)
    for l in sponsor_strong + mobility_strong + talent_strong:
        if l.emails:
            l.emails = sorted(l.emails, key=lambda e: (not _safe_company_domain(e), e))

    print(f"[STATS] sponsor_strong={len(sponsor_strong)} mobility_strong={len(mobility_strong)} talent_strong={len(talent_strong)}", flush=True)

    subject = f"CW Weekly Leads — {datetime.utcnow().strftime('%Y-%m-%d')}"
    if src_date:
        subject += f" (SponsorReg {src_date})"

    _set_step("build email")
    html = build_email(sponsor_strong, mobility_strong, talent_strong)

    print("[EMAIL] sending…", flush=True)
    _set_step("send email")
    send_email(subject, html)
    print("[EMAIL] sent", flush=True)

    end = datetime.utcnow()
    print(f"[DONE] finished @ {end.isoformat()}Z (duration {(end-start).total_seconds():.1f}s)", flush=True)


if __name__ == "__main__":
    main()
