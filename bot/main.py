import os
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List

from .config import Config
from .http_client import make_session
from .persistence import Store, utc_now_iso
from .sponsor_register import load_and_filter
from .companies_house import (
    advanced_incorporated,
    company_profile,
    company_officers,
    company_psc,
    normalize_registered_office,
)
from .scoring import compute_score, bucket_from_score, classify_case_type, base_company_filters
from .enrichment import find_official_homepage, scrape_verified_contacts
from .job_intel import score_hiring_intent
from .reporting import build_html, build_csv
from .emailer import send_email_smtp
from .utils import clean_display_name, org_key


def make_lead_key(source: str, company_number: str, company_name: str, town: str) -> str:
    if company_number:
        return f"CH::{company_number}"
    return f"{source}::NAME::{(company_name or '').upper()}::TOWN::{(town or '').upper()}"


def visa_hint_for(case_type: str, source: str, sponsor_route: str) -> str:
    if source == "SPONSOR_REGISTER":
        return sponsor_route
    if "Expansion" in case_type:
        return "Likely UK Expansion Worker / expansion planning"
    if "GBM" in case_type:
        return "Likely GBM Senior/Specialist Worker (intra-group transfer)"
    if "Sponsor Licence" in case_type:
        return "Likely sponsor licence + Skilled Worker planning"
    return "Watchlist"


def main():
    cfg = Config()

    ch_key = os.environ.get("COMPANIES_HOUSE_API_KEY", "").strip()
    serp_key = os.environ.get("SERPAPI_API_KEY", "").strip()
    smtp_host = os.environ.get("SMTP_HOST", "").strip()
    smtp_port = int(os.environ.get("SMTP_PORT", "587"))
    smtp_user = os.environ.get("SMTP_USER", "").strip()
    smtp_pass = os.environ.get("SMTP_PASS", "").strip()
    email_from = os.environ.get("EMAIL_FROM", "").strip()
    email_to = os.environ.get("EMAIL_TO", "").strip()

    if not ch_key:
        raise RuntimeError("COMPANIES_HOUSE_API_KEY missing.")
    if not smtp_host or not email_from or not email_to:
        raise RuntimeError("SMTP_HOST/EMAIL_FROM/EMAIL_TO missing.")

    session = make_session(retry_total=cfg.ch_retry_count)
    store = Store()
    run_ts = utc_now_iso()

    # Sponsor register (exclusion list only: already-licensed sponsors are NOT our target)
    filtered_rows, _filtered_count = load_and_filter(session, cfg)
    sponsor_name_keys = {org_key(f["name"]) for _row, f in filtered_rows if f.get("name")}

    # Companies House stream (recent incorporations)
    end = datetime.now(timezone.utc).date()
    start = (datetime.now(timezone.utc) - timedelta(days=cfg.lookback_days)).date()

    items = advanced_incorporated(
        session,
        ch_key,
        inc_from=str(start),
        inc_to=str(end),
        size=100,
        max_total=cfg.ch_max_results_total,
        timeout=cfg.ch_search_timeout,
    )

    ch_candidates: List[Dict[str, Any]] = []
    for it in items[: cfg.ch_max_companies_to_check]:
        num = it.get("company_number") or ""
        title = clean_display_name(it.get("company_name") or it.get("title") or "")
        if not num or not title:
            continue
        try:
            prof = company_profile(session, ch_key, num, timeout=cfg.ch_search_timeout)
        except Exception:
            continue
        keep, _drop = base_company_filters(prof)
        if not keep:
            continue
        ch_candidates.append({"company_number": num, "company_name": title, "profile": prof})

    leads: List[Dict[str, Any]] = []

    # CH leads
    for c in ch_candidates:
        num = c["company_number"]
        prof = c["profile"]
        title = c["company_name"]

        ro = prof.get("registered_office_address") or {}
        reg_addr, reg_postcode, reg_town, _reg_country = normalize_registered_office(ro)
        incorporated = prof.get("date_of_creation") or ""

        # Exclude existing sponsors (not our target)
        if org_key(title) in sponsor_name_keys:
            continue

        try:
            officers = company_officers(session, ch_key, num, timeout=cfg.ch_officers_timeout)
        except Exception:
            officers = []

        score_pre, _why_pre, _countries_pre = compute_score(
            cfg,
            source="COMPANIES_HOUSE",
            sponsor_route="",
            profile=prof,
            officers=officers,
            psc_items=[],
        )

        psc_items = []
        if score_pre >= 45:
            try:
                psc_items = company_psc(session, ch_key, num, timeout=cfg.ch_officers_timeout)
            except Exception:
                psc_items = []

        # Initial score (pre-enrichment); updated once we discover website + hiring signals.
        score, why_list, countries = compute_score(
            cfg,
            source="COMPANIES_HOUSE",
            sponsor_route="",
            profile=prof,
            officers=officers,
            psc_items=psc_items,
        )

        bucket = bucket_from_score(cfg, score)
        case_type = classify_case_type(
            source="COMPANIES_HOUSE",
            sponsor_route="",
            score=score,
            countries=countries,
            psc_items=psc_items,
        )
        lead_key = make_lead_key("COMPANIES_HOUSE", num, title, reg_town)

        lead = {
            "lead_key": lead_key,
            "source": "COMPANIES_HOUSE",
            "company_number": num,
            "company_name": title,
            "score": score,
            "bucket": bucket,
            "case_type": case_type,
            "visa_hint": visa_hint_for(case_type, "COMPANIES_HOUSE", ""),
            "why": "; ".join(why_list[:7]),
            "reg_address": reg_addr,
            "reg_postcode": reg_postcode,
            "reg_town": reg_town,
            "incorporated": incorporated,
            "website": "",
            "website_confidence": 0,
            "emails": "",
            "phones": "",
            "contact_source_url": "",
            "job_intent": 0,
            "job_sources": "",
        }

        if not store.is_do_not_contact(lead_key):
            store.upsert_lead(lead)
            leads.append(lead)

    # Dedup
    best: Dict[str, Dict[str, Any]] = {}
    for l in leads:
        k = l["lead_key"]
        if k not in best or l.get("score", 0) > best[k].get("score", 0):
            best[k] = l

    leads = list(best.values())
    leads.sort(
        key=lambda x: (x.get("score", 0), x.get("source", "") != "SPONSOR_REGISTER"),
        reverse=True,
    )

    # Enrichment
    serp_budget = {"calls": 0, "cap": cfg.serp_max_calls_per_run}
    verified_sites = 0

    if serp_key:
        # Stage A: discover likely official homepage candidates using SerpAPI
        stage_a = [l for l in leads if l.get("company_name")][: cfg.serp_stage_a_limit]

        for l in stage_a:
            if serp_budget["calls"] >= serp_budget["cap"]:
                break

            # Allow enrichment for top WATCH leads; Serp is what creates intent signals.
            # Only skip very low-scoring noise to save budget.
            if l.get("score", 0) < 12 and l.get("source") != "SPONSOR_REGISTER":
                continue

            candidates = find_official_homepage(
                session,
                serp_key,
                company_name=l["company_name"],
                reg_postcode=l.get("reg_postcode", ""),
                town=l.get("reg_town", ""),
                serp_sleep=cfg.serp_sleep_seconds,
                serp_budget=serp_budget,
            )

            if candidates:
                l["_homepage_candidates"] = candidates

        # Stage B: scrape verified contacts from the best homepage candidates
        stage_b = [l for l in leads if l.get("_homepage_candidates")][: cfg.serp_stage_b_limit]

        for l in stage_b:
            for base_url in l.get("_homepage_candidates", [])[:2]:
                website, conf, emails, phones, src = scrape_verified_contacts(
                    session,
                    cfg,
                    company_name=l["company_name"],
                    company_number=l.get("company_number", ""),
                    reg_postcode=l.get("reg_postcode", ""),
                    base_url=base_url,
                )

                if not website:
                    continue

                l["website"] = website
                l["website_confidence"] = conf
                l["contact_source_url"] = src

                # Hiring intent scan (snippets only)
                ji, jr, jurls = score_hiring_intent(
                    session,
                    cfg,
                    serp_key,
                    company_name=l.get("company_name", ""),
                    website=website,
                    serp_sleep=cfg.serp_sleep_seconds,
                    serp_budget=serp_budget,
                )

                if ji:
                    l["job_intent"] = ji
                    l["job_sources"] = ", ".join(jurls)
                    # Boost score with intent; keep within 0..100
                    l["score"] = min(100, int(l.get("score", 0)) + int(ji))
                    l["why"] = (l.get("why", "") + "; " + "; ".join(jr)).strip("; ")

                # Only publish contacts once verification threshold is met
                if conf >= cfg.verify_min_score:
                    l["emails"] = emails
                    l["phones"] = phones
                    verified_sites += 1
                    l["score"] = min(100, int(l.get("score", 0)) + 4)
                    l["bucket"] = bucket_from_score(cfg, int(l["score"]))

                store.upsert_lead(l)
                break

    # Backfill + output cap
    output: List[Dict[str, Any]] = leads[: cfg.max_output_leads]
    backfill_count = 0

    if len(output) < cfg.min_total_leads_target:
        hist = store.fetch_recent_leads(days=cfg.lookback_days, limit=400)
        present = {o.get("lead_key") for o in output}
        for h in hist:
            if len(output) >= cfg.max_output_leads:
                break
            if h.get("lead_key") in present:
                continue
            if store.is_do_not_contact(h.get("lead_key", "")):
                continue
            h["why"] = (h.get("why", "") + " | Backfill (not new)").strip(" |")
            output.append(h)
            backfill_count += 1

    output.sort(key=lambda x: x.get("score", 0), reverse=True)
    output = output[: cfg.max_output_leads]

    meta = {
        "run_ts_utc": run_ts,
        "lookback_days": cfg.lookback_days,
        "sponsor_register_exclusions": len(sponsor_name_keys),
        "ch_candidates_considered": len(ch_candidates),
        "serp_calls": serp_budget["calls"],
        "verified_sites": verified_sites,
    }

    html = build_html(meta, output, backfill_count=backfill_count)
    csv = build_csv(output)

    subject = f"Sponsor Licence Leads — Intelligence Brief ({run_ts.split('T')[0]})"
    send_email_smtp(
        host=smtp_host,
        port=smtp_port,
        user=smtp_user,
        password=smtp_pass,
        mail_from=email_from,
        mail_to=email_to,
        subject=subject,
        html=html,
        attachment_name="uk-expansion-leads.csv",
        attachment_csv=csv,
    )

    store.close()
    print("Done. Leads emailed:", len(output))


if __name__ == "__main__":
    main()
