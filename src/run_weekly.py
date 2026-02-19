from __future__ import annotations

import os
import logging
import sys
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta, date
from typing import Any, Dict, List, Tuple
import html

from .config import load_config
from .companies_house import CHClient
from .cache_db import LeadCache
from .sponsor_register import SponsorRegister
from .normalize import approved_hub, is_uk_value, norm_text
from .scoring import Signals, score as score_fn, ALLOWLIST, DENYLIST
from .geo import infer_gb_nation
from .emailer import send_html_email

# -----------------------
# Heartbeat logging setup
# -----------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("cw-bot")


NAME_EXCLUDE_KEYWORDS = [
    # hard excludes you specified
    "properties",
    "property",
    "investments",
    "investment",
    "holding",
    "holdings",
    "management",
    "consulting",
    # extra conservative noise reducers
    "real estate",
    "estate",
]


@dataclass
class Lead:
    company_name: str
    company_number: str
    incorporation_date: str
    sic_codes: List[str]
    directors_count: int
    psc_count: int
    psc_types: List[str]
    town: str
    country: str
    ch_url: str
    sponsor_status: str
    score: int
    reasons: List[str]


def _contains_excluded_name(name: str) -> bool:
    n = norm_text(name)
    return any(kw in n for kw in NAME_EXCLUDE_KEYWORDS)


def _sic_hits(sic_codes: List[str]) -> Tuple[bool, int]:
    allow = any(code in ALLOWLIST for code in sic_codes)
    deny_hits = sum(1 for code in sic_codes if code in DENYLIST)
    return allow, deny_hits


def _company_age_days(inc_date: str) -> int:
    d = datetime.fromisoformat(inc_date).date()
    return (date.today() - d).days


def _list_all_pscs(ch: CHClient, company_number: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    start = 0
    while True:
        data = ch.psc_list(company_number, items_per_page=100, start_index=start)
        items = data.get("items") or []
        out.extend(items)
        if len(items) < 100:
            break
        start += 100
        if start > 1000:
            break
    return out


def _list_all_officers(ch: CHClient, company_number: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    start = 0
    while True:
        data = ch.officers_list(company_number, items_per_page=100, start_index=start)
        items = data.get("items") or []
        out.extend(items)
        if len(items) < 100:
            break
        start += 100
        if start > 2000:
            break
    return out


def _active_directors(officers: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    directors = []
    for o in officers:
        role = (o.get("officer_role") or "").lower()
        if role not in {"director", "corporate-director"}:
            continue
        if o.get("resigned_on"):
            continue
        directors.append(o)
    return directors


def _psc_signals(pscs: List[Dict[str, Any]]) -> Tuple[bool, bool, bool, int, List[str]]:
    """
    Return:
      corporate_psc, foreign_psc_hub, missing_any_country, active_psc_count, psc_types
    """
    corporate = False
    foreign_hub = False
    missing_any = False
    active_count = 0
    psc_types: List[str] = []

    for p in pscs:
        if p.get("ceased_on"):
            continue
        active_count += 1
        kind = (p.get("kind") or "").lower()

        if "corporate-entity" in kind or "legal-person" in kind:
            corporate = True
            if "corporate" not in psc_types:
                psc_types.append("corporate")
            continue

        if "individual" in kind:
            if "individual" not in psc_types:
                psc_types.append("individual")

            nat = p.get("nationality")
            cor = p.get("country_of_residence")

            nat_missing = (not nat) or (norm_text(nat) in {"", "unknown"})
            cor_missing = (not cor) or (norm_text(cor) in {"", "unknown"})
            if nat_missing and cor_missing:
                missing_any = True
                continue

            # Either indicator counts; UK nationality + foreign residence counts
            if (nat and (not is_uk_value(nat)) and approved_hub(nat)) or (
                cor and (not is_uk_value(cor)) and approved_hub(cor)
            ):
                foreign_hub = True

    return corporate, foreign_hub, missing_any, active_count, psc_types


def _director_signals(
    ch: CHClient, company_number: str, active_directors: List[Dict[str, Any]]
) -> Tuple[bool, bool, bool]:
    """
    Return:
      corporate_director, foreign_director_hub, missing_fields_seen

    NOTE: This can be expensive because it may call appointment endpoints.
    """
    corp = any((d.get("officer_role") or "").lower() == "corporate-director" for d in active_directors)
    foreign_hub = False
    missing = False

    for d in active_directors:
        if (d.get("officer_role") or "").lower() == "corporate-director":
            continue

        appt_id = d.get("appointment_id")
        if not appt_id:
            continue

        detail = ch.officer_appointment(company_number, appt_id)
        nat = detail.get("nationality")
        cor = detail.get("country_of_residence")

        nat_missing = (not nat) or (norm_text(nat) in {"", "unknown"})
        cor_missing = (not cor) or (norm_text(cor) in {"", "unknown"})
        if nat_missing and cor_missing:
            missing = True
            continue

        if (nat and (not is_uk_value(nat)) and approved_hub(nat)) or (
            cor and (not is_uk_value(cor)) and approved_hub(cor)
        ):
            foreign_hub = True
            break

    return corp, foreign_hub, missing


def _mid_size_ok(directors_count: int, psc_count: int, corporate_psc: bool, corporate_director: bool) -> bool:
    return (directors_count > 1) or (psc_count > 1) or corporate_psc or corporate_director


def _uk_name_bonus(company_name: str, corporate_psc: bool) -> bool:
    return (" uk " in f" {norm_text(company_name)} ") and corporate_psc


def build_html_email(leads: List[Lead], run_ts: datetime, window_from: str, window_to: str, stats: Dict[str, int]) -> str:
    def esc(x: str) -> str:
        return html.escape(x or "")

    cards = []
    for i, lead in enumerate(leads, start=1):
        reasons = "".join(f"<li>{esc(r)}</li>" for r in lead.reasons)
        sic = ", ".join(esc(x) for x in lead.sic_codes)
        psc_types = ", ".join(esc(x) for x in lead.psc_types) if lead.psc_types else "None/Unknown"

        cards.append(
            f"""
            <div style="padding:14px;border:1px solid #e5e7eb;border-radius:12px;margin:12px 0;">
              <div style="font-size:16px;font-weight:700;">{i}) {esc(lead.company_name)}
                <span style="font-weight:400;color:#6b7280;">({esc(lead.company_number)})</span>
              </div>
              <div style="margin-top:6px;color:#111827;">
                <b>Incorporated:</b> {esc(lead.incorporation_date)} &nbsp; | &nbsp;
                <b>Town/Country:</b> {esc(lead.town)}, {esc(lead.country)} &nbsp; | &nbsp;
                <b>Directors:</b> {lead.directors_count} &nbsp; | &nbsp;
                <b>PSCs:</b> {lead.psc_count} &nbsp; | &nbsp;
                <b>PSC types:</b> {psc_types}
              </div>
              <div style="margin-top:6px;color:#111827;"><b>SIC:</b> {sic or "—"}</div>
              <div style="margin-top:6px;">
                <a href="{esc(lead.ch_url)}">Companies House profile</a>
                &nbsp; | &nbsp;
                <b>Sponsor Register:</b> {esc(lead.sponsor_status)}
                &nbsp; | &nbsp;
                <b>Score:</b> {lead.score}
              </div>
              <div style="margin-top:8px;">
                <b>Why this scored high:</b>
                <ul style="margin-top:6px;">{reasons}</ul>
              </div>
            </div>
            """
        )

    summary = f"""
    <div style="padding:14px;border:1px solid #e5e7eb;border-radius:12px;">
      <div style="font-size:18px;font-weight:800;">CW Weekly Sponsor Leads — {run_ts.date().isoformat()} ({len(leads)})</div>
      <div style="margin-top:8px;color:#111827;">
        <b>Incorporation window:</b> {window_from} → {window_to}<br/>
        <b>Stats:</b> candidates_seen={stats.get("candidates_seen",0)}, cache_excluded={stats.get("cache_excluded",0)},
        sponsor_excluded={stats.get("sponsor_excluded",0)}, geo_excluded={stats.get("geo_excluded",0)},
        name_excluded={stats.get("name_excluded",0)}, sic_missing_excluded={stats.get("sic_missing_excluded",0)},
        qualified_scored={stats.get("qualified_scored",0)}
      </div>
      <div style="margin-top:10px;color:#6b7280;font-size:12px;">
        Public-source intelligence only. Always verify primary sources before outreach. Structured data only (Companies House + Sponsor Register).
      </div>
    </div>
    """
    return "<html><body style='font-family:Arial,Helvetica,sans-serif;'>" + summary + "".join(cards) + "</body></html>"


def main():
    log.info("Starting CW Structured Sponsor Leads Bot")

    # Speed knobs (safe defaults)
    TARGET_QUALIFIED_POOL = int(os.getenv("TARGET_QUALIFIED_POOL", "120"))
    MAX_EVAL_CANDIDATES = int(os.getenv("MAX_EVAL_CANDIDATES", "600"))
    MAX_SEEDED_CANDIDATES = int(os.getenv("MAX_SEEDED_CANDIDATES", "600"))

    cfg = load_config()
    os.makedirs(os.path.dirname(cfg.cache_path), exist_ok=True)

    ch = CHClient(cfg.companies_house_api_key)
    cache = LeadCache(cfg.cache_path)

    log.info("Loading Sponsor Register...")
    sponsor = SponsorRegister.load(cfg.sponsor_register_url)
    log.info("Sponsor Register loaded")

    today = date.today()
    window_to = today.isoformat()
    window_from = (today - timedelta(days=365)).isoformat()
    log.info(f"Incorporation window: {window_from} -> {window_to}")

    stats = {
        "candidates_seen": 0,
        "cache_excluded": 0,
        "sponsor_excluded": 0,
        "geo_excluded": 0,
        "name_excluded": 0,
        "sic_missing_excluded": 0,
        "qualified_scored": 0,
    }

    candidates: Dict[str, Dict[str, Any]] = {}
    log.info("Beginning Companies House advanced search (SIC allowlist seeding)...")
    log.info(
        f"Seeding caps: MAX_SEEDED_CANDIDATES={MAX_SEEDED_CANDIDATES} | "
        f"Eval caps: MAX_EVAL_CANDIDATES={MAX_EVAL_CANDIDATES}, TARGET_QUALIFIED_POOL={TARGET_QUALIFIED_POOL}"
    )

    for sic in sorted(ALLOWLIST):
        log.info(f"Searching SIC {sic}...")
        sic_before = len(candidates)

        for page in range(cfg.max_pages_per_sic):
            start_index = page * cfg.advanced_page_size
            data = ch.advanced_search(
                incorporated_from=window_from,
                incorporated_to=window_to,
                sic_codes=sic,
                company_status="active",
                start_index=start_index,
                size=cfg.advanced_page_size,
            )
            items = data.get("items") or []
            if not items:
                break

            for it in items:
                cn = it.get("company_number")
                if cn:
                    candidates.setdefault(cn, it)

            if len(items) < cfg.advanced_page_size:
                break

            if len(candidates) >= MAX_SEEDED_CANDIDATES:
                break

        sic_added = len(candidates) - sic_before
        log.info(f"SIC {sic} done. Added {sic_added} new candidates. Total candidates now {len(candidates)}")

        if len(candidates) >= MAX_SEEDED_CANDIDATES:
            log.info(f"Reached MAX_SEEDED_CANDIDATES={MAX_SEEDED_CANDIDATES}. Stopping SIC seeding early.")
            break

    log.info(f"Candidate pool size after seeding: {len(candidates)}")
    log.info("Evaluating candidates...")

    leads: List[Lead] = []

    for cn in list(candidates.keys()):
        stats["candidates_seen"] += 1

        # Heartbeat every 25
        if stats["candidates_seen"] % 25 == 0:
            log.info(
                f"Processed {stats['candidates_seen']} candidates | qualified={len(leads)} | "
                f"cache_excl={stats['cache_excluded']} sponsor_excl={stats['sponsor_excluded']} geo_excl={stats['geo_excluded']}"
            )

        # Hard cap evaluation count (speed)
        if stats["candidates_seen"] >= MAX_EVAL_CANDIDATES:
            log.info(f"Reached MAX_EVAL_CANDIDATES={MAX_EVAL_CANDIDATES}. Stopping evaluation early.")
            break

        if cache.has(cn):
            stats["cache_excluded"] += 1
            continue

        profile = ch.company_profile(cn)
        company_name = profile.get("company_name") or ""
        if _contains_excluded_name(company_name):
            stats["name_excluded"] += 1
            continue

        inc_date = profile.get("date_of_creation")
        if not inc_date:
            continue

        age_days = _company_age_days(inc_date)
        if age_days < 0 or age_days > 365:
            continue

        addr = profile.get("registered_office_address") or {}
        town = addr.get("locality") or addr.get("post_town") or ""
        country = addr.get("country") or ""
        postcode = addr.get("postal_code") or ""

        allowed_geo, inferred = infer_gb_nation(country, postcode)
        if not allowed_geo:
            stats["geo_excluded"] += 1
            continue

        sic_codes = [str(x) for x in (profile.get("sic_codes") or []) if str(x).strip()]
        if not sic_codes:
            stats["sic_missing_excluded"] += 1
            continue

        licensed, lic_reason = sponsor.is_licensed(company_name, town)
        if licensed:
            stats["sponsor_excluded"] += 1
            continue

        # PSC signals (strong, structured)
        pscs = _list_all_pscs(ch, cn)
        corporate_psc, foreign_psc_hub, psc_missing_any, psc_count, psc_types = _psc_signals(pscs)

        # Directors (active only)
        officers = _list_all_officers(ch, cn)
        directors = _active_directors(officers)
        directors_count = len(directors)

        corporate_director = any((d.get("officer_role") or "").lower() == "corporate-director" for d in directors)
        foreign_director_hub = False
        director_missing_any = False

        # Only do expensive appointment lookups if PSC didn't already qualify it
        if not corporate_psc and not foreign_psc_hub:
            corp_d, foreign_d, missing_d = _director_signals(ch, cn, directors)
            corporate_director = corporate_director or corp_d
            foreign_director_hub = foreign_d
            director_missing_any = missing_d

        # Strict foreign-linked qualification
        foreign_linked = corporate_psc or foreign_psc_hub or corporate_director or foreign_director_hub
        if not foreign_linked:
            continue

        # Missing data fallback
        if (psc_missing_any or director_missing_any) and not corporate_psc:
            continue

        # No PSC filings yet => exclude (per your rule)
        if psc_count == 0 and not corporate_psc:
            continue

        # Mid-size proxy must pass
        if not _mid_size_ok(directors_count, psc_count, corporate_psc, corporate_director):
            continue

        allow_hit, deny_hits = _sic_hits(sic_codes)
        uk_bonus = _uk_name_bonus(company_name, corporate_psc)

        sig = Signals(
            age_days=age_days,
            corporate_psc=corporate_psc,
            foreign_psc_hub=foreign_psc_hub,
            corporate_director=corporate_director,
            foreign_director_hub=foreign_director_hub,
            directors_count=directors_count,
            psc_count=psc_count,
            uk_in_name_and_corp_psc=uk_bonus,
            allowlist_hit=allow_hit,
            denylist_hits=deny_hits,
        )
        sc, reasons = score_fn(sig)

        leads.append(
            Lead(
                company_name=company_name,
                company_number=cn,
                incorporation_date=inc_date,
                sic_codes=sic_codes,
                directors_count=directors_count,
                psc_count=psc_count,
                psc_types=psc_types,
                town=town,
                country=inferred,
                ch_url=f"https://find-and-update.company-information.service.gov.uk/company/{cn}",
                sponsor_status=f"Not found ({lic_reason})",
                score=sc,
                reasons=reasons,
            )
        )
        stats["qualified_scored"] += 1

        # Quality-preserving early stop: once we have a strong pool to rank
        if len(leads) >= TARGET_QUALIFIED_POOL:
            log.info(f"Reached TARGET_QUALIFIED_POOL={TARGET_QUALIFIED_POOL}. Stopping evaluation early.")
            break

    leads.sort(key=lambda x: x.score, reverse=True)
    selected = leads[: cfg.max_leads]

    log.info(f"Scoring complete. Qualified leads: {len(leads)} | Selected: {len(selected)}")
    log.info("Sending email...")

    run_ts = datetime.now(timezone.utc)
    subject = f"CW Weekly Sponsor Leads — {run_ts.date().isoformat()} ({len(selected)})"
    html_body = build_html_email(selected, run_ts, window_from, window_to, stats)

    send_html_email(
        smtp_host=cfg.smtp_host,
        smtp_port=cfg.smtp_port,
        smtp_user=cfg.smtp_user,
        smtp_pass=cfg.smtp_pass,
        from_addr=cfg.email_from,
        to_addrs=cfg.email_to,
        subject=subject,
        html_body=html_body,
    )

    log.info("Email sent successfully. Updating cache...")
    cache.add_many([(l.company_number, l.company_name) for l in selected])
    cache.close()
    log.info("Cache updated. Run complete.")


if __name__ == "__main__":
    main()
