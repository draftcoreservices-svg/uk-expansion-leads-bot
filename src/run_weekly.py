"""
CW Weekly Leads Bot — Companies House shortlist engine (Sponsor Licence targeting)

Spec (Rushi):
- England/Wales/Scotland only
- Incorporated within last 12 months (rolling)
- Must be foreign-linked (strict country list), OR corporate PSC/director
- Target SIC allowlist + light denylist penalty
- Exclude if sponsor licence already present
- Exclude by company name keywords
- Exactly 30 leads per run
- Cache 'seen' companies in sqlite so we don't repeat leads
- Weekly email + manual run support (handled by GitHub Actions schedule + workflow_dispatch)
"""

from __future__ import annotations

import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Set, Tuple

import requests

from .sources import sponsor_register as sponsor
from .emailer import send_email
from . import db


# -----------------------------
# Config (per your instructions)
# -----------------------------

APPROVED_COUNTRIES = {
    "India", "United States", "USA", "United States of America",
    "China", "United Arab Emirates", "UAE",
    "Australia", "Japan", "South Korea", "Korea, Republic of", "Republic of Korea",
    "Canada", "Singapore", "Hong Kong",
    "Switzerland", "Germany", "France", "Netherlands",
    "Ireland", "Luxembourg",
    "Saudi Arabia", "Qatar",
    "Israel", "Taiwan", "New Zealand",
}

# England/Wales/Scotland only
ALLOWED_UK_ADDRESS_COUNTRIES = {"England", "Wales", "Scotland"}

# Exclude by company name keywords (whole word-ish)
EXCLUDE_NAME_KEYWORDS = {"properties", "investments", "holding", "management", "consulting"}

# Target SIC allowlist (core industries you listed)
# Source list: Companies House SIC condensed list (SIC 2007). Keep this list tight but multi-sector.
SIC_ALLOWLIST = {
    # Tech / software / data
    "58290", "62012", "62020", "62030", "62090", "63110", "63120",
    # Pharma / biotech / life sciences
    "21100", "21200", "72110", "72190", "46460",
    # Engineering / manufacturing (selected)
    "25620", "26110", "26200", "26309", "26511", "26600", "27110", "27900",
    "28110", "28290", "28990",
    # Import/export / wholesale trade (selected)
    "46190", "46510", "46520", "46900", "46460",
    # Finance / fintech
    "64110", "64191", "64999", "66190",
    # Private healthcare
    "86210", "86220", "86900",
}

# Denylist SICs (light penalty only; do NOT hard-exclude because overlap happens)
SIC_DENYLIST = {
    "68100", "68209", "68320",  # property
    "41100", "41201", "41202", "43310", "43320", "43330", "43390", "43999",  # small construction
    "56103", "56101", "56302",  # takeaway/pub
    "96020", "96090",           # hair/beauty/other personal
    "81210", "81299",           # cleaning
    "47190", "47290", "47710", "47799", "47890",  # general retail
    "82990",                    # generic business support (often noise)
}

TARGET_LEADS = 30

CH_API_BASE = "https://api.company-information.service.gov.uk"


# -----------------------------
# Helpers
# -----------------------------

def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip()).lower()


def has_excluded_keyword(company_name: str) -> bool:
    n = norm(company_name)
    for kw in EXCLUDE_NAME_KEYWORDS:
        if re.search(rf"\b{re.escape(kw)}\b", n):
            return True
    return False


def parse_country(val: Optional[str]) -> Optional[str]:
    if not val:
        return None
    v = val.strip()
    # normalise a few common variants
    if v.lower() in {"uk", "u.k.", "united kingdom", "great britain", "gb"}:
        return "United Kingdom"
    if v.lower() in {"uae", "u.a.e.", "united arab emirates"}:
        return "United Arab Emirates"
    if v.lower() in {"usa", "u.s.a.", "united states", "united states of america"}:
        return "United States"
    if v.lower() in {"republic of korea", "korea, republic of", "south korea"}:
        return "South Korea"
    return v


def in_approved_country(val: Optional[str]) -> bool:
    c = parse_country(val)
    if not c:
        return False
    return c in APPROVED_COUNTRIES


def safe_get(d: Dict, path: List[str], default=None):
    cur = d
    for p in path:
        if not isinstance(cur, dict) or p not in cur:
            return default
        cur = cur[p]
    return cur


# -----------------------------
# SQLite cache: "seen companies"
# -----------------------------

def ensure_seen_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS seen_companies (
            company_number TEXT PRIMARY KEY,
            first_seen_utc TEXT NOT NULL
        )
    """)
    conn.commit()


def load_seen(conn) -> Set[str]:
    ensure_seen_table(conn)
    rows = conn.execute("SELECT company_number FROM seen_companies").fetchall()
    return {r[0] for r in rows}


def mark_seen(conn, company_numbers: List[str]):
    ensure_seen_table(conn)
    now = utc_now().isoformat()
    for cn in company_numbers:
        conn.execute(
            "INSERT OR IGNORE INTO seen_companies(company_number, first_seen_utc) VALUES (?, ?)",
            (cn, now),
        )
    conn.commit()


# -----------------------------
# Companies House client
# -----------------------------

@dataclass
class CHCompanyCandidate:
    company_number: str
    company_name: str
    date_of_creation: Optional[str]
    sic_codes: List[str]


class CompaniesHouseClient:
    def __init__(self, api_key: str, session: Optional[requests.Session] = None):
        self.api_key = api_key
        self.s = session or requests.Session()
        self.s.auth = (api_key, "")
        self.s.headers.update({"User-Agent": "CW-LeadsBot/1.0 (Cromwell Wilkes)"})

    def advanced_search(self, incorporated_from: str, incorporated_to: str, sic_codes: List[str], size: int = 2000) -> List[CHCompanyCandidate]:
        params = {
            "incorporated_from": incorporated_from,
            "incorporated_to": incorporated_to,
            "company_status": "active",
            "size": str(size),
        }
        # multiple sic_codes supported: sic_codes=xxx&sic_codes=yyy
        for code in sic_codes:
            params.setdefault("sic_codes", [])
            params["sic_codes"].append(code)

        url = f"{CH_API_BASE}/advanced-search/companies"
        r = self.s.get(url, params=params, timeout=30)
        if r.status_code == 404:
            return []
        r.raise_for_status()
        data = r.json()

        out: List[CHCompanyCandidate] = []
        for item in data.get("items", []) or []:
            out.append(
                CHCompanyCandidate(
                    company_number=item.get("company_number") or "",
                    company_name=item.get("company_name") or "",
                    date_of_creation=item.get("date_of_creation"),
                    sic_codes=item.get("sic_codes") or [],
                )
            )
        return [c for c in out if c.company_number and c.company_name]

    def company_profile(self, company_number: str) -> Dict:
        url = f"{CH_API_BASE}/company/{company_number}"
        r = self.s.get(url, timeout=30)
        r.raise_for_status()
        return r.json()

    def officers(self, company_number: str) -> List[Dict]:
        url = f"{CH_API_BASE}/company/{company_number}/officers"
        r = self.s.get(url, params={"items_per_page": "100"}, timeout=30)
        if r.status_code == 404:
            return []
        r.raise_for_status()
        return (r.json().get("items") or [])

    def psc_list(self, company_number: str) -> List[Dict]:
        url = f"{CH_API_BASE}/company/{company_number}/persons-with-significant-control"
        r = self.s.get(url, timeout=30)
        if r.status_code == 404:
            return []
        r.raise_for_status()
        return (r.json().get("items") or [])


# -----------------------------
# Qualification + scoring
# -----------------------------

@dataclass
class Lead:
    company_name: str
    company_number: str
    incorporation_date: str
    sic_codes: List[str]
    directors_count: int
    psc_count: int
    psc_types: List[str]
    address_country: str
    address_town: str
    ch_url: str
    sponsor_status: str  # "Not found"
    score: int
    why: List[str]


def officer_is_director(off: Dict) -> bool:
    role = (off.get("officer_role") or "").lower()
    return "director" in role


def officer_is_corporate(off: Dict) -> bool:
    # Companies House uses "corporate-director" and corporate officer payloads can differ
    role = (off.get("officer_role") or "").lower()
    if "corporate" in role:
        return True
    # fallback heuristic: company_number present sometimes on corporate officers
    if off.get("company_number"):
        return True
    name = (off.get("name") or "").lower()
    if any(name.endswith(s) for s in [" ltd", " limited", " plc", " llp"]):
        return True
    return False


def get_officer_country(off: Dict) -> Optional[str]:
    # nationality & country_of_residence are optional
    return off.get("country_of_residence") or off.get("nationality")


def psc_is_corporate(psc: Dict) -> bool:
    kind = (psc.get("kind") or "").lower()
    return ("corporate" in kind) or ("legal-person" in kind) or ("beneficial-owner" in kind and "corporate" in kind)


def get_psc_country(psc: Dict) -> Optional[str]:
    # Prefer country_of_residence; fallback to nationality; for corporates use identification.country_registered/place_registered if present
    c = psc.get("country_of_residence") or psc.get("nationality")
    if c:
        return c
    ident = psc.get("identification") or {}
    return ident.get("country_registered") or ident.get("place_registered")


def within_last_12_months(date_str: str, now: datetime) -> bool:
    try:
        d = datetime.fromisoformat(date_str).replace(tzinfo=timezone.utc)
    except Exception:
        return False
    return (now - timedelta(days=365)) <= d <= now


def age_bucket_points(date_str: str, now: datetime) -> int:
    try:
        d = datetime.fromisoformat(date_str).replace(tzinfo=timezone.utc)
    except Exception:
        return 0
    days = (now - d).days
    # bias towards 6–12 months (your insight)
    if days < 90:
        return 3
    if days < 180:
        return 6
    if days < 270:
        return 10
    return 12


def qualifies_foreign_link(pscs: List[Dict], officers: List[Dict]) -> Tuple[bool, Dict]:
    """
    Strict rules (Rushi):
    - Any PSC non-UK person (but only if from approved list)
    - Any PSC corporate entity (UK or non-UK) qualifies
    - Any director non-UK person (but only if from approved list)
    - Any corporate director qualifies
    - If nationality/country missing => allowed ONLY if corporate PSC present
    """
    corporate_psc = any(psc_is_corporate(p) for p in pscs)
    foreign_psc_approved = False
    missing_psc_country = False

    for p in pscs:
        if psc_is_corporate(p):
            continue
        c = get_psc_country(p)
        if not c:
            missing_psc_country = True
            continue
        # strict: only approved hubs count
        if in_approved_country(c):
            # must be non-UK in spirit; approved list doesn't include UK anyway
            foreign_psc_approved = True

    directors = [o for o in officers if officer_is_director(o)]
    corporate_director = any(officer_is_corporate(o) for o in directors)

    foreign_director_approved = False
    missing_dir_country = False
    for o in directors:
        if officer_is_corporate(o):
            continue
        c = get_officer_country(o)
        if not c:
            missing_dir_country = True
            continue
        if in_approved_country(c):
            foreign_director_approved = True

    # strict qualification
    qualifies = (
        corporate_psc
        or corporate_director
        or foreign_psc_approved
        or foreign_director_approved
    )

    # missing country rule: allowed only if corporate PSC present
    if qualifies:
        return True, {
            "corporate_psc": corporate_psc,
            "foreign_psc_approved": foreign_psc_approved,
            "corporate_director": corporate_director,
            "foreign_director_approved": foreign_director_approved,
            "missing_psc_country": missing_psc_country,
            "missing_dir_country": missing_dir_country,
        }

    # if nothing qualifies, but countries missing: only allow if corporate_psc (already handled above)
    return False, {
        "corporate_psc": corporate_psc,
        "foreign_psc_approved": foreign_psc_approved,
        "corporate_director": corporate_director,
        "foreign_director_approved": foreign_director_approved,
        "missing_psc_country": missing_psc_country,
        "missing_dir_country": missing_dir_country,
    }


def score_company(
    company_name: str,
    incorporation_date: str,
    sic_codes: List[str],
    officers: List[Dict],
    pscs: List[Dict],
    foreign_meta: Dict,
) -> Tuple[int, List[str]]:
    why: List[str] = []
    score = 0
    now = utc_now()

    # Age points
    ap = age_bucket_points(incorporation_date, now)
    score += ap
    why.append(f"Incorporation timing signal (+{ap})")

    directors = [o for o in officers if officer_is_director(o)]
    directors_count = len(directors)
    psc_count = len(pscs)

    if directors_count > 1:
        score += 12
        why.append("More than 1 director (+12)")
    if psc_count > 1:
        score += 10
        why.append("More than 1 PSC (+10)")

    if foreign_meta.get("corporate_psc"):
        score += 28
        why.append("Corporate PSC present (+28)")
    if foreign_meta.get("foreign_psc_approved"):
        score += 24
        why.append("Foreign PSC from approved trading hub (+24)")
    if foreign_meta.get("corporate_director"):
        score += 18
        why.append("Corporate director present (+18)")
    if foreign_meta.get("foreign_director_approved"):
        score += 14
        why.append("Foreign director from approved trading hub (+14)")

    # "UK Ltd" + corporate PSC bonus
    if "uk" in norm(company_name) and foreign_meta.get("corporate_psc"):
        score += 8
        why.append('Name includes "UK" + corporate PSC (+8)')

    # SIC denylist penalty (light)
    deny_hits = [c for c in sic_codes if c in SIC_DENYLIST]
    if deny_hits:
        score -= 12
        why.append("Some low-signal SIC codes present (-12)")

    return score, why


# -----------------------------
# Email report
# -----------------------------

def render_email(leads: List[Lead], run_date: str) -> str:
    def esc(s: str) -> str:
        return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    rows = []
    for i, L in enumerate(leads, start=1):
        why_html = "<br/>".join(f"• {esc(x)}" for x in L.why[:6])
        psc_types = ", ".join(L.psc_types) if L.psc_types else "—"
        rows.append(f"""
        <tr>
          <td>{i}</td>
          <td><b>{esc(L.company_name)}</b><br/>
              <a href="{esc(L.ch_url)}">{esc(L.company_number)}</a><br/>
              Incorp: {esc(L.incorporation_date)}<br/>
              SIC: {esc(", ".join(L.sic_codes) if L.sic_codes else "—")}<br/>
              Address: {esc(L.address_town)} ({esc(L.address_country)})<br/>
          </td>
          <td>
              Directors: {L.directors_count}<br/>
              PSCs: {L.psc_count}<br/>
              PSC types: {esc(psc_types)}<br/>
              Sponsor: {esc(L.sponsor_status)}
          </td>
          <td><b>{L.score}</b><br/>{why_html}</td>
        </tr>
        """)

    html = f"""
    <html>
    <body>
      <h2>CW Weekly Leads — {esc(run_date)}</h2>
      <p><i>Public-source intelligence only. Always verify before outreach. Use responsibly.</i></p>
      <p><b>Exactly {TARGET_LEADS} companies</b> — England/Wales/Scotland — incorporated within last 12 months — foreign-linked (strict hubs) — target SICs — not on sponsor register — not previously seen.</p>

      <table border="1" cellspacing="0" cellpadding="8" style="border-collapse:collapse; font-family: Arial, sans-serif; font-size: 13px;">
        <tr style="background:#f2f2f2;">
          <th>#</th>
          <th>Company</th>
          <th>Checks</th>
          <th>Score & Why</th>
        </tr>
        {''.join(rows) if rows else '<tr><td colspan="4">No leads found this run.</td></tr>'}
      </table>

      <p style="margin-top:16px; color:#666;">
        Notes: Sponsor Register matching is best-effort (exact + fuzzy). Companies House data may have missing nationality/country fields; where missing, we only allow if a corporate PSC is present (per spec).
      </p>
    </body>
    </html>
    """
    return html


# -----------------------------
# Main run
# -----------------------------

def run_weekly():
    print(f"[START] CW weekly run (CH shortlist) @ {utc_now().isoformat()}")

    # Storage
    conn = db.connect()
    seen = load_seen(conn)
    print(f"[OK] Storage initialised; seen_companies={len(seen)}")

    # Sponsor register refresh
    sponsor_refresh = sponsor.refresh_register(conn)
    print(f"[OK] Sponsor Register refreshed updated={sponsor_refresh.get('updated')} src_date={sponsor_refresh.get('src_date')}")

    # Clients / env
    ch_key = os.environ.get("COMPANIES_HOUSE_API_KEY", "").strip()
    if not ch_key:
        raise RuntimeError("Missing COMPANIES_HOUSE_API_KEY secret")
    ch = CompaniesHouseClient(ch_key)

    # date window
    now = utc_now()
    incorporated_from = (now - timedelta(days=365)).date().isoformat()
    incorporated_to = now.date().isoformat()

    # Build candidate pool via SIC allowlist, batched
    sic_list = sorted(SIC_ALLOWLIST)
    batches = [sic_list[i:i+15] for i in range(0, len(sic_list), 15)]  # keep requests reasonable
    pool: Dict[str, CHCompanyCandidate] = {}

    print(f"[CFG] incorporated_from={incorporated_from} incorporated_to={incorporated_to} sic_allowlist={len(SIC_ALLOWLIST)} batches={len(batches)} target={TARGET_LEADS}")

    for bi, batch in enumerate(batches, start=1):
        print(f"[SEARCH] advanced_search batch {bi}/{len(batches)} sic_codes={batch}")
        items = ch.advanced_search(incorporated_from, incorporated_to, batch, size=2000)
        print(f"[SEARCH] batch_results={len(items)}")
        for it in items:
            if it.company_number not in pool:
                pool[it.company_number] = it
        time.sleep(0.2)

    print(f"[POOL] unique_candidates={len(pool)}")

    leads_scored: List[Lead] = []
    skipped_seen = 0
    skipped_name = 0
    skipped_geo = 0
    skipped_age = 0
    skipped_sponsor = 0
    skipped_foreign = 0

    # Iterate candidates; stop once we have a decent scored set (but keep going until we can confidently pick top 30)
    # We cap API calls to keep runtime sane.
    MAX_ENRICH = 500

    for idx, cand in enumerate(pool.values(), start=1):
        if len(leads_scored) >= 200 and idx > MAX_ENRICH:
            break

        cn = cand.company_number
        if cn in seen:
            skipped_seen += 1
            continue

        if has_excluded_keyword(cand.company_name):
            skipped_name += 1
            continue

        inc = cand.date_of_creation
        if not inc or not within_last_12_months(inc, now):
            skipped_age += 1
            continue

        # Sponsor register check (exclude if licensed)
        sponsor_hit = sponsor.is_sponsor(conn, cand.company_name)
        if sponsor_hit:
            skipped_sponsor += 1
            continue

        # Profile (geo + sic)
        try:
            prof = ch.company_profile(cn)
        except Exception:
            continue

        addr = prof.get("registered_office_address") or {}
        addr_country = (addr.get("country") or "").strip()
        addr_town = (addr.get("locality") or addr.get("postal_town") or "").strip()

        if addr_country not in ALLOWED_UK_ADDRESS_COUNTRIES:
            skipped_geo += 1
            continue

        sic_codes = prof.get("sic_codes") or cand.sic_codes or []
        sic_codes = [str(x) for x in sic_codes if x]

        # Officers + PSCs
        try:
            officers = ch.officers(cn)
            pscs = ch.psc_list(cn)
        except Exception:
            continue

        directors = [o for o in officers if officer_is_director(o)]
        directors_count = len(directors)
        psc_count = len(pscs)

        # Mid-size proxies (your rules)
        corporate_psc_present = any(psc_is_corporate(p) for p in pscs)
        name_has_uk = "uk" in norm(cand.company_name)
        if not (directors_count > 1 or psc_count > 1 or corporate_psc_present or (name_has_uk and corporate_psc_present)):
            # too weak structurally
            continue

        # Foreign link qualification
        qualifies, foreign_meta = qualifies_foreign_link(pscs, officers)
        if not qualifies:
            skipped_foreign += 1
            continue

        # Score + why
        score, why = score_company(cand.company_name, inc, sic_codes, officers, pscs, foreign_meta)

        # PSC types
        psc_types = []
        for p in pscs:
            psc_types.append("Corporate" if psc_is_corporate(p) else "Individual")
        psc_types = sorted(set(psc_types))

        leads_scored.append(
            Lead(
                company_name=cand.company_name,
                company_number=cn,
                incorporation_date=inc,
                sic_codes=sic_codes,
                directors_count=directors_count,
                psc_count=psc_count,
                psc_types=psc_types,
                address_country=addr_country,
                address_town=addr_town,
                ch_url=f"https://find-and-update.company-information.service.gov.uk/company/{cn}",
                sponsor_status="Not found",
                score=score,
                why=why,
            )
        )

    # Sort and take exactly 30
    leads_scored.sort(key=lambda x: x.score, reverse=True)
    top = leads_scored[:TARGET_LEADS]

    print(f"[STATS] scored={len(leads_scored)} top={len(top)} skipped_seen={skipped_seen} skipped_name={skipped_name} skipped_geo={skipped_geo} skipped_age={skipped_age} skipped_sponsor={skipped_sponsor} skipped_foreign={skipped_foreign}")

    # Email
    run_date = now.date().isoformat()
    subject = f"CW Weekly Leads (CH shortlist) — {run_date}"
    html = render_email(top, run_date)
    send_email(subject=subject, html_body=html)
    print("[EMAIL] sent")

    # Mark seen so we don't repeat
    mark_seen(conn, [l.company_number for l in top])
    print("[CACHE] marked seen")

    print(f"[DONE] finished @ {utc_now().isoformat()}")


if __name__ == "__main__":
    run_weekly()
