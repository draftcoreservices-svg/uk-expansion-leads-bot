# bot/main.py
import os
import io
import re
import time
import sqlite3
import smtplib
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
from bs4 import BeautifulSoup
from rapidfuzz import fuzz
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ============================================================
# CONFIG (tuneable)
# ============================================================

MAX_OUTPUT_LEADS = 20            # email/report cap (and enrichment cap)
LOOKBACK_DAYS = 30               # Companies House incorporation window
VERIFY_MIN_SCORE = 7             # confidence gating threshold (0-10)

# SerpAPI controls
SERP_MAX_CALLS_PER_RUN = 80      # safety cap (free tier friendly)
SERP_SLEEP_SECONDS = 1.2         # rate limiting

# Companies House speed caps
CH_MAX_COMPANIES_TO_CHECK = 120  # max incorporations processed per run
CH_MAX_RESULTS_TOTAL = 600       # max incorporations pulled from advanced search
CH_OFFICERS_TIMEOUT = 15         # seconds
CH_SEARCH_TIMEOUT = 20           # seconds
CH_RETRY_COUNT = 3

# Sponsor register route filters (keep the commercial ones)
SPONSOR_ROUTE_ALLOWLIST = {
    "Skilled Worker",
    "Global Business Mobility: Senior or Specialist Worker",
    "Global Business Mobility: UK Expansion Worker",
}

# Optional: ignore very small/noisy sponsor names
MIN_CLEAN_NAME_LEN = 3
MAX_NON_ALNUM_RATIO = 0.35

PRIORITY_COUNTRIES = {
    "US","USA","UNITED STATES","CANADA","UAE","UNITED ARAB EMIRATES","INDIA","AUSTRALIA",
    "GERMANY","FRANCE","NETHERLANDS","SPAIN","ITALY","IRELAND","SWEDEN","DENMARK","NORWAY",
    "FINLAND","BELGIUM","SWITZERLAND","AUSTRIA","POLAND","CZECHIA","PORTUGAL","GREECE",
    "ROMANIA","BULGARIA","HUNGARY"
}

# ============================================================
# CONSTANTS
# ============================================================

CACHE_DIR = ".cache"
DB_PATH = os.path.join(CACHE_DIR, "state.db")

CH_BASE = "https://api.company-information.service.gov.uk"
SPONSOR_PAGE = "https://www.gov.uk/government/publications/register-of-licensed-sponsors-workers"

EMAIL_STYLE = """
  body { font-family: Arial, Helvetica, sans-serif; background:#f6f8fb; margin:0; padding:0; }
  .wrap { max-width: 980px; margin: 0 auto; padding: 18px; }
  .card { background:#ffffff; border:1px solid #e7ecf3; border-radius:12px; padding:16px; margin-bottom:14px; box-shadow:0 1px 2px rgba(16,24,40,.04); }
  .topbar { background:#0b2345; color:#fff; border-radius:12px; padding:16px; }
  .h1 { font-size:18px; font-weight:700; margin:0 0 6px 0; }
  .muted { color:#c9d3e4; font-size:12px; line-height:1.5; }
  .pill { display:inline-block; padding:3px 10px; border-radius:999px; font-size:12px; border:1px solid #e5e7eb; background:#f9fafb; margin-right:6px; }
  .pill.hot { background:#fff1f2; border-color:#fecdd3; color:#9f1239; }
  .pill.med { background:#fffbeb; border-color:#fde68a; color:#92400e; }
  .pill.watch { background:#eff6ff; border-color:#bfdbfe; color:#1d4ed8; }
  table { width:100%; border-collapse: collapse; font-size: 13px; }
  th { text-align:left; padding:10px 8px; border-bottom:1px solid #e7ecf3; color:#111827; font-size:12px; letter-spacing:.02em; text-transform:uppercase; }
  td { padding:10px 8px; border-bottom:1px solid #f0f3f9; vertical-align:top; }
  .k { color:#6b7280; font-size:12px; }
  .v { color:#111827; font-size:13px; font-weight:600; }
  .small { font-size:12px; color:#374151; margin-top:3px; }
  a { color:#0b5bd3; text-decoration:none; }
  .footer { font-size:11px; color:#6b7280; margin-top:12px; }
  .note { background:#f8fafc; border:1px solid #e7ecf3; padding:10px; border-radius:10px; font-size:12px; color:#374151; }
"""

# ============================================================
# SESSION / RETRIES
# ============================================================

def make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=CH_RETRY_COUNT,
        backoff_factor=0.6,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

# ============================================================
# HELPERS
# ============================================================

def utc_now():
    return datetime.now(timezone.utc).replace(microsecond=0)

def ensure_db():
    os.makedirs(CACHE_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("CREATE TABLE IF NOT EXISTS seen (key TEXT PRIMARY KEY, first_seen_utc TEXT)")
    conn.execute("CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT)")
    conn.commit()
    return conn

def is_seen(conn, key: str) -> bool:
    cur = conn.execute("SELECT 1 FROM seen WHERE key=?", (key,))
    return cur.fetchone() is not None

def mark_seen(conn, key: str, ts: str):
    conn.execute("INSERT OR IGNORE INTO seen(key, first_seen_utc) VALUES(?,?)", (key, ts))

def meta_get(conn, k: str) -> str | None:
    cur = conn.execute("SELECT v FROM meta WHERE k=?", (k,))
    row = cur.fetchone()
    return row[0] if row else None

def meta_set(conn, k: str, v: str):
    conn.execute("INSERT INTO meta(k, v) VALUES(?, ?) ON CONFLICT(k) DO UPDATE SET v=excluded.v", (k, v))

def ch_auth():
    return (os.environ["COMPANIES_HOUSE_API_KEY"], "")

def norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def norm_upper(s: str) -> str:
    return norm(s).upper()

def clean_display_name(name: str) -> str:
    n = norm(name)
    # remove leading junk punctuation / quotes / backticks etc
    n = re.sub(r"^[\s\"\'\`\*\@\[\]\(\)\{\}\<\>\#\!\$\%\^\&\=\+\;\:\,\.\-\/\\]+", "", n)
    n = re.sub(r"\s+", " ", n).strip()
    return n

def non_alnum_ratio(s: str) -> float:
    if not s:
        return 1.0
    non = sum(1 for ch in s if not ch.isalnum() and ch != " ")
    return non / max(len(s), 1)

def extract_emails(text: str) -> list[str]:
    if not text:
        return []
    emails = set(re.findall(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", text, flags=re.I))
    bad_domains = {"example.com", "domain.com"}
    out = []
    for e in emails:
        if any(e.lower().endswith(d) for d in bad_domains):
            continue
        out.append(e)
    return sorted(set(out))[:5]

def extract_phones(text: str) -> list[str]:
    if not text:
        return []
    cands = re.findall(r"(\+?\d[\d\-\s().]{8,}\d)", text)
    cleaned = []
    for c in cands:
        digits = re.sub(r"\D", "", c)
        if len(digits) < 9:
            continue
        cleaned.append(c.strip())
    out, seen = [], set()
    for c in cleaned:
        key = re.sub(r"\D", "", c)
        if key in seen:
            continue
        seen.add(key)
        out.append(c)
    return out[:5]

# ============================================================
# SPONSOR REGISTER
# ============================================================

def find_latest_csv_url(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")
    links = []
    for a in soup.select("a[href]"):
        href = a.get("href")
        if href and ".csv" in href.lower():
            links.append(href)
    if not links:
        raise RuntimeError("Could not find CSV link on sponsor register page.")
    assets = [h for h in links if "assets.publishing.service.gov.uk" in h]
    chosen = assets[0] if assets else links[0]
    if chosen.startswith("/"):
        chosen = "https://www.gov.uk" + chosen
    return chosen

def fetch_sponsor_df(session: requests.Session) -> pd.DataFrame:
    page = session.get(SPONSOR_PAGE, timeout=30)
    page.raise_for_status()
    csv_url = find_latest_csv_url(page.text)
    r = session.get(csv_url, timeout=60)
    r.raise_for_status()
    df = pd.read_csv(io.BytesIO(r.content), dtype=str).fillna("")
    df.columns = [c.strip() for c in df.columns]
    return df

def sponsor_row_key(row: dict) -> str:
    name = norm_upper(row.get("Organisation Name") or row.get("Organization Name") or "")
    town = norm_upper(row.get("Town/City") or row.get("Town") or "")
    route = norm_upper(row.get("Route") or "")
    sub = norm_upper(row.get("Sub Route") or "")
    return f"SPONSOR::{name}::{town}::{route}::{sub}"

def sponsor_row_fields(row: dict) -> dict:
    raw_name = row.get("Organisation Name") or row.get("Organization Name") or ""
    name = clean_display_name(raw_name)

    town = norm(row.get("Town/City") or row.get("Town") or "")
    county = norm(row.get("County") or "")
    route = norm(row.get("Route") or "")
    sub = norm(row.get("Sub Route") or "")

    addr = ", ".join([x for x in [town, county] if x]).strip(", ")
    return {"name": name, "town": town, "county": county, "address": addr, "route": route, "subroute": sub, "raw_name": raw_name}

def sponsor_is_noise(name: str) -> bool:
    n = clean_display_name(name)
    if len(n) < MIN_CLEAN_NAME_LEN:
        return True
    if non_alnum_ratio(n) > MAX_NON_ALNUM_RATIO:
        return True
    return False

# ============================================================
# COMPANIES HOUSE
# ============================================================

def ch_advanced_incorporated(session: requests.Session, inc_from: str, inc_to: str, size: int = 100) -> list[dict]:
    out = []
    start_index = 0
    while True:
        params = {
            "incorporated_from": inc_from,
            "incorporated_to": inc_to,
            "size": size,
            "start_index": start_index,
        }
        r = session.get(
            f"{CH_BASE}/advanced-search/companies",
            params=params,
            auth=ch_auth(),
            timeout=CH_SEARCH_TIMEOUT,
        )
        r.raise_for_status()
        data = r.json()
        items = data.get("items") or []
        out.extend(items)

        if len(items) < size:
            break

        start_index += size
        if start_index >= CH_MAX_RESULTS_TOTAL:
            break

    return out

def ch_company_officers(session: requests.Session, company_number: str) -> list[dict]:
    r = session.get(
        f"{CH_BASE}/company/{company_number}/officers",
        params={"items_per_page": 100},
        auth=ch_auth(),
        timeout=CH_OFFICERS_TIMEOUT,
    )
    r.raise_for_status()
    return r.json().get("items") or []

def ch_company_profile(session: requests.Session, company_number: str) -> dict:
    r = session.get(
        f"{CH_BASE}/company/{company_number}",
        auth=ch_auth(),
        timeout=CH_SEARCH_TIMEOUT,
    )
    r.raise_for_status()
    return r.json()

def ch_search_companies(session: requests.Session, query: str, items_per_page: int = 10) -> list[dict]:
    r = session.get(
        f"{CH_BASE}/search/companies",
        params={"q": query, "items_per_page": items_per_page},
        auth=ch_auth(),
        timeout=CH_SEARCH_TIMEOUT,
    )
    r.raise_for_status()
    return r.json().get("items") or []

def best_ch_match_for_sponsor(session: requests.Session, sponsor_name: str, town: str) -> tuple[str, int]:
    """
    Returns (company_number, confidence_score out of 100)
    """
    q = sponsor_name
    items = ch_search_companies(session, q, items_per_page=10)
    if not items:
        return "", 0

    best_num = ""
    best_score = 0
    town_u = norm_upper(town)

    for it in items:
        title = it.get("title") or ""
        num = it.get("company_number") or ""
        if not num or not title:
            continue

        # similarity 0..100
        sim = fuzz.token_set_ratio(norm_upper(sponsor_name), norm_upper(title))

        # small boost if the company address snippet contains town
        snippet = norm_upper(it.get("address_snippet") or "")
        if town_u and town_u in snippet:
            sim = min(100, sim + 6)

        if sim > best_score:
            best_score = sim
            best_num = num

    return best_num, best_score

def overseas_signal_score(officers: list[dict]) -> tuple[int, list[str], list[str]]:
    reasons = []
    countries = []
    non_uk = 0
    for o in officers:
        addr = o.get("address") or {}
        c = norm_upper(addr.get("country") or "")
        if c:
            countries.append(c.title())
        if c and c not in {"UNITED KINGDOM","UK","ENGLAND","SCOTLAND","WALES","NORTHERN IRELAND"}:
            non_uk += 1

    score = 0
    if non_uk >= 1:
        score += 5
        reasons.append(f"{non_uk} officer(s) show non-UK address country")
    if non_uk >= 2:
        score += 2
        reasons.append("Multiple non-UK officers (stronger overseas signal)")
    if any(norm_upper(c) in PRIORITY_COUNTRIES for c in countries):
        score += 1
        reasons.append("Priority country detected in officer addresses")

    return min(score, 10), reasons, sorted(set(countries))

def commercial_bucket(score: int) -> str:
    if score >= 8:
        return "HOT"
    if score >= 6:
        return "MEDIUM"
    return "WATCH"

def visa_hint(source: str, score: int, route: str = "") -> str:
    if source == "SPONSOR_REGISTER":
        if "UK Expansion Worker" in route:
            return "Likely UK Expansion Worker / Sponsor compliance"
        if "Senior or Specialist Worker" in route:
            return "GBM Senior/Specialist Worker route"
        if "Skilled Worker" in route:
            return "Sponsor compliance / Skilled Worker routes"
        return "Sponsor compliance / worker routes"
    # Companies House
    if score >= 7:
        return "UK Expansion Worker likely (overseas-linked incorporation)"
    if score >= 5:
        return "Possible Expansion Worker / Sponsor needs (review)"
    return "Watchlist"

# ============================================================
# SERPAPI + WEBSITE VERIFICATION + CONTACT EXTRACTION
# ============================================================

def serp_search(session: requests.Session, query: str, api_key: str, num: int = 5) -> list[dict]:
    params = {"engine": "google", "q": query, "api_key": api_key, "num": num}
    r = session.get("https://serpapi.com/search.json", params=params, timeout=60)
    r.raise_for_status()
    return (r.json().get("organic_results") or [])

def url_domain(u: str) -> str:
    if not u:
        return ""
    u = re.sub(r"^https?://", "", u.strip(), flags=re.I)
    return u.split("/")[0].lower()

def get_url_text(session: requests.Session, url: str, timeout: int = 20) -> str:
    try:
        r = session.get(url, timeout=timeout, headers={"User-Agent": "Mozilla/5.0 (compatible; CWLeadsBot/1.0)"})
        if r.status_code >= 400:
            return ""
        return (r.text or "")[:600000]
    except Exception:
        return ""

def find_contact_links(html: str, base_url: str) -> list[str]:
    soup = BeautifulSoup(html, "lxml")
    links = []
    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        label = (a.get_text(" ") or "").strip().lower()

        if any(k in href.lower() for k in ["/contact", "contact-us", "contactus"]) or "contact" in label:
            links.append(href)
        if any(k in href.lower() for k in ["/privacy", "/terms", "/legal", "/imprint", "privacy", "terms"]):
            links.append(href)

    abs_links = []
    for h in links:
        if h.startswith("http"):
            abs_links.append(h)
        elif h.startswith("/"):
            abs_links.append(base_url.rstrip("/") + h)
        else:
            abs_links.append(base_url.rstrip("/") + "/" + h)

    out, seen = [], set()
    for l in abs_links:
        d = l.split("#")[0]
        if d in seen:
            continue
        seen.add(d)
        out.append(d)
    return out[:6]

def verification_score(company_name: str, company_number: str, reg_postcode: str, page_text: str) -> tuple[int, list[str]]:
    ev = []
    score = 0
    t_upper = (page_text or "").upper()

    if company_number and re.search(r"\b" + re.escape(company_number) + r"\b", t_upper):
        score += 6
        ev.append("Company number found on site")

    if reg_postcode:
        pc = reg_postcode.replace(" ", "").upper()
        if pc and pc in t_upper.replace(" ", ""):
            score += 3
            ev.append("Registered postcode found on site")

    if company_name:
        snippet = re.sub(r"\s+", " ", (page_text or "")[:20000])
        sim = fuzz.token_set_ratio(company_name.upper(), snippet.upper())
        if sim >= 75:
            score += 2
            ev.append(f"Name similarity strong ({sim})")
        elif sim >= 60:
            score += 1
            ev.append(f"Name similarity moderate ({sim})")

    return min(score, 10), ev

def enrich_lead_with_contact(http: requests.Session, lead: dict, serp_key: str, serp_budget: dict) -> dict:
    if serp_budget["calls"] >= SERP_MAX_CALLS_PER_RUN:
        lead["enrich_status"] = "Skipped (Serp budget cap)"
        return lead

    company_name = lead.get("company_name", "")
    company_number = lead.get("company_number", "")
    reg_postcode = lead.get("reg_postcode", "")
    town = lead.get("reg_town", "")

    # Better query: name + postcode (if known) else name + town
    q = f"\"{company_name}\" {reg_postcode}".strip() if reg_postcode else f"\"{company_name}\" {town} contact".strip()

    serp_budget["calls"] += 1
    results = serp_search(http, q, serp_key, num=6)
    time.sleep(SERP_SLEEP_SECONDS)

    candidates = []
    for r in results:
        link = r.get("link") or ""
        if not link.startswith("http"):
            continue
        d = url_domain(link)
        if not d:
            continue
        # Skip obvious directories / non-official sources
        if any(x in d for x in [
            "companieshouse.gov.uk","gov.uk","linkedin.com","facebook.com","yell.com","endole.co.uk",
            "opencorporates.com","find-and-update.company-information.service.gov.uk","uk.linkedin.com",
            "bloomberg.com","dnb.com","zoominfo.com","crunchbase.com"
        ]):
            continue
        candidates.append(link)

    candidates = candidates[:3]

    best = {"score": -1, "base_url": "", "evidence": []}

    for url in candidates:
        base_url = "https://" + url_domain(url)
        home_html = get_url_text(http, base_url)
        if not home_html:
            continue

        score, ev = verification_score(company_name, company_number, reg_postcode, home_html)

        links = find_contact_links(home_html, base_url)
        for l in links:
            if score >= 9:
                break
            extra = get_url_text(http, l)
            if not extra:
                continue
            s2, ev2 = verification_score(company_name, company_number, reg_postcode, extra)
            if s2 > score:
                score = s2
                ev = list(set(ev + ev2))

        if score > best["score"]:
            best["score"] = score
            best["base_url"] = base_url
            best["evidence"] = ev

    lead["website"] = best["base_url"] or ""
    lead["website_confidence"] = best["score"] if best["score"] >= 0 else ""
    lead["verification_evidence"] = "; ".join(best["evidence"])[:240] if best["evidence"] else ""

    if best["score"] < VERIFY_MIN_SCORE:
        lead["enrich_status"] = "Manual verify needed (confidence too low)"
        return lead

    # Scrape only if verified
    combined_text = ""
    home_html = get_url_text(http, best["base_url"])
    combined_text += " " + BeautifulSoup(home_html, "lxml").get_text(" ", strip=True)

    links = find_contact_links(home_html, best["base_url"])
    for l in links[:5]:
        extra = get_url_text(http, l)
        if extra:
            combined_text += " " + BeautifulSoup(extra, "lxml").get_text(" ", strip=True)

    emails = extract_emails(combined_text)
    phones = extract_phones(combined_text)

    lead["emails_found"] = ", ".join(emails)
    lead["phones_found"] = ", ".join(phones)
    lead["enrich_status"] = "Verified & scraped" if (emails or phones) else "Verified (no contacts found)"
    return lead

# ============================================================
# EMAIL
# ============================================================

def send_email(subject: str, html: str, csv_bytes: bytes, csv_filename: str):
    host = os.environ["SMTP_HOST"]
    port = int(os.environ.get("SMTP_PORT", "587"))
    user = os.environ["SMTP_USER"]
    pw = os.environ["SMTP_PASS"]
    email_from = os.environ["EMAIL_FROM"]
    email_to = os.environ["EMAIL_TO"]

    msg = MIMEMultipart()
    msg["From"] = email_from
    msg["To"] = email_to
    msg["Subject"] = subject

    msg.attach(MIMEText(html, "html", "utf-8"))

    part = MIMEBase("application", "octet-stream")
    part.set_payload(csv_bytes)
    encoders.encode_base64(part)
    part.add_header("Content-Disposition", f'attachment; filename="{csv_filename}"')
    msg.attach(part)

    with smtplib.SMTP(host, port) as server:
        server.starttls()
        server.login(user, pw)
        server.sendmail(email_from, [email_to], msg.as_string())

def fmt_pill(bucket: str) -> str:
    if bucket == "HOT":
        return '<span class="pill hot">Hot</span>'
    if bucket == "MEDIUM":
        return '<span class="pill med">Medium</span>'
    return '<span class="pill watch">Watchlist</span>'

def html_report(run_meta: dict, leads: list[dict]) -> str:
    rows = ""
    for l in leads:
        bucket = l.get("bucket", "WATCH")
        website = l.get("website") or ""
        website_html = f"<a href='{website}'>{website}</a>" if website else "—"
        route = l.get("sponsor_route") or ""
        route_html = f"<div class='small'><span class='k'>Route:</span> {route}</div>" if route else ""
        verify_ev = l.get("verification_evidence") or ""
        verify_html = f"<div class='small'><span class='k'>Verify:</span> {verify_ev}</div>" if verify_ev else ""

        rows += f"""
        <tr>
          <td>{fmt_pill(bucket)}<div class="small">{l.get('source','')}</div></td>
          <td>
            <div class="v">{l.get('company_name','')}</div>
            <div class="k">Company No: {l.get('company_number','') or '—'} · Incorporated: {l.get('incorporated','') or '—'}</div>
            <div class="small">{l.get('reg_address','')}</div>
            {route_html}
          </td>
          <td>
            <div class="v">{l.get('visa_hint','')}</div>
            <div class="k">Score: {l.get('score','')}</div>
            <div class="small">{l.get('why','')}</div>
          </td>
          <td>
            <div class="v">{website_html}</div>
            <div class="k">Confidence: {l.get('website_confidence') or '—'}</div>
            <div class="small">{l.get('enrich_status','')}</div>
            <div class="small">{l.get('emails_found','')}</div>
            <div class="small">{l.get('phones_found','')}</div>
            {verify_html}
          </td>
        </tr>
        """

    sponsor_note = ""
    if run_meta.get("sponsor_baselined_this_run") == "1":
        sponsor_note = """
        <div class="note" style="margin-top:12px;">
          <b>First run baseline:</b> Sponsor Register has been saved as a baseline snapshot.
          New sponsors will only be reported on subsequent runs.
        </div>
        """


    html = f"""
    <html>
      <head><meta charset="utf-8"><style>{EMAIL_STYLE}</style></head>
      <body>
        <div class="wrap">
          <div class="topbar">
            <div class="h1">UK Expansion Leads — Intelligence Brief</div>
            <div class="muted">
              Run time (UTC): {run_meta.get('run_time_utc','')}<br/>
              Lookback: {run_meta.get('lookback','')}<br/>
              New sponsors (filtered routes): {run_meta.get('new_sponsors',0)} · CH overseas-signal incorporations: {run_meta.get('new_ch_candidates',0)}
              · Verified websites: {run_meta.get('verified_sites',0)} · Serp calls: {run_meta.get('serp_calls',0)}
            </div>
          </div>

          <div class="card">
            <div class="v">Top leads (capped to {MAX_OUTPUT_LEADS})</div>
            <div class="small">Sponsor leads are matched to Companies House where possible, then verified before scraping contact details.</div>
            {sponsor_note}
            <table>
              <thead>
                <tr>
                  <th>Status</th>
                  <th>Company</th>
                  <th>Why / Visa hint</th>
                  <th>Website & contacts</th>
                </tr>
              </thead>
              <tbody>
                {rows if rows else '<tr><td colspan="4">No new leads found in this run.</td></tr>'}
              </tbody>
            </table>
            <div class="footer">
              Internal BD tooling. Information-only; always verify details before outreach.
            </div>
          </div>
        </div>
      </body>
    </html>
    """
    return html

# ============================================================
# MAIN
# ============================================================

def main():
    serp_key = os.environ.get("SERPAPI_API_KEY", "").strip()
    if not serp_key:
        raise RuntimeError("SERPAPI_API_KEY missing")

    http = make_session()

    run_ts = utc_now()
    run_ts_iso = run_ts.isoformat()
    conn = ensure_db()

    # ------------------------------------------------------------
    # 1) Sponsor Register: baseline on first run; diff afterwards
    # ------------------------------------------------------------
    sponsor_error = ""
    sponsor_new = []
    sponsor_total_filtered = 0
    sponsor_baselined = meta_get(conn, "sponsor_baselined") or "0"

    try:
        print("[SPONSOR] Fetching GOV.UK sponsor CSV…", flush=True)
        sponsor_df = fetch_sponsor_df(http)
        records = sponsor_df.to_dict(orient="records")
        print(f"[SPONSOR] Loaded {len(records)} rows.", flush=True)

        filtered = []
        for row in records:
            f = sponsor_row_fields(row)
            route = f["route"]
            if route not in SPONSOR_ROUTE_ALLOWLIST:
                continue
            if sponsor_is_noise(f["name"]):
                continue
            filtered.append((row, f))

        sponsor_total_filtered = len(filtered)
        print(f"[SPONSOR] Filtered to {sponsor_total_filtered} rows (route allowlist + name cleanup).", flush=True)

        # First run: baseline snapshot (mark all as seen; report 0 new)
        sponsor_baselined_this_run = "0"

        if sponsor_baselined != "1":
            for row, f in filtered:
                key = sponsor_row_key(row)
                mark_seen(conn, key, run_ts_iso)
            meta_set(conn, "sponsor_baselined", "1")
            meta_set(conn, "sponsor_baselined_at_utc", run_ts_iso)
            sponsor_baselined = "1"
            sponsor_baselined_this_run = "1"
            sponsor_new = []
            print("[SPONSOR] First run: baselined sponsor register. New sponsors = 0.", flush=True)
        else:
            for row, f in filtered:
                key = sponsor_row_key(row)
                if not is_seen(conn, key):
                    mark_seen(conn, key, run_ts_iso)
                    sponsor_new.append((row, f))
            print(f"[SPONSOR] New sponsor rows (filtered) detected: {len(sponsor_new)}", flush=True)

        else:
            for row, f in filtered:
                key = sponsor_row_key(row)
                if not is_seen(conn, key):
                    mark_seen(conn, key, run_ts_iso)
                    sponsor_new.append((row, f))
            print(f"[SPONSOR] New sponsor rows (filtered) detected: {len(sponsor_new)}", flush=True)

    except Exception as e:
        sponsor_error = str(e)
        print(f"[SPONSOR] ERROR: {sponsor_error}", flush=True)

    # ------------------------------------------------------------
    # 2) Companies House: overseas-signal incorporations
    # ------------------------------------------------------------
    inc_to = run_ts.date().isoformat()
    inc_from = (run_ts.date() - timedelta(days=LOOKBACK_DAYS)).isoformat()

    print(f"[CH] Pulling incorporations from {inc_from} to {inc_to} (max {CH_MAX_RESULTS_TOTAL})…", flush=True)
    ch_items = ch_advanced_incorporated(http, inc_from, inc_to, size=100)
    print(f"[CH] Pulled {len(ch_items)} incorporations (pre-cap).", flush=True)

    ch_items = ch_items[:CH_MAX_COMPANIES_TO_CHECK]
    print(f"[CH] Processing first {len(ch_items)} incorporations (cap={CH_MAX_COMPANIES_TO_CHECK}).", flush=True)

    ch_candidates = []
    for idx, item in enumerate(ch_items, start=1):
        if idx == 1 or idx % 10 == 0:
            print(f"[CH] Officers lookup progress: {idx}/{len(ch_items)}", flush=True)

        company_number = item.get("company_number") or ""
        company_name = clean_display_name(item.get("company_name") or "")
        if not company_number or not company_name:
            continue

        key = f"CH::{company_number}"
        if is_seen(conn, key):
            continue

        # --- Pull registered office early so we can apply a fallback overseas signal
        ro = item.get("registered_office_address") or {}
        ro_country = norm_upper(ro.get("country", ""))

        # Officers-based overseas signal (primary)
        officers = []
        try:
            officers = ch_company_officers(http, company_number)
        except Exception:
            officers = []

        score, reasons, countries = overseas_signal_score(officers)

        # Fallback overseas signal: registered office country is non-UK
        # (This catches foreign-address setups even when officer data is weak/unhelpful.)
        if ro_country and ro_country not in {"UNITED KINGDOM","UK","ENGLAND","SCOTLAND","WALES","NORTHERN IRELAND"}:
            # If officers didn't already push it over the threshold, give it a "balanced" score.
            if score < 5:
                score = max(score, 6)
                reasons = (reasons or []) + [f"Registered office country is non-UK ({ro.get('country','')})"]
                countries = sorted(set((countries or []) + [ro.get("country","").title()]))

        if score < 5:  # balanced threshold
            continue

        address = ", ".join([x for x in [
            ro.get("address_line_1",""),
            ro.get("address_line_2",""),
            ro.get("locality",""),
            ro.get("region",""),
            ro.get("postal_code",""),
            ro.get("country",""),
        ] if x]).strip(", ")
        postcode = norm(ro.get("postal_code",""))
        town = norm(ro.get("locality","") or ro.get("post_town","") or "")

        ch_candidates.append({
            "source": "COMPANIES_HOUSE",
            "company_name": company_name,
            "company_number": company_number,
            "incorporated": item.get("date_of_creation",""),
            "reg_address": address,
            "reg_postcode": postcode,
            "reg_town": town,
            "score": score,
            "why": "; ".join(reasons),
            "countries": ", ".join(countries),
            "sponsor_route": "",
        })

        mark_seen(conn, key, run_ts_iso)

    print(f"[CH] Overseas-signal candidates: {len(ch_candidates)}", flush=True)

    # ------------------------------------------------------------
    # 3) Convert sponsor_new → leads and attempt CH match for enrichment
    # ------------------------------------------------------------
    sponsor_leads = []
    if sponsor_new:
        print("[SPONSOR→CH] Matching new sponsors to Companies House…", flush=True)

    for idx, (row, f) in enumerate(sponsor_new, start=1):
        if idx == 1 or idx % 10 == 0:
            print(f"[SPONSOR→CH] Progress: {idx}/{len(sponsor_new)}", flush=True)

        name = f["name"]
        town = f["town"]
        route = f["route"]
        sub = f["subroute"]
        route_display = route + (f" / {sub}" if sub else "")

        # CH match attempt
        company_number = ""
        ch_match_conf = 0
        reg_address = f["address"]
        reg_postcode = ""
        incorporated = ""

        try:
            company_number, ch_match_conf = best_ch_match_for_sponsor(http, name, town)
            if company_number and ch_match_conf >= 72:
                prof = ch_company_profile(http, company_number)
                ro = prof.get("registered_office_address") or {}
                reg_postcode = norm(ro.get("postal_code",""))
                incorporated = prof.get("date_of_creation","")
                reg_address = ", ".join([x for x in [
                    ro.get("address_line_1",""),
                    ro.get("address_line_2",""),
                    ro.get("locality",""),
                    ro.get("region",""),
                    ro.get("postal_code",""),
                    ro.get("country",""),
                ] if x]).strip(", ")
        except Exception:
            company_number = ""
            ch_match_conf = 0

        sponsor_leads.append({
            "source": "SPONSOR_REGISTER",
            "company_name": name,
            "company_number": company_number,
            "incorporated": incorporated,
            "reg_address": reg_address,
            "reg_postcode": reg_postcode,
            "reg_town": town,
            "score": 7 if "UK Expansion Worker" in route else 6,
            "why": f"Newly listed sponsor (Route: {route_display})" + (f" · CH match confidence: {ch_match_conf}" if company_number else " · CH match: not found"),
            "countries": "",
            "sponsor_route": route_display,
        })

    # ------------------------------------------------------------
    # 4) Combine, dedupe, sort, cap
    # ------------------------------------------------------------
    leads = sponsor_leads + ch_candidates

    # Deduplicate: prefer entries with company_number, then higher score
    by_key = {}
    for l in leads:
        dedupe_key = l.get("company_number") or f"{norm_upper(l.get('company_name',''))}::{norm_upper(l.get('reg_town',''))}::{norm_upper(l.get('sponsor_route',''))}"
        existing = by_key.get(dedupe_key)
        if not existing:
            by_key[dedupe_key] = l
            continue
        # prefer having company_number
        if (not existing.get("company_number")) and l.get("company_number"):
            by_key[dedupe_key] = l
            continue
        # prefer higher score
        if int(l.get("score",0)) > int(existing.get("score",0)):
            by_key[dedupe_key] = l

    leads = list(by_key.values())

    # Populate computed fields
    for l in leads:
        score_i = int(l.get("score", 0))
        l["bucket"] = commercial_bucket(score_i)
        l["visa_hint"] = visa_hint(l["source"], score_i, l.get("sponsor_route",""))
        l.setdefault("website", "")
        l.setdefault("website_confidence", "")
        l.setdefault("emails_found", "")
        l.setdefault("phones_found", "")
        l.setdefault("verification_evidence", "")
        l.setdefault("enrich_status", "Not attempted")

    bucket_rank = {"HOT": 0, "MEDIUM": 1, "WATCH": 2}
    leads.sort(key=lambda x: (bucket_rank.get(x.get("bucket","WATCH"), 9), -int(x.get("score",0)), x.get("source","")))

    # Cap to MAX_OUTPUT_LEADS
    leads = leads[:MAX_OUTPUT_LEADS]
    print(f"[LEADS] Prepared {len(leads)} leads (capped to {MAX_OUTPUT_LEADS}).", flush=True)

    # ------------------------------------------------------------
    # 5) Enrich leads (Sponsor + CH) when we have a CH company number
    # ------------------------------------------------------------
    serp_budget = {"calls": 0}
    print(f"[ENRICH] Starting SerpAPI enrichment (cap calls={SERP_MAX_CALLS_PER_RUN})…", flush=True)

    for i, l in enumerate(leads):
        if not l.get("company_number"):
            l["enrich_status"] = "Skipped (no Companies House number)"
            continue
        leads[i] = enrich_lead_with_contact(http, l, serp_key, serp_budget)

    verified_sites = sum(1 for l in leads if (l.get("website_confidence") or 0) >= VERIFY_MIN_SCORE)
    print(f"[ENRICH] Done. Serp calls used: {serp_budget['calls']}. Verified sites: {verified_sites}.", flush=True)

    conn.commit()
    conn.close()

    # ------------------------------------------------------------
    # 6) Email + CSV
    # ------------------------------------------------------------
    df = pd.DataFrame(leads)
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    csv_name = f"uk-expansion-leads_{run_ts.date().isoformat()}.csv"

    meta = {
        "run_time_utc": run_ts_iso,
        "lookback": f"{LOOKBACK_DAYS} days (Companies House) · Sponsor register (routes: Skilled Worker / GBM Senior-Specialist / UK Expansion Worker)",
        "new_sponsors": len(sponsor_new),
        "new_ch_candidates": len(ch_candidates),
        "serp_calls": serp_budget["calls"],
        "verified_sites": verified_sites,
        "sponsor_error": sponsor_error,
        "sponsor_baselined_this_run": sponsor_baselined_this_run,
        "sponsor_total_filtered": sponsor_total_filtered,
    }

    subject = f"UK Expansion Leads — {run_ts.date().isoformat()}"
    html = html_report(meta, leads)

    print(f"[EMAIL] Sending email to {os.environ.get('EMAIL_TO','')} (CSV attached)…", flush=True)
    send_email(subject, html, csv_bytes, csv_name)
    print("[DONE] Email sent.", flush=True)

if __name__ == "__main__":
    main()
