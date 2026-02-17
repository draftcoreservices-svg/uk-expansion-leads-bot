import os
import io
import re
import json
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

# =========================
# Config (tuneable)
# =========================

MAX_OUTPUT_LEADS = 20          # cap to keep SerpAPI usage low
LOOKBACK_DAYS = 10             # Companies House lookback window
SERP_MAX_CALLS_PER_RUN = 60    # hard safety cap (free plan throughput)
SERP_SLEEP_SECONDS = 1.5       # rate-limiting for SerpAPI free tier
VERIFY_MIN_SCORE = 7           # confidence gating threshold (0-10)

PRIORITY_COUNTRIES = {"US","USA","UNITED STATES","CANADA","UAE","UNITED ARAB EMIRATES","INDIA","AUSTRALIA","GERMANY","FRANCE","NETHERLANDS","SPAIN","ITALY","IRELAND","SWEDEN","DENMARK","NORWAY","FINLAND","BELGIUM","SWITZERLAND","AUSTRIA","POLAND","CZECHIA","PORTUGAL","GREECE","ROMANIA","BULGARIA","HUNGARY"}

# =========================
# Helpers
# =========================

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
  .muted { color:#6b7280; font-size:12px; }
  .pill { display:inline-block; padding:3px 10px; border-radius:999px; font-size:12px; border:1px solid #e5e7eb; background:#f9fafb; margin-right:6px; }
  .pill.hot { background:#fff1f2; border-color:#fecdd3; }
  .pill.med { background:#fffbeb; border-color:#fde68a; }
  .pill.watch { background:#eff6ff; border-color:#bfdbfe; }
  table { width:100%; border-collapse: collapse; font-size: 13px; }
  th { text-align:left; padding:10px 8px; border-bottom:1px solid #e7ecf3; color:#111827; font-size:12px; letter-spacing:.02em; text-transform:uppercase; }
  td { padding:10px 8px; border-bottom:1px solid #f0f3f9; vertical-align:top; }
  .k { color:#6b7280; font-size:12px; }
  .v { color:#111827; font-size:13px; font-weight:600; }
  .small { font-size:12px; color:#374151; }
  a { color:#0b5bd3; text-decoration:none; }
  .footer { font-size:11px; color:#6b7280; margin-top:12px; }
"""

def utc_now():
    return datetime.now(timezone.utc).replace(microsecond=0)

def ensure_db():
    os.makedirs(CACHE_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("CREATE TABLE IF NOT EXISTS seen (key TEXT PRIMARY KEY, first_seen_utc TEXT)")
    conn.commit()
    return conn

def is_seen(conn, key: str) -> bool:
    cur = conn.execute("SELECT 1 FROM seen WHERE key=?", (key,))
    return cur.fetchone() is not None

def mark_seen(conn, key: str, ts: str):
    conn.execute("INSERT OR IGNORE INTO seen(key, first_seen_utc) VALUES(?,?)", (key, ts))

def ch_auth():
    return (os.environ["COMPANIES_HOUSE_API_KEY"], "")

def norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def norm_upper(s: str) -> str:
    return norm(s).upper()

def extract_postcode(text: str) -> str:
    # UK postcode (broad)
    m = re.search(r"\b([A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2})\b", (text or "").upper())
    return m.group(1).replace(" ", "") if m else ""

def extract_emails(text: str) -> list[str]:
    if not text:
        return []
    emails = set(re.findall(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", text, flags=re.I))
    # basic filtering for common false positives
    bad = {"example.com", "domain.com"}
    out = []
    for e in emails:
        if any(e.lower().endswith(d) for d in bad):
            continue
        out.append(e)
    return sorted(set(out))[:5]

def extract_phones(text: str) -> list[str]:
    if not text:
        return []
    # loose UK-style and international phone patterns
    cands = re.findall(r"(\+?\d[\d\-\s().]{8,}\d)", text)
    cleaned = []
    for c in cands:
        cc = re.sub(r"[^\d+]", "", c)
        if len(re.sub(r"\D", "", cc)) < 9:
            continue
        cleaned.append(c.strip())
    # dedupe while keeping order
    out = []
    seen = set()
    for c in cleaned:
        key = re.sub(r"\D", "", c)
        if key in seen:
            continue
        seen.add(key)
        out.append(c)
    return out[:5]

# =========================
# Sponsor register
# =========================

def find_latest_csv_url(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")
    links = []
    for a in soup.select("a[href]"):
        href = a.get("href")
        if not href:
            continue
        if ".csv" in href:
            links.append(href)
    if not links:
        raise RuntimeError("Could not find CSV link on sponsor register page.")
    # prefer assets.publishing.service.gov.uk
    assets = [h for h in links if "assets.publishing.service.gov.uk" in h]
    chosen = assets[0] if assets else links[0]
    if chosen.startswith("/"):
        chosen = "https://www.gov.uk" + chosen
    return chosen

def fetch_sponsor_df() -> pd.DataFrame:
    page = requests.get(SPONSOR_PAGE, timeout=30)
    page.raise_for_status()
    csv_url = find_latest_csv_url(page.text)
    r = requests.get(csv_url, timeout=60)
    r.raise_for_status()
    df = pd.read_csv(io.BytesIO(r.content), dtype=str).fillna("")
    df.columns = [c.strip() for c in df.columns]
    return df

def sponsor_row_key(row: dict) -> str:
    # best-effort robust key
    name = norm_upper(row.get("Organisation Name") or row.get("Organization Name") or "")
    town = norm_upper(row.get("Town/City") or row.get("Town") or "")
    route = norm_upper(row.get("Route") or "")
    return f"SPONSOR::{name}::{town}::{route}"

# =========================
# Companies House
# =========================

def ch_advanced_incorporated(inc_from: str, inc_to: str, size: int = 100) -> list[dict]:
    out = []
    start_index = 0
    while True:
        params = {
            "incorporated_from": inc_from,
            "incorporated_to": inc_to,
            "size": size,
            "start_index": start_index,
        }
        r = requests.get(f"{CH_BASE}/advanced-search/companies", params=params, auth=ch_auth(), timeout=30)
        r.raise_for_status()
        data = r.json()
        items = data.get("items") or []
        out.extend(items)
        if len(items) < size:
            break
        start_index += size
        if start_index >= 2000:
            break
    return out

def ch_company_officers(company_number: str) -> list[dict]:
    r = requests.get(f"{CH_BASE}/company/{company_number}/officers", params={"items_per_page": 100}, auth=ch_auth(), timeout=30)
    r.raise_for_status()
    return r.json().get("items") or []

def overseas_signal_score(officers: list[dict]) -> tuple[int, list[str], list[str]]:
    """
    Returns: score (0-10), reasons, country_list
    """
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
        reasons.append(f"{non_uk} officer(s) show non‑UK address country")
    if non_uk >= 2:
        score += 2
        reasons.append("Multiple non‑UK officers (stronger overseas signal)")

    # Boost if any priority country appears
    if any(norm_upper(c) in PRIORITY_COUNTRIES for c in countries):
        score += 1
        reasons.append("Priority country detected in officer addresses")

    score = min(score, 10)
    return score, reasons, sorted(set(countries))

def visa_hint(source: str, score: int) -> str:
    # simple heuristics
    if source == "SPONSOR_REGISTER":
        return "Sponsor compliance / Skilled Worker routes"
    if score >= 7:
        return "UK Expansion Worker likely (overseas-linked incorporation)"
    if score >= 5:
        return "Possible Expansion Worker / Sponsor needs (review)"
    return "Watchlist"

def commercial_bucket(score: int) -> str:
    if score >= 8:
        return "HOT"
    if score >= 6:
        return "MEDIUM"
    return "WATCH"

# =========================
# SerpAPI enrichment + verification
# =========================

def serp_search(query: str, api_key: str, num: int = 5) -> list[dict]:
    params = {
        "engine": "google",
        "q": query,
        "api_key": api_key,
        "num": num
    }
    r = requests.get("https://serpapi.com/search.json", params=params, timeout=60)
    r.raise_for_status()
    data = r.json()
    return data.get("organic_results") or []

def url_domain(u: str) -> str:
    if not u:
        return ""
    u = u.strip()
    u = re.sub(r"^https?://", "", u, flags=re.I)
    u = u.split("/")[0]
    return u.lower()

def get_url_text(url: str, timeout: int = 20) -> str:
    try:
        r = requests.get(url, timeout=timeout, headers={"User-Agent":"Mozilla/5.0 (compatible; CWLeadsBot/1.0)"})
        if r.status_code >= 400:
            return ""
        # avoid huge downloads
        text = r.text
        return text[:500000]
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
    # normalise to absolute
    abs_links = []
    for h in links:
        if h.startswith("http"):
            abs_links.append(h)
        elif h.startswith("/"):
            abs_links.append(base_url.rstrip("/") + h)
        else:
            abs_links.append(base_url.rstrip("/") + "/" + h)
    # dedupe
    out = []
    seen = set()
    for l in abs_links:
        d = l.split("#")[0]
        if d in seen:
            continue
        seen.add(d)
        out.append(d)
    return out[:6]

def verification_score(company_name: str, company_number: str, reg_postcode: str, page_text: str) -> tuple[int, list[str]]:
    """
    Returns score 0-10 + evidence list
    """
    ev = []
    score = 0
    t = (page_text or "")
    t_upper = t.upper()

    # 1) company number present (strong)
    if company_number and re.search(r"\b" + re.escape(company_number) + r"\b", t_upper):
        score += 6
        ev.append("Company number found on site")

    # 2) postcode match (strong-ish)
    if reg_postcode:
        pc = reg_postcode.replace(" ", "").upper()
        if pc and pc in t_upper.replace(" ", ""):
            score += 3
            ev.append("Registered postcode found on site")

    # 3) legal name similarity (medium)
    if company_name:
        # compare against page title / footer-ish chunks by fuzzy matching
        snippet = re.sub(r"\s+", " ", t[:20000])
        sim = fuzz.token_set_ratio(company_name.upper(), snippet.upper())
        if sim >= 75:
            score += 2
            ev.append(f"Name similarity strong ({sim})")
        elif sim >= 60:
            score += 1
            ev.append(f"Name similarity moderate ({sim})")

    score = min(score, 10)
    return score, ev

def enrich_lead_with_contact(lead: dict, serp_key: str, serp_budget: dict) -> dict:
    """
    Uses SerpAPI to find candidate website, verify it, then scrape contact info.
    """
    if serp_budget["calls"] >= SERP_MAX_CALLS_PER_RUN:
        lead["enrich_status"] = "Skipped (Serp budget cap)"
        return lead

    company_name = lead.get("company_name","")
    company_number = lead.get("company_number","")
    reg_postcode = lead.get("reg_postcode","")
    town = lead.get("reg_town","")

    # Primary query: name + postcode (reduces collisions)
    q = f"\"{company_name}\" {reg_postcode}".strip()
    serp_budget["calls"] += 1
    results = serp_search(q, serp_key, num=5)
    time.sleep(SERP_SLEEP_SECONDS)

    candidates = []
    for r in results:
        link = r.get("link") or ""
        if not link.startswith("http"):
            continue
        d = url_domain(link)
        if not d or any(x in d for x in ["companieshouse.gov.uk","gov.uk","linkedin.com","facebook.com","yell.com","endole.co.uk","opencorporates.com","find-and-update.company-information.service.gov.uk"]):
            continue
        candidates.append(link)

    # If none, do a fallback query: name + town
    if not candidates and town:
        q2 = f"\"{company_name}\" {town} contact"
        serp_budget["calls"] += 1
        results2 = serp_search(q2, serp_key, num=5)
        time.sleep(SERP_SLEEP_SECONDS)
        for r in results2:
            link = r.get("link") or ""
            if not link.startswith("http"):
                continue
            d = url_domain(link)
            if not d or any(x in d for x in ["companieshouse.gov.uk","gov.uk","linkedin.com","facebook.com"]):
                continue
            candidates.append(link)

    candidates = candidates[:3]

    best = {"score": -1, "base_url": "", "evidence": [], "emails": [], "phones": []}

    for url in candidates:
        base_url = "https://" + url_domain(url)
        home_html = get_url_text(base_url)
        if not home_html:
            continue
        score, ev = verification_score(company_name, company_number, reg_postcode, home_html)
        # also try privacy/terms/contact pages to find the company number/postcode if not on homepage
        links = find_contact_links(home_html, base_url)
        for l in links:
            if score >= 9:
                break
            extra = get_url_text(l)
            if not extra:
                continue
            s2, ev2 = verification_score(company_name, company_number, reg_postcode, extra)
            # don't double count company number/postcode evidence too aggressively
            score = max(score, s2) if s2 > score else score
            ev = ev if score != s2 else list(set(ev + ev2))

        if score > best["score"]:
            best["score"] = score
            best["base_url"] = base_url
            best["evidence"] = ev

    if best["score"] < VERIFY_MIN_SCORE:
        lead["website"] = best["base_url"] or ""
        lead["website_confidence"] = best["score"] if best["score"] >= 0 else ""
        lead["enrich_status"] = "Manual verify needed (confidence too low)"
        return lead

    # Verified: scrape contact/legal pages for emails/phones
    combined_text = ""
    home_html = get_url_text(best["base_url"])
    combined_text += " " + BeautifulSoup(home_html, "lxml").get_text(" ", strip=True)

    links = find_contact_links(home_html, best["base_url"])
    for l in links[:5]:
        extra = get_url_text(l)
        if extra:
            combined_text += " " + BeautifulSoup(extra, "lxml").get_text(" ", strip=True)

    emails = extract_emails(combined_text)
    phones = extract_phones(combined_text)

    lead["website"] = best["base_url"]
    lead["website_confidence"] = best["score"]
    lead["verification_evidence"] = "; ".join(best["evidence"])[:240]
    lead["emails_found"] = ", ".join(emails)
    lead["phones_found"] = ", ".join(phones)
    lead["enrich_status"] = "Verified & scraped" if (emails or phones) else "Verified (no contacts found)"
    return lead

# =========================
# Email
# =========================

def send_email(subject: str, html: str, csv_bytes: bytes, csv_filename: str):
    host = os.environ["SMTP_HOST"]
    port = int(os.environ.get("SMTP_PORT","587"))
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
    sponsor_count = run_meta.get("new_sponsors",0)
    ch_count = run_meta.get("new_ch_candidates",0)
    enriched = sum(1 for l in leads if (l.get("website_confidence") or 0) >= VERIFY_MIN_SCORE)

    rows = ""
    for l in leads:
        bucket = l.get("bucket","WATCH")
        rows += f"""
        <tr>
          <td>{fmt_pill(bucket)}<div class="small">{l.get('source','')}</div></td>
          <td>
            <div class="v">{l.get('company_name','')}</div>
            <div class="k">Company No: {l.get('company_number','') or '—'} · Incorporated: {l.get('incorporated','') or '—'}</div>
            <div class="small">{l.get('reg_address','')}</div>
          </td>
          <td>
            <div class="v">{l.get('visa_hint','')}</div>
            <div class="k">Score: {l.get('score','')}</div>
            <div class="small">{l.get('why','')}</div>
          </td>
          <td>
            <div class="v">{(l.get('website') and f"<a href='{l.get('website')}'>{l.get('website')}</a>") or "—"}</div>
            <div class="k">Confidence: {l.get('website_confidence') or '—'}</div>
            <div class="small">{l.get('enrich_status','')}</div>
            <div class="small">{l.get('emails_found','')}</div>
            <div class="small">{l.get('phones_found','')}</div>
          </td>
        </tr>
        """

    html = f"""
    <html>
      <head><meta charset="utf-8"><style>{EMAIL_STYLE}</style></head>
      <body>
        <div class="wrap">
          <div class="topbar">
            <div class="h1">UK Expansion Leads — Intelligence Brief</div>
            <div class="muted">
              Run time (UTC): {run_meta.get('run_time_utc','')} · Lookback: {run_meta.get('lookback','')}
              <br/>New sponsors detected: {sponsor_count} · Overseas-signal incorporations: {ch_count} · Verified websites: {enriched}
            </div>
          </div>

          <div class="card">
            <div class="v">Top leads (capped to {MAX_OUTPUT_LEADS})</div>
            <div class="muted">Only verified company websites are scraped for public contact details (confidence gating).</div>
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

# =========================
# Main
# =========================

def main():
    serp_key = os.environ.get("SERPAPI_API_KEY","").strip()
    if not serp_key:
        raise RuntimeError("SERPAPI_API_KEY missing")

    run_ts = utc_now()
    run_ts_iso = run_ts.isoformat()

    conn = ensure_db()

    # -------- Sponsors: diff against seen keys
    new_sponsor_rows = []
    try:
        sponsor_df = fetch_sponsor_df()
        sponsor_records = sponsor_df.to_dict(orient="records")
        for row in sponsor_records:
            key = sponsor_row_key(row)
            if not is_seen(conn, key):
                mark_seen(conn, key, run_ts_iso)
                new_sponsor_rows.append(row)
    except Exception as e:
        new_sponsor_rows = []
        sponsor_error = str(e)
    else:
        sponsor_error = ""

    # -------- Companies House: lookback window
    inc_to = run_ts.date().isoformat()
    inc_from = (run_ts.date() - timedelta(days=LOOKBACK_DAYS)).isoformat()

    ch_items = ch_advanced_incorporated(inc_from, inc_to, size=100)

    ch_candidates = []
    for item in ch_items:
        company_number = item.get("company_number") or ""
        company_name = item.get("company_name") or ""
        if not company_number or not company_name:
            continue

        key = f"CH::{company_number}"
        if is_seen(conn, key):
            continue

        # officers for overseas score
        try:
            officers = ch_company_officers(company_number)
        except Exception:
            continue

        score, reasons, countries = overseas_signal_score(officers)
        if score < 5:
            continue  # balanced threshold

        # registered office best-effort from item
        ro = item.get("registered_office_address") or {}
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
        })

    # Mark CH candidates as seen now (so we won't re-notify if enrichment fails)
    for c in ch_candidates:
        mark_seen(conn, f"CH::{c['company_number']}", run_ts_iso)

    # Sponsor leads formatting (new sponsors only; treat as medium)
    sponsor_leads = []
    for r in new_sponsor_rows:
        name = norm(r.get("Organisation Name") or r.get("Organization Name") or "")
        town = norm(r.get("Town/City") or r.get("Town") or "")
        county = norm(r.get("County") or "")
        route = norm(r.get("Route") or "")
        sub = norm(r.get("Sub Route") or "")
        addr = ", ".join([x for x in [town, county] if x])
        sponsor_leads.append({
            "source": "SPONSOR_REGISTER",
            "company_name": name,
            "company_number": "",
            "incorporated": "",
            "reg_address": addr,
            "reg_postcode": "",
            "reg_town": town,
            "score": 6,
            "why": f"Newly listed sponsor (Route: {route} {('/ ' + sub) if sub else ''})".strip(),
        })

    # Combine, score buckets, sort
    leads = sponsor_leads + ch_candidates
    for l in leads:
        l["visa_hint"] = visa_hint(l["source"], int(l.get("score",0)))
        l["bucket"] = commercial_bucket(int(l.get("score",0)))

    # Sort: HOT first, then score desc, then sponsors first
    bucket_rank = {"HOT":0, "MEDIUM":1, "WATCH":2}
    leads.sort(key=lambda x: (bucket_rank.get(x.get("bucket","WATCH"),9), -int(x.get("score",0)), x.get("source","")))

    # cap to top N for enrichment + email body
    leads = leads[:MAX_OUTPUT_LEADS]

    # Enrich (only for leads with company_number — sponsor register entries often don't have it)
    serp_budget = {"calls": 0}
    for i, l in enumerate(leads):
        l.setdefault("website","")
        l.setdefault("website_confidence","")
        l.setdefault("emails_found","")
        l.setdefault("phones_found","")
        l.setdefault("verification_evidence","")
        l.setdefault("enrich_status","Not attempted")

        # Only enrich CH leads (most verifiable). You can enable sponsor enrichment later if desired.
        if l.get("source") != "COMPANIES_HOUSE":
            l["enrich_status"] = "Skipped (no Companies House identifier)"
            continue

        leads[i] = enrich_lead_with_contact(l, serp_key, serp_budget)

    conn.commit()
    conn.close()

    # CSV attachment
    df = pd.DataFrame(leads)
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    csv_name = f"uk-expansion-leads_{run_ts.date().isoformat()}.csv"

    meta = {
        "run_time_utc": run_ts_iso,
        "lookback": f"{LOOKBACK_DAYS} days (Companies House) · Sponsor register diff (latest vs seen)",
        "new_sponsors": len(new_sponsor_rows),
        "new_ch_candidates": len(ch_candidates),
        "serp_calls": serp_budget["calls"],
        "sponsor_error": sponsor_error,
    }

    subject = f"UK Expansion Leads — {run_ts.date().isoformat()}"
    html = html_report(meta, leads)

    send_email(subject, html, csv_bytes, csv_name)

if __name__ == "__main__":
    main()
