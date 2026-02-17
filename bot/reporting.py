from typing import List, Dict, Any
from html import escape
from datetime import datetime, timezone

CSS = """
body{font-family:Inter,Segoe UI,Arial,sans-serif;background:#0b0f19;color:#e7eaf3;margin:0;padding:0}
.container{max-width:980px;margin:0 auto;padding:24px}
.card{background:#121a2b;border:1px solid #22304f;border-radius:14px;padding:16px;margin:14px 0;box-shadow:0 8px 24px rgba(0,0,0,0.25)}
.h1{font-size:22px;margin:0 0 6px 0}
.small{color:#aab3c5;font-size:12px}
.badge{display:inline-block;padding:4px 10px;border-radius:999px;font-size:12px;margin-right:6px;border:1px solid #2a3a60;background:#0f1730}
.badge.hot{border-color:#7c5cff}
.badge.med{border-color:#2dd4bf}
.badge.watch{border-color:#94a3b8}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:14px}
.kv{background:#0f1730;border:1px solid #22304f;border-radius:12px;padding:12px}
.k{color:#aab3c5;font-size:12px}
.v{font-size:14px;margin-top:4px}
table{width:100%;border-collapse:collapse}
th,td{text-align:left;padding:10px;border-bottom:1px solid #22304f;vertical-align:top}
th{color:#aab3c5;font-weight:600;font-size:12px}
a{color:#8ab4ff}
.note{background:#0f1730;border:1px dashed #36518b;border-radius:12px;padding:10px;color:#c8d2ea}
"""


def _badge(bucket: str) -> str:
    b = (bucket or "").upper()
    cls = "watch"
    if b == "HOT":
        cls = "hot"
    elif b == "MEDIUM":
        cls = "med"
    return f'<span class="badge {cls}">{escape(b)}</span>'


def build_html(meta: Dict[str, Any], leads: List[Dict[str, Any]], backfill_count: int) -> str:
    run_ts = meta.get("run_ts_utc") or datetime.now(timezone.utc).isoformat()
    sponsor_excl = meta.get("sponsor_register_exclusions", 0)
    ch_candidates = meta.get("ch_candidates_considered", 0)
    serp_calls = meta.get("serp_calls", 0)
    verified = meta.get("verified_sites", 0)

    summary = f"""
    <div class="card">
      <div class="h1">Sponsor Licence Leads — Intelligence Brief</div>
      <div class="small">Run time (UTC): {escape(run_ts)} · Lookback: {escape(str(meta.get('lookback_days', '')))} days</div>
      <div style="margin-top:10px" class="grid">
        <div class="kv"><div class="k">Sponsor register exclusions loaded</div><div class="v">{sponsor_excl}</div></div>
        <div class="kv"><div class="k">CH candidates considered</div><div class="v">{ch_candidates}</div></div>
        <div class="kv"><div class="k">SerpAPI calls</div><div class="v">{serp_calls}</div></div>
        <div class="kv"><div class="k">Verified websites scraped</div><div class="v">{verified}</div></div>
      </div>
      <div class="note" style="margin-top:12px">
        Output is capped. Only verified company websites are scraped for public contact details. Always verify before outreach.
        Backfill items included: {backfill_count}.
      </div>
    </div>
    """

    rows = []
    for l in leads:
        rows.append(f"""
        <tr>
          <td>{_badge(l.get('bucket',''))}</td>
          <td><b>{escape(l.get('company_name',''))}</b><div class="small">{escape(l.get('company_number','') or '')}</div></td>
          <td>{escape(l.get('case_type',''))}<div class="small">{escape(l.get('visa_hint',''))}</div></td>
          <td>
            <div class="small">{escape(l.get('reg_town','') or '')}</div>
            <div class="small">{escape(l.get('incorporated','') or '')}</div>
          </td>
          <td>
            {escape(l.get('website','') or '')}
            <div class="small">{escape(l.get('emails','') or '')}</div>
            <div class="small">{escape(l.get('phones','') or '')}</div>
          </td>
          <td class="small">{escape(l.get('why','') or '')}</td>
        </tr>
        """)

    table = """<div class="card"><div class="h1">Top leads</div>
    <table><thead><tr>
      <th>Status</th><th>Company</th><th>Case type</th><th>Location / date</th><th>Website & contacts</th><th>Why</th>
    </tr></thead><tbody>""" + "\n".join(rows) + """</tbody></table></div>"""

    return f"""<!doctype html><html><head><meta charset="utf-8"><style>{CSS}</style></head>
    <body><div class="container">{summary}{table}</div></body></html>"""


def build_csv(leads: List[Dict[str, Any]]) -> str:
    import csv
    import io

    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["bucket","company_name","company_number","case_type","visa_hint","score","job_intent","job_sources","incorporated","reg_town","website","emails","phones","why","source"])
    for l in leads:
        w.writerow([
            l.get("bucket", ""),
            l.get("company_name", ""),
            l.get("company_number", ""),
            l.get("case_type", ""),
            l.get("visa_hint", ""),
            l.get("score", ""),
            l.get("job_intent", ""),
            l.get("job_sources", ""),
            l.get("incorporated", ""),
            l.get("reg_town", ""),
            l.get("website", ""),
            l.get("emails", ""),
            l.get("phones", ""),
            l.get("why", ""),
            l.get("source", ""),
        ])
    return out.getvalue()
