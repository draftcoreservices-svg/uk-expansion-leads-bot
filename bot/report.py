from __future__ import annotations

import io
import pandas as pd

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


def _pill(bucket: str) -> str:
    if bucket == 'HOT':
        return '<span class="pill hot">Hot</span>'
    if bucket == 'MEDIUM':
        return '<span class="pill med">Medium</span>'
    return '<span class="pill watch">Watchlist</span>'


def html_report(run_meta: dict, leads: list[dict], max_output: int) -> str:
    rows = ''
    for l in leads:
        bucket = l.get('bucket', 'WATCH')
        website = l.get('website') or ''
        website_html = f"<a href='{website}'>{website}</a>" if website else '—'
        route = l.get('sponsor_route') or ''
        route_html = f"<div class='small'><span class='k'>Route:</span> {route}</div>" if route else ''

        verify_ev = l.get('verification_evidence') or ''
        verify_html = f"<div class='small'><span class='k'>Verify:</span> {verify_ev}</div>" if verify_ev else ''

        rows += f"""
        <tr>
          <td>{_pill(bucket)}<div class=\"small\">{l.get('source','')}</div></td>
          <td>
            <div class=\"v\">{l.get('company_name','')}</div>
            <div class=\"k\">Company No: {l.get('company_number','') or '—'} · Incorporated: {l.get('incorporated','') or '—'}</div>
            <div class=\"small\">{l.get('reg_address','')}</div>
            {route_html}
          </td>
          <td>
            <div class=\"v\">{l.get('visa_hint','')}</div>
            <div class=\"k\">Score: {l.get('score','')}</div>
            <div class=\"small\">{l.get('why','')}</div>
          </td>
          <td>
            <div class=\"v\">{website_html}</div>
            <div class=\"k\">Level: {l.get('website_level') or '—'} · Score: {l.get('website_score') or '—'}</div>
            <div class=\"small\">{l.get('enrich_status','')}</div>
            <div class=\"small\">{l.get('emails_found','')}</div>
            <div class=\"small\">{l.get('phones_found','')}</div>
            {verify_html}
          </td>
        </tr>
        """

    sponsor_note = ''
    if run_meta.get('sponsor_baselined_this_run') == '1':
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
              New sponsors (filtered routes): {run_meta.get('new_sponsors',0)} · CH overseas-linked candidates: {run_meta.get('new_ch_candidates',0)}
              · Verified websites: {run_meta.get('verified_sites',0)} · Serp calls: {run_meta.get('serp_calls',0)}
            </div>
          </div>

          <div class="card">
            <div class="v">Top leads (capped to {max_output})</div>
            <div class="small">Internal BD tooling. Information-only; always verify before outreach.</div>
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
              Sources: GOV.UK Sponsor Register, Companies House, SerpAPI. Always verify primary sources.
            </div>
          </div>
        </div>
      </body>
    </html>
    """
    return html


def leads_to_csv_bytes(leads: list[dict]) -> bytes:
    df = pd.DataFrame(leads)
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    return buf.getvalue().encode('utf-8')
