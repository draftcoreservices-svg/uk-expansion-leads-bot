from datetime import datetime
from typing import List, Dict
from .leads import Lead


def _escape(s: str) -> str:
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _lead_block(l: Lead) -> str:
    ch_line = ""
    if l.companies_house and l.companies_house.get("company_number"):
        ch_line = (
            f"<div><b>Companies House:</b> {l.companies_house.get('company_name','')} "
            f"({l.companies_house.get('company_number')})</div>"
        )

    sponsor_line = ""
    if l.lead_type == "sponsor_licence":
        if l.on_sponsor_register is True:
            sponsor_line = "<div><b>Sponsor Register:</b> Yes</div>"
        elif l.on_sponsor_register is False:
            sponsor_line = "<div><b>Sponsor Register:</b> No</div>"
        else:
            sponsor_line = "<div><b>Sponsor Register:</b> Unknown</div>"

    contacts = ""
    if l.contact_emails or l.contact_phones:
        bits = []
        if l.contact_emails:
            bits.append(f"<b>Email(s):</b> {', '.join(l.contact_emails)}")
        if l.contact_phones:
            bits.append(f"<b>Phone(s):</b> {', '.join(l.contact_phones)}")
        contacts = f"<div>{' | '.join(bits)}</div>"

    why_list = "".join(f"<li>{r}</li>" for r in (l.reasons or [])[:6])
    ai = ""
    if l.ai_summary:
        ai = f"""
        <div style="margin-top:8px;">
          <b>AI summary:</b> {l.ai_summary}<br>
          <b>Outreach angle:</b> {l.ai_outreach_angle}<br>
          <b>Signal quote:</b> <i>{l.ai_quote}</i>
        </div>
        """

    url = l.final_url or l.url
    label = _escape(l.company_or_person or l.title)
    snippet = _escape((l.snippet or "")[:220])
    return f"""
    <tr>
      <td style="padding:12px 12px 12px 0; vertical-align:top;">
        <div style="font-weight:700; font-size:14px;">{label}</div>
        <div style="margin-top:6px; font-size:12px; color:#334155;">{snippet}</div>
        <div style="margin-top:8px;"><a href="{url}" style="color:#2563eb; text-decoration:none;">Open source</a></div>
        {contacts}
      </td>
      <td style="padding:12px; vertical-align:top; width:180px;">
        <div style="font-size:12px; color:#0f172a;"><b>Score:</b> {l.score}</div>
        {sponsor_line}
        {ch_line}
      </td>
      <td style="padding:12px; vertical-align:top;">
        <div style="font-size:12px; color:#0f172a;">{('<ul style="margin:6px 0 0 18px; padding:0;">'+why_list+'</ul>') if why_list else ''}</div>
        {ai}
      </td>
    </tr>
    """


def _group_by_company(leads: List[Lead]) -> List[Lead]:
    """De-duplicate multiple URLs for the same company; keep highest-scoring lead per company."""
    seen: Dict[str, Lead] = {}
    for l in leads:
        k = (l.company_or_person or l.title or "").strip().lower()
        if not k:
            continue
        if k not in seen or l.score > seen[k].score:
            seen[k] = l
    out = list(seen.values())
    out.sort(key=lambda x: x.score, reverse=True)
    return out


def _section(title: str, leads: List[Lead], note: str = "") -> str:
    if not leads:
        return f"<h2 style=\"margin:24px 0 8px; font-size:16px;\">{_escape(title)}</h2><p style=\"margin:0; color:#334155;\">No strong leads this run.</p>"

    # De-dupe per company for readability
    leads2 = _group_by_company(leads)

    rows = "\n".join(_lead_block(l) for l in leads2)
    note_html = f"<div style=\"margin:6px 0 10px; color:#475569; font-size:12px;\">{_escape(note)}</div>" if note else ""

    return f"""
    <h2 style="margin:24px 0 8px; font-size:16px;">{_escape(title)}</h2>
    {note_html}
    <table role="presentation" cellpadding="0" cellspacing="0" width="100%" style="border-collapse:collapse; border:1px solid #e2e8f0; border-radius:12px; overflow:hidden;">
      <thead>
        <tr style="background:#f8fafc;">
          <th align="left" style="padding:10px 12px; font-size:12px; color:#0f172a;">Lead</th>
          <th align="left" style="padding:10px 12px; font-size:12px; color:#0f172a; width:180px;">Checks</th>
          <th align="left" style="padding:10px 12px; font-size:12px; color:#0f172a;">Why this matters</th>
        </tr>
      </thead>
      <tbody>
        {rows}
      </tbody>
    </table>
    """


def build_email(sponsor: List[Lead], mobility: List[Lead], talent: List[Lead]) -> str:
    now = datetime.utcnow().strftime("%Y-%m-%d")
    counts = f"Sponsor: {len(sponsor)} | Mobility: {len(mobility)} | Talent: {len(talent)}"
    return f"""
    <html>
      <body style="font-family:Arial, sans-serif; background:#0b1220; color:#e7eefc; padding:18px;">
        <div style="max-width:980px; margin:0 auto; background:#ffffff; color:#0f172a; padding:22px; border-radius:16px;">
          <div style="display:flex; justify-content:space-between; align-items:flex-end; gap:12px;">
            <div>
              <div style="font-size:20px; font-weight:800;">CW Weekly Leads — {now}</div>
              <div style="margin-top:4px; color:#475569; font-size:12px;">Public-source intelligence only. Always verify before outreach. Use responsibly.</div>
            </div>
            <div style="font-size:12px; color:#475569;">{_escape(counts)}</div>
          </div>

          {_section("1) Sponsor licence needed — Strong leads", sponsor, note="Deduped by company. Focus: UK hiring + sponsorship language + likely not licensed.")}
          {_section("2) Global mobility / UK expansion — Strong leads", mobility, note="Recency-weighted. Focus: new UK entity/office, UK leadership, or hiring tied to expansion.")}
          {_section("3) Global Talent / Exceptional Promise — Signals", talent, note="Optional. These are inbound-style opportunities, not corporate BD targets.")}

          <div style="margin-top:22px; padding-top:12px; border-top:1px solid #e2e8f0; color:#64748b; font-size:11px;">
            Notes: Sponsor Register matching is best-effort (exact + fuzzy). Companies House matches are only shown when high-confidence.
          </div>
        </div>
      </body>
    </html>
    """
