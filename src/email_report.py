from datetime import datetime
from typing import List
from .leads import Lead


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

    return f"""
    <div style="padding:12px;border:1px solid #2a3550;border-radius:12px;margin:12px 0;background:#0e1730;">
      <div style="font-size:16px;"><b>{l.company_or_person or l.title}</b> — <span style="opacity:0.9;">Score {l.score}</span></div>
      <div style="margin-top:6px;"><a style="color:#9cc3ff;" href="{l.final_url or l.url}">{l.final_url or l.url}</a></div>
      {ch_line}
      {sponsor_line}
      {contacts}
      <div style="margin-top:10px;"><b>Why:</b><ul>{why_list}</ul></div>
      {ai}
      <div style="margin-top:8px;opacity:0.75;"><i>{(l.snippet or '')[:260]}</i></div>
    </div>
    """


def _section(title: str, leads: List[Lead]) -> str:
    if not leads:
        return f"<h2>{title}</h2><p>No strong leads this run.</p>"
    blocks = "\n".join(_lead_block(l) for l in leads)
    return f"<h2>{title}</h2>{blocks}"


def build_email(sponsor: List[Lead], mobility: List[Lead], talent: List[Lead]) -> str:
    now = datetime.utcnow().strftime("%Y-%m-%d")
    return f"""
    <html>
      <body style="font-family:Arial, sans-serif; background:#0b1220; color:#e7eefc; padding:18px;">
        <h1 style="margin-top:0;">CW Weekly Leads — {now}</h1>
        <p style="opacity:0.9;">
          Public-source intelligence only. Always verify before outreach. Use responsibly.
        </p>
        {_section("1) Sponsor licence needed — Strong leads", sponsor)}
        {_section("2) Global mobility route — Strong leads", mobility)}
        {_section("3) Global Talent / Exceptional Promise — Strong leads", talent)}
      </body>
    </html>
    """
