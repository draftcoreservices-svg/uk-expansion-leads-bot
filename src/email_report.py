from datetime import datetime
from typing import List
from .leads import Lead
from .config import CFG


def _lead_block(l: Lead) -> str:
    ch = ""
    if l.companies_house and l.companies_house.get("company_number"):
        ch = f"<br><b>Companies House:</b> {l.companies_house.get('company_name','')} ({l.companies_house.get('company_number')})"

    sponsor = ""
    if l.lead_type == "sponsor_licence":
        if l.sponsor_register is True:
            sponsor = "<br><b>Sponsor Register:</b> Yes"
        elif l.sponsor_register is False:
            sponsor = "<br><b>Sponsor Register:</b> No"
        else:
            sponsor = "<br><b>Sponsor Register:</b> Unknown"

    reasons = "".join(f"<li>{r}</li>" for r in l.reasons[:6])
    return f"""
    <div style="padding:12px;border:1px solid #333;border-radius:10px;margin:10px 0;">
      <div style="font-size:16px;"><b>{l.company_or_person or l.title}</b> — <span style="opacity:0.9;">Score {l.score}</span></div>
      <div style="margin-top:6px;"><a href="{l.final_url or l.url}">{l.final_url or l.url}</a></div>
      {ch}{sponsor}
      <div style="margin-top:8px;"><b>Why:</b><ul>{reasons}</ul></div>
      <div style="margin-top:8px;opacity:0.85;"><i>{(l.snippet or '')[:240]}</i></div>
    </div>
    """


def build_email(sponsor: List[Lead], mobility: List[Lead], talent: List[Lead]) -> str:
    now = datetime.utcnow().strftime("%Y-%m-%d")
    def section(title: str, leads: List[Lead]) -> str:
        if not leads:
            return f"<h2>{title}</h2><p>No strong leads this run.</p>"
        blocks = "\n".join(_lead_block(l) for l in leads[:CFG.max_leads_per_bucket])
        return f"<h2>{title}</h2>{blocks}"

    return f"""
    <html>
      <body style="font-family:Arial, sans-serif; background:#0b1220; color:#e7eefc; padding:18px;">
        <h1 style="margin-top:0;">CW Weekly Leads — {now}</h1>
        <p style="opacity:0.9;">
          Public-source intelligence only. Always verify before outreach. Output is deduped week-to-week.
        </p>
        {section("1) Sponsor licence needed — Strong leads", sponsor)}
        {section("2) Global mobility route — Strong leads", mobility)}
        {section("3) Global Talent / Exceptional Promise — Strong leads", talent)}
      </body>
    </html>
    """
