from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any


@dataclass
class Lead:
    # "sponsor_licence" | "global_mobility" | "global_talent"
    lead_type: str

    title: str
    url: str
    snippet: str
    query: str = ""

    # Enrichment
    final_url: str = ""
    page_text: str = ""
    company_or_person: str = ""

    # Scoring
    score: int = 0
    reasons: List[str] = field(default_factory=list)
    ai_summary: str = ""
    ai_outreach_angle: str = ""
    ai_quote: str = ""

    # Checks
    on_sponsor_register: Optional[bool] = None
    companies_house: Optional[Dict[str, Any]] = None

    # Contacts (best-effort from public page text)
    contact_emails: List[str] = field(default_factory=list)
    contact_phones: List[str] = field(default_factory=list)
