from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any


@dataclass
class Lead:
    lead_type: str  # "sponsor_licence" | "global_mobility" | "global_talent"
    title: str
    url: str
    snippet: str

    company_or_person: str = ""
    final_url: str = ""
    page_text: str = ""

    score: int = 0
    reasons: List[str] = field(default_factory=list)

    sponsor_register: Optional[bool] = None
    companies_house: Optional[Dict[str, Any]] = None

    contact_emails: List[str] = field(default_factory=list)
    contact_phones: List[str] = field(default_factory=list)
