from __future__ import annotations
from typing import Optional
from urllib.parse import urlparse
import re

def extract_company_website_from_ats(page_url: str, page_text: str) -> Optional[str]:
    """
    Attempt to extract the employer's real website from ATS pages (Greenhouse/Lever/Workable).
    Returns a URL like https://company.com or None.
    """
    if not page_text:
        return None

    # Look for obvious website links in the HTML/text
    candidates = set()

    # Common patterns like "Website" / "Company website"
    for m in re.finditer(r'https?://[^\s"<>()]+', page_text, flags=re.IGNORECASE):
        candidates.add(m.group(0).rstrip(").,;\"'"))

    # Remove ATS links and junk
    def is_good(u: str) -> bool:
        try:
            host = (urlparse(u).netloc or "").lower()
            if not host:
                return False
            if host.endswith("greenhouse.io") or host.endswith("lever.co") or host.endswith("workable.com"):
                return False
            if host.endswith("linkedin.com") or host.endswith("facebook.com") or host.endswith("twitter.com") or host.endswith("x.com"):
                return False
            return True
        except Exception:
            return False

    good = [u for u in candidates if is_good(u)]
    if not good:
        return None

    # Prefer shortest plausible root domain
    good.sort(key=len)
    return good[0]
