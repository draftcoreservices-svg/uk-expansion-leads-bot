import re
import requests
from bs4 import BeautifulSoup
from typing import Tuple


def fetch_page(url: str, max_chars: int = 12000) -> Tuple[str, str, str]:
    """Returns (final_url, cleaned_text_excerpt, raw_html)."""
    headers = {"User-Agent": "Mozilla/5.0 (compatible; CWLeadsBot/1.0)"}
    r = requests.get(url, headers=headers, timeout=30, allow_redirects=True)
    r.raise_for_status()
    final_url = r.url
    raw_html = r.text or ""

    soup = BeautifulSoup(raw_html, "lxml")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    text = soup.get_text(" ", strip=True)
    text = re.sub(r"\s+", " ", text).strip()
    return final_url, text[:max_chars], raw_html


def fetch_page_text(url: str, max_chars: int = 12000) -> Tuple[str, str]:
    """Backwards-compatible wrapper: returns (final_url, cleaned_text_excerpt)."""
    final_url, text, _html = fetch_page(url, max_chars=max_chars)
    return final_url, text
