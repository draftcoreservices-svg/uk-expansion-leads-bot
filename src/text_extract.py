import re
import requests
from bs4 import BeautifulSoup
from typing import Tuple


def fetch_page_text(url: str, max_chars: int = 12000) -> Tuple[str, str]:
    """
    Returns (final_url, text).
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; CWLeadsBot/1.0)"
    }
    r = requests.get(url, headers=headers, timeout=30, allow_redirects=True)
    r.raise_for_status()
    final_url = r.url

    soup = BeautifulSoup(r.text, "lxml")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    text = soup.get_text(" ", strip=True)
    text = re.sub(r"\s+", " ", text).strip()
    return final_url, text[:max_chars]
