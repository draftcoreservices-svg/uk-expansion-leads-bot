import re
from typing import Tuple

import requests
from bs4 import BeautifulSoup

DEFAULT_TIMEOUT = (10, 60)


def _clean_text(s: str) -> str:
    if not s:
        return ""
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def _extract_visible_text_from_html(html: str) -> str:
    soup = BeautifulSoup(html or "", "lxml")

    # Remove obvious non-content
    for tag in soup(["script", "style", "noscript", "svg", "canvas", "iframe"]):
        try:
            tag.decompose()
        except Exception:
            pass

    # Get text
    text = soup.get_text(separator=" ")
    return _clean_text(text)


def fetch_page(url: str, max_chars: int = 12000) -> Tuple[str, str, str]:
    """
    Fetch a web page and return:
      (final_url, visible_text, html)

    This is the function src.run_weekly expects.
    """
    if not url:
        raise ValueError("URL is empty")

    r = requests.get(
        url,
        timeout=DEFAULT_TIMEOUT,
        headers={"User-Agent": "Mozilla/5.0 (CWLeadsBot/1.0)"},
        allow_redirects=True,
    )
    r.raise_for_status()

    final_url = str(r.url)
    html = r.text or ""
    text = _extract_visible_text_from_html(html)

    if max_chars and len(text) > max_chars:
        text = text[:max_chars]

    return final_url, text, html


# Backwards compatibility (in case other modules import older names)
def fetch_text(url: str, max_chars: int = 12000) -> Tuple[str, str]:
    final_url, text, _html = fetch_page(url, max_chars=max_chars)
    return final_url, text
