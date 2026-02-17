import re
import time
import requests
from bs4 import BeautifulSoup
from typing import Tuple


DEFAULT_TIMEOUT = (10, 30)  # (connect, read)


def fetch_page_text(url: str, max_chars: int = 12000) -> Tuple[str, str]:
    """Fetch a URL and return (final_url, cleaned_text_excerpt).

    Uses conservative timeouts + a small retry to avoid CI hangs.
    """
    headers = {"User-Agent": "Mozilla/5.0 (compatible; CWLeadsBot/1.0)"}
    last_err: Exception | None = None

    for attempt in range(2):
        try:
            r = requests.get(url, headers=headers, timeout=DEFAULT_TIMEOUT, allow_redirects=True)
            r.raise_for_status()
            final_url = r.url

            soup = BeautifulSoup(r.text, "lxml")
            for tag in soup(["script", "style", "noscript"]):
                tag.decompose()

            text = soup.get_text(" ", strip=True)
            text = re.sub(r"\s+", " ", text).strip()
            if len(text) > max_chars:
                text = text[:max_chars]
            return final_url, text
        except Exception as e:
            last_err = e
            time.sleep(1.0)

    raise last_err  # type: ignore
