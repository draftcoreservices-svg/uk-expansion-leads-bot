from __future__ import annotations

from urllib.parse import urlparse, urljoin
from typing import Optional

from bs4 import BeautifulSoup


ATS_HOSTS = {
    "job-boards.greenhouse.io",
    "boards.greenhouse.io",
    "jobs.lever.co",
    "apply.workable.com",
}


def _is_probably_company_site(url: str) -> bool:
    try:
        u = urlparse(url)
        host = (u.netloc or "").lower()
        if not host:
            return False
        # Exclude ATS + obvious non-company targets
        if host in ATS_HOSTS:
            return False
        if host.endswith(".greenhouse.io") or host.endswith(".lever.co") or host.endswith(".workable.com"):
            return False
        return True
    except Exception:
        return False


def _extract_from_html(page_url: str, html_or_text: str) -> Optional[str]:
    """
    Parse ATS HTML/text and attempt to find the employer's real website URL.
    Works best on Greenhouse/Lever/Workable pages.
    """
    if not html_or_text:
        return None

    # If it's plain text, BeautifulSoup still works but link detection may be limited.
    soup = BeautifulSoup(html_or_text, "lxml")

    # 1) Look for canonical / og:url / employer website patterns
    # Sometimes ATS pages include footer "Website" links or brand links.
    candidates: list[str] = []

    # Common meta tags
    for meta in soup.find_all("meta"):
        prop = (meta.get("property") or "").lower()
        name = (meta.get("name") or "").lower()
        content = meta.get("content") or ""
        if not content:
            continue
        if prop in {"og:site_name", "og:url"}:
            # og:url might still be ATS, but keep it as a hint
            candidates.append(content.strip())
        if name in {"twitter:domain", "twitter:url"}:
            candidates.append(content.strip())

    # Any anchor links that look like "website", "company website", etc.
    for a in soup.find_all("a"):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        text = (a.get_text(" ", strip=True) or "").lower()
        if any(k in text for k in ["website", "company site", "visit website", "company website"]):
            candidates.append(urljoin(page_url, href))

    # Also collect brand/logo links (often point to main site)
    for a in soup.find_all("a"):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        # Many ATS headers have a logo link (empty text but has aria-label)
        aria = (a.get("aria-label") or "").lower()
        if "home" in aria or "website" in aria or "company" in aria:
            candidates.append(urljoin(page_url, href))

    # 2) Normalize + pick first likely company site
    for c in candidates:
        if not c:
            continue
        c = c.strip()
        # If relative, join
        if c.startswith("/"):
            c = urljoin(page_url, c)

        # Must be absolute http(s)
        if not (c.startswith("http://") or c.startswith("https://")):
            continue

        if _is_probably_company_site(c):
            return c

    return None


def extract_company_name_from_ats(html_or_text: str) -> Optional[str]:
    """Best-effort extraction of employer brand name from ATS HTML."""
    if not html_or_text:
        return None
    soup = BeautifulSoup(html_or_text, "lxml")

    # Meta tags commonly carry the brand name
    for meta in soup.find_all("meta"):
        prop = (meta.get("property") or "").lower()
        name = (meta.get("name") or "").lower()
        content = (meta.get("content") or "").strip()
        if not content:
            continue
        if prop in {"og:site_name", "application-name"}:
            return content[:120]
        if name in {"application-name", "apple-mobile-web-app-title"}:
            return content[:120]

    # Title tag often includes the company name
    title = (soup.title.get_text(" ", strip=True) if soup.title else "")
    if title:
        for sep in ["|", " - ", " — ", " · "]:
            if sep in title:
                title = title.split(sep)[-1].strip()
                break
        # Avoid generic leftovers
        if 2 <= len(title) <= 80 and title.lower() not in {"jobs", "careers"}:
            return title[:120]
    return None


def extract_company_website_from_ats(
    page_url: Optional[str] = None,
    page_text: Optional[str] = None,
    page_html_or_text: Optional[str] = None,
    **kwargs,
) -> Optional[str]:
    """
    Backwards-compatible helper.

    Supports calling styles:
      extract_company_website_from_ats(url, text)
      extract_company_website_from_ats(page_url=url, page_text=text)
      extract_company_website_from_ats(page_url=url, page_html_or_text=text)

    Any extra kwargs are ignored to avoid future breakage.
    """
    # Allow old keyword names (if present)
    if page_url is None:
        page_url = kwargs.get("url") or kwargs.get("lead_url")

    # Prefer page_html_or_text if provided, else page_text
    html_or_text = page_html_or_text if page_html_or_text is not None else page_text
    if not page_url or not html_or_text:
        return None

    # Only attempt extraction for ATS hosts (optional safeguard)
    try:
        host = (urlparse(page_url).netloc or "").lower()
        if host and host not in ATS_HOSTS:
            # Still allow extraction, but it’s less likely useful
            pass
    except Exception:
        pass

    return _extract_from_html(page_url, html_or_text)
