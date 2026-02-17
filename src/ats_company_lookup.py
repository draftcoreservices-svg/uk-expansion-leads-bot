from urllib.parse import urlparse
from bs4 import BeautifulSoup


def extract_company_website_from_ats(final_url: str, page_text: str) -> str | None:
    """
    Attempts to extract the real company homepage from ATS pages
    (Greenhouse / Lever / Workable).
    Returns company website URL if found.
    """

    try:
        soup = BeautifulSoup(page_text, "lxml")

        # Look for anchor tags that likely point to company homepage
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()

            # Must be absolute URL
            if not href.startswith("http"):
                continue

            host = urlparse(href).netloc.lower()

            # Ignore ATS hosts
            if any(x in host for x in [
                "greenhouse.io",
                "lever.co",
                "workable.com",
                "boards.greenhouse.io",
            ]):
                continue

            # Ignore social
            if any(x in host for x in [
                "linkedin.com",
                "twitter.com",
                "x.com",
                "facebook.com",
                "instagram.com",
            ]):
                continue

            # If link text looks like homepage/company
            text = (a.get_text() or "").lower()
            if any(k in text for k in ["home", "company", "website", "about"]):
                return href

        # Fallback: meta property og:url
        og = soup.find("meta", property="og:url")
        if og and og.get("content", "").startswith("http"):
            return og["content"]

        return None

    except Exception:
        return None
