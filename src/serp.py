import os
import time
import requests
from typing import Dict, List, Any, Optional

SERP_ENDPOINT = "https://serpapi.com/search.json"


def _get_serp_key(explicit: Optional[str] = None) -> Optional[str]:
    """Resolve SerpAPI key from multiple env var names (common source of CI issues)."""
    return (
        explicit
        or os.environ.get("SERPAPI_API_KEY")
        or os.environ.get("SERPAPI_KEY")
        or os.environ.get("SERP_API_KEY")
    )


class SerpClient:
    def __init__(self, api_key: Optional[str] = None, pause_s: float = 1.0, timeout_s: float = 30.0):
        self.api_key = _get_serp_key(api_key)
        if not self.api_key:
            raise RuntimeError(
                "Missing SerpAPI key. Set SERPAPI_API_KEY (preferred) or SERPAPI_KEY / SERP_API_KEY in env."
            )
        self.pause_s = pause_s
        self.timeout_s = timeout_s

    def search(self, q: str, num: int = 10) -> List[Dict[str, Any]]:
        params = {
            "engine": "google",
            "q": q,
            "api_key": self.api_key,
            "num": max(1, min(num, 10)),
        }
        r = requests.get(SERP_ENDPOINT, params=params, timeout=self.timeout_s)
        r.raise_for_status()
        data = r.json()
        time.sleep(self.pause_s)

        results: List[Dict[str, Any]] = []
        for item in data.get("organic_results", [])[:num]:
            results.append(
                {
                    "title": item.get("title", ""),
                    "link": item.get("link", ""),
                    "snippet": item.get("snippet", ""),
                    "position": item.get("position"),
                    "source": "serpapi",
                }
            )
        return results
