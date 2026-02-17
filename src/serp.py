import os
import time
import requests
from typing import Dict, List, Any, Optional


SERP_ENDPOINT = "https://serpapi.com/search.json"


class SerpClient:
    def __init__(self, api_key: Optional[str] = None, pause_s: float = 1.0):
        self.api_key = api_key or os.environ.get("SERPAPI_API_KEY")
        if not self.api_key:
            raise RuntimeError("Missing SERPAPI_API_KEY")
        self.pause_s = pause_s

    def search(self, q: str, num: int = 10) -> List[Dict[str, Any]]:
        params = {
            "engine": "google",
            "q": q,
            "api_key": self.api_key,
            "num": max(1, min(num, 10)),
        }
        r = requests.get(SERP_ENDPOINT, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        time.sleep(self.pause_s)

        results = []
        for item in data.get("organic_results", [])[:num]:
            results.append({
                "title": item.get("title", ""),
                "link": item.get("link", ""),
                "snippet": item.get("snippet", ""),
                "source": "serp",
                "query": q,
            })
        return results
