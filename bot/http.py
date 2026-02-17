import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def make_session(retry_total: int = 3) -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=retry_total,
        backoff_factor=0.6,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount('https://', adapter)
    s.mount('http://', adapter)
    return s
