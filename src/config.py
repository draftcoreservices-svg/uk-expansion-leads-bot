from __future__ import annotations

import os
from dataclasses import dataclass
from typing import List


def _env(name: str, default: str | None = None) -> str:
    v = os.getenv(name)
    if v is None or v == "":
        if default is None:
            raise RuntimeError(f"Missing required environment variable: {name}")
        return default
    return v


def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if not v:
        return default
    return int(v)


@dataclass(frozen=True)
class Config:
    companies_house_api_key: str

    smtp_host: str
    smtp_port: int
    smtp_user: str
    smtp_pass: str
    email_from: str
    email_to: List[str]

    sponsor_register_url: str | None

    cache_path: str
    max_leads: int

    advanced_page_size: int
    max_pages_per_sic: int


def load_config() -> Config:
    email_to_raw = _env("EMAIL_TO")
    email_to = [x.strip() for x in email_to_raw.split(",") if x.strip()]

    return Config(
        companies_house_api_key=_env("COMPANIES_HOUSE_API_KEY"),
        smtp_host=_env("SMTP_HOST"),
        smtp_port=_env_int("SMTP_PORT", 587),
        smtp_user=_env("SMTP_USER"),
        smtp_pass=_env("SMTP_PASS"),
        email_from=_env("EMAIL_FROM"),
        email_to=email_to,
        sponsor_register_url=os.getenv("SPONSOR_REGISTER_URL"),
        cache_path=os.getenv("CW_CACHE_PATH", "data/cw_cache.sqlite"),
        max_leads=_env_int("MAX_LEADS", 50),
        advanced_page_size=_env_int("ADVANCED_PAGE_SIZE", 200),
        max_pages_per_sic=_env_int("MAX_PAGES_PER_SIC", 10),
    )
