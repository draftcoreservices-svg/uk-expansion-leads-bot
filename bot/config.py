import os
from dataclasses import dataclass, field
from typing import List, Set


def _env_int(name: str, default: int) -> int:
    v = os.environ.get(name)
    if v is None or str(v).strip() == "":
        return default
    try:
        return int(v)
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    v = os.environ.get(name)
    if v is None or str(v).strip() == "":
        return default
    try:
        return float(v)
    except Exception:
        return default


def _env_str(name: str, default: str) -> str:
    v = os.environ.get(name)
    return default if v is None else str(v)


@dataclass
class Config:
    # Output
    max_output_leads: int = field(default_factory=lambda: _env_int("MAX_OUTPUT_LEADS", 20))
    lookback_days: int = field(default_factory=lambda: _env_int("LOOKBACK_DAYS", 30))

    # Companies House throughput caps
    ch_max_results_total: int = field(default_factory=lambda: _env_int("CH_MAX_RESULTS_TOTAL", 1200))
    ch_max_companies_to_check: int = field(default_factory=lambda: _env_int("CH_MAX_COMPANIES_TO_CHECK", 300))
    ch_search_timeout: int = field(default_factory=lambda: _env_int("CH_SEARCH_TIMEOUT", 20))
    ch_officers_timeout: int = field(default_factory=lambda: _env_int("CH_OFFICERS_TIMEOUT", 15))
    ch_retry_count: int = field(default_factory=lambda: _env_int("CH_RETRY_COUNT", 3))

    # SerpAPI controls
    serp_max_calls_per_run: int = field(default_factory=lambda: _env_int("SERP_MAX_CALLS_PER_RUN", 120))
    serp_sleep_seconds: float = field(default_factory=lambda: _env_float("SERP_SLEEP_SECONDS", 1.2))
    serp_stage_a_limit: int = field(default_factory=lambda: _env_int("SERP_STAGE_A_LIMIT", 30))  # homepage discovery
    serp_stage_b_limit: int = field(default_factory=lambda: _env_int("SERP_STAGE_B_LIMIT", 20))  # verified scraping

    # Website verification
    verify_min_score: int = field(default_factory=lambda: _env_int("VERIFY_MIN_SCORE", 7))

    # Sponsor register routes (allowlist)
    sponsor_route_allowlist: Set[str] = field(default_factory=lambda: set(filter(None, [
        "Skilled Worker",
        "Global Business Mobility: Senior or Specialist Worker",
        "Global Business Mobility: UK Expansion Worker",
    ])))

    # Name noise filters
    min_clean_name_len: int = field(default_factory=lambda: _env_int("MIN_CLEAN_NAME_LEN", 3))
    max_non_alnum_ratio: float = field(default_factory=lambda: _env_float("MAX_NON_ALNUM_RATIO", 0.35))

    # Classification thresholds (score 0..100)
    score_hot: int = field(default_factory=lambda: _env_int("SCORE_HOT", 75))
    score_medium: int = field(default_factory=lambda: _env_int("SCORE_MEDIUM", 55))

    # Backfill
    min_total_leads_target: int = field(default_factory=lambda: _env_int("MIN_TOTAL_LEADS_TARGET", 15))

    # Contact hygiene
    allow_personal_emails: bool = (_env_str("ALLOW_PERSONAL_EMAILS", "0").strip() == "1")
    allowed_email_prefixes: Set[str] = field(default_factory=lambda: set(filter(None, [
        "info", "hello", "contact", "sales", "support", "enquiries", "enquiry", "admin", "office", "hr", "careers",
    ])))

    mailbox_penalty_phrases: List[str] = field(default_factory=lambda: [
        "KEMP HOUSE",
        "SHELTON STREET",
        "OFFICE ",
        "SUITE ",
        "PO BOX",
        "INTERNATIONAL HOUSE",
        "WEWORK",
        "REGUS",
        "VIRTUAL OFFICE",
        "MAILBOX",
    ])

    # SIC heuristics (keyword-based, applied to SIC descriptions if present)
    sic_penalty_keywords: List[str] = field(default_factory=lambda: [
        "taxi", "minicab", "takeaway", "restaurant", "cafe", "hair", "beauty", "barber",
        "nail", "cleaning", "laundry", "retail", "online retail", "clothing",
    ])

    sic_boost_keywords: List[str] = field(default_factory=lambda: [
        "software", "information", "technology", "engineering", "manufacturing",
        "pharmaceutical", "medical", "logistics", "freight", "shipping", "energy",
        "financial", "consultancy", "management consultancy",
    ])
