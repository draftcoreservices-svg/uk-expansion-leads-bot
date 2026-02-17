import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    # Output controls
    max_output_leads: int = int(os.getenv('MAX_OUTPUT_LEADS', '25'))
    lookback_days: int = int(os.getenv('LOOKBACK_DAYS', '30'))

    # Enrichment
    serp_max_calls_per_run: int = int(os.getenv('SERP_MAX_CALLS_PER_RUN', '80'))
    serp_sleep_seconds: float = float(os.getenv('SERP_SLEEP_SECONDS', '1.2'))
    verify_min_score: int = int(os.getenv('VERIFY_MIN_SCORE', '7'))  # 0..10 hard verify
    enrich_cache_days: int = int(os.getenv('ENRICH_CACHE_DAYS', '60'))

    # Companies House speed caps
    ch_max_companies_to_check: int = int(os.getenv('CH_MAX_COMPANIES_TO_CHECK', '140'))
    ch_max_results_total: int = int(os.getenv('CH_MAX_RESULTS_TOTAL', '800'))
    ch_officers_timeout: int = int(os.getenv('CH_OFFICERS_TIMEOUT', '15'))
    ch_search_timeout: int = int(os.getenv('CH_SEARCH_TIMEOUT', '20'))
    ch_retry_count: int = int(os.getenv('CH_RETRY_COUNT', '3'))

    # Matching
    sponsor_match_min_score: int = int(os.getenv('SPONSOR_MATCH_MIN_SCORE', '72'))  # 0..100

    # Filtering
    min_clean_name_len: int = int(os.getenv('MIN_CLEAN_NAME_LEN', '3'))
    max_non_alnum_ratio: float = float(os.getenv('MAX_NON_ALNUM_RATIO', '0.35'))

    # Sponsor register allowlist
    sponsor_route_allowlist = {
        'Skilled Worker',
        'Global Business Mobility: Senior or Specialist Worker',
        'Global Business Mobility: UK Expansion Worker',
    }

    # High-priority countries (for small boosts)
    priority_countries = {
        'US','USA','UNITED STATES','CANADA','UAE','UNITED ARAB EMIRATES','INDIA','AUSTRALIA',
        'GERMANY','FRANCE','NETHERLANDS','SPAIN','ITALY','IRELAND','SWEDEN','DENMARK','NORWAY',
        'FINLAND','BELGIUM','SWITZERLAND','AUSTRIA','POLAND','CZECHIA','PORTUGAL','GREECE',
        'ROMANIA','BULGARIA','HUNGARY'
    }


def require_env(name: str) -> str:
    v = os.getenv(name, '').strip()
    if not v:
        raise RuntimeError(f'{name} missing')
    return v
