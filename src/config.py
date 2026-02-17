from dataclasses import dataclass, field
import os


# GOV.UK landing page (stable) that links to the current Register file
SPONSOR_REGISTER_PAGE = os.environ.get(
    "SPONSOR_REGISTER_PAGE",
    "https://www.gov.uk/government/publications/register-of-licensed-sponsors-workers",
)


def _split_csv(s: str) -> list[str]:
    if not s:
        return []
    return [x.strip() for x in s.split(",") if x.strip()]


@dataclass(frozen=True)
class Config:
    # Serp behaviour
    serp_num: int = 10

    # Domain hard-deny (prevents obvious noise)
    deny_domains: set[str] = field(default_factory=lambda: {
        "indeed.com",
        "reed.co.uk",
        "totaljobs.com",
        "glassdoor.co.uk",
        "glassdoor.com",
        "linkedin.com",
        "gov.uk",
        "workpermit.com",
        "ukvisajobs.com",
        "ifmosawork.com",
        "immigram.io",
    })

    # TLD deny (rarely needed; keep empty by default)
    deny_tlds: set[str] = field(default_factory=set)

    # Phrase-based exclusion (applied to SERP title+snippet BEFORE fetching)
    content_exclude_phrases: list[str] = field(default_factory=lambda: [
        "shortage occupation",
        "shortage occupation list",
        "skilled worker shortage",
        "visa sponsorship jobs",
        "jobs with visa sponsorship",
        "uk visa jobs",
        "certificate of sponsorship explained",
        "how to apply",
        "guidance",
        "what is a skilled worker visa",
        "immigration advice",
        "immigration opportunities",
        "visa blog",
        "tier 2 guide",
        "ukvi guidance",
    ])

    # Score thresholds
    min_score_sponsor: int = 70
    min_score_mobility: int = 70
    min_score_talent: int = 75

    # Queries (core engine)
    queries: dict[str, list[str]] = field(default_factory=lambda: {
        "sponsor": [
            'site:greenhouse.io ("visa sponsorship" OR "Skilled Worker" OR "Certificate of Sponsorship") ("United Kingdom" OR UK OR London OR Manchester OR Bristol OR Leeds)',
            'site:lever.co ("visa sponsorship" OR "Skilled Worker") ("United Kingdom" OR UK OR London)',
            'site:workable.com ("visa sponsorship" OR "Skilled Worker") ("United Kingdom" OR UK OR London)',
            '"visa sponsorship" ("United Kingdom" OR UK OR London) (site:greenhouse.io OR site:lever.co OR site:workable.com)',
            '"we can sponsor" ("United Kingdom" OR UK OR London) (site:greenhouse.io OR site:lever.co OR site:workable.com)',
            '"Skilled Worker" "visa sponsorship" ("United Kingdom" OR UK OR London) (site:greenhouse.io OR site:lever.co OR site:workable.com)',
        ],
        "mobility": [
            '"opens" (UK OR "United Kingdom" OR London) ("new office" OR "UK office") (press OR newsroom OR announcement)',
            '"launches" ("UK subsidiary" OR "United Kingdom subsidiary" OR "UK entity") (press OR newsroom OR announcement)',
            '"establishes" ("UK subsidiary" OR "UK office" OR "London office") (press OR newsroom OR announcement)',
            '"appoints" ("UK Managing Director" OR "Head of UK" OR "UK Country Manager") (press OR newsroom OR announcement)',
            '"entering the UK market" (press OR newsroom OR announcement)',
        ],
        "talent": [
            '"Global Talent visa" ("Exceptional Promise" OR "Exceptional Talent") ("my application" OR "I applied" OR "endorsed")',
            '"Tech Nation" "Global Talent" ("endorsed" OR "endorsement") ("blog" OR "experience")',
            '"Global Talent visa" "endorsement" ("timeline" OR "guide") ("Exceptional Promise" OR "Exceptional Talent")',
        ],
    })


def _build_config() -> Config:
    serp_num = int(os.environ.get("SERP_NUM", "10") or "10")

    deny_domains = set(Config().deny_domains)
    deny_domains |= set(_split_csv(os.environ.get("DENY_DOMAINS", "")))

    phrases = list(Config().content_exclude_phrases)
    phrases.extend(_split_csv(os.environ.get("CONTENT_EXCLUDE_PHRASES", "")))

    min_score_sponsor = int(os.environ.get("MIN_SCORE_SPONSOR", str(Config().min_score_sponsor)))
    min_score_mobility = int(os.environ.get("MIN_SCORE_MOBILITY", str(Config().min_score_mobility)))
    min_score_talent = int(os.environ.get("MIN_SCORE_TALENT", str(Config().min_score_talent)))

    return Config(
        serp_num=serp_num,
        deny_domains=deny_domains,
        deny_tlds=set(Config().deny_tlds),
        content_exclude_phrases=phrases,
        min_score_sponsor=min_score_sponsor,
        min_score_mobility=min_score_mobility,
        min_score_talent=min_score_talent,
        queries=Config().queries,
    )


CFG = _build_config()
