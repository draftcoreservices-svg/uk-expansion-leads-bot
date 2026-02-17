from dataclasses import dataclass, field
import os


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
        "technation.io",   # keep corporate BD clean; talent bucket can still surface personal pages
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
        # Sponsor licence needed — search hiring intent + sponsorship language on real company/ATS pages
        "sponsor": [
            'site:greenhouse.io ("visa sponsorship" OR "Skilled Worker" OR "Certificate of Sponsorship") ("United Kingdom" OR UK OR London OR Manchester OR Bristol OR Leeds)',
            'site:lever.co ("visa sponsorship" OR "Skilled Worker") ("United Kingdom" OR UK OR London)',
            'site:workable.com ("visa sponsorship" OR "Skilled Worker") ("United Kingdom" OR UK OR London)',
            '"visa sponsorship" ("United Kingdom" OR UK OR London) (site:greenhouse.io OR site:lever.co OR site:workable.com)',
            '"we can sponsor" ("United Kingdom" OR UK OR London) (site:greenhouse.io OR site:lever.co OR site:workable.com)',
            '"Skilled Worker" "visa sponsorship" ("United Kingdom" OR UK OR London) (site:greenhouse.io OR site:lever.co OR site:workable.com)',
        ],

        # Global mobility / UK expansion — corporate announcements in last ~12–18 months
        "mobility": [
            '"opens" (UK OR "United Kingdom" OR London) ("new office" OR "UK office") (press OR newsroom OR announcement)',
            '"launches" ("UK subsidiary" OR "United Kingdom subsidiary" OR "UK entity") (press OR newsroom OR announcement)',
            '"establishes" ("UK subsidiary" OR "UK office" OR "London office") (press OR newsroom OR announcement)',
            '"appoints" ("UK Managing Director" OR "Head of UK" OR "UK Country Manager") (press OR newsroom OR announcement)',
            '"entering the UK market" (press OR newsroom OR announcement)',
        ],

        # Global Talent signals — keep light, avoid diluting corporate BD
        "talent": [
            '"Global Talent visa" ("Exceptional Promise" OR "Exceptional Talent") ("my application" OR "I applied" OR "endorsed")',
            '"Tech Nation" "Global Talent" ("endorsed" OR "endorsement") ("blog" OR "experience")',
            '"Global Talent visa" "endorsement" ("timeline" OR "guide") ("Exceptional Promise" OR "Exceptional Talent")',
        ],
    })


# Allow minimal env overrides without breaking defaults
def _build_config() -> Config:
    serp_num = int(os.environ.get("SERP_NUM", "10") or "10")

    # Extend deny domains if provided
    deny_domains = set(Config().deny_domains)
    deny_domains |= set(_split_csv(os.environ.get("DENY_DOMAINS", "")))

    # Extend exclude phrases if provided
    phrases = list(Config().content_exclude_phrases)
    phrases.extend(_split_csv(os.environ.get("CONTENT_EXCLUDE_PHRASES", "")))

    # Allow overriding thresholds
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
