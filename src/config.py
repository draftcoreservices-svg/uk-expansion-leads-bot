from dataclasses import dataclass, field
from typing import List, Set

SPONSOR_REGISTER_PAGE = "https://www.gov.uk/government/publications/register-of-licensed-sponsors-workers"


@dataclass
class Config:
    # --- Run limits ---
    max_results_per_query: int = 10
    max_strong_per_bucket: int = 25

    max_pages_to_fetch: int = 120
    max_openai_calls: int = 40
    page_text_max_chars: int = 12000

    # --- Scoring thresholds ---
    strong_threshold: int = 70
    medium_threshold: int = 55

    # If True: "strong" sponsor/mobility leads must have a real company signal (email/domain)
    require_company_signal_for_strong: bool = True

    # --- Deny filters ---
    deny_tlds: Set[str] = field(default_factory=lambda: {
        ".gov.uk", ".nhs.uk", ".ac.uk"
    })

    # Hard domain deny-list: aggregators, job boards, social, obvious noise
    deny_domains: Set[str] = field(default_factory=lambda: {
        # gov/admin
        "cqc.org.uk",
        "companieshouse.gov.uk",
        "find-and-update.company-information.service.gov.uk",

        # job boards / aggregators
        "reed.co.uk",
        "indeed.com",
        "indeed.co.uk",
        "glassdoor.com",
        "glassdoor.co.uk",
        "totaljobs.com",
        "cv-library.co.uk",
        "monster.co.uk",
        "jobsite.co.uk",
        "adzuna.co.uk",
        "ziprecruiter.com",
        "ziprecruiter.co.uk",
        "workcircle.com",
        "jobrapido.com",
        "careerjet.co.uk",
        "jooble.org",
        "neuvoo.com",
        "jobs.nhs.uk",
        "findajob.dwp.gov.uk",

        # social / noisy sources for lead gen
        "linkedin.com",
        "uk.linkedin.com",
        "facebook.com",
        "twitter.com",
        "x.com",
        "instagram.com",
        "tiktok.com",

        # generic directory / list style sites
        "wikipedia.org",

        # immigration guides / visa-job list sites (noise for corporate BD)
        "workpermit.com",
        "ukvisajobs.com",
        "ifmosawork.com",
        "immigram.io",
        "technomads.io",
        "technation.io",
    })

    # Phrases that indicate the result is a listing page, aggregator, or generic guide
    # (Used in run_weekly.py via CFG.content_exclude_phrases)
    content_exclude_phrases: List[str] = field(default_factory=lambda: [
        # job-board listing patterns
        "jobs in london",
        "job in london",
        "visa sponsorship jobs",
        "skilled worker jobs",
        "certificate of sponsorship jobs",
        "search jobs",
        "job search",
        "browse jobs",
        "new jobs",
        "apply now",
        "create job alert",
        "salary guide",

        # guides/how-to content (these tend to be consultants/blogs, not targets)
        "what is a sponsor licence",
        "what is a sponsor license",
        "how to get a sponsor licence",
        "how to get a sponsor license",
        "how to",
        "guide",
        "tips",
        "what you need to know",
        "considerations",
        "set up a subsidiary",
        "setting up a uk subsidiary",
        "registering a uk branch",
        "open a uk office",
        "open a business in uk",
        "uk expansion guide",
    ])

    # Exclude public bodies etc. based on name heuristics
    name_exclude_keywords: List[str] = field(default_factory=lambda: [
        "COUNCIL", "BOROUGH", "CITY COUNCIL", "DISTRICT COUNCIL", "PARISH COUNCIL",
        "NHS", "TRUST", "FOUNDATION TRUST",
        "UNIVERSITY", "COLLEGE", "ACADEMY", "SCHOOL",
        "GOVERNMENT", "MINISTRY", "COMMISSION", "REGULATOR",
        "POLICE", "FIRE SERVICE",
    ])

    # --- Phrase signals used by scoring.py ---
    sponsor_phrases: List[str] = field(default_factory=lambda: [
        "visa sponsorship",
        "sponsorship available",
        "skilled worker",
        "we can sponsor",
        "tier 2",
        "certificate of sponsorship",
        "cos available",
        "sponsor licence",
        "sponsor license",
        "right to work sponsorship",
    ])

    uk_location_phrases: List[str] = field(default_factory=lambda: [
        "united kingdom",
        " uk ",
        "london",
        "manchester",
        "birmingham",
        "edinburgh",
        "bristol",
        "leeds",
        "glasgow",
        "remote uk",
        "hybrid uk",
    ])

    expansion_phrases: List[str] = field(default_factory=lambda: [
        "opening a london office",
        "opening our london office",
        "opening a uk office",
        "expanding into the uk",
        "launches in the uk",
        "uk launch",
        "establishing a uk entity",
        "entering the uk market",
        "opening in london",
        "opening in the uk",
        "opening a london hub",
        "setting up a uk subsidiary",
        "establishing a uk subsidiary",
        "uk subsidiary",
    ])

    # Keep, but treat as "inbound opportunities" more than company leads
    global_talent_phrases: List[str] = field(default_factory=lambda: [
        "global talent visa",
        "exceptional promise",
        "exceptional talent",
        "endorsement",
        "endorsed",
        "tech nation",
        "arts council",
        "royal society",
        "ukri",
        "british academy",
        "research fellowship",
    ])

    # --- Search queries ---
    # Sponsor: bias ATS pages + direct employer pages; avoid obvious aggregators
    sponsor_queries: List[str] = field(default_factory=lambda: [
        'site:job-boards.greenhouse.io ("United Kingdom" OR London) ("visa sponsorship" OR "Skilled Worker" OR "sponsor")',
        'site:jobs.lever.co ("London" OR "United Kingdom") ("visa sponsorship" OR "Skilled Worker" OR sponsor)',
        'site:apply.workable.com ("United Kingdom" OR London) ("visa" OR sponsorship OR sponsor)',
        '"visa sponsorship" (careers OR jobs) (London OR "United Kingdom") -site:reed.co.uk -site:indeed.co.uk -site:indeed.com -site:linkedin.com',
        '"Skilled Worker" (careers OR jobs) (London OR "United Kingdom") -site:reed.co.uk -site:indeed.co.uk -site:indeed.com -site:linkedin.com',
        '"certificate of sponsorship" (careers OR jobs) (London OR "United Kingdom") -site:reed.co.uk -site:indeed.co.uk -site:indeed.com -site:linkedin.com',
    ])

    # Mobility: bias toward press/newsroom/company announcements
    mobility_queries: List[str] = field(default_factory=lambda: [
        '"opens" "London office" (press OR newsroom OR announcement)',
        '"opening" "London office" (press OR newsroom OR announcement)',
        '"launches" "UK office" (press OR newsroom OR announcement)',
        '"establishes" "UK subsidiary" (press OR announcement)',
        '"first UK office" (press OR newsroom OR announcement)',
        '"appointed" "UK Country Manager" "London"',
    ])

    # Talent: mostly content, but can surface people who may need help (inbound capture)
    talent_queries: List[str] = field(default_factory=lambda: [
        '"Global Talent visa" "my application"',
        '"Exceptional Promise" "Global Talent" "I got endorsed"',
        '"Tech Nation" "Global Talent" "endorsement" "my case"',
        '"Global Talent" "moving to London"',
        '"Global Talent visa" "endorsed" "Tech Nation"',
        '"Exceptional Promise" "endorsement" "Tech Nation"',
    ])

    # ATS hosts we allow (used by run_weekly.py for signal logic)
    ats_hosts: Set[str] = field(default_factory=lambda: {
        "job-boards.greenhouse.io",
        "boards.greenhouse.io",
        "jobs.lever.co",
        "apply.workable.com",
    })


CFG = Config()
