from dataclasses import dataclass, field
from typing import List, Set

SPONSOR_REGISTER_PAGE = "https://www.gov.uk/government/publications/register-of-licensed-sponsors-workers"


@dataclass
class Config:
    max_results_per_query: int = 10
    max_strong_per_bucket: int = 25

    max_pages_to_fetch: int = 120
    max_openai_calls: int = 40
    page_text_max_chars: int = 12000

    strong_threshold: int = 70
    medium_threshold: int = 55

    # If True: "strong" leads must have a real company signal (email/domain)
    require_company_signal_for_strong: bool = True

    deny_tlds: Set[str] = field(default_factory=lambda: {".gov.uk", ".nhs.uk", ".ac.uk"})

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

        # generic “directory / list” style sites
        "wikipedia.org",
    })

    # Phrases that indicate the result is a listing page, aggregator, or generic guide
    content_exclude_phrases: List[str] = field(default_factory=lambda: [
        # job-board listing patterns
        "jobs in london",
        "job in london",
        "visa sponsorship jobs",
        "skilled worker jobs",
        "certificate of sponsorship jobs",
        "1,000+",
        "5000+",
        "search jobs",
        "job search",
        "browse jobs",
        "new jobs",
        "apply now",

        # guides/how-to content
        "what is a sponsor licence",
        "how to get a sponsor licence",
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

    sponsor_phrases: List[str] = field(default_factory=lambda: [
        "visa sponsorship", "sponsorship available", "skilled worker", "we can sponsor",
        "tier 2", "certificate of sponsorship", "cos available", "sponsor licence",
        "sponsor license", "right to work sponsorship"
    ])

    uk_location_phrases: List[str] = field(default_factory=lambda: [
        "united kingdom", " uk ", "london", "manchester", "birmingham", "edinburgh",
        "bristol", "leeds", "glasgow", "remote uk", "hybrid uk"
    ])

    expansion_phrases: List[str] = field(default_factory=lambda: [
        "opening a london office", "opening our london office", "opening a uk office",
        "expanding into the uk", "launches in the uk", "uk launch", "establishing a uk entity",
        "entering the uk market", "opening in london", "opening in the uk",
        "opening a london hub", "setting up a uk subsidiary"
    ])

    # Optional — keep, but we’ll treat as “individual lead / inbound capture” rather than strong company lead
    global_talent_phrases: List[str] = field(default_factory=lambda: [
        "global talent visa", "exceptional promise", "exceptional talent",
        "endorsement", "endorsed", "tech nation", "arts council", "royal society",
        "ukri", "british academy", "academia", "research fellowship"
    ])

    # Stronger sponsor queries: focus ATS pages and direct employer pages
    sponsor_queries: List[str] = field(default_factory=lambda: [
        'site:greenhouse.io ("United Kingdom" OR London) ("visa sponsorship" OR "Skilled Worker" OR "sponsor")',
        'site:lever.co ("London" OR "United Kingdom") ("visa sponsorship" OR "Skilled Worker" OR sponsor)',
        'site:workable.com ("United Kingdom" OR London) ("visa" OR sponsorship OR sponsor)',
        '"visa sponsorship" (site:*.co.uk OR site:*.com) (careers OR jobs) (London OR "United Kingdom") -reed -indeed -linkedin',
        '"Skilled Worker" (careers OR jobs) (London OR "United Kingdom") -reed -indeed -linkedin',
        '"certificate of sponsorship" (careers OR jobs) (London OR "United Kingdom") -reed -indeed -linkedin',
    ])

    # Stronger mobility queries: bias toward press/newsroom announcements on company domains
    mobility_queries: List[str] = field(default_factory=lambda: [
        '"opens" "London office" (press OR newsroom OR announcement)',
        '"launches" "UK office" (press OR newsroom OR announcement)',
        '"establishes" "UK subsidiary" (press OR announcement)',
        '"appointed" "UK Country Manager" "office"',
        '"first UK office" (press OR newsroom OR announcement)',
        '"we are hiring" ("London" OR "United Kingdom") ("headquartered in" OR "HQ in")',
    ])

    # Keep, but expect mostly content. We'll mark as "inbound opportunities"
    talent_queries: List[str] = field(default_factory=lambda: [
        '"Global Talent visa" "my application"',
        '"Exceptional Promise" "Global Talent" "I got endorsed"',
        '"Tech Nation" "Global Talent" "endorsement" "my case"',
        '"Global Talent" "moving to London"',
        '"Global Talent visa" "endorsed" "Tech Nation"',
        '"Exceptional Promise" "endorsement" "Tech Nation"',
    ])

    # ATS hosts we allow, but will extract the employer from the URL
    ats_hosts: Set[str] = field(default_factory=lambda: {
        "job-boards.greenhouse.io",
        "boards.greenhouse.io",
        "jobs.lever.co",
        "apply.workable.com",
    })


CFG = Config()
