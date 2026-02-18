from dataclasses import dataclass, field
from typing import List, Set
import os

# GOV.UK landing page (stable) that links to the current register file
SPONSOR_REGISTER_PAGE = os.environ.get(
    "SPONSOR_REGISTER_PAGE",
    "https://www.gov.uk/government/publications/register-of-licensed-sponsors-workers",
)


@dataclass
class Config:
    # --- Run limits ---
    max_results_per_query: int = int(os.environ.get("MAX_RESULTS_PER_QUERY", "10"))
    max_strong_per_bucket: int = int(os.environ.get("MAX_STRONG_PER_BUCKET", "25"))

    max_pages_to_fetch: int = int(os.environ.get("MAX_PAGES_TO_FETCH", "120"))
    max_openai_calls: int = int(os.environ.get("MAX_OPENAI_CALLS", "40"))
    page_text_max_chars: int = int(os.environ.get("PAGE_TEXT_MAX_CHARS", "12000"))

    # --- Scoring thresholds ---
    strong_threshold: int = int(os.environ.get("STRONG_THRESHOLD", "70"))
    medium_threshold: int = int(os.environ.get("MEDIUM_THRESHOLD", "55"))

    # If True: "strong" sponsor/mobility leads must have a real company signal (email/domain)
    require_company_signal_for_strong: bool = os.environ.get("REQUIRE_COMPANY_SIGNAL_FOR_STRONG", "true").lower() in (
        "1",
        "true",
        "yes",
    )

    # --- Deny filters ---
    deny_tlds: Set[str] = field(default_factory=lambda: {".gov.uk", ".nhs.uk", ".ac.uk"})

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

        # PR / syndication wires (high noise for "UK expansion" leads)
        "prnewswire.com",
        "www.prnewswire.com",
        "businesswire.com",
        "www.businesswire.com",
        "globenewswire.com",
        "www.globenewswire.com",
        "newswire.ca",
        "www.newswire.ca",
        "einpresswire.com",
        "www.einpresswire.com",
        "accesswire.com",
        "www.accesswire.com",
        "marketwatch.com",
        "www.marketwatch.com",

        # immigration guides / visa-job list sites (noise for corporate BD)
        "workpermit.com",
        "ukvisajobs.com",
        "ifmosawork.com",
        "immigram.io",
        "technomads.io",
        "technation.io",
    })

    # SERP title/snippet filters (pre-fetch)
    content_exclude_phrases: List[str] = field(default_factory=lambda: [
        # list/guide patterns
        "shortage occupation",
        "shortage occupation list",
        "skilled worker shortage",
        "visa sponsorship jobs",
        "jobs with visa sponsorship",
        "uk visa jobs",
        "certificate of sponsorship explained",
        "what is a skilled worker visa",
        "immigration advice",
        "visa blog",
        "tier 2 guide",
        "ukvi guidance",
        "apply now",
        "create job alert",
        "salary guide",
    ])

    # Hard exclude based on *name/title* (used by scoring.py hard_excluded())
    name_exclude_keywords: List[str] = field(default_factory=lambda: [
        # generic list/guide noise
        "shortage occupation",
        "shortage occupation list",
        "visa sponsorship jobs",
        "jobs with visa sponsorship",
        "uk visa jobs",
        "certificate of sponsorship explained",
        "what is a skilled worker visa",
        "immigration advice",
        "visa blog",
        "tier 2 guide",
        "ukvi guidance",

        # recruiters / middlemen / non-employer signals
        "recruitment",
        "recruiter",
        "staffing",
        "agency",
        "talent acquisition",

        # job-board style labels
        "jobs",
        "job",
        "careers",
        "vacancies",
        "apply now",
        "job alert",
        "salary guide",
    ])

    # Phrase signals used by scoring.py (keep consistent with your scoring rules)
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
    sponsor_queries: List[str] = field(default_factory=lambda: [
        'site:job-boards.greenhouse.io ("United Kingdom" OR London) ("visa sponsorship" OR "Skilled Worker" OR "certificate of sponsorship" OR sponsor)',
        'site:jobs.lever.co ("London" OR "United Kingdom") ("visa sponsorship" OR "Skilled Worker" OR sponsor)',
        'site:apply.workable.com ("United Kingdom" OR London) ("visa sponsorship" OR "Skilled Worker" OR sponsor)',
        '"visa sponsorship" (careers OR jobs) (London OR "United Kingdom") -site:reed.co.uk -site:indeed.co.uk -site:indeed.com -site:linkedin.com',
        '"Skilled Worker" (careers OR jobs) (London OR "United Kingdom") -site:reed.co.uk -site:indeed.co.uk -site:indeed.com -site:linkedin.com',
        '"certificate of sponsorship" (careers OR jobs) (London OR "United Kingdom") -site:reed.co.uk -site:indeed.co.uk -site:indeed.com -site:linkedin.com',
    ])

    mobility_queries: List[str] = field(default_factory=lambda: [
        '"opens" "London office" (press OR newsroom OR announcement)',
        '"opening" "London office" (press OR newsroom OR announcement)',
        '"launches" "UK office" (press OR newsroom OR announcement)',
        '"establishes" "UK subsidiary" (press OR announcement OR newsroom)',
        '"first UK office" (press OR newsroom OR announcement)',
        '"appointed" ("UK Country Manager" OR "Head of UK" OR "UK Managing Director") (London OR UK)',
    ])

    talent_queries: List[str] = field(default_factory=lambda: [
        '"Global Talent visa" "my application"',
        '"Exceptional Promise" "Global Talent" "I got endorsed"',
        '"Tech Nation" "Global Talent" "endorsement" "my case"',
        '"Global Talent" "moving to London"',
        '"Global Talent visa" "endorsed" "Tech Nation"',
    ])

    # ATS hosts we allow (used by run_weekly.py for signal logic)
    ats_hosts: Set[str] = field(default_factory=lambda: {
        "job-boards.greenhouse.io",
        "boards.greenhouse.io",
        "jobs.lever.co",
        "apply.workable.com",
    })


CFG = Config()
