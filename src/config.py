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

    deny_tlds: Set[str] = field(default_factory=lambda: {".gov.uk", ".nhs.uk", ".ac.uk"})
    deny_domains: Set[str] = field(default_factory=lambda: {
        "cqc.org.uk",
        "companieshouse.gov.uk",
        "credencedata.com",
        "find-and-update.company-information.service.gov.uk",
    })

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
    global_talent_phrases: List[str] = field(default_factory=lambda: [
        "global talent visa", "exceptional promise", "exceptional talent",
        "endorsement", "endorsed", "tech nation", "arts council", "royal society",
        "ukri", "british academy", "academia", "research fellowship"
    ])

    sponsor_queries: List[str] = field(default_factory=lambda: [
        'site:greenhouse.io ("United Kingdom" OR London) ("visa sponsorship" OR "Skilled Worker" OR "sponsor")',
        'site:lever.co ("London" OR "United Kingdom") ("visa sponsorship" OR "Skilled Worker" OR sponsor)',
        'site:workable.com ("United Kingdom" OR London) ("visa" OR sponsorship OR sponsor)',
        '"visa sponsorship" ("careers" OR "jobs") ("London" OR "United Kingdom")',
        '"Skilled Worker" ("careers" OR "jobs") ("London" OR "United Kingdom")',
        '"certificate of sponsorship" ("careers" OR "jobs") ("London" OR "United Kingdom")',
    ])
    mobility_queries: List[str] = field(default_factory=lambda: [
        '"opening a London office" company',
        '"expanding into the UK" company',
        '"launches in the UK" headquartered',
        '"establishing a UK subsidiary" company',
        '"UK expansion" "opening" "office" company',
        '"we are hiring" "London" "headquartered in"',
    ])
    talent_queries: List[str] = field(default_factory=lambda: [
        '"Global Talent visa" endorsed',
        '"Exceptional Promise" "Global Talent" UK',
        '"Global Talent visa" "moving to the UK"',
        '"Global Talent" endorsement "London"',
        '"Tech Nation" endorsement "Global Talent"',
        '"Exceptional Promise" endorsement "UK"',
    ])

CFG = Config()
