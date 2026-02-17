from dataclasses import dataclass, field
from typing import List, Set


@dataclass
class Config:
    # Output sizes
    max_results_per_query: int = 10
    max_leads_per_bucket: int = 25

    # Scoring thresholds
    strong_threshold: int = 70
    medium_threshold: int = 55

    # Safety / quality filters
    deny_tlds: Set[str] = field(default_factory=lambda: {".gov.uk", ".nhs.uk", ".ac.uk"})
    deny_domains: Set[str] = field(default_factory=lambda: {
        "cqc.org.uk",
        "credencedata.com",
        "companieshouse.gov.uk",
    })
    name_exclude_keywords: List[str] = field(default_factory=lambda: [
        "COUNCIL", "BOROUGH", "CITY COUNCIL", "DISTRICT COUNCIL", "PARISH COUNCIL",
        "NHS", "TRUST", "FOUNDATION TRUST",
        "UNIVERSITY", "COLLEGE", "ACADEMY", "SCHOOL",
        "GOVERNMENT", "MINISTRY", "COMMISSION", "REGULATOR",
        "POLICE", "FIRE SERVICE",
    ])

    # Intent phrases
    sponsor_phrases: List[str] = field(default_factory=lambda: [
        "visa sponsorship", "sponsorship available", "skilled worker", "we can sponsor",
        "tier 2", "certificate of sponsorship", "cos available"
    ])
    uk_hiring_phrases: List[str] = field(default_factory=lambda: [
        "united kingdom", "uk", "london", "manchester", "birmingham", "edinburgh",
        "bristol", "leeds", "glasgow", "remote uk"
    ])
    expansion_phrases: List[str] = field(default_factory=lambda: [
        "opening a london office", "opening our london office", "opening a uk office",
        "expanding into the uk", "launches in the uk", "uk launch", "establishing a uk entity",
        "entering the uk market", "opening in london", "opening in the uk"
    ])
    overseas_hq_phrases: List[str] = field(default_factory=lambda: [
        "headquartered in", "hq in", "based in", "founded in"
    ])
    global_talent_phrases: List[str] = field(default_factory=lambda: [
        "global talent visa", "exceptional promise", "exceptional talent",
        "endorsement", "endorsed", "tech nation", "arts council", "royal society",
        "ukri", "british academy"
    ])

    # Queries (SERP)
    sponsor_queries: List[str] = field(default_factory=lambda: [
        'site:greenhouse.io ("United Kingdom" OR London) ("visa sponsorship" OR "Skilled Worker")',
        'site:lever.co ("London" OR "United Kingdom") ("visa sponsorship" OR "Skilled Worker")',
        'site:workable.com ("United Kingdom" OR London) ("visa" OR sponsorship)',
        '"visa sponsorship" ("careers" OR "jobs") ("London" OR "United Kingdom")',
        '"Skilled Worker" ("careers" OR "jobs") ("London" OR "United Kingdom")',
    ])
    mobility_queries: List[str] = field(default_factory=lambda: [
        '"opening a London office" company',
        '"expanding into the UK" company',
        '"launches in the UK" headquartered',
        '"establishing a UK subsidiary" company',
        '"UK expansion" "opening" "office" company',
    ])
    talent_queries: List[str] = field(default_factory=lambda: [
        '"Global Talent visa" endorsed',
        '"Exceptional Promise" "Global Talent" UK',
        '"Global Talent visa" "moving to the UK"',
        '"Global Talent" endorsement "London"',
        '"Tech Nation" endorsement "Global Talent"',
    ])


CFG = Config()
