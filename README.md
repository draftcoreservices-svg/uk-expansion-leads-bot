# UK Expansion Leads Bot (GitHub Actions)

Internal BD intelligence tooling for identifying companies that may need UK immigration support.

## What it does
- **Sponsor Register stream**: detects **new sponsor listings since baseline** (routes allowlisted in `bot/config.py`).
- **Companies House stream**: finds **recent incorporations** with overseas / group-structure signals.
- **Enrichment**: uses **SerpAPI** to discover official websites, then scrapes **only verified company sites** for public contact details.
- **Stateful**: persists `.cache/state.db` via `actions/cache` so you don't re-process the same items.

## Case type labels
- A — New Sponsor (Skilled Worker / compliance / CoS usage)
- B — Likely Sponsor Licence Applicant
- C — Likely GBM Senior/Specialist
- D — Likely UK Expansion Worker
- E — Watchlist

## Setup
Add GitHub Secrets:
- `COMPANIES_HOUSE_API_KEY`
- `SERPAPI_API_KEY` (optional but recommended)
- `SMTP_HOST`, `SMTP_PORT`, `SMTP_USER`, `SMTP_PASS`
- `EMAIL_FROM`, `EMAIL_TO`

Run the workflow via **Actions → UK Expansion Leads Bot → Run workflow**.
