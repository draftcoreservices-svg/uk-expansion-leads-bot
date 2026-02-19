# CW Structured Sponsor Leads Bot (Companies House + Sponsor Register only)

This repo runs a GitHub Actions workflow weekly (Monday morning) and emails a ranked shortlist of **up to 30** high-probability sponsor-licence leads.

Structured sources only:
- Companies House Public Data API
- Home Office "Register of licensed sponsors: workers" CSV (downloaded from GOV.UK)

No job boards, SERP scraping, or email scraping.

## Run locally

1) Install:
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
