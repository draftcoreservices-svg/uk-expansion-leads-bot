# UK Expansion Leads Bot (Manual Run)

This repo runs a manual GitHub Action that:
- Diffs the Home Office **Register of Licensed Sponsors (Workers)** (public CSV)
- Pulls newly incorporated UK companies from **Companies House API**
- Scores likely overseas expansion signals (based on officers' non‑UK countries, etc.)
- (Optional but enabled) Uses **SerpAPI** to find candidate official websites
- Verifies websites against Companies House details (confidence gating)
- Extracts public contact emails/phones from verified company websites only
- Sends **one styled email** + attaches a CSV

## 1) GitHub Secrets (required)

Repo → Settings → Secrets and variables → Actions → New repository secret:

**APIs**
- `COMPANIES_HOUSE_API_KEY`
- `SERPAPI_API_KEY`

**Email (Gmail SMTP)**
- `SMTP_HOST` = `smtp.gmail.com`
- `SMTP_PORT` = `587`
- `SMTP_USER` = your gmail address
- `SMTP_PASS` = Gmail **App Password** (recommended)
- `EMAIL_FROM` = sender address (e.g. same as SMTP_USER)
- `EMAIL_TO` = recipient address

## 2) Run it
Actions → **UK Expansion Leads Bot** → Run workflow.

## 3) Notes
- The bot caches a small SQLite database in `.cache/` to avoid repeat leads between runs.
- Output is capped to the top 20 leads for enrichment + email body to keep SerpAPI usage low.
- This project only uses public sources (GOV.UK CSV, Companies House API, public company websites).
