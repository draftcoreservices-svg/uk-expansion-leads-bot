CW Weekly Leads Bot

Runs weekly on GitHub Actions and emails a scored list of strong leads in three buckets:
1) Sponsor licence needed
2) Global mobility route (UK expansion / senior or specialist / expansion worker)
3) Global Talent / Exceptional Promise

Uses:
- SerpAPI (search discovery)
- GOV.UK sponsor register CSV (auto-refreshed each run)
- Companies House API (best-effort enrichment)
- OpenAI (optional) triage to reduce false positives + generate outreach angle
- SMTP (send the email)
