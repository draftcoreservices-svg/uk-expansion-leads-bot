import os
import json
from typing import Optional, Dict, Any

from openai import OpenAI


def enabled() -> bool:
    return bool(os.environ.get("OPENAI_API_KEY"))


def _model_default() -> str:
    # Keep configurable to avoid CI failures due to unavailable models.
    return os.environ.get("OPENAI_MODEL") or "gpt-4o-mini"


def classify_lead(
    lead_type_hint: str,
    label: str,
    url: str,
    title: str,
    snippet: str,
    page_text: str,
    model: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        return None

    # Ensure we don't hang indefinitely.
    client = OpenAI(api_key=api_key, timeout=30.0)

    excerpt = (page_text or "")[:5000]

    system = (
        "You are a strict business-immigration lead triage assistant for a UK law firm. "
        "Your job is to detect REAL COMPANY leads and reject noise (blogs, aggregators, generic visa content). "
        "Be conservative: if unsure, mark as not actionable. Output valid JSON only."
    )

    user = {
        "lead_type_hint": lead_type_hint,
        "label": label,
        "url": url,
        "title": title,
        "snippet": snippet,
        "page_excerpt": excerpt,
        "task": "Classify this as actionable or not, and explain briefly why.",
        "rules": [
            "Reject immigration advice articles, generic guides, government pages.",
            "Reject job aggregators/directories (Indeed, Reed, etc.).",
            "Prefer corporate domains and ATS pages (Greenhouse/Lever/Workable).",
            "For sponsor-licence leads: hiring in UK + sponsorship language + NOT already sponsor-licensed.",
            "For global mobility leads: UK entry/office/subsidiary/leadership signals in last 18 months.",
        ],
        "return_schema": {
            "actionable": "boolean",
            "bucket": "one of: sponsor|mobility|talent|reject",
            "confidence": "0-100 integer",
            "reason": "short string",
            "notes": "short string",
        },
    }

    try:
        resp = client.chat.completions.create(
            model=(model or _model_default()),
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": json.dumps(user)},
            ],
            temperature=0.1,
        )
        content = resp.choices[0].message.content or ""
        data = json.loads(content)
        return data if isinstance(data, dict) else None
    except Exception:
        # If AI fails, do not block the pipeline.
        return None
