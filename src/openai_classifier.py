import os, json
from typing import Optional, Dict, Any
from openai import OpenAI


def enabled() -> bool:
    return bool(os.environ.get("OPENAI_API_KEY"))


def classify_lead(
    lead_type_hint: str,
    label: str,
    url: str,
    title: str,
    snippet: str,
    page_text: str,
    model: str = "gpt-5",
) -> Optional[Dict[str, Any]]:
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        return None

    client = OpenAI(api_key=api_key)
    excerpt = (page_text or "")[:4500]

    instructions = (
        "You are a UK business-immigration lead triage assistant for a law firm. "
        "Classify whether the page indicates a strong potential case for: "
        "(1) Sponsor Licence needed, (2) Global Mobility (UK expansion / senior or specialist worker / expansion worker), "
        "(3) Global Talent / Exceptional Promise. "
        "Return ONLY valid JSON (no markdown)."
    )

    schema = {
        "type": "object",
        "properties": {
            "bucket": {"type": "string", "enum": ["sponsor_licence", "global_mobility", "global_talent", "none"]},
            "score": {"type": "integer", "minimum": 0, "maximum": 100},
            "summary": {"type": "string"},
            "reasons": {"type": "array", "items": {"type": "string"}, "minItems": 2, "maxItems": 6},
            "outreach_angle": {"type": "string"},
            "sponsorship_signal_quote": {"type": "string"},
        },
        "required": ["bucket", "score", "summary", "reasons", "outreach_angle", "sponsorship_signal_quote"],
        "additionalProperties": False,
    }

    payload = (
        f"Lead type hint: {lead_type_hint}\n"
        f"Label: {label}\nTitle: {title}\nURL: {url}\nSnippet: {snippet}\n\n"
        f"Page excerpt:\n{excerpt}"
    )

    # Attempt 1: enforce JSON schema
    try:
        resp = client.responses.create(
            model=model,
            instructions=instructions,
            input=payload,
            text={
                "format": {
                    "type": "json_schema",
                    "name": "lead_triage",
                    "schema": schema,
                }
            },
        )
        return json.loads(resp.output_text)
    except Exception:
        pass

    # Attempt 2: plain JSON fallback
    try:
        resp = client.responses.create(
            model=model,
            instructions=instructions + " Output JSON only.",
            input=payload,
        )
        txt = (resp.output_text or "").strip()
        start = txt.find("{")
        end = txt.rfind("}")
        if start != -1 and end != -1 and end > start:
            txt = txt[start : end + 1]
        return json.loads(txt)
    except Exception:
        return None
