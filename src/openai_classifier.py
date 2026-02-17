import os
import json
from typing import Optional, Dict, Any

from openai import OpenAI


def enabled() -> bool:
    # True only when key is actually present and non-empty
    return bool(os.getenv("OPENAI_API_KEY", "").strip())


def _safe_str(x: Any, max_len: int = 4000) -> str:
    s = "" if x is None else str(x)
    return s[:max_len]


def _extract_output_text(resp: Any) -> str:
    """
    Try multiple shapes returned by the OpenAI SDK.
    We prefer resp.output_text if available, otherwise fall back
    to concatenating text parts in resp.output.
    """
    # Newer SDK often exposes this convenience property:
    txt = getattr(resp, "output_text", None)
    if isinstance(txt, str) and txt.strip():
        return txt.strip()

    # Fallback: walk resp.output blocks
    out = getattr(resp, "output", None)
    if not out:
        return ""

    parts: list[str] = []
    for item in out:
        content = getattr(item, "content", None)
        if not content:
            continue
        for c in content:
            # Most common: c.type == "output_text" with c.text
            ctype = getattr(c, "type", None)
            if ctype == "output_text":
                t = getattr(c, "text", "")
                if t:
                    parts.append(str(t))
            else:
                # Try any .text field just in case
                t = getattr(c, "text", None)
                if t:
                    parts.append(str(t))

    return "\n".join(parts).strip()


def _coerce_json(text: str) -> Optional[Dict[str, Any]]:
    """
    Extract the first JSON object from the model output and parse it.
    """
    if not text:
        return None

    text = text.strip()

    # If it's already clean JSON:
    if text.startswith("{") and text.endswith("}"):
        try:
            return json.loads(text)
        except Exception:
            pass

    # Otherwise, try to find a JSON object inside:
    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1 and end > start:
        candidate = text[start : end + 1]
        try:
            return json.loads(candidate)
        except Exception:
            return None

    return None


def classify_lead(
    lead_type_hint: str,
    label: str,
    url: str,
    title: str,
    snippet: str,
    page_text: str,
    model: str = "gpt-5",
) -> Optional[Dict[str, Any]]:
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        return None

    client = OpenAI(api_key=api_key)

    excerpt = _safe_str(page_text, 6000)  # keep it small-ish for cost

    instructions = (
        "You are a UK business-immigration lead triage assistant for a law firm. "
        "Classify whether the page indicates a strong potential case for: "
        "(1) Sponsor Licence needed, "
        "(2) Global Mobility (UK expansion / Senior or Specialist Worker / Expansion Worker), "
        "(3) Global Talent / Exceptional Promise. "
        "Return ONLY valid JSON (no markdown, no commentary)."
    )

    schema = {
        "type": "object",
        "properties": {
            "bucket": {
                "type": "string",
                "enum": ["sponsor_licence", "global_mobility", "global_talent", "none"],
            },
            "score": {"type": "integer", "minimum": 0, "maximum": 100},
            "summary": {"type": "string"},
            "reasons": {
                "type": "array",
                "items": {"type": "string"},
                "minItems": 2,
                "maxItems": 6,
            },
            "outreach_angle": {"type": "string"},
            "sponsorship_signal_quote": {"type": "string"},
        },
        "required": [
            "bucket",
            "score",
            "summary",
            "reasons",
            "outreach_angle",
            "sponsorship_signal_quote",
        ],
        "additionalProperties": False,
    }

    payload = (
        f"Lead type hint: {_safe_str(lead_type_hint, 60)}\n"
        f"Label: {_safe_str(label, 140)}\n"
        f"Title: {_safe_str(title, 180)}\n"
        f"URL: {_safe_str(url, 500)}\n"
        f"Snippet: {_safe_str(snippet, 400)}\n\n"
        f"Page excerpt:\n{excerpt}"
    )

    # Attempt 1: strict JSON schema output (best)
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
                    "strict": True,
                }
            },
            temperature=0.2,
        )
        txt = _extract_output_text(resp)
        data = _coerce_json(txt)
        return data
    except Exception as e:
        # Don't kill the whole run; fall back to non-strict JSON
        print(f"[OPENAI] schema call failed: {type(e).__name__}: {e}")

    # Attempt 2: “JSON only” without schema
    try:
        resp = client.responses.create(
            model=model,
            instructions=instructions + " Output JSON only.",
            input=payload,
            temperature=0.2,
        )
        txt = _extract_output_text(resp)
        data = _coerce_json(txt)
        return data
    except Exception as e:
        print(f"[OPENAI] fallback call failed: {type(e).__name__}: {e}")
        return None
