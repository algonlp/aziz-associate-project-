import json
import os
import asyncio
from typing import Any, Dict, List

from openai import OpenAI

from logger_config import get_logger

logger = get_logger("LeadSummary")


def _extract_json_block(text: str) -> Dict[str, Any]:
    if not text:
        return {}
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return {}
    try:
        return json.loads(text[start : end + 1])
    except json.JSONDecodeError:
        return {}


def _normalize_interest_level(value: str) -> str:
    if not value:
        return "none"
    level = value.strip().lower()
    if level in {"none", "low", "medium", "high"}:
        return level
    if level in {"mild", "weak"}:
        return "low"
    if level in {"strong", "very interested"}:
        return "high"
    return "none"


def _normalize_summary(payload: Dict[str, Any]) -> Dict[str, Any]:
    summary = (payload.get("summary") or "").strip()
    if not summary:
        summary = "No concise summary available."
    key_points_raw = payload.get("key_points") or []
    if isinstance(key_points_raw, str):
        key_points_raw = [key_points_raw]
    key_points: List[str] = []
    for item in key_points_raw:
        item_text = str(item).strip()
        if item_text:
            key_points.append(item_text)
    interest_level = _normalize_interest_level(str(payload.get("interest_level", "")))
    is_interested = payload.get("is_interested")
    if is_interested is None:
        is_interested = interest_level != "none"
    else:
        is_interested = bool(is_interested)
    next_steps = (payload.get("next_steps") or "").strip()
    interest_reason = (payload.get("interest_reason") or "").strip()
    return {
        "is_interested": is_interested,
        "interest_level": interest_level,
        "summary": summary,
        "key_points": key_points,
        "next_steps": next_steps,
        "interest_reason": interest_reason,
    }


def _call_openai_summary(model: str, transcript_text: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

    meta_lines = [
        f"call_sid: {metadata.get('call_sid') or ''}",
        f"agent_name: {metadata.get('agent_name') or ''}",
        f"lead_phone: {metadata.get('lead_phone') or metadata.get('caller_phone') or ''}",
    ]
    prompt = (
        "Analyze the call transcript and decide whether the caller showed any interest "
        "in the offer (including mild interest like asking about pricing, availability, or next steps). "
        "If interest is unclear or negative, mark it as none. "
        "Return a concise summary and key points for sales follow-up."
        "\n\nCall metadata:\n"
        + "\n".join(meta_lines)
        + "\n\nTranscript:\n"
        + transcript_text
    )

    tool_def = {
        "name": "lead_summary",
        "description": "Summarize lead interest and key points from a sales call transcript.",
        "parameters": {
            "type": "object",
            "properties": {
                "is_interested": {"type": "boolean"},
                "interest_level": {
                    "type": "string",
                    "enum": ["none", "low", "medium", "high"],
                },
                "summary": {"type": "string"},
                "key_points": {"type": "array", "items": {"type": "string"}},
                "next_steps": {"type": "string"},
                "interest_reason": {"type": "string"},
            },
            "required": ["is_interested", "interest_level", "summary", "key_points"],
        },
    }

    lower_model = (model or "").lower()
    restricted_temp = lower_model.startswith(("o1", "o3", "gpt-5", "gpt-4.1"))
    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": "You output structured data for lead follow-up."},
            {"role": "user", "content": prompt},
        ],
        tools=[{"type": "function", "function": tool_def}],
        tool_choice={"type": "function", "function": {"name": "lead_summary"}},
        **({} if restricted_temp else {"temperature": 0}),
        max_completion_tokens=400,
    )

    message = response.choices[0].message
    args = ""
    tool_calls = getattr(message, "tool_calls", None) or []
    if tool_calls:
        args = tool_calls[0].function.arguments or ""
    else:
        args = message.content or ""
    payload = _extract_json_block(args)
    return _normalize_summary(payload)


async def summarize_lead(transcript_text: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
    model = (
        os.getenv("LEAD_SUMMARY_MODEL")
        or os.getenv("OPENAI_MODEL")
        or os.getenv("LLM_DEFAULT_MODEL")
        or "gpt-4o-mini"
    )
    if not transcript_text.strip():
        return {
            "is_interested": False,
            "interest_level": "none",
            "summary": "Transcript was empty.",
            "key_points": [],
            "next_steps": "",
            "interest_reason": "",
        }
    max_chars = int(os.getenv("LEAD_SUMMARY_MAX_CHARS", "8000"))
    trimmed = transcript_text.strip()
    if len(trimmed) > max_chars:
        trimmed = trimmed[-max_chars:]
    return await asyncio.to_thread(_call_openai_summary, model, trimmed, metadata)
