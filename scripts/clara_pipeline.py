from __future__ import annotations

import copy
import json
import logging
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests

try:
    from schemas import (
        DAYS_ORDER,
        empty_agent_spec,
        empty_contact,
        empty_memo,
        normalize_memo,
        validate_agent_spec_schema,
        validate_memo_schema,
    )
except ImportError:  # pragma: no cover - direct script execution from /scripts
    sys.path.append(str(Path(__file__).resolve().parents[1]))
    from schemas import (  # type: ignore
        DAYS_ORDER,
        empty_agent_spec,
        empty_contact,
        empty_memo,
        normalize_memo,
        validate_agent_spec_schema,
        validate_memo_schema,
    )

LOGGER = logging.getLogger("clara_pipeline")

DAY_NAME_MAP = {
    "mon": "Monday",
    "monday": "Monday",
    "tue": "Tuesday",
    "tues": "Tuesday",
    "tuesday": "Tuesday",
    "wed": "Wednesday",
    "wednesday": "Wednesday",
    "thu": "Thursday",
    "thur": "Thursday",
    "thurs": "Thursday",
    "thursday": "Thursday",
    "fri": "Friday",
    "friday": "Friday",
    "sat": "Saturday",
    "saturday": "Saturday",
    "sun": "Sunday",
    "sunday": "Sunday",
}

DAY_TOKEN = (
    r"(?:mon(?:day)?|tue(?:s|sday)?|wed(?:nesday)?|"
    r"thu(?:r|rs|rsday)?|fri(?:day)?|sat(?:urday)?|sun(?:day)?)"
)
TIME_12 = r"(?:1[0-2]|0?[1-9])(?::[0-5]\d)?\s*(?:am|pm)"
TIME_24 = r"(?:[01]?\d|2[0-3]):[0-5]\d"

BUSINESS_HOURS_PATTERNS = [
    re.compile(
        rf"(?P<days>{DAY_TOKEN}(?:\s*(?:to|-)\s*{DAY_TOKEN})?)\s*(?:from\s*)?"
        rf"(?P<start>{TIME_12})\s*(?:to|-)\s*(?P<end>{TIME_12})"
        rf"(?:\s*(?P<tz>[A-Z]{{2,5}}))?",
        flags=re.IGNORECASE,
    ),
    re.compile(
        rf"(?P<days>{DAY_TOKEN}(?:\s*(?:to|-)\s*{DAY_TOKEN})?)\s*(?:from\s*)?"
        rf"(?P<start>{TIME_24})\s*(?:to|-)\s*(?P<end>{TIME_24})"
        rf"(?:\s*(?P<tz>[A-Z]{{2,5}}))?",
        flags=re.IGNORECASE,
    ),
    re.compile(
        rf"(?P<start>{TIME_12})\s*(?:to|-)\s*(?P<end>{TIME_12})"
        rf"(?:\s*(?P<tz>[A-Z]{{2,5}}))?\s*(?:on|,)?\s*"
        rf"(?P<days>{DAY_TOKEN}(?:\s*(?:to|-)\s*{DAY_TOKEN})?)",
        flags=re.IGNORECASE,
    ),
    re.compile(
        rf"(?P<start>{TIME_24})\s*(?:to|-)\s*(?P<end>{TIME_24})"
        rf"(?:\s*(?P<tz>[A-Z]{{2,5}}))?\s*(?:on|,)?\s*"
        rf"(?P<days>{DAY_TOKEN}(?:\s*(?:to|-)\s*{DAY_TOKEN})?)",
        flags=re.IGNORECASE,
    ),
]

COMPANY_PATTERNS = [
    re.compile(r"\bWe\s+are\s+([^\n.]+)", flags=re.IGNORECASE),
    re.compile(r"\bThis\s+is\s+([^\n.]+)", flags=re.IGNORECASE),
    re.compile(r"\bCompany\s*:\s*([^\n.]+)", flags=re.IGNORECASE),
]


def configure_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8").strip()


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")


def _load_json_or_default(path: Path, default_payload: Any) -> Any:
    if path.exists():
        return _read_json(path)
    return copy.deepcopy(default_payload)


def _relative_to_repo(path: Path) -> str:
    try:
        return path.resolve().relative_to(Path.cwd().resolve()).as_posix()
    except Exception:
        return path.resolve().as_posix()


def _statements(text: str) -> List[str]:
    cleaned = text.replace("\r", "\n")
    raw = re.split(r"(?<=[.!?])\s+|\n+", cleaned)
    values = [item.strip() for item in raw if item and item.strip()]
    return values


def _sentence_snippet(text: str, span: Tuple[int, int], context: int = 60) -> str:
    start = max(0, span[0] - context)
    end = min(len(text), span[1] + context)
    return re.sub(r"\s+", " ", text[start:end]).strip()


def _dedupe(values: Iterable[str]) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    for value in values:
        item = value.strip()
        if not item:
            continue
        if item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _normalize_time(token: str) -> str:
    value = token.strip().lower().replace(".", "")
    ampm_match = re.match(r"^(\d{1,2})(?::(\d{2}))?\s*(am|pm)$", value)
    if ampm_match:
        hour = int(ampm_match.group(1))
        minute = int(ampm_match.group(2) or "0")
        meridiem = ampm_match.group(3)
        if meridiem == "am":
            hour = 0 if hour == 12 else hour
        else:
            hour = 12 if hour == 12 else hour + 12
        return f"{hour:02d}:{minute:02d}"

    hh_match = re.match(r"^(\d{1,2}):(\d{2})$", value)
    if hh_match:
        return f"{int(hh_match.group(1)):02d}:{int(hh_match.group(2)):02d}"

    return ""


def _expand_day_range(start_day: str, end_day: str) -> List[str]:
    start_idx = DAYS_ORDER.index(start_day)
    end_idx = DAYS_ORDER.index(end_day)
    if start_idx <= end_idx:
        return DAYS_ORDER[start_idx : end_idx + 1]
    return DAYS_ORDER[start_idx:] + DAYS_ORDER[: end_idx + 1]


def _parse_days(days_fragment: str) -> List[str]:
    cleaned = re.sub(r"\s+", " ", days_fragment.strip().lower())
    parts = re.split(r"\s*(?:to|-)\s*", cleaned)
    if len(parts) == 2:
        left = DAY_NAME_MAP.get(parts[0].strip())
        right = DAY_NAME_MAP.get(parts[1].strip())
        if left and right:
            return _expand_day_range(left, right)

    days: List[str] = []
    for token in re.split(r",|\band\b", cleaned):
        name = DAY_NAME_MAP.get(token.strip())
        if name:
            days.append(name)
    return _dedupe(days)


def _extract_company_name(text: str) -> str:
    for pattern in COMPANY_PATTERNS:
        match = pattern.search(text)
        if match:
            return match.group(1).strip()
    return ""


def _extract_business_hours(text: str) -> Dict[str, Any]:
    candidates: List[Dict[str, Any]] = []
    for pattern in BUSINESS_HOURS_PATTERNS:
        for match in pattern.finditer(text):
            days = _parse_days(match.group("days"))
            start = _normalize_time(match.group("start"))
            end = _normalize_time(match.group("end"))
            timezone = str(match.group("tz") or "").upper().strip()
            if not days or not start or not end:
                continue
            candidates.append(
                {
                    "value": {
                        "days": days,
                        "start": start,
                        "end": end,
                        "timezone": timezone,
                    },
                    "snippet": _sentence_snippet(text, match.span()),
                }
            )

    if not candidates:
        return {"value": None, "conflict": None, "snippet": ""}

    unique: Dict[str, Dict[str, Any]] = {}
    for candidate in candidates:
        key = json.dumps(candidate["value"], sort_keys=True)
        unique[key] = candidate

    if len(unique) > 1:
        return {
            "value": None,
            "conflict": "Multiple business-hours values found.",
            "snippet": " | ".join(item["snippet"] for item in unique.values()),
        }

    only = next(iter(unique.values()))
    return {"value": only["value"], "conflict": None, "snippet": only["snippet"]}


def _extract_office_address(text: str) -> str:
    for pattern in [
        r"office\s+address\s*(?:is|:)\s*([^\n.]+)",
        r"address\s*(?:is|:)\s*([^\n.]+)",
    ]:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            return match.group(1).strip()
    return ""


def _extract_services_supported(text: str) -> List[str]:
    match = re.search(
        r"(?:services\s+supported|services\s+include|we\s+support)\s*(?:are|:)?\s*([^\n.]+)",
        text,
        flags=re.IGNORECASE,
    )
    if not match:
        return []
    parts = re.split(r",|\bor\b|\band\b", match.group(1), flags=re.IGNORECASE)
    values = [part.strip().lower() for part in parts if part.strip()]
    return _dedupe(values)


def _extract_emergency_definition(text: str) -> List[str]:
    for pattern in [
        r"\bemergency\s+means?\s+([^\n.]+)",
        r"\bemergencies\s+include\s+([^\n.]+)",
    ]:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if not match:
            continue
        parts = re.split(r",|\bor\b|\band\b", match.group(1), flags=re.IGNORECASE)
        values = [part.strip().lower() for part in parts if part.strip()]
        return _dedupe(values)
    return []


def _extract_transfer_timeout_candidates(text: str) -> List[Tuple[int, str]]:
    candidates: List[Tuple[int, str]] = []
    for match in re.finditer(
        r"(?:transfer|transferred|route|routed)[^.\n]{0,80}?(?:within|in)\s+(\d+)\s*seconds?",
        text,
        flags=re.IGNORECASE,
    ):
        candidates.append((int(match.group(1)), _sentence_snippet(text, match.span())))
    for match in re.finditer(
        r"(?:within|in)\s+(\d+)\s*seconds?[^.\n]{0,80}?(?:transfer|transferred|route|routed)",
        text,
        flags=re.IGNORECASE,
    ):
        candidates.append((int(match.group(1)), _sentence_snippet(text, match.span())))
    return candidates


def _extract_transfer_timeout(text: str) -> Dict[str, Any]:
    candidates = _extract_transfer_timeout_candidates(text)
    if not candidates:
        return {"value": None, "conflict": None, "snippet": ""}
    unique_values = sorted({value for value, _ in candidates})
    if len(unique_values) > 1:
        return {
            "value": None,
            "conflict": "Multiple transfer timeout values found.",
            "snippet": " | ".join(snippet for _, snippet in candidates),
        }
    value = unique_values[0]
    snippet = next(snippet for candidate, snippet in candidates if candidate == value)
    return {"value": value, "conflict": None, "snippet": snippet}


def _extract_retries(text: str) -> Optional[int]:
    for pattern in [
        r"retries?\s*[:=]?\s*(\d+)",
        r"retry(?:ing)?\s+(?:up to\s+)?(\d+)\s+times?",
        r"(\d+)\s+retries",
    ]:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            return int(match.group(1))
    return None


def _extract_fail_message(text: str) -> str:
    for statement in _statements(text):
        lowered = statement.lower()
        if "if transfer fails" in lowered or "if unable to transfer" in lowered:
            return statement.strip()
    return ""


def _extract_target_role(statement: str) -> str:
    match = re.search(r"\bto\s+([A-Za-z][A-Za-z0-9 &/\-]{1,80})", statement, flags=re.IGNORECASE)
    if not match:
        return ""
    target = match.group(1).strip(" .")
    target = re.sub(r"\bwithin\s+\d+\s+seconds?\b.*", "", target, flags=re.IGNORECASE).strip(" .")
    target = re.sub(r"^the\s+", "", target, flags=re.IGNORECASE)
    return target.lower()


def _extract_emergency_routing(text: str) -> Dict[str, Any]:
    routing: Dict[str, Any] = {
        "contacts": [],
        "fallback": "",
        "notes": "",
    }
    statements = _statements(text)
    for statement in statements:
        lowered = statement.lower()
        if "emergency" not in lowered:
            continue
        if "transfer" in lowered or "route" in lowered:
            role = _extract_target_role(statement)
            if role:
                contact = empty_contact()
                contact["role"] = role
                contact["priority_order"] = 1
                routing["contacts"] = [contact]
                routing["notes"] = statement.strip()
                break
            if "after hours" in lowered or "after-hours" in lowered:
                routing["notes"] = statement.strip()
                break

    for statement in statements:
        lowered = statement.lower()
        if "if transfer fails" in lowered or "if unable to transfer" in lowered:
            routing["fallback"] = statement.strip()
            if not routing["notes"]:
                routing["notes"] = statement.strip()
            break
    return routing


def _extract_non_emergency_routing(text: str) -> Dict[str, Any]:
    routing: Dict[str, Any] = {
        "contacts": [],
        "notes": "",
    }
    statements = _statements(text)
    for statement in statements:
        lowered = statement.lower()
        if "non-emergency" not in lowered and "non emergency" not in lowered:
            continue
        routing["notes"] = statement.strip()
        role = _extract_target_role(statement)
        if role:
            contact = empty_contact()
            contact["role"] = role
            contact["priority_order"] = 1
            routing["contacts"] = [contact]
        break
    return routing


def _extract_integration_constraints(text: str) -> List[str]:
    constraints: List[str] = []
    for statement in _statements(text):
        lowered = statement.lower()
        if any(token in lowered for token in ["never", "do not", "don't", "must not", "cannot"]):
            constraints.append(statement.strip())
    return _dedupe(constraints)


def _extract_after_hours_flow(text: str) -> str:
    for statement in _statements(text):
        lowered = statement.lower()
        if "after hours" in lowered or "after-hours" in lowered:
            return statement.strip()
    return ""


def _extract_office_hours_flow(text: str, business_hours_snippet: str) -> str:
    for statement in _statements(text):
        lowered = statement.lower()
        if "business hours" in lowered or "office hours" in lowered:
            return statement.strip()
        if "operate" in lowered and any(day.lower() in lowered for day in DAYS_ORDER):
            return statement.strip()
    if business_hours_snippet:
        return business_hours_snippet
    return ""


def _build_questions_or_unknowns(memo: Dict[str, Any], extra_questions: Optional[List[str]] = None) -> List[str]:
    questions: List[str] = []

    if not memo["company_name"]:
        questions.append("Please provide company_name.")

    business_hours = memo["business_hours"]
    if not business_hours["days"]:
        questions.append("Please provide business_hours.days.")
    if not business_hours["start"]:
        questions.append("Please provide business_hours.start.")
    if not business_hours["end"]:
        questions.append("Please provide business_hours.end.")
    if not business_hours["timezone"]:
        questions.append("Please provide business_hours.timezone.")

    if not memo["office_address"]:
        questions.append("Please provide office_address.")
    if not memo["services_supported"]:
        questions.append("Please provide services_supported.")
    if not memo["emergency_definition"]:
        questions.append("Please provide emergency_definition.")

    emergency_routing = memo["emergency_routing_rules"]
    if not emergency_routing["contacts"]:
        questions.append("Please provide emergency_routing_rules.contacts.")
    if not emergency_routing["fallback"]:
        questions.append("Please provide emergency_routing_rules.fallback.")

    non_emergency = memo["non_emergency_routing_rules"]
    if not non_emergency["contacts"] and not non_emergency["notes"]:
        questions.append("Please provide non_emergency_routing_rules.")

    transfer = memo["call_transfer_rules"]
    if transfer["timeout_seconds"] is None:
        questions.append("Please provide call_transfer_rules.timeout_seconds.")
    if transfer["retries"] is None:
        questions.append("Please provide call_transfer_rules.retries.")
    if not transfer["fail_message"]:
        questions.append("Please provide call_transfer_rules.fail_message.")

    if not memo["integration_constraints"]:
        questions.append("Please provide integration_constraints.")
    if not memo["after_hours_flow_summary"]:
        questions.append("Please provide after_hours_flow_summary.")
    if not memo["office_hours_flow_summary"]:
        questions.append("Please provide office_hours_flow_summary.")

    if extra_questions:
        questions.extend(extra_questions)

    return _dedupe(questions)


def build_v1_memo(account_id: str, transcript_text: str) -> Dict[str, Any]:
    memo = empty_memo(account_id)
    memo["company_name"] = _extract_company_name(transcript_text)

    business_hours = _extract_business_hours(transcript_text)
    if business_hours["value"]:
        memo["business_hours"] = business_hours["value"]

    memo["office_address"] = _extract_office_address(transcript_text)
    memo["services_supported"] = _extract_services_supported(transcript_text)
    memo["emergency_definition"] = _extract_emergency_definition(transcript_text)
    memo["emergency_routing_rules"] = _extract_emergency_routing(transcript_text)
    memo["non_emergency_routing_rules"] = _extract_non_emergency_routing(transcript_text)

    timeout = _extract_transfer_timeout(transcript_text)
    if timeout["value"] is not None:
        memo["call_transfer_rules"]["timeout_seconds"] = timeout["value"]

    retries = _extract_retries(transcript_text)
    if retries is not None:
        memo["call_transfer_rules"]["retries"] = retries

    fail_message = _extract_fail_message(transcript_text)
    if fail_message:
        memo["call_transfer_rules"]["fail_message"] = fail_message

    if memo["emergency_routing_rules"]["notes"]:
        memo["call_transfer_rules"]["routing_notes"] = memo["emergency_routing_rules"]["notes"]

    memo["integration_constraints"] = _extract_integration_constraints(transcript_text)
    memo["after_hours_flow_summary"] = _extract_after_hours_flow(transcript_text)
    memo["office_hours_flow_summary"] = _extract_office_hours_flow(
        transcript_text, business_hours.get("snippet", "")
    )
    memo["notes"] = "Generated from demo transcript."

    extra_questions: List[str] = []
    if business_hours["conflict"]:
        extra_questions.append("On demo transcript, business_hours has conflicting values.")
    if timeout["conflict"]:
        extra_questions.append("On demo transcript, call_transfer_rules.timeout_seconds has conflicting values.")

    memo["questions_or_unknowns"] = _build_questions_or_unknowns(memo, extra_questions)
    memo = normalize_memo(account_id, memo)

    ok, errors = validate_memo_schema(memo)
    if not ok:
        raise ValueError(f"Invalid v1 memo schema for {account_id}: {errors}")
    return memo


def _format_hours_for_prompt(hours: Dict[str, Any]) -> str:
    days = hours.get("days", [])
    start = hours.get("start", "")
    end = hours.get("end", "")
    timezone = hours.get("timezone", "")
    if not days or not start or not end:
        return "Not confirmed"
    return f"{', '.join(days)} {start}-{end} {timezone}".strip()


def build_agent_spec(memo: Dict[str, Any], version: str) -> Dict[str, Any]:
    spec = empty_agent_spec(version)
    agent_base = memo["company_name"] or memo["account_id"]
    spec["agent_name"] = f"{agent_base} Front Desk Agent"

    timeout = memo["call_transfer_rules"]["timeout_seconds"]
    retries = memo["call_transfer_rules"]["retries"]
    timeout_text = str(timeout) if timeout is not None else "not set"
    retries_text = str(retries) if retries is not None else "not set"

    spec["system_prompt"] = (
        f"You are Clara, the phone assistant for {memo['company_name'] or 'the company'}.\n"
        "Do not mention internal implementation details to callers.\n"
        "Collect only information needed for routing and follow-up.\n\n"
        f"Business hours: {_format_hours_for_prompt(memo['business_hours'])}.\n\n"
        "Business-hours flow:\n"
        "1. Greet the caller and confirm purpose.\n"
        "2. Collect caller name and callback number.\n"
        "3. Route according to office-hours rules.\n"
        "4. If transfer fails, use the fallback message and confirm follow-up.\n"
        "5. Ask if anything else is needed, then close.\n\n"
        "After-hours flow:\n"
        "1. Greet the caller and confirm whether this is an emergency.\n"
        "2. For emergency calls, collect caller name, callback number, and location if needed.\n"
        "3. Attempt emergency transfer using configured transfer settings.\n"
        "4. If transfer fails, use the fallback message exactly and confirm escalation.\n"
        "5. For non-emergency calls, collect minimal details and confirm follow-up.\n"
        "6. Ask if anything else is needed, then close.\n"
        f"Transfer settings: timeout={timeout_text}, retries={retries_text}."
    )

    spec["key_variables"] = {
        "timezone": memo["business_hours"]["timezone"],
        "business_hours": memo["business_hours"],
        "address": memo["office_address"],
        "emergency_routing": memo["emergency_routing_rules"],
    }

    timeout_placeholder = timeout if timeout is not None else "{{timeout_seconds}}"
    retries_placeholder = retries if retries is not None else "{{retries}}"
    spec["tool_invocation_placeholders"] = {
        "create_ticket": "[[TOOL:CREATE_TICKET account_id={{account_id}} priority={{priority}}]]",
        "notify_dispatch": "[[TOOL:NOTIFY_DISPATCH target={{dispatch_target}}]]",
        "transfer_call": (
            f"[[TOOL:TRANSFER_CALL target={{{{target_contact}}}} timeout={timeout_placeholder} "
            f"retries={retries_placeholder}]]"
        ),
    }

    spec["call_transfer_protocol"] = {
        "steps": [
            "Attempt transfer to the primary contact.",
            "Retry based on configured retries.",
            "If transfer still fails, run fallback protocol.",
        ],
        "timeout_seconds": timeout,
        "retries": retries,
    }

    fallback_message = memo["call_transfer_rules"]["fail_message"] or (
        "I am sorry, I could not connect you right now. I will notify the team and have someone call you back shortly."
    )
    spec["fallback_protocol_if_transfer_fails"] = {
        "exact_caller_message": fallback_message,
        "steps": [
            "Apologize and acknowledge urgency.",
            "Confirm callback number and relevant details.",
            "Notify fallback contact or dispatch.",
            "Confirm next follow-up step and close politely.",
        ],
    }

    ok, errors = validate_agent_spec_schema(spec)
    if not ok:
        raise ValueError(f"Invalid {version} agent spec schema for {memo['account_id']}: {errors}")
    return spec


def _json_fingerprint(payload: Any) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _normalize_string_list(value: Any) -> List[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str):
        return [value.strip()] if value.strip() else []
    return []


def _to_optional_int(value: Any) -> Optional[int]:
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        cleaned = value.strip()
        if cleaned.isdigit():
            return int(cleaned)
    return None


def _normalize_business_hours(value: Any) -> Dict[str, Any]:
    business_hours = value if isinstance(value, dict) else {}
    days_raw = business_hours.get("days", [])
    if isinstance(days_raw, str):
        parsed = _parse_days(days_raw)
        days = parsed if parsed else ([days_raw.strip()] if days_raw.strip() else [])
    elif isinstance(days_raw, list):
        days = [str(day).strip() for day in days_raw if str(day).strip()]
    else:
        days = []

    return {
        "days": days,
        "start": str(business_hours.get("start", "") or "").strip(),
        "end": str(business_hours.get("end", "") or "").strip(),
        "timezone": str(business_hours.get("timezone", "") or "").strip(),
    }


def _normalize_routing(value: Any, allow_fallback: bool) -> Dict[str, Any]:
    if isinstance(value, str):
        payload: Dict[str, Any] = {"contacts": [], "notes": value.strip()}
        if allow_fallback:
            payload["fallback"] = ""
        return payload
    payload = value if isinstance(value, dict) else {}
    result: Dict[str, Any] = {
        "contacts": _normalize_contact_list(payload.get("contacts")),
        "notes": str(payload.get("notes", "") or "").strip(),
    }
    if allow_fallback:
        result["fallback"] = str(payload.get("fallback", "") or "").strip()
    return result


def _normalize_transfer_rules(value: Any) -> Dict[str, Any]:
    if isinstance(value, str):
        return {
            "timeout_seconds": None,
            "retries": None,
            "fail_message": value.strip(),
            "routing_notes": "",
        }
    payload = value if isinstance(value, dict) else {}
    return {
        "timeout_seconds": _to_optional_int(payload.get("timeout_seconds")),
        "retries": _to_optional_int(payload.get("retries")),
        "fail_message": str(payload.get("fail_message", "") or "").strip(),
        "routing_notes": str(payload.get("routing_notes", "") or "").strip(),
    }


def _normalize_and_validate_memo_for_write(account_id: str, memo: Any) -> Tuple[Dict[str, Any], bool]:
    raw_payload = memo if isinstance(memo, dict) else {}
    normalized = normalize_memo(account_id, raw_payload)

    normalized["account_id"] = account_id
    normalized["company_name"] = str(normalized.get("company_name", "") or "").strip()
    normalized["business_hours"] = _normalize_business_hours(normalized.get("business_hours"))
    normalized["office_address"] = str(normalized.get("office_address", "") or "").strip()
    normalized["services_supported"] = _normalize_string_list(normalized.get("services_supported"))
    normalized["emergency_definition"] = _normalize_string_list(normalized.get("emergency_definition"))
    normalized["emergency_routing_rules"] = _normalize_routing(
        normalized.get("emergency_routing_rules"), allow_fallback=True
    )
    normalized["non_emergency_routing_rules"] = _normalize_routing(
        normalized.get("non_emergency_routing_rules"), allow_fallback=False
    )
    normalized["call_transfer_rules"] = _normalize_transfer_rules(normalized.get("call_transfer_rules"))
    normalized["integration_constraints"] = _normalize_string_list(normalized.get("integration_constraints"))
    normalized["after_hours_flow_summary"] = str(normalized.get("after_hours_flow_summary", "") or "").strip()
    normalized["office_hours_flow_summary"] = str(normalized.get("office_hours_flow_summary", "") or "").strip()
    normalized["questions_or_unknowns"] = _normalize_string_list(normalized.get("questions_or_unknowns"))
    normalized["notes"] = str(normalized.get("notes", "") or "").strip()

    normalized = normalize_memo(account_id, normalized)
    ok, errors = validate_memo_schema(normalized)
    if not ok:
        raise ValueError(f"Schema-invalid memo for {account_id}: {errors}")

    changed = _json_fingerprint(raw_payload) != _json_fingerprint(normalized)
    return normalized, changed


def _normalize_and_validate_agent_spec_for_write(
    spec: Any, version: str, account_id: str
) -> Tuple[Dict[str, Any], bool]:
    raw_payload = spec if isinstance(spec, dict) else {}
    normalized = empty_agent_spec(version)

    if isinstance(raw_payload, dict):
        for key, default_value in normalized.items():
            incoming = raw_payload.get(key)
            if isinstance(default_value, dict) and isinstance(incoming, dict):
                merged = copy.deepcopy(default_value)
                merged.update(incoming)
                normalized[key] = merged
            elif incoming is not None:
                normalized[key] = incoming

    normalized["agent_name"] = str(normalized.get("agent_name", "") or "").strip()
    normalized["voice_style"] = str(normalized.get("voice_style", "") or "").strip()
    normalized["system_prompt"] = str(normalized.get("system_prompt", "") or "").strip()
    normalized["version"] = version

    key_vars = normalized.get("key_variables")
    if not isinstance(key_vars, dict):
        key_vars = empty_agent_spec(version)["key_variables"]
    key_vars["timezone"] = str(key_vars.get("timezone", "") or "").strip()
    key_vars["business_hours"] = _normalize_business_hours(key_vars.get("business_hours"))
    key_vars["address"] = str(key_vars.get("address", "") or "").strip()
    key_vars["emergency_routing"] = _normalize_routing(key_vars.get("emergency_routing"), allow_fallback=True)
    normalized["key_variables"] = key_vars

    placeholders = normalized.get("tool_invocation_placeholders")
    if not isinstance(placeholders, dict):
        placeholders = empty_agent_spec(version)["tool_invocation_placeholders"]
    normalized["tool_invocation_placeholders"] = {
        "create_ticket": str(placeholders.get("create_ticket", "") or "").strip(),
        "notify_dispatch": str(placeholders.get("notify_dispatch", "") or "").strip(),
        "transfer_call": str(placeholders.get("transfer_call", "") or "").strip(),
    }

    transfer = normalized.get("call_transfer_protocol")
    if not isinstance(transfer, dict):
        transfer = empty_agent_spec(version)["call_transfer_protocol"]
    steps = transfer.get("steps", [])
    normalized["call_transfer_protocol"] = {
        "steps": _normalize_string_list(steps),
        "timeout_seconds": _to_optional_int(transfer.get("timeout_seconds")),
        "retries": _to_optional_int(transfer.get("retries")),
    }

    fallback = normalized.get("fallback_protocol_if_transfer_fails")
    if not isinstance(fallback, dict):
        fallback = empty_agent_spec(version)["fallback_protocol_if_transfer_fails"]
    normalized["fallback_protocol_if_transfer_fails"] = {
        "exact_caller_message": str(fallback.get("exact_caller_message", "") or "").strip(),
        "steps": _normalize_string_list(fallback.get("steps", [])),
    }

    ok, errors = validate_agent_spec_schema(normalized)
    if not ok:
        raise ValueError(f"Schema-invalid {version} agent spec for {account_id}: {errors}")

    changed = _json_fingerprint(raw_payload) != _json_fingerprint(normalized)
    return normalized, changed


def _normalize_changes(changes: Any) -> Tuple[List[Dict[str, Any]], bool]:
    if not isinstance(changes, list):
        return [], False
    normalized: List[Dict[str, Any]] = []
    valid = True
    for row in changes:
        if not isinstance(row, dict):
            valid = False
            continue
        normalized_row = {
            "field": str(row.get("field", "") or "").strip(),
            "old_value": row.get("old_value"),
            "new_value": row.get("new_value"),
            "source": str(row.get("source", "onboarding") or "onboarding").strip(),
            "rationale": str(row.get("rationale", "") or "").strip(),
        }
        if not normalized_row["field"] or not normalized_row["rationale"]:
            valid = False
        normalized.append(normalized_row)
    return normalized, valid


def _get_nested(payload: Dict[str, Any], path: str) -> Any:
    current: Any = payload
    for part in path.split("."):
        current = current[part]
    return current


def _set_nested(payload: Dict[str, Any], path: str, value: Any) -> None:
    parts = path.split(".")
    current: Any = payload
    for part in parts[:-1]:
        current = current[part]
    current[parts[-1]] = value


def _add_change(changes: List[Dict[str, Any]], field: str, old_value: Any, new_value: Any, rationale: str) -> None:
    changes.append(
        {
            "field": field,
            "old_value": old_value,
            "new_value": new_value,
            "source": "onboarding",
            "rationale": rationale,
        }
    )


def _normalize_contact_list(raw_contacts: Any) -> List[Dict[str, Any]]:
    if not isinstance(raw_contacts, list):
        return []
    contacts: List[Dict[str, Any]] = []
    for index, item in enumerate(raw_contacts, start=1):
        if not isinstance(item, dict):
            continue
        contact = empty_contact()
        contact["name"] = str(item.get("name", "") or "").strip()
        contact["role"] = str(item.get("role", "") or "").strip().lower()
        contact["phone"] = str(item.get("phone", "") or "").strip()
        priority = item.get("priority_order")
        contact["priority_order"] = priority if isinstance(priority, int) else index
        contacts.append(contact)
    return contacts


def _normalize_business_hours_from_form(raw: Any) -> Optional[Dict[str, Any]]:
    if isinstance(raw, dict):
        raw_days = raw.get("days", [])
        days = _parse_days(raw_days) if isinstance(raw_days, str) else raw_days
        if not isinstance(days, list):
            days = []
        return {
            "days": [str(day) for day in days if str(day)],
            "start": str(raw.get("start", "") or "").strip(),
            "end": str(raw.get("end", "") or "").strip(),
            "timezone": str(raw.get("timezone", "") or "").strip(),
        }
    if isinstance(raw, str):
        extracted = _extract_business_hours(raw)
        return extracted["value"]
    return None


def _extract_onboarding_updates_text(
    onboarding_text: str,
) -> Tuple[Dict[str, Any], List[str], List[Dict[str, Any]]]:
    updates: Dict[str, Any] = {}
    questions: List[str] = []
    conflicts: List[Dict[str, Any]] = []

    company_name = _extract_company_name(onboarding_text)
    if company_name:
        updates["company_name"] = company_name

    business_hours = _extract_business_hours(onboarding_text)
    if business_hours["conflict"]:
        conflicts.append(
            {
                "field": "business_hours",
                "rationale": "Conflicting business_hours statements found. Kept previous value.",
            }
        )
        questions.append("Onboarding has conflicting business_hours values. Please confirm the final schedule.")
    elif business_hours["value"]:
        updates["business_hours"] = business_hours["value"]

    timeout = _extract_transfer_timeout(onboarding_text)
    if timeout["conflict"]:
        conflicts.append(
            {
                "field": "call_transfer_rules.timeout_seconds",
                "rationale": "Conflicting transfer timeout statements found. Kept previous value.",
            }
        )
        questions.append("Onboarding has conflicting timeout_seconds values. Please confirm.")
    elif timeout["value"] is not None:
        updates["call_transfer_rules.timeout_seconds"] = timeout["value"]

    retries = _extract_retries(onboarding_text)
    if retries is not None:
        updates["call_transfer_rules.retries"] = retries

    fail_message = _extract_fail_message(onboarding_text)
    if fail_message:
        updates["call_transfer_rules.fail_message"] = fail_message

    emergency_routing = _extract_emergency_routing(onboarding_text)
    if emergency_routing["contacts"]:
        updates["emergency_routing_rules.contacts"] = emergency_routing["contacts"]
    if emergency_routing["fallback"]:
        updates["emergency_routing_rules.fallback"] = emergency_routing["fallback"]
    if emergency_routing["notes"]:
        updates["emergency_routing_rules.notes"] = emergency_routing["notes"]

    non_emergency = _extract_non_emergency_routing(onboarding_text)
    if non_emergency["contacts"]:
        updates["non_emergency_routing_rules.contacts"] = non_emergency["contacts"]
    if non_emergency["notes"]:
        updates["non_emergency_routing_rules.notes"] = non_emergency["notes"]

    address = _extract_office_address(onboarding_text)
    if address:
        updates["office_address"] = address

    services = _extract_services_supported(onboarding_text)
    if services:
        updates["services_supported"] = services

    emergencies = _extract_emergency_definition(onboarding_text)
    if emergencies:
        updates["emergency_definition"] = emergencies

    constraints = _extract_integration_constraints(onboarding_text)
    if constraints:
        updates["integration_constraints"] = constraints

    after_hours = _extract_after_hours_flow(onboarding_text)
    if after_hours:
        updates["after_hours_flow_summary"] = after_hours

    office_hours = _extract_office_hours_flow(onboarding_text, business_hours.get("snippet", ""))
    if office_hours:
        updates["office_hours_flow_summary"] = office_hours

    return updates, questions, conflicts


def _extract_onboarding_updates_form(
    form_payload: Dict[str, Any],
) -> Tuple[Dict[str, Any], List[str], List[Dict[str, Any]]]:
    updates: Dict[str, Any] = {}
    questions: List[str] = []
    conflicts: List[Dict[str, Any]] = []

    company_name = form_payload.get("company_name")
    if isinstance(company_name, str) and company_name.strip():
        updates["company_name"] = company_name.strip()

    business_hours = _normalize_business_hours_from_form(form_payload.get("business_hours"))
    if business_hours:
        updates["business_hours"] = business_hours

    office_address = form_payload.get("office_address")
    if isinstance(office_address, str):
        updates["office_address"] = office_address.strip()

    services = form_payload.get("services_supported")
    if isinstance(services, list):
        updates["services_supported"] = [str(item).strip() for item in services if str(item).strip()]

    emergency_def = form_payload.get("emergency_definition")
    if isinstance(emergency_def, list):
        updates["emergency_definition"] = [str(item).strip().lower() for item in emergency_def if str(item).strip()]

    emergency_routing = form_payload.get("emergency_routing_rules")
    if isinstance(emergency_routing, dict):
        contacts = _normalize_contact_list(emergency_routing.get("contacts"))
        if contacts:
            updates["emergency_routing_rules.contacts"] = contacts
        for key in ["fallback", "notes"]:
            value = emergency_routing.get(key)
            if isinstance(value, str):
                updates[f"emergency_routing_rules.{key}"] = value.strip()

    non_emergency = form_payload.get("non_emergency_routing_rules")
    if isinstance(non_emergency, dict):
        contacts = _normalize_contact_list(non_emergency.get("contacts"))
        if contacts:
            updates["non_emergency_routing_rules.contacts"] = contacts
        notes = non_emergency.get("notes")
        if isinstance(notes, str):
            updates["non_emergency_routing_rules.notes"] = notes.strip()

    transfer = form_payload.get("call_transfer_rules")
    if isinstance(transfer, dict):
        timeout = transfer.get("timeout_seconds")
        retries = transfer.get("retries")
        fail_message = transfer.get("fail_message")
        routing_notes = transfer.get("routing_notes")
        if isinstance(timeout, int):
            updates["call_transfer_rules.timeout_seconds"] = timeout
        if isinstance(retries, int):
            updates["call_transfer_rules.retries"] = retries
        if isinstance(fail_message, str):
            updates["call_transfer_rules.fail_message"] = fail_message.strip()
        if isinstance(routing_notes, str):
            updates["call_transfer_rules.routing_notes"] = routing_notes.strip()

    constraints = form_payload.get("integration_constraints")
    if isinstance(constraints, list):
        updates["integration_constraints"] = [str(item).strip() for item in constraints if str(item).strip()]

    for key in ["after_hours_flow_summary", "office_hours_flow_summary", "notes"]:
        value = form_payload.get(key)
        if isinstance(value, str):
            updates[key] = value.strip()

    return updates, questions, conflicts


def apply_onboarding_patch(
    v1_memo: Dict[str, Any],
    onboarding_source: Dict[str, Any],
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    memo = copy.deepcopy(v1_memo)
    changes: List[Dict[str, Any]] = []

    source_type = onboarding_source["type"]
    source_payload = onboarding_source["payload"]
    if source_type == "json":
        updates, conflict_questions, conflicts = _extract_onboarding_updates_form(source_payload)
    else:
        updates, conflict_questions, conflicts = _extract_onboarding_updates_text(source_payload)

    for conflict in conflicts:
        field = conflict["field"]
        old_value = copy.deepcopy(_get_nested(memo, field))
        _add_change(changes, field, old_value, old_value, conflict["rationale"])

    for field in sorted(updates.keys()):
        new_value = updates[field]
        old_value = copy.deepcopy(_get_nested(memo, field))

        if field == "integration_constraints":
            merged = _dedupe((old_value if isinstance(old_value, list) else []) + new_value)
            if merged != old_value:
                _set_nested(memo, field, merged)
                _add_change(changes, field, old_value, merged, "Updated from onboarding input.")
            continue

        if old_value != new_value:
            _set_nested(memo, field, copy.deepcopy(new_value))
            _add_change(changes, field, old_value, new_value, "Updated from onboarding input.")

    memo["notes"] = "Updated from onboarding input."
    memo["questions_or_unknowns"] = _build_questions_or_unknowns(memo, conflict_questions)
    memo = normalize_memo(v1_memo["account_id"], memo)

    ok, errors = validate_memo_schema(memo)
    if not ok:
        raise ValueError(f"Invalid v2 memo schema for {v1_memo['account_id']}: {errors}")

    return memo, changes


def _github_headers(token: str) -> Dict[str, str]:
    return {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def _find_issue(repo: str, token: str, title: str) -> Optional[Dict[str, Any]]:
    response = requests.get(
        f"https://api.github.com/repos/{repo}/issues",
        headers=_github_headers(token),
        params={"state": "all", "per_page": 100},
        timeout=30,
    )
    response.raise_for_status()
    for issue in response.json():
        if "pull_request" in issue:
            continue
        if issue.get("title") == title:
            return issue
    return None


def _create_issue(repo: str, token: str, title: str, body: str) -> Dict[str, Any]:
    response = requests.post(
        f"https://api.github.com/repos/{repo}/issues",
        headers=_github_headers(token),
        json={"title": title, "body": body},
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def ensure_account_issue(account_id: str, company_name: str) -> Dict[str, Any]:
    repo = os.getenv("GITHUB_REPO", "").strip()
    token = os.getenv("GITHUB_TOKEN", "").strip()
    title = f"Clara Automation Task - {account_id}"

    if not repo or not token:
        return {
            "type": "github_issues",
            "mocked": True,
            "repository": repo,
            "title": title,
            "issue_number": None,
            "issue_url": "",
            "note": "GITHUB_REPO or GITHUB_TOKEN missing. Local mocked task record used.",
        }

    try:
        issue = _find_issue(repo, token, title)
        if issue is None:
            issue = _create_issue(
                repo,
                token,
                title,
                (
                    f"Automated tracker for Clara pipeline.\n\n"
                    f"account_id: {account_id}\n"
                    f"company_name: {company_name or 'unknown'}"
                ),
            )
            note = "Issue created."
        else:
            note = "Issue reused."
        return {
            "type": "github_issues",
            "mocked": False,
            "repository": repo,
            "title": title,
            "issue_number": issue.get("number"),
            "issue_url": issue.get("html_url", ""),
            "note": note,
        }
    except Exception as exc:  # pragma: no cover - network edge case
        LOGGER.warning("GitHub issue integration failed for %s: %s", account_id, exc)
        return {
            "type": "github_issues",
            "mocked": True,
            "repository": repo,
            "title": title,
            "issue_number": None,
            "issue_url": "",
            "note": f"GitHub integration failed. Local mocked task record used. Error: {exc}",
        }

def _default_task_payload(account_id: str, tracker: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "account_id": account_id,
        "tracker": tracker,
        "status": "pending",
        "stages": {
            "pipeline_a_v1": {
                "status": "pending",
                "memo_path": "",
                "agent_spec_path": "",
            },
            "pipeline_b_v2": {
                "status": "pending",
                "memo_path": "",
                "agent_spec_path": "",
                "changes_path": "",
            },
        },
    }


def _merge_tracker(existing: Any, incoming: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(existing, dict):
        return incoming
    existing_mocked = bool(existing.get("mocked", True))
    incoming_mocked = bool(incoming.get("mocked", True))
    if existing_mocked and not incoming_mocked:
        return incoming
    if not existing_mocked and incoming_mocked:
        return existing
    return incoming


def _update_global_tasks(accounts_root: Path, task_payload: Dict[str, Any], task_path: Path) -> None:
    outputs_root = accounts_root.parent
    global_path = outputs_root / "tasks.json"
    global_payload = _load_json_or_default(global_path, {"items": []})
    if not isinstance(global_payload, dict):
        global_payload = {"items": []}
    items = global_payload.get("items")
    if not isinstance(items, list):
        items = []

    v1_stage = task_payload["stages"]["pipeline_a_v1"]
    v2_stage = task_payload["stages"]["pipeline_b_v2"]
    item = {
        "account_id": task_payload["account_id"],
        "status": task_payload["status"],
        "task_file": _relative_to_repo(task_path),
        "tracker": task_payload["tracker"],
        "v1_memo": v1_stage["memo_path"],
        "v1_agent_spec": v1_stage["agent_spec_path"],
        "v2_memo": v2_stage["memo_path"],
        "v2_agent_spec": v2_stage["agent_spec_path"],
        "v2_changes": v2_stage["changes_path"],
    }

    found = False
    for idx, existing_item in enumerate(items):
        if isinstance(existing_item, dict) and existing_item.get("account_id") == item["account_id"]:
            items[idx] = item
            found = True
            break
    if not found:
        items.append(item)

    items_sorted = sorted(
        [entry for entry in items if isinstance(entry, dict)],
        key=lambda row: str(row.get("account_id", "")),
    )
    _write_json(global_path, {"items": items_sorted})


def update_account_task(
    accounts_root: Path,
    account_id: str,
    tracker: Dict[str, Any],
    stage: str,
    memo_path: Path,
    agent_spec_path: Path,
    changes_path: Optional[Path] = None,
) -> Dict[str, Any]:
    account_root = accounts_root / account_id
    task_path = account_root / "task.json"
    task_payload = _load_json_or_default(task_path, _default_task_payload(account_id, tracker))

    if not isinstance(task_payload, dict):
        task_payload = _default_task_payload(account_id, tracker)
    task_payload["account_id"] = account_id
    task_payload["tracker"] = _merge_tracker(task_payload.get("tracker"), tracker)
    defaults = _default_task_payload(account_id, tracker)["stages"]
    if "stages" not in task_payload or not isinstance(task_payload["stages"], dict):
        task_payload["stages"] = defaults
    else:
        for stage_name, stage_defaults in defaults.items():
            existing_stage = task_payload["stages"].get(stage_name)
            if not isinstance(existing_stage, dict):
                task_payload["stages"][stage_name] = copy.deepcopy(stage_defaults)
                continue
            merged_stage = copy.deepcopy(stage_defaults)
            for key in stage_defaults.keys():
                if key in existing_stage:
                    merged_stage[key] = existing_stage[key]
            task_payload["stages"][stage_name] = merged_stage

    if stage == "pipeline_a_v1":
        task_payload["stages"]["pipeline_a_v1"] = {
            "status": "completed",
            "memo_path": _relative_to_repo(memo_path),
            "agent_spec_path": _relative_to_repo(agent_spec_path),
        }
    elif stage == "pipeline_b_v2":
        task_payload["stages"]["pipeline_b_v2"] = {
            "status": "completed",
            "memo_path": _relative_to_repo(memo_path),
            "agent_spec_path": _relative_to_repo(agent_spec_path),
            "changes_path": _relative_to_repo(changes_path) if changes_path else "",
        }
    else:
        raise ValueError(f"Unsupported task stage: {stage}")

    a_done = task_payload["stages"]["pipeline_a_v1"]["status"] == "completed"
    b_done = task_payload["stages"]["pipeline_b_v2"]["status"] == "completed"
    task_payload["status"] = "completed" if a_done and b_done else "in_progress"

    _write_json(task_path, task_payload)
    _update_global_tasks(accounts_root, task_payload, task_path)
    return task_payload


def _list_files(path: Path, suffixes: Tuple[str, ...]) -> List[Path]:
    if not path.exists():
        return []
    files = [item for item in path.iterdir() if item.is_file() and item.suffix.lower() in suffixes]
    return sorted(files, key=lambda item: item.name.lower())


def run_pipeline_a(
    demo_dir: Path,
    accounts_root: Path,
    force: bool = False,
) -> None:
    demo_files = _list_files(demo_dir, (".txt",))
    if not demo_files:
        LOGGER.warning("No demo transcript files found in %s", demo_dir)
        return

    accounts_root.mkdir(parents=True, exist_ok=True)
    for demo_file in demo_files:
        account_id = demo_file.stem
        account_root = accounts_root / account_id
        v1_dir = account_root / "v1"
        memo_path = v1_dir / "memo.json"
        agent_spec_path = v1_dir / "agent_spec.json"
        transcript = _read_text(demo_file)

        should_rebuild = force or not (memo_path.exists() and agent_spec_path.exists())
        if not should_rebuild:
            try:
                memo, memo_changed = _normalize_and_validate_memo_for_write(account_id, _read_json(memo_path))
                agent_spec, spec_changed = _normalize_and_validate_agent_spec_for_write(
                    _read_json(agent_spec_path), version="v1", account_id=account_id
                )
                if memo_changed:
                    _write_json(memo_path, memo)
                if spec_changed:
                    _write_json(agent_spec_path, agent_spec)
                if memo_changed or spec_changed:
                    LOGGER.info("Pipeline A repaired schema-invalid/legacy v1 outputs for %s", account_id)
                else:
                    LOGGER.info("Pipeline A skipped for %s (existing v1 schema valid)", account_id)
            except Exception as exc:
                LOGGER.warning(
                    "Pipeline A existing outputs invalid for %s, rebuilding from transcript: %s",
                    account_id,
                    exc,
                )
                should_rebuild = True

        if should_rebuild:
            memo = build_v1_memo(account_id, transcript)
            memo, _ = _normalize_and_validate_memo_for_write(account_id, memo)
            agent_spec = build_agent_spec(memo, version="v1")
            agent_spec, _ = _normalize_and_validate_agent_spec_for_write(
                agent_spec, version="v1", account_id=account_id
            )
            _write_json(memo_path, memo)
            _write_json(agent_spec_path, agent_spec)
            LOGGER.info("Pipeline A completed for %s", account_id)

        tracker = ensure_account_issue(account_id, str(memo.get("company_name", "")))
        update_account_task(
            accounts_root=accounts_root,
            account_id=account_id,
            tracker=tracker,
            stage="pipeline_a_v1",
            memo_path=memo_path,
            agent_spec_path=agent_spec_path,
        )


def _parse_onboarding_input(path: Path) -> Dict[str, Any]:
    if path.suffix.lower() == ".json":
        payload = _read_json(path)
        if not isinstance(payload, dict):
            raise ValueError(f"Onboarding form JSON must be an object: {path}")
        return {"type": "json", "payload": payload}
    return {"type": "text", "payload": _read_text(path)}


def run_pipeline_b(
    onboarding_dir: Path,
    accounts_root: Path,
    force: bool = False,
) -> None:
    onboarding_files = _list_files(onboarding_dir, (".txt", ".json"))
    if not onboarding_files:
        LOGGER.warning("No onboarding files found in %s", onboarding_dir)
        return

    accounts_root.mkdir(parents=True, exist_ok=True)
    for onboarding_file in onboarding_files:
        account_id = onboarding_file.stem
        account_root = accounts_root / account_id
        v1_memo_path = account_root / "v1" / "memo.json"
        v2_dir = account_root / "v2"
        v2_memo_path = v2_dir / "memo.json"
        v2_agent_spec_path = v2_dir / "agent_spec.json"
        changes_path = v2_dir / "changes.json"

        if not v1_memo_path.exists():
            LOGGER.warning("Pipeline B skipped for %s (missing v1 memo)", account_id)
            continue

        v1_memo, v1_changed = _normalize_and_validate_memo_for_write(account_id, _read_json(v1_memo_path))
        if v1_changed:
            _write_json(v1_memo_path, v1_memo)
            LOGGER.info("Pipeline B repaired schema-invalid/legacy v1 memo for %s", account_id)

        should_rebuild = force or not (
            v2_memo_path.exists() and v2_agent_spec_path.exists() and changes_path.exists()
        )

        if not should_rebuild:
            try:
                v2_memo, memo_changed = _normalize_and_validate_memo_for_write(account_id, _read_json(v2_memo_path))
                v2_agent_spec, spec_changed = _normalize_and_validate_agent_spec_for_write(
                    _read_json(v2_agent_spec_path), version="v2", account_id=account_id
                )
                changes_payload, changes_valid = _normalize_changes(_read_json(changes_path))
                if not changes_valid:
                    raise ValueError("Existing changes.json is schema-invalid.")
                if memo_changed:
                    _write_json(v2_memo_path, v2_memo)
                if spec_changed:
                    _write_json(v2_agent_spec_path, v2_agent_spec)
                if _json_fingerprint(_read_json(changes_path)) != _json_fingerprint(changes_payload):
                    _write_json(changes_path, changes_payload)
                if memo_changed or spec_changed:
                    LOGGER.info("Pipeline B repaired schema-invalid/legacy v2 outputs for %s", account_id)
                else:
                    LOGGER.info("Pipeline B skipped for %s (existing v2 schema valid)", account_id)
            except Exception as exc:
                LOGGER.warning(
                    "Pipeline B existing outputs invalid for %s, rebuilding from onboarding source: %s",
                    account_id,
                    exc,
                )
                should_rebuild = True

        if should_rebuild:
            onboarding_source = _parse_onboarding_input(onboarding_file)
            v2_memo, changes = apply_onboarding_patch(v1_memo, onboarding_source)
            v2_memo, _ = _normalize_and_validate_memo_for_write(account_id, v2_memo)
            v2_agent_spec = build_agent_spec(v2_memo, version="v2")
            v2_agent_spec, _ = _normalize_and_validate_agent_spec_for_write(
                v2_agent_spec, version="v2", account_id=account_id
            )
            changes, _ = _normalize_changes(changes)

            _write_json(v2_memo_path, v2_memo)
            _write_json(v2_agent_spec_path, v2_agent_spec)
            _write_json(changes_path, changes)
            LOGGER.info("Pipeline B completed for %s", account_id)

        tracker = ensure_account_issue(account_id, str(v2_memo.get("company_name", "")))
        update_account_task(
            accounts_root=accounts_root,
            account_id=account_id,
            tracker=tracker,
            stage="pipeline_b_v2",
            memo_path=v2_memo_path,
            agent_spec_path=v2_agent_spec_path,
            changes_path=changes_path,
        )


def run_all(
    demo_dir: Path,
    onboarding_dir: Path,
    accounts_root: Path,
    force: bool = False,
) -> None:
    run_pipeline_a(demo_dir=demo_dir, accounts_root=accounts_root, force=force)
    run_pipeline_b(onboarding_dir=onboarding_dir, accounts_root=accounts_root, force=force)


# Backward-compatible alias used in older files.
ensure_task_tracker = ensure_account_issue
