import copy
import json
import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests


LOGGER = logging.getLogger("clara_pipeline")

EMPTY_BUSINESS_HOURS = {"days": "", "start": "", "end": "", "timezone": ""}
EMPTY_EMERGENCY_ROUTING = {"who_to_call": [], "order": [], "fallback": ""}
EMPTY_NON_EMERGENCY_ROUTING = {"office_hours": "", "after_hours": ""}
EMPTY_CALL_TRANSFER_RULES = {
    "timeout_seconds": None,
    "retries": None,
    "what_to_say_if_transfer_fails": "",
}


@dataclass
class ExtractResult:
    value: Any
    evidence: str
    conflict: Optional[str] = None


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
    path.write_text(json.dumps(payload, indent=4, ensure_ascii=True), encoding="utf-8")


def _dedupe_keep_order(values: List[str]) -> List[str]:
    seen = set()
    result = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


def _sentence_snippet(text: str, span: Tuple[int, int], context: int = 55) -> str:
    start = max(0, span[0] - context)
    end = min(len(text), span[1] + context)
    snippet = re.sub(r"\s+", " ", text[start:end]).strip()
    return snippet


def _normalize_time_24h(token: str) -> str:
    normalized = token.strip().lower().replace(".", "")
    match = re.match(r"^(\d{1,2})(?::(\d{2}))?\s*(am|pm)$", normalized)
    if not match:
        return normalized
    hour = int(match.group(1))
    minute = int(match.group(2) or "0")
    meridiem = match.group(3)
    if meridiem == "am":
        hour = 0 if hour == 12 else hour
    else:
        hour = 12 if hour == 12 else hour + 12
    return f"{hour:02d}:{minute:02d}"


def _normalize_days(days_fragment: str) -> str:
    compact = re.sub(r"\s+", " ", days_fragment.strip().lower())
    if "mon" in compact and "fri" in compact:
        return "Monday-Friday"
    return days_fragment.strip()


def _company_name(text: str) -> str:
    patterns = [
        r"\bWe are\s+([^\n.]+)",
        r"\bCompany\s*:\s*([^\n.]+)",
        r"\bCompany name\s*:\s*([^\n.]+)",
        r"\bOur company is\s+([^\n.]+)",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            return match.group(1).strip()
    return ""


def _business_hours_candidates(text: str) -> List[Tuple[Dict[str, str], str]]:
    pattern = re.compile(
        r"(?P<days>monday\s*(?:to|-)\s*friday|mon(?:day)?\s*(?:to|-)\s*fri(?:day)?)"
        r"\s*(?:from\s*)?"
        r"(?P<start>\d{1,2}(?::\d{2})?\s*(?:am|pm))"
        r"\s*(?:to|-)\s*"
        r"(?P<end>\d{1,2}(?::\d{2})?\s*(?:am|pm))"
        r"(?:\s*(?P<tz>[A-Z]{2,5}))?",
        flags=re.IGNORECASE,
    )
    candidates: List[Tuple[Dict[str, str], str]] = []
    for match in pattern.finditer(text):
        hours = {
            "days": _normalize_days(match.group("days")),
            "start": _normalize_time_24h(match.group("start")),
            "end": _normalize_time_24h(match.group("end")),
            "timezone": (match.group("tz") or "").upper().strip(),
        }
        candidates.append((hours, _sentence_snippet(text, match.span())))
    return candidates


def _extract_business_hours(text: str) -> ExtractResult:
    candidates = _business_hours_candidates(text)
    if not candidates:
        return ExtractResult(value=None, evidence="")

    unique_map: Dict[str, Tuple[Dict[str, str], str]] = {}
    for candidate, evidence in candidates:
        key = json.dumps(candidate, sort_keys=True)
        if key not in unique_map:
            unique_map[key] = (candidate, evidence)

    if len(unique_map) > 1:
        evidence = " | ".join([item[1] for item in unique_map.values()])
        return ExtractResult(
            value=None,
            evidence=evidence,
            conflict="Multiple business-hours values found in onboarding text.",
        )

    only = list(unique_map.values())[0]
    value = only[0]
    if not value.get("timezone"):
        value["timezone"] = "EST"
    return ExtractResult(value=value, evidence=only[1])


def _split_clause_items(clause: str) -> List[str]:
    normalized = clause.replace("/", ",")
    normalized = re.sub(r"\s+or\s+", ",", normalized, flags=re.IGNORECASE)
    normalized = re.sub(r"\s+and\s+", ",", normalized, flags=re.IGNORECASE)
    return [part.strip(" .").lower() for part in normalized.split(",") if part.strip(" .")]


def _extract_emergency_definition(text: str) -> ExtractResult:
    explicit = re.search(r"\bemergency\s+means\s+([^\n.]+)", text, flags=re.IGNORECASE)
    if explicit:
        items = _split_clause_items(explicit.group(1))
        return ExtractResult(value=items, evidence=_sentence_snippet(text, explicit.span()))

    return ExtractResult(value=[], evidence="")


def _extract_emergency_routing(text: str) -> ExtractResult:
    patterns = [
        r"after\s+hours[^.\n]*emergency[^.\n]*transfer[^.\n]*to\s+([a-zA-Z ]+)",
        r"emergency[^.\n]*go(?:es)?\s+(?:directly\s+)?to\s+([a-zA-Z ]+)",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            target = match.group(1).strip(" .").lower()
            target = re.sub(r"^the\s+", "", target)
            return ExtractResult(
                value={
                    "who_to_call": [target],
                    "order": [target],
                    "fallback": "",
                },
                evidence=_sentence_snippet(text, match.span()),
            )
    return ExtractResult(value=copy.deepcopy(EMPTY_EMERGENCY_ROUTING), evidence="")


def _extract_transfer_timeout(text: str) -> ExtractResult:
    patterns = [
        re.compile(r"transfer[^.\n]{0,60}?within\s+(\d+)\s*seconds?", flags=re.IGNORECASE),
        re.compile(r"within\s+(\d+)\s*seconds?[^.\n]{0,60}?transfer", flags=re.IGNORECASE),
    ]
    found: List[Tuple[int, str]] = []
    seen_spans = set()
    for pattern in patterns:
        for match in pattern.finditer(text):
            span = match.span()
            if span in seen_spans:
                continue
            seen_spans.add(span)
            value = int(match.group(1))
            found.append((value, _sentence_snippet(text, span)))

    if not found:
        return ExtractResult(value=None, evidence="")

    unique_values = sorted({item[0] for item in found})
    if len(unique_values) > 1:
        evidence = " | ".join([item[1] for item in found])
        return ExtractResult(
            value=None,
            evidence=evidence,
            conflict="Multiple transfer timeout values found in onboarding text.",
        )

    return ExtractResult(value=found[0][0], evidence=found[0][1])


def _extract_retries(text: str) -> ExtractResult:
    patterns = [
        r"retry(?:ing)?\s+(?:up to\s+)?(\d+)\s+times?",
        r"(\d+)\s+retries",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            return ExtractResult(value=int(match.group(1)), evidence=_sentence_snippet(text, match.span()))
    return ExtractResult(value=None, evidence="")


def _extract_transfer_fail_message(text: str) -> ExtractResult:
    match = re.search(r"(if transfer fails[^.]*\.)", text, flags=re.IGNORECASE)
    if match:
        sentence = re.sub(r"\s+", " ", match.group(1)).strip()
        return ExtractResult(value=sentence, evidence=_sentence_snippet(text, match.span()))
    return ExtractResult(value="", evidence="")


def _extract_office_address(text: str) -> ExtractResult:
    patterns = [
        r"office address\s*(?:is|:)\s*([^\n.]+)",
        r"address\s*(?:is|:)\s*([^\n.]+)",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            return ExtractResult(value=match.group(1).strip(), evidence=_sentence_snippet(text, match.span()))
    return ExtractResult(value="", evidence="")


def _extract_services_supported(text: str) -> ExtractResult:
    match = re.search(
        r"(?:services supported|we support|services include)\s*(?:are|:)?\s*([^\n.]+)",
        text,
        flags=re.IGNORECASE,
    )
    if not match:
        return ExtractResult(value=[], evidence="")
    items = [part.strip(" .").lower() for part in re.split(r",|\bor\b|\band\b", match.group(1)) if part.strip(" .")]
    return ExtractResult(value=items, evidence=_sentence_snippet(text, match.span()))


def _extract_non_emergency_routing(text: str) -> ExtractResult:
    office_match = re.search(
        r"non[- ]emergency[^.\n]*during (?:business|office) hours[^.\n]*",
        text,
        flags=re.IGNORECASE,
    )
    after_match = re.search(
        r"non[- ]emergency[^.\n]*after[- ]hours[^.\n]*",
        text,
        flags=re.IGNORECASE,
    )
    value = copy.deepcopy(EMPTY_NON_EMERGENCY_ROUTING)
    evidence_parts: List[str] = []
    if office_match:
        value["office_hours"] = re.sub(r"\s+", " ", office_match.group(0)).strip(" .")
        evidence_parts.append(_sentence_snippet(text, office_match.span()))
    if after_match:
        value["after_hours"] = re.sub(r"\s+", " ", after_match.group(0)).strip(" .")
        evidence_parts.append(_sentence_snippet(text, after_match.span()))
    return ExtractResult(value=value, evidence=" | ".join(evidence_parts))


def _extract_integration_constraints(text: str) -> ExtractResult:
    constraints: List[str] = []
    evidence_parts: List[str] = []
    for match in re.finditer(r"(never[^.\n]*\.)", text, flags=re.IGNORECASE):
        sentence = re.sub(r"\s+", " ", match.group(1)).strip()
        constraints.append(sentence)
        evidence_parts.append(_sentence_snippet(text, match.span()))
    for match in re.finditer(r"(do not[^.\n]*\.)", text, flags=re.IGNORECASE):
        sentence = re.sub(r"\s+", " ", match.group(1)).strip()
        constraints.append(sentence)
        evidence_parts.append(_sentence_snippet(text, match.span()))
    return ExtractResult(value=_dedupe_keep_order(constraints), evidence=" | ".join(evidence_parts))


def _build_empty_memo(account_id: str) -> Dict[str, Any]:
    return {
        "account_id": account_id,
        "company_name": "",
        "business_hours": copy.deepcopy(EMPTY_BUSINESS_HOURS),
        "office_address": "",
        "services_supported": [],
        "emergency_definition": [],
        "emergency_routing_rules": copy.deepcopy(EMPTY_EMERGENCY_ROUTING),
        "non_emergency_routing_rules": copy.deepcopy(EMPTY_NON_EMERGENCY_ROUTING),
        "call_transfer_rules": copy.deepcopy(EMPTY_CALL_TRANSFER_RULES),
        "integration_constraints": [],
        "after_hours_flow_summary": "",
        "office_hours_flow_summary": "",
        "questions_or_unknowns": [],
        "notes": "",
    }


def _refresh_questions(memo: Dict[str, Any], extra: Optional[List[str]] = None) -> None:
    questions: List[str] = []
    if not memo["company_name"]:
        questions.append("What is the confirmed company name?")
    if not memo["business_hours"]["days"] or not memo["business_hours"]["start"] or not memo["business_hours"]["end"]:
        questions.append("What are the exact business hours (days, start, end, timezone)?")
    if not memo["office_address"]:
        questions.append("What is the office address?")
    if not memo["services_supported"]:
        questions.append("Which services are supported by the voice agent?")
    if not memo["emergency_definition"]:
        questions.append("What should be treated as an emergency?")
    if not memo["emergency_routing_rules"]["who_to_call"]:
        questions.append("Who should emergency calls be routed to, and in what order?")
    if not memo["non_emergency_routing_rules"]["office_hours"] and not memo["non_emergency_routing_rules"]["after_hours"]:
        questions.append("What are the non-emergency routing rules during and after office hours?")
    if memo["call_transfer_rules"]["timeout_seconds"] is None:
        questions.append("What transfer timeout (seconds) should be used?")
    if memo["call_transfer_rules"]["retries"] is None:
        questions.append("How many transfer retries should be attempted?")
    if not memo["call_transfer_rules"]["what_to_say_if_transfer_fails"]:
        questions.append("What should Clara say when transfer fails?")
    if not memo["integration_constraints"]:
        questions.append("Are there integration constraints (for example, systems Clara must avoid updating)?")
    if extra:
        questions.extend(extra)
    memo["questions_or_unknowns"] = _dedupe_keep_order(questions)


def _rebuild_flow_summaries(memo: Dict[str, Any]) -> None:
    if memo["emergency_routing_rules"]["who_to_call"]:
        targets = ", ".join(memo["emergency_routing_rules"]["who_to_call"])
        memo["after_hours_flow_summary"] = (
            f"After-hours emergency calls should transfer to {targets}. "
            "If transfer fails, fallback behavior must follow configured protocol."
        )
    else:
        memo["after_hours_flow_summary"] = (
            "After-hours emergency routing was not explicitly stated in transcript."
        )

    if memo["business_hours"]["days"] and memo["business_hours"]["start"] and memo["business_hours"]["end"]:
        memo["office_hours_flow_summary"] = (
            f"Office hours identified as {memo['business_hours']['days']} "
            f"{memo['business_hours']['start']}-{memo['business_hours']['end']} "
            f"{memo['business_hours']['timezone']}. Detailed office-hours routing steps were not explicitly stated."
        )
    else:
        memo["office_hours_flow_summary"] = (
            "Office-hours flow details were not explicitly stated in transcript."
        )


def build_v1_memo(account_id: str, demo_text: str) -> Dict[str, Any]:
    memo = _build_empty_memo(account_id)

    memo["company_name"] = _company_name(demo_text)

    hours_result = _extract_business_hours(demo_text)
    if hours_result.value:
        memo["business_hours"] = hours_result.value

    memo["office_address"] = _extract_office_address(demo_text).value
    memo["services_supported"] = _extract_services_supported(demo_text).value
    memo["emergency_definition"] = _extract_emergency_definition(demo_text).value
    memo["emergency_routing_rules"] = _extract_emergency_routing(demo_text).value
    memo["non_emergency_routing_rules"] = _extract_non_emergency_routing(demo_text).value

    timeout_result = _extract_transfer_timeout(demo_text)
    retries_result = _extract_retries(demo_text)
    fail_message_result = _extract_transfer_fail_message(demo_text)

    memo["call_transfer_rules"]["timeout_seconds"] = timeout_result.value
    memo["call_transfer_rules"]["retries"] = retries_result.value
    memo["call_transfer_rules"]["what_to_say_if_transfer_fails"] = fail_message_result.value

    memo["integration_constraints"] = _extract_integration_constraints(demo_text).value
    memo["notes"] = "Generated from demo transcript using rule-based extraction."

    _rebuild_flow_summaries(memo)
    _refresh_questions(memo)
    return memo


def _prompt_business_hours(memo: Dict[str, Any]) -> str:
    hours = memo["business_hours"]
    if hours["days"] and hours["start"] and hours["end"]:
        return f"{hours['days']} {hours['start']}-{hours['end']} {hours['timezone']}".strip()
    return "Not confirmed"


def build_agent_spec(memo: Dict[str, Any], version: str) -> Dict[str, Any]:
    timeout = memo["call_transfer_rules"]["timeout_seconds"]
    timeout_placeholder = timeout if timeout is not None else "UNSET"
    fallback_line = memo["call_transfer_rules"]["what_to_say_if_transfer_fails"] or (
        "I am sorry I could not connect you right now. I will notify dispatch and arrange a quick follow-up."
    )
    retries = memo["call_transfer_rules"]["retries"]

    system_prompt = (
        f"You are Clara, the voice assistant for {memo['company_name'] or 'this company'}.\n"
        "Do not mention internal tools, APIs, or function calls to callers.\n"
        "Collect only information needed for routing and dispatch.\n\n"
        f"BUSINESS HOURS: {_prompt_business_hours(memo)}\n\n"
        "BUSINESS-HOURS FLOW:\n"
        "1. Greet caller.\n"
        "2. Ask purpose.\n"
        "3. Collect caller name and number.\n"
        "4. Route or transfer appropriately.\n"
        "5. If transfer fails, apologize and confirm next step.\n"
        "6. Ask if anything else is needed.\n"
        "7. Close call.\n\n"
        "AFTER-HOURS FLOW:\n"
        "1. Greet caller.\n"
        "2. Ask purpose.\n"
        "3. Confirm if issue is emergency.\n"
        "4. If emergency, collect name, number, and address immediately.\n"
        "5. Attempt transfer based on emergency routing.\n"
        "6. If transfer fails, apologize and assure quick follow-up.\n"
        "7. If non-emergency, collect essential details and confirm business-hours follow-up.\n"
        "8. Ask if anything else is needed.\n"
        "9. Close call."
    )

    return {
        "agent_name": f"{memo['company_name'] or memo['account_id']} Voice Agent",
        "voice_style": "professional, calm, concise",
        "system_prompt": system_prompt,
        "key_variables": {
            "timezone": memo["business_hours"]["timezone"],
            "business_hours": memo["business_hours"],
            "address": memo["office_address"],
            "emergency_routing": memo["emergency_routing_rules"],
        },
        "tool_invocation_placeholders": [
            f'[[TOOL:TRANSFER_CALL target="dispatch" timeout={timeout_placeholder}]]',
            '[[TOOL:LOG_CALLER_DETAILS required="name,phone,purpose"]]',
            '[[TOOL:NOTIFY_DISPATCH reason="transfer_failed"]]',
        ],
        "call_transfer_protocol": {
            "timeout_seconds": timeout,
            "retries": retries,
            "steps": [
                "Attempt transfer to primary target.",
                "If no connection, retry according to retries setting.",
                "If still unsuccessful, trigger fallback protocol.",
            ],
        },
        "fallback_protocol_if_transfer_fails": {
            "caller_message": fallback_line,
            "steps": [
                "Apologize and acknowledge urgency.",
                "Capture/confirm callback details.",
                "Notify dispatch or fallback contact path.",
                "Confirm expected follow-up window.",
            ],
        },
        "version": version,
    }


def _record_change(
    changes: List[Dict[str, Any]],
    field_path: str,
    old_value: Any,
    new_value: Any,
    reason: str,
    evidence_snippet: str,
) -> None:
    changes.append(
        {
            "field_path": field_path,
            "old_value": old_value,
            "new_value": new_value,
            "reason": reason,
            "evidence_snippet": evidence_snippet,
        }
    )


def apply_onboarding_patch(v1_memo: Dict[str, Any], onboarding_text: str) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    memo = copy.deepcopy(v1_memo)
    changes: List[Dict[str, Any]] = []
    conflict_questions: List[str] = []

    hours_result = _extract_business_hours(onboarding_text)
    if hours_result.conflict:
        _record_change(
            changes,
            "business_hours",
            memo["business_hours"],
            memo["business_hours"],
            f"Conflict: {hours_result.conflict} Kept v1 value.",
            hours_result.evidence,
        )
        conflict_questions.append(
            "Onboarding includes conflicting business-hours values. Please confirm final hours."
        )
    elif hours_result.value and hours_result.value != memo["business_hours"]:
        old_value = copy.deepcopy(memo["business_hours"])
        memo["business_hours"] = hours_result.value
        _record_change(
            changes,
            "business_hours",
            old_value,
            memo["business_hours"],
            "Onboarding transcript explicitly updated business hours.",
            hours_result.evidence,
        )

    timeout_result = _extract_transfer_timeout(onboarding_text)
    if timeout_result.conflict:
        _record_change(
            changes,
            "call_transfer_rules.timeout_seconds",
            memo["call_transfer_rules"]["timeout_seconds"],
            memo["call_transfer_rules"]["timeout_seconds"],
            f"Conflict: {timeout_result.conflict} Kept v1 value.",
            timeout_result.evidence,
        )
        conflict_questions.append(
            "Onboarding includes conflicting transfer timeout values. Please confirm timeout_seconds."
        )
    elif (
        timeout_result.value is not None
        and timeout_result.value != memo["call_transfer_rules"]["timeout_seconds"]
    ):
        old_timeout = memo["call_transfer_rules"]["timeout_seconds"]
        memo["call_transfer_rules"]["timeout_seconds"] = timeout_result.value
        _record_change(
            changes,
            "call_transfer_rules.timeout_seconds",
            old_timeout,
            timeout_result.value,
            "Onboarding transcript explicitly updated transfer timeout.",
            timeout_result.evidence,
        )

    retries_result = _extract_retries(onboarding_text)
    if retries_result.value is not None and retries_result.value != memo["call_transfer_rules"]["retries"]:
        old_retries = memo["call_transfer_rules"]["retries"]
        memo["call_transfer_rules"]["retries"] = retries_result.value
        _record_change(
            changes,
            "call_transfer_rules.retries",
            old_retries,
            retries_result.value,
            "Onboarding transcript explicitly updated transfer retries.",
            retries_result.evidence,
        )

    fail_message_result = _extract_transfer_fail_message(onboarding_text)
    if (
        fail_message_result.value
        and fail_message_result.value != memo["call_transfer_rules"]["what_to_say_if_transfer_fails"]
    ):
        old_message = memo["call_transfer_rules"]["what_to_say_if_transfer_fails"]
        memo["call_transfer_rules"]["what_to_say_if_transfer_fails"] = fail_message_result.value
        _record_change(
            changes,
            "call_transfer_rules.what_to_say_if_transfer_fails",
            old_message,
            fail_message_result.value,
            "Onboarding transcript explicitly updated transfer-failure message.",
            fail_message_result.evidence,
        )

    emergency_routing_result = _extract_emergency_routing(onboarding_text)
    if (
        emergency_routing_result.value["who_to_call"]
        and emergency_routing_result.value != memo["emergency_routing_rules"]
    ):
        old_route = copy.deepcopy(memo["emergency_routing_rules"])
        new_route = copy.deepcopy(emergency_routing_result.value)
        if not new_route.get("fallback"):
            new_route["fallback"] = old_route.get("fallback", "")
        memo["emergency_routing_rules"] = new_route
        _record_change(
            changes,
            "emergency_routing_rules",
            old_route,
            memo["emergency_routing_rules"],
            "Onboarding transcript explicitly updated emergency routing target.",
            emergency_routing_result.evidence,
        )

    integration_result = _extract_integration_constraints(onboarding_text)
    if integration_result.value:
        merged_constraints = _dedupe_keep_order(
            memo["integration_constraints"] + integration_result.value
        )
        if merged_constraints != memo["integration_constraints"]:
            old_constraints = copy.deepcopy(memo["integration_constraints"])
            memo["integration_constraints"] = merged_constraints
            _record_change(
                changes,
                "integration_constraints",
                old_constraints,
                merged_constraints,
                "Onboarding transcript introduced integration constraints.",
                integration_result.evidence,
            )

    non_emergency_result = _extract_non_emergency_routing(onboarding_text)
    if (
        non_emergency_result.value != EMPTY_NON_EMERGENCY_ROUTING
        and non_emergency_result.value != memo["non_emergency_routing_rules"]
    ):
        old_non_emergency = copy.deepcopy(memo["non_emergency_routing_rules"])
        merged_non_emergency = copy.deepcopy(old_non_emergency)
        for key in ["office_hours", "after_hours"]:
            if non_emergency_result.value.get(key):
                merged_non_emergency[key] = non_emergency_result.value[key]
        memo["non_emergency_routing_rules"] = merged_non_emergency
        _record_change(
            changes,
            "non_emergency_routing_rules",
            old_non_emergency,
            merged_non_emergency,
            "Onboarding transcript explicitly updated non-emergency routing.",
            non_emergency_result.evidence,
        )

    memo["notes"] = "v2 generated from onboarding transcript patch."
    _rebuild_flow_summaries(memo)
    _refresh_questions(memo, conflict_questions)
    return memo, changes


def _github_headers(token: str) -> Dict[str, str]:
    return {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def _find_existing_issue(repo: str, token: str, title: str) -> Optional[Dict[str, Any]]:
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
    token = os.getenv("GITHUB_TOKEN", "").strip()
    repo = os.getenv("GITHUB_REPO", "").strip()
    title = f"[Clara Automation] {account_id} task tracker"

    if not token or not repo:
        return {
            "provider": "github_issues",
            "repository": repo,
            "mocked": True,
            "title": title,
            "issue_number": None,
            "issue_url": "",
            "note": "GITHUB_TOKEN or GITHUB_REPO missing; task tracking mocked locally.",
        }

    try:
        issue = _find_existing_issue(repo, token, title)
        if issue is None:
            body = (
                f"Account ID: {account_id}\n"
                f"Company: {company_name or 'unknown'}\n\n"
                "This issue tracks Clara automation artifacts (v1/v2 memo + agent spec + changelog)."
            )
            issue = _create_issue(repo, token, title, body)
            action = "created"
        else:
            action = "reused"

        return {
            "provider": "github_issues",
            "repository": repo,
            "mocked": False,
            "title": title,
            "issue_number": issue.get("number"),
            "issue_url": issue.get("html_url", ""),
            "note": f"GitHub issue {action}.",
        }
    except Exception as exc:  # pragma: no cover - network/auth dependent
        LOGGER.warning("GitHub issue integration failed for %s: %s", account_id, exc)
        return {
            "provider": "github_issues",
            "repository": repo,
            "mocked": True,
            "title": title,
            "issue_number": None,
            "issue_url": "",
            "note": f"GitHub integration failed; mocked locally. Error: {exc}",
        }


def _update_global_task_tracker(accounts_root: Path) -> None:
    tracker_path = accounts_root.parent / "task_tracker" / "items.json"
    items: List[Dict[str, Any]] = []
    for account_dir in sorted([path for path in accounts_root.iterdir() if path.is_dir()]):
        task_path = account_dir / "task.json"
        if not task_path.exists():
            continue
        items.append(_read_json(task_path))
    _write_json(tracker_path, {"items": items})


def update_account_task(
    account_id: str,
    tracker_info: Dict[str, Any],
    stage_key: str,
    artifacts: List[str],
) -> None:
    account_task_path = Path("outputs") / "accounts" / account_id / "task.json"
    if account_task_path.exists():
        existing_payload = _read_json(account_task_path)
        payload = {
            "account_id": account_id,
            "tracker": existing_payload.get("tracker", tracker_info),
            "stages": existing_payload.get(
                "stages",
                {
                    "pipeline_a_v1": {"status": "pending", "artifacts": []},
                    "pipeline_b_v2": {"status": "pending", "artifacts": []},
                },
            ),
        }
    else:
        payload = {
            "account_id": account_id,
            "tracker": tracker_info,
            "stages": {
                "pipeline_a_v1": {"status": "pending", "artifacts": []},
                "pipeline_b_v2": {"status": "pending", "artifacts": []},
            },
        }

    payload["tracker"] = tracker_info
    stages = payload.setdefault("stages", {})
    stage_payload = stages.setdefault(stage_key, {"status": "pending", "artifacts": []})
    stage_payload["status"] = "completed"
    stage_payload["artifacts"] = sorted(artifacts)

    _write_json(account_task_path, payload)
    _update_global_task_tracker(Path("outputs") / "accounts")


def run_pipeline_a(demo_dir: Path, accounts_root: Path) -> None:
    LOGGER.info("Running Pipeline A on demo transcripts from %s", demo_dir)
    for transcript_path in sorted(demo_dir.glob("*.txt")):
        account_id = transcript_path.stem
        demo_text = _read_text(transcript_path)
        memo = build_v1_memo(account_id, demo_text)
        spec = build_agent_spec(memo, version="v1")

        v1_dir = accounts_root / account_id / "v1"
        memo_path = v1_dir / "memo.json"
        spec_path = v1_dir / "agent_spec.json"
        _write_json(memo_path, memo)
        _write_json(spec_path, spec)

        tracker_info = ensure_account_issue(account_id, memo.get("company_name", ""))
        update_account_task(
            account_id=account_id,
            tracker_info=tracker_info,
            stage_key="pipeline_a_v1",
            artifacts=[str(memo_path), str(spec_path)],
        )
        LOGGER.info("v1 generated for %s", account_id)


def run_pipeline_b(onboarding_dir: Path, accounts_root: Path) -> None:
    LOGGER.info("Running Pipeline B on onboarding transcripts from %s", onboarding_dir)
    for transcript_path in sorted(onboarding_dir.glob("*.txt")):
        account_id = transcript_path.stem
        v1_memo_path = accounts_root / account_id / "v1" / "memo.json"
        if not v1_memo_path.exists():
            LOGGER.warning("Skipping %s: missing v1 memo at %s", account_id, v1_memo_path)
            continue

        onboarding_text = _read_text(transcript_path)
        v1_memo = _read_json(v1_memo_path)
        v2_memo, changes = apply_onboarding_patch(v1_memo, onboarding_text)
        v2_spec = build_agent_spec(v2_memo, version="v2")

        v2_dir = accounts_root / account_id / "v2"
        v2_memo_path = v2_dir / "memo.json"
        v2_spec_path = v2_dir / "agent_spec.json"
        changes_path = accounts_root / account_id / "changes.json"

        _write_json(v2_memo_path, v2_memo)
        _write_json(v2_spec_path, v2_spec)
        _write_json(changes_path, changes)

        task_path = accounts_root / account_id / "task.json"
        existing_task = _read_json(task_path) if task_path.exists() else {}
        tracker_info = existing_task.get("tracker") or ensure_account_issue(
            account_id, v2_memo.get("company_name", "")
        )
        update_account_task(
            account_id=account_id,
            tracker_info=tracker_info,
            stage_key="pipeline_b_v2",
            artifacts=[str(v2_memo_path), str(v2_spec_path), str(changes_path)],
        )
        LOGGER.info("v2 generated for %s with %d changes", account_id, len(changes))


def run_all(demo_dir: Path, onboarding_dir: Path, accounts_root: Path) -> None:
    run_pipeline_a(demo_dir=demo_dir, accounts_root=accounts_root)
    run_pipeline_b(onboarding_dir=onboarding_dir, accounts_root=accounts_root)
