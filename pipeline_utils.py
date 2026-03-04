import copy
import json
import os
import re


BUSINESS_HOURS_KEYS = ["days", "start", "end", "timezone"]
TASK_TRACKER_PATH = os.path.join("outputs", "task_tracker", "items.json")


def _parse_time_token(token):
    token = token.strip().lower().replace(".", "")
    match = re.match(r"^(\d{1,2})(?::(\d{2}))?\s*(am|pm)?$", token)
    if not match:
        return token

    hour = int(match.group(1))
    minute = int(match.group(2) or "0")
    meridiem = match.group(3)

    if meridiem == "am":
        hour = 0 if hour == 12 else hour
    elif meridiem == "pm":
        hour = 12 if hour == 12 else hour + 12

    return f"{hour:02d}:{minute:02d}"


def _parse_business_hours(text):
    text = text or ""
    pattern = re.compile(
        r"(Monday\s*(?:to|-)\s*Friday)\s+(\d{1,2}(?::\d{2})?\s*(?:am|pm)?)\s*(?:to|-)\s*(\d{1,2}(?::\d{2})?\s*(?:am|pm)?)\s*([A-Za-z]{2,5})?",
        re.IGNORECASE,
    )
    match = pattern.search(text)
    if not match:
        return None

    timezone_hint = (match.group(4) or "").upper().strip()
    return {
        "days": "Monday-Friday",
        "start": _parse_time_token(match.group(2)),
        "end": _parse_time_token(match.group(3)),
        "timezone": timezone_hint or "EST",
    }


def _parse_company_name(text):
    text = text or ""
    patterns = [
        r"\bWe are\s+([^.]+)\.",
        r"\bOur company is\s+([^.]+)\.",
        r"\bThis is\s+([^.]+)\.",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(1).strip()
    return ""


def _parse_emergency_definition(text):
    lowered = (text or "").lower()
    items = []
    if "sprinkler" in lowered:
        items.append("sprinkler leak")
    if "fire alarm" in lowered:
        items.append("fire alarm triggered")
    if "no heat" in lowered:
        items.append("no heat")
    if "smoke" in lowered:
        items.append("smoke condition")
    return items


def _parse_transfer_timeout_seconds(text):
    match = re.search(r"within\s+(\d+)\s*seconds?", text or "", re.IGNORECASE)
    if not match:
        return None
    return int(match.group(1))


def _default_business_hours():
    return {"days": "", "start": "", "end": "", "timezone": ""}


def _default_transfer_rules():
    return {
        "emergency_transfer_timeout_seconds": "",
        "retry_attempts": 1,
        "failure_script": (
            "I'm sorry I could not connect you right now. "
            "I will alert dispatch immediately and ensure a quick callback."
        ),
    }


def _default_emergency_routing_rules():
    return {
        "who_to_call": ["dispatch"],
        "order": ["dispatch"],
        "fallback": "Collect emergency details and trigger urgent dispatch callback.",
    }


def _default_non_emergency_routing_rules():
    return {
        "office_hours": "Transfer to office queue after collecting caller name and number.",
        "after_hours": "Collect details and schedule callback during business hours.",
    }


def _normalize_business_hours(value):
    if isinstance(value, dict):
        return {key: str(value.get(key, "")).strip() for key in BUSINESS_HOURS_KEYS}

    if isinstance(value, str) and value.strip():
        parsed = _parse_business_hours(value)
        if parsed:
            return parsed

    return _default_business_hours()


def _normalize_transfer_rules(value):
    base = _default_transfer_rules()
    if isinstance(value, dict):
        merged = copy.deepcopy(base)
        merged.update(value)
        return merged
    return base


def normalize_memo_schema(memo):
    normalized = {
        "account_id": str(memo.get("account_id", "")),
        "company_name": str(memo.get("company_name", "")).strip(),
        "business_hours": _normalize_business_hours(memo.get("business_hours")),
        "office_address": str(memo.get("office_address", "")).strip(),
        "services_supported": memo.get("services_supported")
        if isinstance(memo.get("services_supported"), list)
        else [],
        "emergency_definition": memo.get("emergency_definition")
        if isinstance(memo.get("emergency_definition"), list)
        else [],
        "emergency_routing_rules": memo.get("emergency_routing_rules")
        if isinstance(memo.get("emergency_routing_rules"), dict)
        else _default_emergency_routing_rules(),
        "non_emergency_routing_rules": memo.get("non_emergency_routing_rules")
        if isinstance(memo.get("non_emergency_routing_rules"), dict)
        else _default_non_emergency_routing_rules(),
        "call_transfer_rules": _normalize_transfer_rules(memo.get("call_transfer_rules")),
        "integration_constraints": memo.get("integration_constraints")
        if isinstance(memo.get("integration_constraints"), list)
        else ([] if not memo.get("integration_constraints") else [str(memo.get("integration_constraints"))]),
        "after_hours_flow_summary": str(memo.get("after_hours_flow_summary", "")).strip(),
        "office_hours_flow_summary": str(memo.get("office_hours_flow_summary", "")).strip(),
        "questions_or_unknowns": memo.get("questions_or_unknowns")
        if isinstance(memo.get("questions_or_unknowns"), list)
        else [],
        "notes": str(memo.get("notes", "")).strip(),
    }
    return normalized


def build_memo_from_demo(account_id, transcript):
    transcript = transcript or ""
    memo = normalize_memo_schema(
        {
            "account_id": account_id,
            "company_name": _parse_company_name(transcript),
            "business_hours": _parse_business_hours(transcript) or _default_business_hours(),
            "office_address": "",
            "services_supported": [],
            "emergency_definition": _parse_emergency_definition(transcript),
            "emergency_routing_rules": _default_emergency_routing_rules(),
            "non_emergency_routing_rules": _default_non_emergency_routing_rules(),
            "call_transfer_rules": _default_transfer_rules(),
            "integration_constraints": [],
            "after_hours_flow_summary": (
                "After hours, confirm emergency, collect name/number/address for emergencies, "
                "attempt transfer, then fallback to urgent callback handling if transfer fails."
            ),
            "office_hours_flow_summary": (
                "During business hours, greet caller, capture purpose and contact details, "
                "transfer or route call, then confirm next steps and close."
            ),
            "questions_or_unknowns": [],
            "notes": "Generated from demo transcript.",
        }
    )

    if not memo["company_name"]:
        memo["questions_or_unknowns"].append("Company name missing from demo transcript.")

    if not any(memo["business_hours"].values()):
        memo["questions_or_unknowns"].append("Business hours missing from demo transcript.")

    timeout_seconds = _parse_transfer_timeout_seconds(transcript)
    if timeout_seconds is not None:
        memo["call_transfer_rules"]["emergency_transfer_timeout_seconds"] = timeout_seconds

    if re.search(r"after hours.*dispatch", transcript, re.IGNORECASE):
        memo["emergency_routing_rules"] = {
            "who_to_call": ["dispatch"],
            "order": ["dispatch"],
            "fallback": "If dispatch transfer fails, collect details and trigger urgent callback.",
        }

    return memo


def _format_business_hours_for_prompt(hours):
    return (
        f"{hours.get('days', '')}, {hours.get('start', '')}-{hours.get('end', '')} {hours.get('timezone', '')}"
        .strip()
        .strip(",")
    )


def build_system_prompt(memo):
    hours_display = _format_business_hours_for_prompt(memo["business_hours"])
    emergency_timeout = memo["call_transfer_rules"].get("emergency_transfer_timeout_seconds", "")
    timeout_line = (
        f"Emergency transfer timeout: {emergency_timeout} seconds."
        if emergency_timeout
        else "Emergency transfer timeout: use default operational timeout."
    )

    return f"""
You are Clara, the voice assistant for {memo['company_name'] or 'this company'}.
Never mention internal tools, function calls, or implementation details to callers.

BUSINESS HOURS:
{hours_display or 'Not confirmed yet'}

BUSINESS HOURS FLOW:
1. Greet caller.
2. Ask purpose briefly.
3. Collect caller name and callback number.
4. Route or transfer based on request.
5. If transfer fails, apologize and provide clear follow-up expectation.
6. Confirm next steps.
7. Ask if they need anything else.
8. Close the call professionally.

AFTER HOURS FLOW:
1. Greet caller.
2. Ask purpose.
3. Confirm whether issue is an emergency.
4. For emergency: collect name, number, and address immediately.
5. Attempt transfer to emergency contact path.
6. If transfer fails, apologize and assure urgent follow-up.
7. For non-emergency: collect details and confirm business-hours follow-up.
8. Ask if they need anything else.
9. Close the call.

CALL TRANSFER PROTOCOL:
{timeout_line}
- Retry based on configured retry attempts.
- If transfer repeatedly fails, trigger fallback callback workflow and communicate clearly.
""".strip()


def generate_agent_spec(memo, version):
    memo = normalize_memo_schema(memo)

    return {
        "agent_name": f"{memo['company_name'] or memo['account_id']} Voice Agent",
        "voice_style": "calm, professional, empathetic",
        "system_prompt": build_system_prompt(memo),
        "key_variables": {
            "timezone": memo["business_hours"].get("timezone", ""),
            "business_hours": memo["business_hours"],
            "office_address": memo["office_address"],
            "emergency_routing": memo["emergency_routing_rules"],
        },
        "tool_invocation_placeholders": [
            "transfer_call_to_dispatch",
            "log_callback_request",
            "notify_dispatch_on_transfer_failure",
        ],
        "call_transfer_protocol": memo["call_transfer_rules"],
        "fallback_protocol": {
            "on_transfer_failure": memo["call_transfer_rules"]["failure_script"],
            "dispatch_notification": "Notify dispatch with caller details and urgency.",
            "customer_confirmation": "Confirm quick follow-up and close politely.",
        },
        "version": version,
        "business_hours": memo["business_hours"],
        "emergency_definition": memo["emergency_definition"],
    }


def apply_onboarding_updates(existing_memo, onboarding_text):
    memo = normalize_memo_schema(existing_memo)
    updates = []

    hours = _parse_business_hours(onboarding_text)
    if hours and hours != memo["business_hours"]:
        updates.append(
            {
                "field": "business_hours",
                "old_value": memo["business_hours"],
                "new_value": hours,
                "source": "onboarding transcript",
                "reason": "Updated hours confirmed during onboarding.",
            }
        )
        memo["business_hours"] = hours

    timeout_seconds = _parse_transfer_timeout_seconds(onboarding_text)
    if timeout_seconds is not None:
        old_rules = copy.deepcopy(memo["call_transfer_rules"])
        new_rules = copy.deepcopy(old_rules)
        new_rules["emergency_transfer_timeout_seconds"] = timeout_seconds

        if new_rules != old_rules:
            updates.append(
                {
                    "field": "call_transfer_rules",
                    "old_value": old_rules,
                    "new_value": new_rules,
                    "source": "onboarding transcript",
                    "reason": "Emergency transfer timeout clarified in onboarding.",
                }
            )
            memo["call_transfer_rules"] = new_rules

    memo["notes"] = "Updated using onboarding transcript."
    return memo, updates


def build_change_log(version, changes):
    return {"version": version, "changes": changes}


def _load_json_or_default(path, default_payload):
    if not os.path.exists(path):
        return copy.deepcopy(default_payload)
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def update_task_tracker(account_id, stage, status, artifacts, summary):
    tracker_payload = _load_json_or_default(TASK_TRACKER_PATH, {"items": []})
    items = tracker_payload.get("items", [])
    task_id = f"{account_id}:{stage}"

    new_item = {
        "task_id": task_id,
        "account_id": account_id,
        "stage": stage,
        "status": status,
        "artifacts": artifacts,
        "summary": summary,
    }

    replaced = False
    for index, item in enumerate(items):
        if item.get("task_id") == task_id:
            items[index] = new_item
            replaced = True
            break

    if not replaced:
        items.append(new_item)

    tracker_payload["items"] = sorted(items, key=lambda item: item.get("task_id", ""))
    write_json(TASK_TRACKER_PATH, tracker_payload)


def write_json(path, payload):
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=4)
