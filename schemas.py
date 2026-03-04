import copy
from typing import Any, Dict, List, Tuple

DAYS_ORDER = [
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
    "Sunday",
]


def empty_business_hours() -> Dict[str, Any]:
    return {
        "days": [],
        "start": "",
        "end": "",
        "timezone": "",
    }


def empty_contact() -> Dict[str, Any]:
    return {
        "name": "",
        "role": "",
        "phone": "",
        "priority_order": None,
    }


def empty_emergency_routing() -> Dict[str, Any]:
    return {
        "contacts": [],
        "fallback": "",
        "notes": "",
    }


def empty_non_emergency_routing() -> Dict[str, Any]:
    return {
        "contacts": [],
        "notes": "",
    }


def empty_call_transfer_rules() -> Dict[str, Any]:
    return {
        "timeout_seconds": None,
        "retries": None,
        "fail_message": "",
        "routing_notes": "",
    }


def empty_memo(account_id: str = "") -> Dict[str, Any]:
    return {
        "account_id": account_id,
        "company_name": "",
        "business_hours": empty_business_hours(),
        "office_address": "",
        "services_supported": [],
        "emergency_definition": [],
        "emergency_routing_rules": empty_emergency_routing(),
        "non_emergency_routing_rules": empty_non_emergency_routing(),
        "call_transfer_rules": empty_call_transfer_rules(),
        "integration_constraints": [],
        "after_hours_flow_summary": "",
        "office_hours_flow_summary": "",
        "questions_or_unknowns": [],
        "notes": "",
    }


def empty_agent_spec(version: str = "v1") -> Dict[str, Any]:
    return {
        "agent_name": "",
        "voice_style": "professional, calm, concise",
        "system_prompt": "",
        "key_variables": {
            "timezone": "",
            "business_hours": empty_business_hours(),
            "address": "",
            "emergency_routing": empty_emergency_routing(),
        },
        "tool_invocation_placeholders": {
            "create_ticket": "",
            "notify_dispatch": "",
            "transfer_call": "",
        },
        "call_transfer_protocol": {
            "steps": [],
            "timeout_seconds": None,
            "retries": None,
        },
        "fallback_protocol_if_transfer_fails": {
            "exact_caller_message": "",
            "steps": [],
        },
        "version": version,
    }


def _is_contact_list(value: Any) -> bool:
    if not isinstance(value, list):
        return False
    for item in value:
        if not isinstance(item, dict):
            return False
        for key in ["name", "role", "phone", "priority_order"]:
            if key not in item:
                return False
    return True


def validate_memo_schema(memo: Dict[str, Any]) -> Tuple[bool, List[str]]:
    errors: List[str] = []
    expected = empty_memo(account_id=str(memo.get("account_id", "")))

    for key in expected.keys():
        if key not in memo:
            errors.append(f"Missing memo field: {key}")

    if not isinstance(memo.get("account_id", ""), str):
        errors.append("account_id must be string")
    if not isinstance(memo.get("company_name", ""), str):
        errors.append("company_name must be string")

    business_hours = memo.get("business_hours")
    if not isinstance(business_hours, dict):
        errors.append("business_hours must be object")
    else:
        for key in ["days", "start", "end", "timezone"]:
            if key not in business_hours:
                errors.append(f"business_hours missing key: {key}")
        if "days" in business_hours and not isinstance(business_hours.get("days"), list):
            errors.append("business_hours.days must be list")

    if not isinstance(memo.get("office_address", ""), str):
        errors.append("office_address must be string")
    if not isinstance(memo.get("services_supported", []), list):
        errors.append("services_supported must be list")
    if not isinstance(memo.get("emergency_definition", []), list):
        errors.append("emergency_definition must be list")

    emergency_routing = memo.get("emergency_routing_rules")
    if not isinstance(emergency_routing, dict):
        errors.append("emergency_routing_rules must be object")
    else:
        for key in ["contacts", "fallback", "notes"]:
            if key not in emergency_routing:
                errors.append(f"emergency_routing_rules missing key: {key}")
        if "contacts" in emergency_routing and not _is_contact_list(emergency_routing.get("contacts")):
            errors.append("emergency_routing_rules.contacts must be list of contacts")

    non_emergency_routing = memo.get("non_emergency_routing_rules")
    if not isinstance(non_emergency_routing, dict):
        errors.append("non_emergency_routing_rules must be object")
    else:
        for key in ["contacts", "notes"]:
            if key not in non_emergency_routing:
                errors.append(f"non_emergency_routing_rules missing key: {key}")
        if "contacts" in non_emergency_routing and not _is_contact_list(non_emergency_routing.get("contacts")):
            errors.append("non_emergency_routing_rules.contacts must be list of contacts")

    transfer_rules = memo.get("call_transfer_rules")
    if not isinstance(transfer_rules, dict):
        errors.append("call_transfer_rules must be object")
    else:
        for key in ["timeout_seconds", "retries", "fail_message", "routing_notes"]:
            if key not in transfer_rules:
                errors.append(f"call_transfer_rules missing key: {key}")

    if not isinstance(memo.get("integration_constraints", []), list):
        errors.append("integration_constraints must be list")
    if not isinstance(memo.get("after_hours_flow_summary", ""), str):
        errors.append("after_hours_flow_summary must be string")
    if not isinstance(memo.get("office_hours_flow_summary", ""), str):
        errors.append("office_hours_flow_summary must be string")
    if not isinstance(memo.get("questions_or_unknowns", []), list):
        errors.append("questions_or_unknowns must be list")
    if not isinstance(memo.get("notes", ""), str):
        errors.append("notes must be string")

    return len(errors) == 0, errors


def validate_agent_spec_schema(spec: Dict[str, Any]) -> Tuple[bool, List[str]]:
    errors: List[str] = []
    expected = empty_agent_spec(version=str(spec.get("version", "v1")))
    for key in expected.keys():
        if key not in spec:
            errors.append(f"Missing agent spec field: {key}")

    if not isinstance(spec.get("agent_name", ""), str):
        errors.append("agent_name must be string")
    if not isinstance(spec.get("voice_style", ""), str):
        errors.append("voice_style must be string")
    if not isinstance(spec.get("system_prompt", ""), str):
        errors.append("system_prompt must be string")

    key_vars = spec.get("key_variables")
    if not isinstance(key_vars, dict):
        errors.append("key_variables must be object")
    else:
        for key in ["timezone", "business_hours", "address", "emergency_routing"]:
            if key not in key_vars:
                errors.append(f"key_variables missing key: {key}")

    placeholders = spec.get("tool_invocation_placeholders")
    if not isinstance(placeholders, dict):
        errors.append("tool_invocation_placeholders must be object")

    transfer = spec.get("call_transfer_protocol")
    if not isinstance(transfer, dict):
        errors.append("call_transfer_protocol must be object")
    else:
        for key in ["steps", "timeout_seconds", "retries"]:
            if key not in transfer:
                errors.append(f"call_transfer_protocol missing key: {key}")

    fallback = spec.get("fallback_protocol_if_transfer_fails")
    if not isinstance(fallback, dict):
        errors.append("fallback_protocol_if_transfer_fails must be object")
    else:
        for key in ["exact_caller_message", "steps"]:
            if key not in fallback:
                errors.append(f"fallback_protocol_if_transfer_fails missing key: {key}")

    version = spec.get("version")
    if version not in ["v1", "v2"]:
        errors.append("version must be v1 or v2")

    return len(errors) == 0, errors


def normalize_memo(account_id: str, memo: Dict[str, Any]) -> Dict[str, Any]:
    base = empty_memo(account_id)
    merged = copy.deepcopy(base)
    for key, value in memo.items():
        if key in ["business_hours", "emergency_routing_rules", "non_emergency_routing_rules", "call_transfer_rules"] and isinstance(value, dict):
            merged[key].update(value)
        elif key in merged:
            merged[key] = value
    merged["account_id"] = account_id
    return merged
