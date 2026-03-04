import json
import sys
from pathlib import Path
from typing import Any, Dict, List


REQUIRED_MEMO_FIELDS = [
    "account_id",
    "company_name",
    "business_hours",
    "office_address",
    "services_supported",
    "emergency_definition",
    "emergency_routing_rules",
    "non_emergency_routing_rules",
    "call_transfer_rules",
    "integration_constraints",
    "after_hours_flow_summary",
    "office_hours_flow_summary",
    "questions_or_unknowns",
    "notes",
]

REQUIRED_AGENT_FIELDS = [
    "agent_name",
    "voice_style",
    "system_prompt",
    "key_variables",
    "tool_invocation_placeholders",
    "call_transfer_protocol",
    "fallback_protocol_if_transfer_fails",
    "version",
]

REQUIRED_CHANGES_FIELDS = ["field", "old_value", "new_value", "source", "rationale"]


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _check_fields_exact(payload: Dict[str, Any], required: List[str], label: str, errors: List[str]) -> None:
    keys = list(payload.keys())
    for key in required:
        if key not in payload:
            errors.append(f"{label}: missing key '{key}'")
    for key in keys:
        if key not in required:
            errors.append(f"{label}: unexpected key '{key}'")


def _check_contact_list(value: Any, label: str, errors: List[str]) -> None:
    if not isinstance(value, list):
        errors.append(f"{label}: expected list")
        return
    for idx, row in enumerate(value, start=1):
        if not isinstance(row, dict):
            errors.append(f"{label}: item #{idx} must be object")
            continue
        for key in ["name", "role", "phone", "priority_order"]:
            if key not in row:
                errors.append(f"{label}: item #{idx} missing key '{key}'")


def _validate_memo(memo: Dict[str, Any], label: str, errors: List[str]) -> None:
    _check_fields_exact(memo, REQUIRED_MEMO_FIELDS, label, errors)

    if not isinstance(memo.get("account_id"), str):
        errors.append(f"{label}: account_id must be string")
    if not isinstance(memo.get("company_name"), str):
        errors.append(f"{label}: company_name must be string")

    business_hours = memo.get("business_hours")
    if not isinstance(business_hours, dict):
        errors.append(f"{label}: business_hours must be object")
    else:
        for key in ["days", "start", "end", "timezone"]:
            if key not in business_hours:
                errors.append(f"{label}: business_hours missing '{key}'")
        if not isinstance(business_hours.get("days"), list):
            errors.append(f"{label}: business_hours.days must be list")

    if not isinstance(memo.get("office_address"), str):
        errors.append(f"{label}: office_address must be string")
    if not isinstance(memo.get("services_supported"), list):
        errors.append(f"{label}: services_supported must be list")
    if not isinstance(memo.get("emergency_definition"), list):
        errors.append(f"{label}: emergency_definition must be list")
    if not isinstance(memo.get("integration_constraints"), list):
        errors.append(f"{label}: integration_constraints must be list")
    if not isinstance(memo.get("questions_or_unknowns"), list):
        errors.append(f"{label}: questions_or_unknowns must be list")
    if not isinstance(memo.get("notes"), str):
        errors.append(f"{label}: notes must be string")

    emergency = memo.get("emergency_routing_rules")
    if not isinstance(emergency, dict):
        errors.append(f"{label}: emergency_routing_rules must be object")
    else:
        for key in ["contacts", "fallback", "notes"]:
            if key not in emergency:
                errors.append(f"{label}: emergency_routing_rules missing '{key}'")
        _check_contact_list(emergency.get("contacts"), f"{label}.emergency_routing_rules.contacts", errors)

    non_emergency = memo.get("non_emergency_routing_rules")
    if not isinstance(non_emergency, dict):
        errors.append(f"{label}: non_emergency_routing_rules must be object")
    else:
        for key in ["contacts", "notes"]:
            if key not in non_emergency:
                errors.append(f"{label}: non_emergency_routing_rules missing '{key}'")
        _check_contact_list(non_emergency.get("contacts"), f"{label}.non_emergency_routing_rules.contacts", errors)

    transfer = memo.get("call_transfer_rules")
    if not isinstance(transfer, dict):
        errors.append(f"{label}: call_transfer_rules must be object")
    else:
        for key in ["timeout_seconds", "retries", "fail_message", "routing_notes"]:
            if key not in transfer:
                errors.append(f"{label}: call_transfer_rules missing '{key}'")


def _validate_agent(spec: Dict[str, Any], label: str, expected_version: str, errors: List[str]) -> None:
    _check_fields_exact(spec, REQUIRED_AGENT_FIELDS, label, errors)
    if spec.get("version") != expected_version:
        errors.append(f"{label}: expected version '{expected_version}', got '{spec.get('version')}'")
    if not isinstance(spec.get("system_prompt"), str):
        errors.append(f"{label}: system_prompt must be string")
    else:
        prompt = spec["system_prompt"].lower()
        if "business-hours flow" not in prompt:
            errors.append(f"{label}: system_prompt missing business-hours flow section")
        if "after-hours flow" not in prompt:
            errors.append(f"{label}: system_prompt missing after-hours flow section")


def _validate_changes(changes: Any, label: str, errors: List[str]) -> None:
    if not isinstance(changes, list):
        errors.append(f"{label}: changes must be list")
        return
    for idx, row in enumerate(changes, start=1):
        if not isinstance(row, dict):
            errors.append(f"{label}: change #{idx} must be object")
            continue
        for key in REQUIRED_CHANGES_FIELDS:
            if key not in row:
                errors.append(f"{label}: change #{idx} missing key '{key}'")


def _validate_task(task: Dict[str, Any], account_id: str, errors: List[str]) -> None:
    if task.get("account_id") != account_id:
        errors.append(f"{account_id}: task account_id mismatch")
    if not isinstance(task.get("tracker"), dict):
        errors.append(f"{account_id}: task.tracker must be object")
    stages = task.get("stages")
    if not isinstance(stages, dict):
        errors.append(f"{account_id}: task.stages must be object")
        return
    for stage in ["pipeline_a_v1", "pipeline_b_v2"]:
        if stage not in stages:
            errors.append(f"{account_id}: task missing stage '{stage}'")


def main() -> None:
    errors: List[str] = []
    root = Path(".")

    required_repo_files = [
        root / "README.md",
        root / "requirements.txt",
        root / "scripts" / "run_all.py",
        root / "workflows" / "n8n_clara_pipeline.json",
    ]
    for path in required_repo_files:
        if not path.exists():
            errors.append(f"Missing required repository file: {path}")

    accounts_root = root / "outputs" / "accounts"
    if not accounts_root.exists():
        print("VALIDATION FAILED")
        print("- outputs/accounts missing")
        sys.exit(1)

    account_dirs = sorted([path for path in accounts_root.iterdir() if path.is_dir()])
    if not account_dirs:
        print("VALIDATION FAILED")
        print("- no account directories found")
        sys.exit(1)

    for account_dir in account_dirs:
        account_id = account_dir.name
        v1_memo_path = account_dir / "v1" / "memo.json"
        v1_spec_path = account_dir / "v1" / "agent_spec.json"
        v2_memo_path = account_dir / "v2" / "memo.json"
        v2_spec_path = account_dir / "v2" / "agent_spec.json"
        changes_path = account_dir / "v2" / "changes.json"
        task_path = account_dir / "task.json"

        required_files = [
            v1_memo_path,
            v1_spec_path,
            v2_memo_path,
            v2_spec_path,
            changes_path,
            task_path,
        ]
        missing = [path for path in required_files if not path.exists()]
        if missing:
            for path in missing:
                errors.append(f"{account_id}: missing file {path}")
            continue

        v1_memo = _read_json(v1_memo_path)
        v1_spec = _read_json(v1_spec_path)
        v2_memo = _read_json(v2_memo_path)
        v2_spec = _read_json(v2_spec_path)
        changes = _read_json(changes_path)
        task = _read_json(task_path)

        _validate_memo(v1_memo, f"{account_id}.v1.memo", errors)
        _validate_memo(v2_memo, f"{account_id}.v2.memo", errors)
        _validate_agent(v1_spec, f"{account_id}.v1.agent_spec", "v1", errors)
        _validate_agent(v2_spec, f"{account_id}.v2.agent_spec", "v2", errors)
        _validate_changes(changes, f"{account_id}.v2.changes", errors)
        _validate_task(task, account_id, errors)

    global_tasks = root / "outputs" / "tasks.json"
    if not global_tasks.exists():
        errors.append("Missing outputs/tasks.json")
    else:
        payload = _read_json(global_tasks)
        if not isinstance(payload, dict) or not isinstance(payload.get("items"), list):
            errors.append("outputs/tasks.json must be {\"items\": [...]}")

    if errors:
        print("VALIDATION FAILED")
        for error in errors:
            print(f"- {error}")
        sys.exit(1)

    print("VALIDATION PASSED")
    print(f"Accounts checked: {len(account_dirs)}")


if __name__ == "__main__":
    main()
