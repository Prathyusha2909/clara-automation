import json
import os
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

REQUIRED_HOUR_KEYS = ["days", "start", "end", "timezone"]
REQUIRED_TRANSFER_KEYS = ["timeout_seconds", "retries", "what_to_say_if_transfer_fails"]
REQUIRED_CHANGE_KEYS = [
    "field_path",
    "old_value",
    "new_value",
    "reason",
    "evidence_snippet",
]


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _is_memo_valid(memo: Dict[str, Any], label: str, errors: List[str]) -> None:
    for field in REQUIRED_MEMO_FIELDS:
        if field not in memo:
            errors.append(f"{label}: missing memo field '{field}'")

    hours = memo.get("business_hours")
    if not isinstance(hours, dict):
        errors.append(f"{label}: business_hours must be an object")
    else:
        for key in REQUIRED_HOUR_KEYS:
            if key not in hours:
                errors.append(f"{label}: business_hours missing key '{key}'")

    emergency_routing = memo.get("emergency_routing_rules")
    if not isinstance(emergency_routing, dict):
        errors.append(f"{label}: emergency_routing_rules must be an object")
    else:
        for key in ["who_to_call", "order", "fallback"]:
            if key not in emergency_routing:
                errors.append(f"{label}: emergency_routing_rules missing key '{key}'")

    transfer = memo.get("call_transfer_rules")
    if not isinstance(transfer, dict):
        errors.append(f"{label}: call_transfer_rules must be an object")
    else:
        for key in REQUIRED_TRANSFER_KEYS:
            if key not in transfer:
                errors.append(f"{label}: call_transfer_rules missing key '{key}'")

    if not isinstance(memo.get("services_supported"), list):
        errors.append(f"{label}: services_supported must be a list")
    if not isinstance(memo.get("emergency_definition"), list):
        errors.append(f"{label}: emergency_definition must be a list")
    if not isinstance(memo.get("questions_or_unknowns"), list):
        errors.append(f"{label}: questions_or_unknowns must be a list")


def _is_agent_valid(spec: Dict[str, Any], label: str, version: str, errors: List[str]) -> None:
    for field in REQUIRED_AGENT_FIELDS:
        if field not in spec:
            errors.append(f"{label}: missing agent_spec field '{field}'")
    if spec.get("version") != version:
        errors.append(f"{label}: expected version {version}, found {spec.get('version')}")
    if "Do not mention internal tools" not in spec.get("system_prompt", ""):
        errors.append(f"{label}: system_prompt missing tool-disclosure guardrail")


def _is_changes_valid(changes: Any, label: str, errors: List[str]) -> None:
    if not isinstance(changes, list):
        errors.append(f"{label}: changes.json must be a list")
        return
    for idx, row in enumerate(changes, start=1):
        if not isinstance(row, dict):
            errors.append(f"{label}: change entry #{idx} must be an object")
            continue
        for key in REQUIRED_CHANGE_KEYS:
            if key not in row:
                errors.append(f"{label}: change entry #{idx} missing key '{key}'")


def _is_task_valid(task: Dict[str, Any], account_id: str, errors: List[str]) -> None:
    if task.get("account_id") != account_id:
        errors.append(f"{account_id}: task.json account_id mismatch")
    tracker = task.get("tracker")
    if not isinstance(tracker, dict):
        errors.append(f"{account_id}: task.json missing tracker object")
    stages = task.get("stages")
    if not isinstance(stages, dict):
        errors.append(f"{account_id}: task.json missing stages object")
        return
    for stage in ["pipeline_a_v1", "pipeline_b_v2"]:
        row = stages.get(stage)
        if not isinstance(row, dict):
            errors.append(f"{account_id}: task.json missing stage '{stage}'")
            continue
        if row.get("status") != "completed":
            errors.append(f"{account_id}: stage '{stage}' must be completed")


def main() -> None:
    errors: List[str] = []
    root = Path(".")
    required_repo_files = [
        root / "workflows" / "n8n_clara_pipeline.json",
        root / "scripts" / "run_all.py",
        root / "docker-compose.yml",
        root / "requirements.txt",
    ]
    for path in required_repo_files:
        if not path.exists():
            errors.append(f"Missing repository requirement: {path}")

    accounts_root = root / "outputs" / "accounts"
    if not accounts_root.exists():
        print("FAIL: outputs/accounts does not exist")
        sys.exit(1)

    accounts = sorted([path for path in accounts_root.iterdir() if path.is_dir()])
    if not accounts:
        print("FAIL: no account folders found")
        sys.exit(1)

    for account_dir in accounts:
        account_id = account_dir.name
        v1_memo_path = account_dir / "v1" / "memo.json"
        v2_memo_path = account_dir / "v2" / "memo.json"
        v1_spec_path = account_dir / "v1" / "agent_spec.json"
        v2_spec_path = account_dir / "v2" / "agent_spec.json"
        changes_path = account_dir / "changes.json"
        task_path = account_dir / "task.json"

        for path in [v1_memo_path, v2_memo_path, v1_spec_path, v2_spec_path, changes_path, task_path]:
            if not path.exists():
                errors.append(f"{account_id}: missing required file {path}")

        if errors:
            continue

        v1_memo = _read_json(v1_memo_path)
        v2_memo = _read_json(v2_memo_path)
        v1_spec = _read_json(v1_spec_path)
        v2_spec = _read_json(v2_spec_path)
        changes = _read_json(changes_path)
        task = _read_json(task_path)

        _is_memo_valid(v1_memo, f"{account_id} v1", errors)
        _is_memo_valid(v2_memo, f"{account_id} v2", errors)
        _is_agent_valid(v1_spec, f"{account_id} v1", "v1", errors)
        _is_agent_valid(v2_spec, f"{account_id} v2", "v2", errors)
        _is_changes_valid(changes, f"{account_id}", errors)
        _is_task_valid(task, account_id, errors)

    global_tracker = root / "outputs" / "task_tracker" / "items.json"
    if not global_tracker.exists():
        errors.append("Missing global task tracker outputs/task_tracker/items.json")
    else:
        payload = _read_json(global_tracker)
        if not isinstance(payload.get("items"), list):
            errors.append("Global task tracker must include items list")

    if errors:
        print("VALIDATION FAILED")
        for error in errors:
            print(f"- {error}")
        sys.exit(1)

    print("VALIDATION PASSED")
    print(f"Accounts checked: {len(accounts)}")


if __name__ == "__main__":
    main()
