import json
import os
import sys


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
    "fallback_protocol",
    "version",
]

REQUIRED_HOURS_KEYS = ["days", "start", "end", "timezone"]
REQUIRED_REPO_FILES = [
    os.path.join("workflows", "n8n_clara_pipeline.json"),
    "docker-compose.yml",
    "Dockerfile.n8n",
]


def _load_json(path):
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def _validate_memo(path, label, errors):
    memo = _load_json(path)
    missing = [field for field in REQUIRED_MEMO_FIELDS if field not in memo]
    if missing:
        errors.append(f"{label}: memo missing fields {missing}")
        return memo

    hours = memo.get("business_hours")
    if not isinstance(hours, dict):
        errors.append(f"{label}: business_hours must be an object")
    else:
        missing_hours = [key for key in REQUIRED_HOURS_KEYS if key not in hours]
        if missing_hours:
            errors.append(f"{label}: business_hours missing keys {missing_hours}")

    if not isinstance(memo.get("questions_or_unknowns"), list):
        errors.append(f"{label}: questions_or_unknowns must be a list")

    return memo


def _validate_agent(path, label, expected_version, errors):
    spec = _load_json(path)
    missing = [field for field in REQUIRED_AGENT_FIELDS if field not in spec]
    if missing:
        errors.append(f"{label}: agent_spec missing fields {missing}")
    if spec.get("version") != expected_version:
        errors.append(
            f"{label}: version expected {expected_version}, found {spec.get('version')}"
        )
    if "Never mention internal tools" not in spec.get("system_prompt", ""):
        errors.append(f"{label}: system_prompt missing no-tool-disclosure guardrail")
    return spec


def _validate_changes(path, label, errors):
    data = _load_json(path)
    changes = data.get("changes")
    if not isinstance(changes, list):
        errors.append(f"{label}: changes.json must contain a changes list")
        return []
    for index, change in enumerate(changes, 1):
        for key in ["field", "old_value", "new_value", "source", "reason"]:
            if key not in change:
                errors.append(f"{label}: change #{index} missing key '{key}'")
    return changes


def _validate_task_tracker(accounts, errors):
    tracker_path = os.path.join("outputs", "task_tracker", "items.json")
    if not os.path.exists(tracker_path):
        errors.append("task tracker missing: outputs/task_tracker/items.json")
        return

    payload = _load_json(tracker_path)
    items = payload.get("items")
    if not isinstance(items, list):
        errors.append("task tracker payload must contain a list under 'items'")
        return

    by_task_id = {item.get("task_id"): item for item in items if isinstance(item, dict)}
    required_stages = ["demo_v1_generation", "onboarding_v2_update"]

    for account in accounts:
        for stage in required_stages:
            task_id = f"{account}:{stage}"
            item = by_task_id.get(task_id)
            if not item:
                errors.append(f"task tracker missing item: {task_id}")
                continue
            if item.get("status") != "completed":
                errors.append(f"task tracker item {task_id} must be completed")

    for account in accounts:
        account_task_path = os.path.join("outputs", "accounts", account, "task.json")
        if not os.path.exists(account_task_path):
            errors.append(f"per-account task file missing: {account_task_path}")
            continue
        payload = _load_json(account_task_path)
        entries = payload.get("items")
        if not isinstance(entries, list):
            errors.append(f"{account_task_path} must include an items list")
            continue
        required_task_ids = {
            f"{account}:demo_v1_generation",
            f"{account}:onboarding_v2_update",
        }
        found = {entry.get("task_id") for entry in entries if isinstance(entry, dict)}
        missing = sorted(required_task_ids - found)
        if missing:
            errors.append(f"{account_task_path} missing task ids: {missing}")


def main():
    errors = []
    for path in REQUIRED_REPO_FILES:
        if not os.path.exists(path):
            errors.append(f"required repository file missing: {path}")

    accounts_root = os.path.join("outputs", "accounts")
    if not os.path.isdir(accounts_root):
        print("FAIL: outputs/accounts directory does not exist")
        sys.exit(1)

    accounts = sorted(
        [
            name
            for name in os.listdir(accounts_root)
            if os.path.isdir(os.path.join(accounts_root, name))
        ]
    )
    if not accounts:
        print("FAIL: no account output directories found")
        sys.exit(1)

    _validate_task_tracker(accounts, errors)

    for account in accounts:
        base = os.path.join(accounts_root, account)
        v1_memo = os.path.join(base, "v1", "memo.json")
        v2_memo = os.path.join(base, "v2", "memo.json")
        v1_spec = os.path.join(base, "v1", "agent_spec.json")
        v2_spec = os.path.join(base, "v2", "agent_spec.json")
        changes_path = os.path.join(base, "changes.json")

        for path in [v1_memo, v2_memo, v1_spec, v2_spec, changes_path]:
            if not os.path.exists(path):
                errors.append(f"{account}: required file missing: {path}")

        if errors:
            continue

        old_memo = _validate_memo(v1_memo, f"{account} v1", errors)
        new_memo = _validate_memo(v2_memo, f"{account} v2", errors)
        _validate_agent(v1_spec, f"{account} v1", "v1", errors)
        _validate_agent(v2_spec, f"{account} v2", "v2", errors)
        changes = _validate_changes(changes_path, f"{account} changes", errors)

        change_fields = {change.get("field") for change in changes}
        if old_memo.get("business_hours") != new_memo.get("business_hours"):
            if "business_hours" not in change_fields:
                errors.append(f"{account}: business_hours changed but not in changes.json")

        if old_memo.get("call_transfer_rules") != new_memo.get("call_transfer_rules"):
            if "call_transfer_rules" not in change_fields:
                errors.append(
                    f"{account}: call_transfer_rules changed but not in changes.json"
                )

        timeout = (
            new_memo.get("call_transfer_rules", {})
            .get("emergency_transfer_timeout_seconds")
        )
        if timeout != 60:
            errors.append(
                f"{account}: expected emergency transfer timeout 60, found {timeout}"
            )

    if errors:
        print("VALIDATION FAILED")
        for issue in errors:
            print("-", issue)
        sys.exit(1)

    print("VALIDATION PASSED")
    print(f"Accounts checked: {len(accounts)}")


if __name__ == "__main__":
    main()
