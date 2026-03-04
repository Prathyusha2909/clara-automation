import json
import os

from pipeline_utils import (
    apply_onboarding_updates,
    build_change_log,
    generate_agent_spec,
    normalize_memo_schema,
    update_task_tracker,
    write_json,
)


def run_onboarding_pipeline():
    onboarding_folder = "inputs/onboarding"
    files = sorted([name for name in os.listdir(onboarding_folder) if name.endswith(".txt")])

    for file_name in files:
        account_id = file_name.replace(".txt", "")
        v1_memo_path = os.path.join("outputs", "accounts", account_id, "v1", "memo.json")

        if not os.path.exists(v1_memo_path):
            print(f"Skipping {account_id}: missing v1 memo at {v1_memo_path}")
            continue

        with open(v1_memo_path, "r", encoding="utf-8") as handle:
            existing_memo = normalize_memo_schema(json.load(handle))

        onboarding_path = os.path.join(onboarding_folder, file_name)
        with open(onboarding_path, "r", encoding="utf-8") as handle:
            onboarding_text = handle.read()

        updated_memo, changes = apply_onboarding_updates(existing_memo, onboarding_text)
        agent_spec = generate_agent_spec(updated_memo, "v2")

        v2_path = os.path.join("outputs", "accounts", account_id, "v2")
        os.makedirs(v2_path, exist_ok=True)

        write_json(os.path.join(v2_path, "memo.json"), updated_memo)
        write_json(os.path.join(v2_path, "agent_spec.json"), agent_spec)
        write_json(
            os.path.join("outputs", "accounts", account_id, "changes.json"),
            build_change_log("v2", changes),
        )
        update_task_tracker(
            account_id=account_id,
            stage="onboarding_v2_update",
            status="completed",
            artifacts=[
                os.path.join(v2_path, "memo.json"),
                os.path.join(v2_path, "agent_spec.json"),
                os.path.join("outputs", "accounts", account_id, "changes.json"),
            ],
            summary="Applied onboarding updates and produced v2 memo/spec with changelog.",
        )

        print(f"{account_id} v2 created successfully! ({len(changes)} field updates)")


if __name__ == "__main__":
    run_onboarding_pipeline()
