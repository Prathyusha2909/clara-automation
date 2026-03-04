import os

from pipeline_utils import (
    build_memo_from_demo,
    generate_agent_spec,
    update_task_tracker,
    write_json,
)


def run_demo_pipeline():
    demo_folder = "inputs/demo"
    files = sorted([name for name in os.listdir(demo_folder) if name.endswith(".txt")])

    for file_name in files:
        account_id = file_name.replace(".txt", "")
        transcript_path = os.path.join(demo_folder, file_name)

        with open(transcript_path, "r", encoding="utf-8") as handle:
            transcript = handle.read()

        memo = build_memo_from_demo(account_id, transcript)
        agent_spec = generate_agent_spec(memo, "v1")

        v1_path = os.path.join("outputs", "accounts", account_id, "v1")
        os.makedirs(v1_path, exist_ok=True)

        write_json(os.path.join(v1_path, "memo.json"), memo)
        write_json(os.path.join(v1_path, "agent_spec.json"), agent_spec)
        update_task_tracker(
            account_id=account_id,
            stage="demo_v1_generation",
            status="completed",
            artifacts=[
                os.path.join(v1_path, "memo.json"),
                os.path.join(v1_path, "agent_spec.json"),
            ],
            summary="Generated preliminary memo and agent spec from demo transcript.",
        )
        print(f"{account_id} v1 created successfully!")


if __name__ == "__main__":
    run_demo_pipeline()
