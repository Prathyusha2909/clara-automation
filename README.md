# Clara AI Automation

## What This Project Does

This repository implements a zero-cost, reproducible pipeline for the Clara assignment:

1. Demo transcript -> `v1` account memo + agent spec
2. Onboarding transcript -> `v2` updated memo + agent spec
3. Per-account changelog (`v1 -> v2`)
4. Local task-tracker items for workflow visibility
5. n8n workflow export and local n8n setup

## Repository Layout

- `inputs/demo/` and `inputs/onboarding/`: source transcripts
- `outputs/accounts/<account_id>/v1/`: demo-derived artifacts
- `outputs/accounts/<account_id>/v2/`: onboarding-updated artifacts
- `outputs/accounts/<account_id>/changes.json`: field-level update log
- `outputs/task_tracker/items.json`: local free task-tracker records
- `scripts/`: wrapper entrypoints used by n8n execute-command nodes
- `workflows/n8n_clara_pipeline.json`: importable n8n workflow JSON
- `run_demo.py`, `run_onboarding.py`, `pipeline_utils.py`: core pipeline code
- `validate_assignment.py`: compliance and integrity checks

## Prerequisites

- Python 3.10+
- Docker Desktop (optional but recommended for n8n)
- Node.js (optional fallback to run n8n via `npx`)

## Run the Pipeline (CLI)

```bash
python run_demo.py
python run_onboarding.py
python validate_assignment.py
```

## Run with n8n

### Option A: Docker (recommended)

1. Create env file:

```bash
cp .env.example .env
```

2. Start n8n:

```bash
docker compose up -d --build
```

3. Open n8n: `http://localhost:5678`

4. Import workflow file:
`workflows/n8n_clara_pipeline.json`

5. Execute workflow from the Manual Trigger node.

### Option B: Local fallback (if Docker daemon is unavailable)

```bash
npx --yes n8n start --host 127.0.0.1 --port 5678
```

Then import and run the same workflow JSON from the UI.

## n8n Workflow Steps

The workflow runs these commands in sequence:

1. `python scripts/run_demo.py`
2. `python scripts/run_onboarding.py`
3. `python validate_assignment.py`

When using Docker, the repository is mounted at `/workspace`.

## Output Contract

For each account:

- `v1/memo.json`
- `v1/agent_spec.json`
- `v2/memo.json`
- `v2/agent_spec.json`
- `changes.json`

Global tracking:

- `outputs/task_tracker/items.json`

## Assignment Coverage

Implemented:

- Batch processing over 5 demo + 5 onboarding files
- Deterministic extraction/update flow (no paid APIs)
- Structured memo fields and prompt hygiene
- Versioning (`v1` and `v2`) with explicit change logging
- n8n workflow export and local setup instructions
- Validation script for reproducibility checks

Not part of codebase:

- Loom walkthrough video (submission artifact)
- Paid or external PM integrations
  - Current zero-cost fallback: local task tracker JSON

## Retell Free-Tier Path

If Retell API automation is unavailable on free tier:

1. Create/update agent manually in Retell UI.
2. Copy values from `outputs/accounts/<account_id>/v1/agent_spec.json` or `v2`.
3. Paste `system_prompt`.
4. Configure transfer and fallback behavior from:
   - `call_transfer_protocol`
   - `fallback_protocol`
