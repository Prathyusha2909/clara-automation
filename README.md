# Clara AI Assignment - Automation Pipeline

This repository implements a zero-cost, reproducible Clara onboarding automation pipeline.

## Architecture and Data Flow

### Pipeline A (Demo -> v1)
1. Read demo transcript from `inputs/demo/<account_id>.txt`
2. Extract structured account memo fields (rule-based parsing, no paid LLMs)
3. Generate:
   - `outputs/accounts/<account_id>/v1/memo.json`
   - `outputs/accounts/<account_id>/v1/agent_spec.json`
4. Create/reuse task tracker issue (GitHub Issues) or mock locally
5. Persist account task metadata:
   - `outputs/accounts/<account_id>/task.json`

### Pipeline B (Onboarding -> v2 patch)
1. Read onboarding transcript from `inputs/onboarding/<account_id>.txt`
2. Load v1 memo
3. Extract explicit updates (business hours, transfer timeout, routing, constraints)
4. Apply patch safely without overwriting unrelated fields
5. Handle conflicts by keeping v1 value unless explicit override is clear
6. Generate:
   - `outputs/accounts/<account_id>/v2/memo.json`
   - `outputs/accounts/<account_id>/v2/agent_spec.json`
   - `outputs/accounts/<account_id>/changes.json`

### Orchestration
- n8n workflow export: `workflows/n8n_clara_pipeline.json`
- n8n command node runs: `python3 scripts/run_all.py`

## Repository Structure

- `inputs/demo/`, `inputs/onboarding/`: input transcripts
- `scripts/`
  - `run_all.py` (primary CLI entrypoint)
  - `run_demo.py` (Pipeline A only)
  - `run_onboarding.py` (Pipeline B only)
  - `clara_pipeline.py` (shared schema + parsing + patch logic + tracker logic)
- `workflows/n8n_clara_pipeline.json`: n8n export (Manual Trigger -> Execute Command)
- `outputs/accounts/<account_id>/...`: generated account artifacts
- `outputs/task_tracker/items.json`: global task tracker snapshot
- `docker-compose.yml`, `Dockerfile.n8n`: local n8n runtime
- `validate_assignment.py`: compliance validator

## How to Run Locally (Python)

1. Install dependencies:

```bash
pip install -r requirements.txt
```

2. Run all pipelines in batch mode:

```bash
python scripts/run_all.py
```

3. Optional split runs:

```bash
python scripts/run_demo.py
python scripts/run_onboarding.py
```

4. Validate outputs:

```bash
python validate_assignment.py
```

## How to Run with n8n (Docker)

1. Copy env template:

```bash
cp .env.example .env
```

2. Start n8n:

```bash
docker compose up -d --build
```

3. Open n8n: `http://localhost:5678`
4. Import workflow JSON: `workflows/n8n_clara_pipeline.json`
5. Execute from Manual Trigger

The workflow runs `python3 scripts/run_all.py` against this mounted repository (`/workspace`).

## Environment Variables

- `GITHUB_REPO` (example: `Prathyusha2909/clara-automation`)
- `GITHUB_TOKEN` (optional)
  - If provided: creates/reuses one GitHub Issue per account
  - If missing: task tracker is mocked locally (`mocked: true` in task.json)
- `N8N_BASIC_AUTH_ACTIVE`, `N8N_BASIC_AUTH_USER`, `N8N_BASIC_AUTH_PASSWORD` (for n8n)

## Dataset Plug-in

Add transcript files as:
- `inputs/demo/<account_id>.txt`
- `inputs/onboarding/<account_id>.txt`

The pipeline derives `account_id` from the filename stem.

## Outputs

Per account:
- `outputs/accounts/<account_id>/v1/memo.json`
- `outputs/accounts/<account_id>/v1/agent_spec.json`
- `outputs/accounts/<account_id>/v2/memo.json`
- `outputs/accounts/<account_id>/v2/agent_spec.json`
- `outputs/accounts/<account_id>/changes.json`
- `outputs/accounts/<account_id>/task.json`

Global:
- `outputs/task_tracker/items.json`

## Retell Setup and Manual Import

This project generates "Retell Agent Draft Spec JSON" (`agent_spec.json`).

If Retell API automation is unavailable on free tier:
1. Open Retell UI and create/update an agent manually.
2. Copy from account `agent_spec.json`:
   - `agent_name`
   - `voice_style`
   - `system_prompt`
   - transfer/fallback protocol sections
3. Apply business-hours and emergency-routing variables from `key_variables`.
4. Keep tool placeholders internal; do not surface tool-call language to callers.

## Known Limitations

- Extraction is deterministic regex/rule-based and optimized for assignment-style transcripts.
- Issue reuse checks first 100 issues in repository for matching tracker title.
- No paid external services are used.

## Future Improvements

- Add richer NLP parsing for broader transcript patterns.
- Add stronger conflict-resolution policy engine and human-approval queue.
- Add dashboard/UI for diff viewing across v1/v2.
