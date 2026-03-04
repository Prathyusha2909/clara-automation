# Clara Answers Intern Assignment - Automation Repo

This repository implements both required pipelines with deterministic, zero-cost parsing and strict output schemas.

## Architecture

### Pipeline A (Demo -> v1)
1. Load each file from `inputs/demo/*.txt`.
2. Extract explicit facts only (company name, business hours, emergency definition, routing, transfer rules).
3. Write:
   - `outputs/accounts/<account_id>/v1/memo.json`
   - `outputs/accounts/<account_id>/v1/agent_spec.json`
4. Create/reuse a task tracker item (GitHub Issue if configured, otherwise mocked local tracker).
5. Write task metadata:
   - `outputs/accounts/<account_id>/task.json`
   - `outputs/tasks.json`

### Pipeline B (Onboarding -> v2)
1. Load each file from `inputs/onboarding/*.txt` or `inputs/onboarding/*.json`.
2. Load `v1/memo.json` for same account.
3. Parse onboarding updates and patch only explicit fields.
4. Keep v1 values when onboarding is conflicting/ambiguous.
5. Write:
   - `outputs/accounts/<account_id>/v2/memo.json`
   - `outputs/accounts/<account_id>/v2/agent_spec.json`
   - `outputs/accounts/<account_id>/v2/changes.json`
6. Update local/global tracker files.

### Deterministic + No Hallucination Rules
- No paid APIs or LLM calls.
- Only explicit transcript/form values are written.
- Missing values remain blank/empty and are added to `questions_or_unknowns`.
- Idempotent behavior:
  - Existing v1/v2 outputs are skipped unless `--force`.
  - `changes.json` is rewritten deterministically and not duplicated.

## Required Output Schemas

### Account Memo JSON (v1 and v2)
Exact fields:
- `account_id`
- `company_name`
- `business_hours` (`days`, `start`, `end`, `timezone`) where `days` is always an array
- `office_address`
- `services_supported`
- `emergency_definition`
- `emergency_routing_rules` (`contacts`, `fallback`, `notes`)
- `non_emergency_routing_rules` (`contacts`, `notes`)
- `call_transfer_rules` (`timeout_seconds`, `retries`, `fail_message`, `routing_notes`)
- `integration_constraints`
- `after_hours_flow_summary`
- `office_hours_flow_summary`
- `questions_or_unknowns`
- `notes`

### Retell Agent Draft Spec JSON (v1 and v2)
Fields:
- `agent_name`
- `voice_style`
- `system_prompt` (includes both business-hours and after-hours flow)
- `key_variables` (timezone, business hours, address, emergency routing)
- `tool_invocation_placeholders`
- `call_transfer_protocol`
- `fallback_protocol_if_transfer_fails`
- `version` (`v1`/`v2`)

## Folder Structure

- `scripts/clara_pipeline.py` shared parser/patch/task logic
- `scripts/run_demo.py` Pipeline A CLI
- `scripts/run_onboarding.py` Pipeline B CLI
- `scripts/run_all.py` batch CLI (A then B)
- `run_demo.py`, `run_onboarding.py` root wrappers
- `schemas.py` shared schema constructors + validators
- `workflows/n8n_clara_pipeline.json` n8n export
- `workflows/local_file_pipeline.json` mocked orchestrator export
- `outputs/accounts/<account_id>/v1|v2/...` artifacts
- `outputs/tasks.json` free local task tracker

## Local Setup

```bash
pip install -r requirements.txt
```

## Batch Commands

Run exactly with required CLI style:

```bash
python run_demo.py --input inputs/demo --output outputs/accounts
python run_onboarding.py --input inputs/onboarding --output outputs/accounts
```

Optional full run:

```bash
python scripts/run_all.py --demo-input inputs/demo --onboarding-input inputs/onboarding --output outputs/accounts
```

Force rebuild:

```bash
python run_demo.py --input inputs/demo --output outputs/accounts --force
python run_onboarding.py --input inputs/onboarding --output outputs/accounts --force
```

## n8n Orchestration (Docker, zero-cost local)

1. Copy env file:
   ```bash
   cp .env.example .env
   ```
2. Start n8n:
   ```bash
   docker compose up -d --build
   ```
3. Open `http://localhost:5678`.
4. Import `workflows/n8n_clara_pipeline.json`.
5. Run workflow manually.

Workflow topology:
- `Manual Trigger` -> `Execute Command`
- command: `cd /workspace && python3 scripts/run_all.py`

## Environment Variables

- `GITHUB_REPO` (example: `Prathyusha2909/clara-automation`)
- `GITHUB_TOKEN` (optional)
  - set: create/reuse GitHub Issue per account
  - unset: local mocked task record is used (`mocked: true`)
- n8n auth env vars from `.env.example`

## Dataset Placement

- Demo transcripts: `inputs/demo/<account_id>.txt`
- Onboarding updates:
  - `inputs/onboarding/<account_id>.txt`
  - or `inputs/onboarding/<account_id>.json`

## Output Locations

Per account:
- `outputs/accounts/<account_id>/v1/memo.json`
- `outputs/accounts/<account_id>/v1/agent_spec.json`
- `outputs/accounts/<account_id>/v2/memo.json`
- `outputs/accounts/<account_id>/v2/agent_spec.json`
- `outputs/accounts/<account_id>/v2/changes.json`
- `outputs/accounts/<account_id>/task.json`

Global:
- `outputs/tasks.json`

## Retell Setup + Manual Import Steps

This repo outputs Retell-ready draft specs in each `agent_spec.json`.

Manual import flow:
1. Open Retell and create/select an agent.
2. Copy fields from `agent_spec.json`:
   - `agent_name`
   - `voice_style`
   - `system_prompt`
   - `call_transfer_protocol`
   - `fallback_protocol_if_transfer_fails`
3. Configure variables from `key_variables`.
4. Keep `tool_invocation_placeholders` internal only (not spoken to callers).

## Known Limitations

- Parsing is regex/rule-based and tuned for assignment transcript patterns.
- GitHub Issue reuse checks first 100 issues by title.
- No auto-correction of ambiguous source statements; ambiguity is surfaced via `questions_or_unknowns`.

## Future Improvements

1. Add richer parser coverage for varied transcript phrasing.
2. Add stricter conflict evidence capture in changelog.
3. Add CI check to enforce schema and idempotency on every push.
