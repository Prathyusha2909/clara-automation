# Scripts

Primary CLI entrypoints:

- `run_all.py`: runs Pipeline A then Pipeline B in batch mode.
- `run_demo.py`: runs only Pipeline A (demo -> v1).
- `run_onboarding.py`: runs only Pipeline B (onboarding -> v2).

Examples:

```bash
python scripts/run_all.py
python scripts/run_demo.py --input inputs/demo --output outputs/accounts
python scripts/run_onboarding.py --input inputs/onboarding --output outputs/accounts
```
