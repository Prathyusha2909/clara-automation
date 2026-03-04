# Scripts

Primary CLI entrypoints:

- `run_all.py`: runs Pipeline A then Pipeline B in batch mode.
- `run_demo.py`: runs only Pipeline A (demo -> v1).
- `run_onboarding.py`: runs only Pipeline B (onboarding -> v2).

Examples:

```bash
python scripts/run_all.py
python scripts/run_demo.py --demo-dir inputs/demo
python scripts/run_onboarding.py --onboarding-dir inputs/onboarding
```
